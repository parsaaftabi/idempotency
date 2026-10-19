package idempotency

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func post(t *testing.T, h http.Handler, key, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/pay", strings.NewReader(body))
	if key != "" {
		req.Header.Set(DefaultKeyHeader, key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestMiddleware_ReplaysResponse(t *testing.T) {
	var calls atomic.Int32
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("X-Custom", "yes")
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("charged"))
	})
	h := Middleware(NewMemoryStore())(next)

	first := post(t, h, "key-1", `{"amount":100}`)
	if first.Code != http.StatusCreated || first.Body.String() != "charged" {
		t.Fatalf("first: code=%d body=%q", first.Code, first.Body.String())
	}

	second := post(t, h, "key-1", `{"amount":100}`)
	if second.Code != http.StatusCreated || second.Body.String() != "charged" {
		t.Fatalf("replay: code=%d body=%q", second.Code, second.Body.String())
	}
	if second.Header().Get("X-Custom") != "yes" {
		t.Error("replayed response lost headers")
	}
	if calls.Load() != 1 {
		t.Errorf("handler ran %d times, want 1", calls.Load())
	}
}

func TestMiddleware_SameKeyDifferentPayloadIs422(t *testing.T) {
	h := Middleware(NewMemoryStore())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	post(t, h, "key-2", `{"amount":100}`)
	res := post(t, h, "key-2", `{"amount":999}`)
	if res.Code != http.StatusUnprocessableEntity {
		t.Errorf("code=%d, want 422", res.Code)
	}
}

func TestMiddleware_InflightDuplicateIs409(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	h := Middleware(NewMemoryStore())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		w.Write([]byte("done"))
	}))

	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { firstDone <- post(t, h, "key-3", "x") }()
	<-entered // first request is inside the handler

	second := post(t, h, "key-3", "x")
	if second.Code != http.StatusConflict {
		t.Errorf("in-flight duplicate: code=%d, want 409", second.Code)
	}

	close(release)
	first := <-firstDone
	if first.Code != http.StatusOK {
		t.Errorf("first: code=%d, want 200", first.Code)
	}

	// After completion, the same key replays.
	third := post(t, h, "key-3", "x")
	if third.Code != http.StatusOK || third.Body.String() != "done" {
		t.Errorf("replay after completion: code=%d body=%q", third.Code, third.Body.String())
	}
}

func TestMiddleware_PassthroughWithoutKey(t *testing.T) {
	var calls atomic.Int32
	h := Middleware(NewMemoryStore())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Write([]byte("ok"))
	}))
	for i := 0; i < 2; i++ {
		res := post(t, h, "", "same-body")
		if res.Code != http.StatusOK {
			t.Fatalf("code=%d", res.Code)
		}
	}
	if calls.Load() != 2 {
		t.Errorf("handler ran %d times, want 2 (no key = no dedup)", calls.Load())
	}
}

func TestMiddleware_RequireKey(t *testing.T) {
	h := Middleware(NewMemoryStore(), WithRequireKey(true))(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	res := post(t, h, "", "x")
	if res.Code != http.StatusBadRequest {
		t.Errorf("code=%d, want 400", res.Code)
	}
}

func TestMiddleware_MethodNotCovered(t *testing.T) {
	var calls atomic.Int32
	h := Middleware(NewMemoryStore())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
	}))
	req := httptest.NewRequest(http.MethodGet, "/pay", nil)
	req.Header.Set(DefaultKeyHeader, "key-4")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	h.ServeHTTP(rec, req)
	if calls.Load() != 2 {
		t.Errorf("GET ran %d times, want 2 (not deduplicated)", calls.Load())
	}
}

func TestMiddleware_KeyTooLong(t *testing.T) {
	h := Middleware(NewMemoryStore())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	res := post(t, h, strings.Repeat("k", MaxKeyLength+1), "x")
	if res.Code != http.StatusBadRequest {
		t.Errorf("code=%d, want 400", res.Code)
	}
}

func TestMiddleware_OversizedResponseNotCached(t *testing.T) {
	var calls atomic.Int32
	h := Middleware(NewMemoryStore(), WithMaxResponseBytes(16))(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Write([]byte("this response is definitely longer than sixteen bytes"))
	}))
	post(t, h, "key-5", "x")
	post(t, h, "key-5", "x")
	if calls.Load() != 2 {
		t.Errorf("handler ran %d times, want 2 (oversized responses are not cached)", calls.Load())
	}
}

func TestMiddleware_PanicAbandonsClaim(t *testing.T) {
	h := Middleware(NewMemoryStore())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	}))
	func() {
		defer func() { _ = recover() }()
		post(t, h, "key-6", "x")
	}()

	// The abandoned key must be claimable again (no permanent 409).
	store := NewMemoryStore()
	h3 := Middleware(store)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	}))
	func() {
		defer func() { _ = recover() }()
		post(t, h3, "key-7", "x")
	}()
	h4 := Middleware(store)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("recovered"))
	}))
	res := post(t, h4, "key-7", "x")
	if res.Code != http.StatusOK || res.Body.String() != "recovered" {
		t.Errorf("after panic+abandon: code=%d body=%q, want 200/recovered", res.Code, res.Body.String())
	}
}

func TestMiddleware_RequestBodyStillReadable(t *testing.T) {
	h := Middleware(NewMemoryStore())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Write(body)
	}))
	res := post(t, h, "key-8", "hello-body")
	if res.Body.String() != "hello-body" {
		t.Errorf("handler saw body %q, want it intact", res.Body.String())
	}
}

func TestMiddleware_TTLExpiry(t *testing.T) {
	var calls atomic.Int32
	h := Middleware(NewMemoryStore(), WithTTL(50*time.Millisecond))(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Write([]byte("ok"))
	}))
	post(t, h, "key-9", "x")
	time.Sleep(100 * time.Millisecond)
	post(t, h, "key-9", "x")
	if calls.Load() != 2 {
		t.Errorf("handler ran %d times, want 2 (key should have expired)", calls.Load())
	}
}
