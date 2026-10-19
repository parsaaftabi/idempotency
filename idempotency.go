// Package idempotency provides HTTP middleware that makes requests safely
// retryable via idempotency keys, Stripe-style.
//
// A client sends "Idempotency-Key: <unique-value>" with a request. The first
// request executes normally and its response is stored. Retries with the same
// key and the same payload receive the stored response without re-executing
// the handler. The same key with a different payload is rejected with 422:
// reusing a key for a different operation is always a client bug.
//
// A duplicate arriving while the first is still in flight gets 409 Conflict.
// Keys expire after the TTL (default 24h).
//
//	mw := idempotency.Middleware(idempotency.NewMemoryStore())
//	http.ListenAndServe(":8080", mw(router))
//
// For production, use NewRedisStore or NewPostgresStore instead of the
// in-memory store.
package idempotency

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"io"
	"net/http"
	"time"
)

const (
	// DefaultKeyHeader is the request header carrying the idempotency key.
	DefaultKeyHeader = "Idempotency-Key"
	// DefaultTTL is how long a stored response is replayed.
	DefaultTTL = 24 * time.Hour
	// DefaultMaxBodyBytes bounds request bodies that get fingerprinted.
	// Larger bodies pass through without idempotency protection.
	DefaultMaxBodyBytes = 1 << 20 // 1 MiB
	// DefaultMaxResponseBytes bounds responses that get cached.
	// Larger responses are served live and never stored.
	DefaultMaxResponseBytes = 1 << 20 // 1 MiB
	// MaxKeyLength rejects absurd keys before they touch the store.
	MaxKeyLength = 255
)

// Record is a stored request outcome.
type Record struct {
	Fingerprint string      `json:"fingerprint"`
	Status      int         `json:"status"`
	Header      http.Header `json:"header"`
	Body        []byte      `json:"body"`
	InProgress  bool        `json:"in_progress"`
	CreatedAt   time.Time   `json:"created_at"`
}

// Store persists idempotency records. Implementations must be safe for
// concurrent use. Keys are opaque strings chosen by the client.
type Store interface {
	// Load returns the record for key, or (nil, nil) when absent or expired.
	Load(ctx context.Context, key string) (*Record, error)
	// TryClaim atomically marks key as in-progress. It returns false when
	// the key is already claimed or completed, in which case the caller
	// must re-read the record instead of executing the handler.
	TryClaim(ctx context.Context, key, fingerprint string, ttl time.Duration) (bool, error)
	// Complete replaces the in-progress marker with the finished response.
	Complete(ctx context.Context, key string, rec Record, ttl time.Duration) error
	// Abandon deletes the key. Used when the handler panics or the
	// response turns out to be uncacheable.
	Abandon(ctx context.Context, key string) error
}

// Config tunes the middleware. Use Options with Middleware; the zero Config
// is not valid.
type Config struct {
	KeyHeader        string
	Methods          []string
	TTL              time.Duration
	MaxBodyBytes     int64
	MaxResponseBytes int
	RequireKey       bool
}

// Option configures the middleware.
type Option func(*Config)

// WithKeyHeader sets the header carrying the idempotency key.
// Default DefaultKeyHeader ("Idempotency-Key").
func WithKeyHeader(h string) Option {
	return func(c *Config) { c.KeyHeader = h }
}

// WithMethods sets which HTTP methods are deduplicated. Others pass
// through untouched. Default POST only: GET/HEAD/PUT/DELETE are either
// safe to retry or carry their own semantics.
func WithMethods(methods ...string) Option {
	return func(c *Config) { c.Methods = methods }
}

// WithTTL sets how long completed records are replayed. Default DefaultTTL.
func WithTTL(ttl time.Duration) Option {
	return func(c *Config) { c.TTL = ttl }
}

// WithMaxBodyBytes sets the largest request body that gets fingerprinted.
// Larger bodies pass through without idempotency protection, because the
// middleware cannot verify the retry carries the same payload.
// Default DefaultMaxBodyBytes.
func WithMaxBodyBytes(n int64) Option {
	return func(c *Config) { c.MaxBodyBytes = n }
}

// WithMaxResponseBytes sets the largest response that gets cached. Larger
// responses stream to the client live and are never stored.
// Default DefaultMaxResponseBytes.
func WithMaxResponseBytes(n int) Option {
	return func(c *Config) { c.MaxResponseBytes = n }
}

// WithRequireKey rejects requests without an idempotency key (400) instead
// of passing them through. Default false.
func WithRequireKey(require bool) Option {
	return func(c *Config) { c.RequireKey = require }
}

func defaultConfig() *Config {
	return &Config{
		KeyHeader:        DefaultKeyHeader,
		Methods:          []string{http.MethodPost},
		TTL:              DefaultTTL,
		MaxBodyBytes:     DefaultMaxBodyBytes,
		MaxResponseBytes: DefaultMaxResponseBytes,
	}
}

// Middleware deduplicates requests by idempotency key. See the package
// documentation for the full contract.
func Middleware(store Store, opts ...Option) func(http.Handler) http.Handler {
	cfg := defaultConfig()
	for _, o := range opts {
		o(cfg)
	}
	methods := make(map[string]bool, len(cfg.Methods))
	for _, m := range cfg.Methods {
		methods[m] = true
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := r.Header.Get(cfg.KeyHeader)
			if key == "" {
				if cfg.RequireKey {
					http.Error(w, "missing idempotency key", http.StatusBadRequest)
					return
				}
				next.ServeHTTP(w, r)
				return
			}
			if len(key) > MaxKeyLength {
				http.Error(w, "idempotency key too long", http.StatusBadRequest)
				return
			}
			if !methods[r.Method] {
				next.ServeHTTP(w, r)
				return
			}

			body, ok := readBody(r, cfg.MaxBodyBytes)
			if !ok {
				// Too large to fingerprint: serve live, cache nothing.
				next.ServeHTTP(w, r)
				return
			}
			fp := fingerprint(r.Method, r.URL.RequestURI(), body)

			if served := serveStored(w, r, store, key, fp); served {
				return
			}

			claimed, err := store.TryClaim(r.Context(), key, fp, cfg.TTL)
			if err != nil {
				// The store is a correctness dependency: failing open
				// would double-execute. Fail closed.
				http.Error(w, "idempotency store unavailable", http.StatusInternalServerError)
				return
			}
			if !claimed {
				// Lost the race; re-read once in case it completed.
				if served := serveStored(w, r, store, key, fp); served {
					return
				}
				http.Error(w, "request with this idempotency key is already in progress", http.StatusConflict)
				return
			}

			// We own the key. A panicking handler must not leave a
			// permanent in-progress marker behind.
			defer func() {
				if p := recover(); p != nil {
					_ = store.Abandon(context.Background(), key)
					panic(p)
				}
			}()

			rec := &recorder{dst: w, header: make(http.Header), limit: cfg.MaxResponseBytes}
			next.ServeHTTP(rec, r)

			if rec.overflow {
				// Response streamed live already; just don't cache it.
				_ = store.Abandon(r.Context(), key)
				return
			}
			if !rec.wroteHeader {
				rec.WriteHeader(http.StatusOK)
			}
			_ = store.Complete(r.Context(), key, Record{
				Fingerprint: fp,
				Status:      rec.status,
				Header:      rec.header.Clone(),
				Body:        rec.body,
				CreatedAt:   time.Now().UTC(),
			}, cfg.TTL)
			// The response was already written live; a Complete failure
			// only costs us the replay. Best effort by design.
		})
	}
}

// serveStored replays a stored response, or rejects conflicts.
// It returns true when it wrote a response.
func serveStored(w http.ResponseWriter, r *http.Request, store Store, key, fp string) bool {
	rec, err := store.Load(r.Context(), key)
	if err != nil {
		http.Error(w, "idempotency store unavailable", http.StatusInternalServerError)
		return true
	}
	if rec == nil {
		return false
	}
	if rec.InProgress {
		http.Error(w, "request with this idempotency key is already in progress", http.StatusConflict)
		return true
	}
	if subtle.ConstantTimeCompare([]byte(rec.Fingerprint), []byte(fp)) != 1 {
		http.Error(w, "idempotency key already used with a different payload", http.StatusUnprocessableEntity)
		return true
	}
	copyHeaders(w.Header(), rec.Header)
	status := rec.Status
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	_, _ = w.Write(rec.Body)
	return true
}

// fingerprint identifies the request payload. Same key + same bytes must
// replay; same key + different bytes is a client bug (422).
func fingerprint(method, uri string, body []byte) string {
	h := sha256.New()
	h.Write([]byte(method))
	h.Write([]byte{0})
	h.Write([]byte(uri))
	h.Write([]byte{0})
	h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}

// readBody buffers the request body for fingerprinting and rewinds it for
// the handler. It returns false when the body is too large to fingerprint;
// in that case r.Body still yields the full stream for the handler.
func readBody(r *http.Request, max int64) ([]byte, bool) {
	if r.Body == nil || r.Body == http.NoBody {
		return nil, true
	}
	if r.ContentLength > max {
		return nil, false // known-large: don't buffer at all
	}
	buf, err := io.ReadAll(io.LimitReader(r.Body, max+1))
	if err != nil {
		return nil, false
	}
	if int64(len(buf)) > max {
		// Prefix was consumed; chain it back so the handler still sees
		// the whole body.
		r.Body = io.NopCloser(io.MultiReader(bytes.NewReader(buf), r.Body))
		return nil, false
	}
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(buf))
	return buf, true
}

// recorder tees the response: it streams everything to the client live
// while capturing a bounded copy for the idempotency record.
type recorder struct {
	dst         http.ResponseWriter
	header      http.Header
	body        []byte
	status      int
	wroteHeader bool
	overflow    bool
	limit       int
}

func (r *recorder) Header() http.Header { return r.header }

func (r *recorder) WriteHeader(status int) {
	if r.wroteHeader {
		return
	}
	r.wroteHeader = true
	r.status = status
	copyHeaders(r.dst.Header(), r.header)
	r.dst.WriteHeader(status)
}

func (r *recorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	if !r.overflow {
		if len(r.body)+len(b) > r.limit {
			r.overflow = true
		} else {
			r.body = append(r.body, b...)
		}
	}
	return r.dst.Write(b)
}

// Flush passes through so streaming handlers (SSE) keep working;
// their output simply overflows the capture and is never cached.
func (r *recorder) Flush() {
	if f, ok := r.dst.(http.Flusher); ok {
		f.Flush()
	}
}

func copyHeaders(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}
