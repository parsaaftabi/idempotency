package idempotency

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// testStoreConformance exercises the Store contract every implementation
// must honor: claim exclusivity, replay, expiry, abandon, and completion
// overwriting the in-progress marker.
func testStoreConformance(t *testing.T, s Store) {
	t.Helper()
	ctx := context.Background()

	// Absent key.
	if rec, err := s.Load(ctx, "nope"); err != nil || rec != nil {
		t.Fatalf("Load absent: rec=%v err=%v", rec, err)
	}

	// Claim wins once.
	claimed, err := s.TryClaim(ctx, "k1", "fp1", time.Minute)
	if err != nil || !claimed {
		t.Fatalf("first TryClaim: claimed=%v err=%v", claimed, err)
	}
	claimed, err = s.TryClaim(ctx, "k1", "fp1", time.Minute)
	if err != nil || claimed {
		t.Fatalf("second TryClaim: claimed=%v err=%v (want false)", claimed, err)
	}

	// In-progress record is visible.
	rec, err := s.Load(ctx, "k1")
	if err != nil || rec == nil || !rec.InProgress || rec.Fingerprint != "fp1" {
		t.Fatalf("Load in-progress: rec=%+v err=%v", rec, err)
	}

	// Complete overwrites the marker.
	cr := Record{Fingerprint: "fp1", Status: 201, Body: []byte("done")}
	if err := s.Complete(ctx, "k1", cr, time.Minute); err != nil {
		t.Fatal(err)
	}
	rec, err = s.Load(ctx, "k1")
	if err != nil || rec == nil || rec.InProgress || rec.Status != 201 || string(rec.Body) != "done" {
		t.Fatalf("Load completed: rec=%+v err=%v", rec, err)
	}

	// Completed key cannot be re-claimed.
	if claimed, _ := s.TryClaim(ctx, "k1", "fp1", time.Minute); claimed {
		t.Fatal("TryClaim on completed key returned true")
	}

	// Abandon releases the key.
	if err := s.Abandon(ctx, "k1"); err != nil {
		t.Fatal(err)
	}
	if rec, _ := s.Load(ctx, "k1"); rec != nil {
		t.Fatal("Load after Abandon: want nil")
	}
	if claimed, _ := s.TryClaim(ctx, "k1", "fp1", time.Minute); !claimed {
		t.Fatal("TryClaim after Abandon: want true")
	}

	// TTL expiry.
	if _, err := s.TryClaim(ctx, "k2", "fp2", 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(120 * time.Millisecond)
	if rec, _ := s.Load(ctx, "k2"); rec != nil {
		t.Fatal("expired key still loads")
	}
	if claimed, _ := s.TryClaim(ctx, "k2", "fp2", time.Minute); !claimed {
		t.Fatal("expired key cannot be re-claimed")
	}
}

func TestMemoryStore_Conformance(t *testing.T) {
	testStoreConformance(t, NewMemoryStore())
}

func TestRedisStore_Conformance(t *testing.T) {
	addr := os.Getenv("IDEMPOTENCY_TEST_REDIS")
	if addr == "" {
		t.Skip("IDEMPOTENCY_TEST_REDIS not set (e.g. 127.0.0.1:6379)")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()
	if err := client.FlushDB(context.Background()).Err(); err != nil {
		t.Skipf("redis not reachable: %v", err)
	}
	testStoreConformance(t, NewRedisStore(client))
}

func TestPostgresStore_Conformance(t *testing.T) {
	dsn := os.Getenv("IDEMPOTENCY_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("IDEMPOTENCY_TEST_POSTGRES not set")
	}
	db, err := openTestPostgres(dsn)
	if err != nil {
		t.Skipf("postgres not reachable: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(Schema); err != nil {
		t.Fatalf("schema: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM idempotency_keys`); err != nil {
		t.Fatal(err)
	}
	testStoreConformance(t, NewPostgresStore(db))
}
