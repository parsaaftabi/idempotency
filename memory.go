package idempotency

import (
	"context"
	"sync"
	"time"
)

type memEntry struct {
	rec       Record
	expiresAt time.Time
}

// MemoryStore is an in-process Store. It is correct for single-instance
// services, tests, and development. For anything behind a load balancer,
// use RedisStore or PostgresStore: two instances with separate memory will
// happily double-execute the same key.
type MemoryStore struct {
	mu sync.Mutex
	m  map[string]memEntry
}

// NewMemoryStore returns an empty in-process store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{m: make(map[string]memEntry)}
}

func (s *MemoryStore) Load(ctx context.Context, key string) (*Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.m[key]
	if !ok || !time.Now().Before(e.expiresAt) {
		delete(s.m, key)
		return nil, nil
	}
	cp := e.rec
	cp.Header = e.rec.Header.Clone()
	cp.Body = append([]byte(nil), e.rec.Body...)
	return &cp, nil
}

func (s *MemoryStore) TryClaim(ctx context.Context, key, fingerprint string, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.m[key]; ok && time.Now().Before(e.expiresAt) {
		return false, nil
	}
	s.m[key] = memEntry{
		rec: Record{
			Fingerprint: fingerprint,
			InProgress:  true,
			CreatedAt:   time.Now().UTC(),
		},
		expiresAt: time.Now().Add(ttl),
	}
	return true, nil
}

func (s *MemoryStore) Complete(ctx context.Context, key string, rec Record, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec.CreatedAt = time.Now().UTC()
	s.m[key] = memEntry{rec: rec, expiresAt: time.Now().Add(ttl)}
	return nil
}

func (s *MemoryStore) Abandon(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, key)
	return nil
}
