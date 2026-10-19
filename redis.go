package idempotency

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStore is a Store backed by Redis. It is the right choice when more
// than one service instance serves traffic: the claim is atomic across the
// fleet via SET NX.
type RedisStore struct {
	client *redis.Client
	prefix string
}

// NewRedisStore builds a store over client. Keys are namespaced as
// "idempotency:<key>"; pass your own *redis.Client so connection pooling,
// timeouts, and clustering stay in your control.
func NewRedisStore(client *redis.Client) *RedisStore {
	return &RedisStore{client: client, prefix: "idempotency:"}
}

func (s *RedisStore) redisKey(key string) string { return s.prefix + key }

func (s *RedisStore) Load(ctx context.Context, key string) (*Record, error) {
	data, err := s.client.Get(ctx, s.redisKey(key)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var rec Record
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (s *RedisStore) TryClaim(ctx context.Context, key, fingerprint string, ttl time.Duration) (bool, error) {
	rec := Record{
		Fingerprint: fingerprint,
		InProgress:  true,
		CreatedAt:   time.Now().UTC(),
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return false, err
	}
	// SET NX is atomic: exactly one claimant wins across the fleet.
	res, err := s.client.SetArgs(ctx, s.redisKey(key), data, redis.SetArgs{
		TTL:  ttl,
		Mode: "NX",
	}).Result()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return res == "OK", nil
}

func (s *RedisStore) Complete(ctx context.Context, key string, rec Record, ttl time.Duration) error {
	rec.CreatedAt = time.Now().UTC()
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return s.client.Set(ctx, s.redisKey(key), data, ttl).Err()
}

func (s *RedisStore) Abandon(ctx context.Context, key string) error {
	return s.client.Del(ctx, s.redisKey(key)).Err()
}
