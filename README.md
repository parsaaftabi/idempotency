# idempotency

[![CI](https://github.com/parsaaftabi/idempotency/actions/workflows/ci.yml/badge.svg)](https://github.com/parsaaftabi/idempotency/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/parsaaftabi/idempotency.svg)](https://pkg.go.dev/github.com/parsaaftabi/idempotency)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Stripe-style idempotency keys for Go HTTP APIs. Send `Idempotency-Key: <uuid>` with a request; retries with the same key and payload get the stored response instead of re-executing your handler. Zero framework lock-in — it's plain `net/http` middleware.

```go
store := idempotency.NewRedisStore(redisClient) // or NewPostgresStore(db), NewMemoryStore()
mux := http.NewServeMux()
mux.HandleFunc("/payments", createPayment)

http.ListenAndServe(":8080", idempotency.Middleware(store)(mux))
```

## Contract

| Situation | Result |
|---|---|
| First request with a key | Executes, response stored for TTL |
| Retry, same key + same payload | Stored response replayed, handler not run |
| Same key, different payload | `422` — key reuse across operations is a client bug |
| Duplicate while first is in flight | `409 Conflict` |
| No key | Passes through (or `400` with `WithRequireKey(true)`) |
| Store unavailable | `500` — failing open would double-execute; we fail closed |
| Handler panics | Claim released, panic re-raised; the key is reusable |
| Response larger than `MaxResponseBytes` | Streamed live, never cached |

The payload fingerprint is `sha256(method + URI + body)`, compared in constant time.

## Stores

- **MemoryStore** — single instance, tests, dev. Two instances behind a load balancer will double-execute; don't.
- **RedisStore** — the fleet-safe choice. Claim is atomic via `SET NX`.
- **PostgresStore** — when Redis isn't in the stack. Claim is atomic via `INSERT ... ON CONFLICT DO NOTHING`; expired rows are reclaimed so a crashed process can't poison a key. Apply `idempotency.Schema` with your migration tool.

## Options

```go
idempotency.Middleware(store,
    idempotency.WithKeyHeader("X-Idempotency-Key"), // default "Idempotency-Key"
    idempotency.WithMethods("POST", "PATCH"),       // default POST only
    idempotency.WithTTL(48*time.Hour),              // default 24h
    idempotency.WithMaxBodyBytes(4<<20),            // bodies too big to fingerprint pass through
    idempotency.WithMaxResponseBytes(4<<20),        // responses too big to cache stream live
    idempotency.WithRequireKey(true),               // 400 instead of passthrough
)
```

## Notes

- Request bodies are buffered for fingerprinting and rewound, so handlers read them normally.
- Bodies larger than `MaxBodyBytes` skip idempotency entirely — the middleware can't prove a retry carries the same payload.
- Streaming handlers (SSE, `http.Flusher`) work; their output just isn't cached.
- Store implementations must be safe for concurrent use.

## Testing stores

```bash
IDEMPOTENCY_TEST_REDIS=127.0.0.1:6379 go test ./...
IDEMPOTENCY_TEST_POSTGRES="postgres://user:pass@localhost/db?sslmode=disable" go test ./...
```

## License

MIT — see [LICENSE](LICENSE).
