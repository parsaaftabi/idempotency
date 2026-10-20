package idempotency

// Registers the pgx stdlib driver ("pgx") for the Postgres conformance
// tests. Test-only: the library itself stays driver-agnostic and only
// requires database/sql.
import _ "github.com/jackc/pgx/v5/stdlib"
