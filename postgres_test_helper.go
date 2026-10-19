package idempotency

import (
	"database/sql"
	"os"
)

// openTestPostgres opens a *sql.DB for integration tests using whichever
// Postgres driver the test binary was built with. The driver name is taken
// from IDEMPOTENCY_TEST_POSTGRES_DRIVER (default "pgx").
func openTestPostgres(dsn string) (*sql.DB, error) {
	driver := "pgx"
	if d := os.Getenv("IDEMPOTENCY_TEST_POSTGRES_DRIVER"); d != "" {
		driver = d
	}
	return sql.Open(driver, dsn)
}
