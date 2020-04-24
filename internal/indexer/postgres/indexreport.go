package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/quay/claircore"
)

const (
	selectIndexReport = `SELECT scan_result FROM indexreport WHERE manifest_hash = $1`
)

func indexReport(ctx context.Context, pool *pgxpool.Pool, hash claircore.Digest) (*claircore.IndexReport, bool, error) {
	// we scan into a jsonbIndexReport which has value/scan method set
	// then type convert back to scanner.domain object
	var jsr jsonbIndexReport

	err := pool.QueryRow(ctx, selectIndexReport, hash).Scan(&jsr)
	switch {
	case err == nil:
	case errors.Is(err, pgx.ErrNoRows):
		return nil, false, nil
	default:
		return nil, false, fmt.Errorf("store:indexReport failed to retrieve scanResult: %v", err)
	}

	var sr claircore.IndexReport
	sr = claircore.IndexReport(jsr)
	return &sr, true, nil
}
