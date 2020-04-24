package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/quay/claircore/internal/indexer"
)

func registerScanners(ctx context.Context, pool *pgxpool.Pool, scnrs indexer.VersionedScanners) error {
	const (
		insertScanner = `
		INSERT INTO scanner (name, version, kind)
		VALUES ($1, $2, $3)
		ON CONFLICT (name, version, kind) DO NOTHING;
		`
		selectScanner = `
		SELECT id
		FROM scanner
		WHERE name = $1
		  AND version = $2
		  AND kind = $3;
		`
	)
	// check if all scanners scanners exist
	ids := make([]sql.NullInt64, len(scnrs))
	for i, scnr := range scnrs {
		err := pool.QueryRow(ctx, selectScanner, scnr.Name(), scnr.Version(), scnr.Kind()).Scan(&ids[i])
		if err != nil {
			fmt.Errorf("failed to get scanner id for scnr %v: %v", scnr, err)
		}
	}

	// register scanners not found
	for i, id := range ids {
		if !id.Valid {
			s := scnrs[i]
			_, err := pool.Exec(ctx, insertScanner, s.Name(), s.Version(), s.Kind())
			if err != nil {
				return fmt.Errorf("failed to insert scanner %v: %v", s, err)
			}
		}
	}

	return nil
}
