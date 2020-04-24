package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/quay/claircore"
	"github.com/quay/claircore/internal/indexer"
)

// ManifestScanned determines if a manifest has been scanned by ALL the provided
// scanners.
func manifestScanned(ctx context.Context, pool *pgxpool.Pool, hash claircore.Digest, scnrs indexer.VersionedScanners) (bool, error) {
	const (
		selectScanner = `
		SELECT id
		FROM scanner
		WHERE name = $1
		  AND version = $2
		  AND kind = $3;
		`
		selectScanned = `SELECT scanner_id FROM scanned_manifest WHERE manifest_hash = $1;`
	)

	// TODO Use passed-in Context.
	// get the ids of the scanners we are testing for.
	var expectedIDs []int64
	for _, scnr := range scnrs {
		var id int64
		err := pool.QueryRow(ctx, selectScanner, scnr.Name(), scnr.Version(), scnr.Kind()).Scan(&id)
		if err != nil {
			return false, fmt.Errorf("store:manifestScanned failed to retrieve expected scanner id for scnr %v: %v", scnr, err)
		}
		expectedIDs = append(expectedIDs, id)
	}

	// get a map of the found ids which have scanned this package
	rows, err := pool.Query(ctx, selectScanned, hash)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	var foundIDs = make(map[int64]struct{}, len(expectedIDs))
	var id int64
	for rows.Next() {
		if err := rows.Scan(&id); err != nil {
			return false, fmt.Errorf("store:manifestScanned failed to select scanner IDs for manifest: %v", err)
		}
		foundIDs[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}

	// if we are unable to find any scanner ids for this manifest hash, we have
	// never scanned this manifest.
	if len(foundIDs) == 0 {
		return false, nil
	}

	// compare the expectedIDs array with our foundIDs. if we get a lookup
	// miss we can say the manifest has not been scanned by all the layers provided
	for _, id := range expectedIDs {
		if _, ok := foundIDs[id]; !ok {
			return false, nil
		}
	}

	return true, nil
}
