package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/quay/claircore"
	"github.com/quay/claircore/internal/indexer"
)

func layerScanned(ctx context.Context, pool *pgxpool.Pool, hash claircore.Digest, scnr indexer.VersionedScanner) (bool, error) {
	const (
		// this query will return ErrNoRows  if the scanner is not present in the database
		// (scanner_id: X, layer_hash: null) if the layer was not scanned by the provided scanner
		// (scanner_id: X, layer_hash: XYZ)  if the layer has been scanned by the provided scnr
		selectScanned = `
		SELECT
			id AS scanner_id,
			CASE WHEN (id IS NOT null) THEN
				(SELECT layer_hash FROM scanned_layer WHERE layer_hash = $1 AND scanner_id = scanner_id)
			END AS layer_hash
		FROM scanner
		WHERE name = $2
		  AND version = $3
		  AND kind = $4;
		`
	)

	var scannerID int64
	var layerHash claircore.Digest

	err := pool.QueryRow(ctx, selectScanned, hash, scnr.Name(), scnr.Version(), scnr.Kind()).
		Scan(&scannerID, &layerHash)
	switch {
	case err == nil:
	case errors.Is(err, pgx.ErrNoRows):
		return false, fmt.Errorf("scanner name and version not found in store")
	default:
		return false, err
	}

	if layerHash.String() == "" {
		// Scanner exists, but layer not found.
		return false, nil
	}
	return true, nil
}
