package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/quay/claircore"
	"github.com/quay/claircore/internal/indexer"
)

func distributionsByLayer(ctx context.Context, pool *pgxpool.Pool, hash claircore.Digest, scnrs indexer.VersionedScanners) ([]*claircore.Distribution, error) {
	const (
		selectScanner = `
		SELECT id
		FROM scanner
		WHERE name = $1
		  AND version = $2
		  AND kind = $3;
		`
		query = `
		SELECT dist.id,
			   dist.name,
			   dist.did,
			   dist.version,
			   dist.version_code_name,
			   dist.version_id,
			   dist.arch,
			   dist.cpe,
			   dist.pretty_name
		FROM dist_scanartifact
			 LEFT JOIN dist ON dist_scanartifact.dist_id = dist.id
		WHERE dist_scanartifact.layer_hash = $1
		  AND dist_scanartifact.scanner_id = ANY($2);
		`
	)
	if len(scnrs) == 0 {
		return []*claircore.Distribution{}, nil
	}
	// get scanner ids
	scannerIDs := []int64{}
	for _, scnr := range scnrs {
		var scannerID int64
		err := pool.QueryRow(ctx, selectScanner, scnr.Name(), scnr.Version(), scnr.Kind()).Scan(&scannerID)
		if err != nil {
			return nil, fmt.Errorf("store:distributionseByLayer failed to retrieve scanner ids for scnr %v: %v", scnr, err)
		}
		scannerIDs = append(scannerIDs, scannerID)
	}

	rows, err := pool.Query(ctx, query, hash, scannerIDs)
	switch {
	case err == nil:
	case errors.Is(err, pgx.ErrNoRows):
		return nil, fmt.Errorf("store:distributionsByLayer no distribution found for hash %v and scnrs %v", hash, scnrs)
	default:
		return nil, fmt.Errorf("store:distributionsByLayer failed to retrieve package rows for hash %v and scanners %v: %v", hash, scnrs, err)
	}
	defer rows.Close()

	res := []*claircore.Distribution{}
	for rows.Next() {
		var dist claircore.Distribution

		var id int64
		err := rows.Scan(
			&id,
			&dist.Name,
			&dist.DID,
			&dist.Version,
			&dist.VersionCodeName,
			&dist.VersionID,
			&dist.Arch,
			&dist.CPE,
			&dist.PrettyName,
		)
		dist.ID = strconv.FormatInt(id, 10)
		if err != nil {
			return nil, fmt.Errorf("store:distributionsByLayer failed to scan distribution: %v", err)
		}

		res = append(res, &dist)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}
