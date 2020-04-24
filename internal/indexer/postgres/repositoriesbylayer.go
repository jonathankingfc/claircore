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

func repositoriesByLayer(ctx context.Context, pool *pgxpool.Pool, hash claircore.Digest, scnrs indexer.VersionedScanners) ([]*claircore.Repository, error) {
	const (
		selectScanner = `
		SELECT id
		FROM scanner
		WHERE name = $1
		  AND version = $2
		  AND kind = $3;
		`
		query = `
		SELECT repo.id,
			   repo.name,
			   repo.key,
			   repo.uri
		FROM repo_scanartifact
			LEFT JOIN repo ON repo_scanartifact.repo_id = repo.id
		WHERE repo_scanartifact.layer_hash = $1
		  AND repo_scanartifact.scanner_id = ANY($2);
		`
	)
	if len(scnrs) == 0 {
		return []*claircore.Repository{}, nil
	}
	// get scanner ids
	scannerIDs := []int64{}
	for _, scnr := range scnrs {
		var scannerID int64
		err := pool.QueryRow(ctx, selectScanner, scnr.Name(), scnr.Version(), scnr.Kind()).Scan(&scannerID)
		if err != nil {
			return nil, fmt.Errorf("store:repositoriesByLayer failed to retrieve scanner ids for scnr %v: %v", scnr, err)
		}
		scannerIDs = append(scannerIDs, scannerID)
	}

	res := []*claircore.Repository{}

	rows, err := pool.Query(ctx, query, hash, scannerIDs)
	switch {
	case err == nil:
	case errors.Is(err, pgx.ErrNoRows):
		return nil, fmt.Errorf("store:repositoriesByLayer no repositories found for hash %v and scnrs %v", hash, scnrs)
	default:
		return nil, fmt.Errorf("store:repositoriesByLayer failed to retrieve package rows for hash %v and scanners %v: %v", hash, scnrs, err)
	}
	defer rows.Close()

	for rows.Next() {
		var repo claircore.Repository

		var id int64
		err := rows.Scan(
			&id,
			&repo.Name,
			&repo.Key,
			&repo.URI,
		)
		repo.ID = strconv.FormatInt(id, 10)
		if err != nil {
			return nil, fmt.Errorf("store:repositoriesByLayer failed to scan repositories: %v", err)
		}

		res = append(res, &repo)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}
