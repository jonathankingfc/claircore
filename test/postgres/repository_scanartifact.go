package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/quay/claircore"
	"github.com/quay/claircore/internal/indexer"
)

func InsertRepoScanArtifact(ctx context.Context, pool *pgxpool.Pool, layerHash claircore.Digest, repos []*claircore.Repository, scnrs indexer.VersionedScanners) error {
	n := len(scnrs)
	for i, repo := range repos {
		nn := i % n
		_, err := pool.Exec(ctx,
			`INSERT INTO repo_scanartifact
			(layer_hash, repo_id, scanner_id)
		VALUES
			($1, $2, $3)`,
			&layerHash, &repo.ID, &nn)
		if err != nil {
			return fmt.Errorf("failed to insert repo scan artifact: %v", err)
		}
	}

	return nil
}
