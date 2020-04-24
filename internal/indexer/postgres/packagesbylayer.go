package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/quay/claircore"
	"github.com/quay/claircore/internal/indexer"
)

func packagesByLayer(ctx context.Context, pool *pgxpool.Pool, hash claircore.Digest, scnrs indexer.VersionedScanners) ([]*claircore.Package, error) {
	const (
		selectScanner = `
		SELECT id
		FROM scanner
		WHERE name = $1
		  AND version = $2
		  AND kind = $3;
		`
		query = `
		SELECT package.id,
			   package.name,
			   package.kind,
			   package.version,
			   package.norm_kind,
			   package.norm_version,
			   package.module,
			   package.arch,

			   source_package.id,
			   source_package.name,
			   source_package.kind,
			   source_package.version,
			   source_package.module,
			   source_package.arch,

			   package_scanartifact.package_db,
			   package_scanartifact.repository_hint
		FROM package_scanartifact
			LEFT JOIN package ON package_scanartifact.package_id = package.id
			LEFT JOIN package source_package ON package_scanartifact.source_id = source_package.id
		WHERE package_scanartifact.layer_hash = $1
		  AND package_scanartifact.scanner_id = ANY($2);`
	)

	if len(scnrs) == 0 {
		return []*claircore.Package{}, nil
	}

	// get scanner ids
	scannerIDs := make([]int64, len(scnrs))
	for i, s := range scnrs {
		err := pool.QueryRow(ctx, selectScanner, s.Name(), s.Version(), s.Kind()).Scan(&scannerIDs[i])
		if err != nil {
			return nil, fmt.Errorf("store:packagesByLayer failed to retrieve id for scanner %v: %v", s, err)
		}
	}

	// allocate result array
	var res []*claircore.Package = []*claircore.Package{}

	rows, err := pool.Query(ctx, query, hash, scannerIDs)
	switch {
	case err == nil:
	case errors.Is(err, pgx.ErrNoRows):
		return nil, fmt.Errorf("store:packagesByLayer no packages found for hash %v and scnrs %v", hash, scnrs)
	default:
		return nil, fmt.Errorf("store:packagesByLayer failed to retrieve package rows for hash %v and scanners %v: %v", hash, scnrs, err)
	}
	defer rows.Close()

	for rows.Next() {
		var pkg claircore.Package
		var spkg claircore.Package

		var id, srcID int64
		var nKind *string
		var nVer pgtype.Int4Array
		err := rows.Scan(
			&id,
			&pkg.Name,
			&pkg.Kind,
			&pkg.Version,
			&nKind,
			&nVer,
			&pkg.Module,
			&pkg.Arch,
			&srcID,
			&spkg.Name,
			&spkg.Kind,
			&spkg.Version,
			&spkg.Module,
			&spkg.Arch,
			&pkg.PackageDB,
			&pkg.RepositoryHint,
		)
		pkg.ID = strconv.FormatInt(id, 10)
		spkg.ID = strconv.FormatInt(srcID, 10)
		if err != nil {
			return nil, fmt.Errorf("store:packagesByLayer failed to scan packages: %v", err)
		}
		if nKind != nil {
			pkg.NormalizedVersion.Kind = *nKind
			for i, n := range nVer.Elements {
				pkg.NormalizedVersion.V[i] = n.Int
			}
		}
		// nest source package
		pkg.Source = &spkg

		res = append(res, &pkg)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}
