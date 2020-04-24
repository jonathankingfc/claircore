package postgres

import (
	"context"

	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/quay/claircore"
	"github.com/quay/claircore/internal/indexer"
)

var _ indexer.Store = (*store)(nil)

// store implements the claircore.Store interface.
// implements all persistence features
type store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *store {
	return &store{
		pool: pool,
	}
}

func (s *store) Close(_ context.Context) error {
	s.pool.Close()
	return nil
}

func (s *store) PersistManifest(ctx context.Context, manifest claircore.Manifest) error {
	return persistManifest(ctx, s.pool, manifest)
}

func (s *store) ManifestScanned(ctx context.Context, hash claircore.Digest, scnrs indexer.VersionedScanners) (bool, error) {
	return manifestScanned(ctx, s.pool, hash, scnrs)
}

func (s *store) LayerScanned(ctx context.Context, hash claircore.Digest, scnr indexer.VersionedScanner) (bool, error) {
	return layerScanned(ctx, s.pool, hash, scnr)
}

func (s *store) SetLayerScanned(ctx context.Context, hash claircore.Digest, scnr indexer.VersionedScanner) error {
	return setLayerScanned(ctx, s.pool, hash, scnr)
}

func (s *store) IndexPackages(ctx context.Context, pkgs []*claircore.Package, l *claircore.Layer, scnr indexer.VersionedScanner) error {
	err := indexPackages(ctx, s.pool, pkgs, l, scnr)
	return err
}

func (s *store) IndexDistributions(ctx context.Context, dists []*claircore.Distribution, l *claircore.Layer, scnr indexer.VersionedScanner) error {
	return indexDistributions(ctx, s.pool, dists, l, scnr)
}

func (s *store) IndexRepositories(ctx context.Context, repos []*claircore.Repository, l *claircore.Layer, scnr indexer.VersionedScanner) error {
	return indexRepositories(ctx, s.pool, repos, l, scnr)
}

func (s *store) PackagesByLayer(ctx context.Context, hash claircore.Digest, scnrs indexer.VersionedScanners) ([]*claircore.Package, error) {
	return packagesByLayer(ctx, s.pool, hash, scnrs)
}

func (s *store) DistributionsByLayer(ctx context.Context, hash claircore.Digest, scnrs indexer.VersionedScanners) ([]*claircore.Distribution, error) {
	return distributionsByLayer(ctx, s.pool, hash, scnrs)
}

func (s *store) RepositoriesByLayer(ctx context.Context, hash claircore.Digest, scnrs indexer.VersionedScanners) ([]*claircore.Repository, error) {
	return repositoriesByLayer(ctx, s.pool, hash, scnrs)
}

func (s *store) RegisterScanners(ctx context.Context, scnrs indexer.VersionedScanners) error {
	return registerScanners(ctx, s.pool, scnrs)
}

func (s *store) IndexReport(ctx context.Context, hash claircore.Digest) (*claircore.IndexReport, bool, error) {
	return indexReport(ctx, s.pool, hash)
}

func (s *store) SetIndexReport(ctx context.Context, sr *claircore.IndexReport) error {
	return setIndexReport(ctx, s.pool, sr)
}

func (s *store) SetIndexFinished(ctx context.Context, ir *claircore.IndexReport, scnrs indexer.VersionedScanners) error {
	return setScanFinished(ctx, s.pool, ir, scnrs)
}
