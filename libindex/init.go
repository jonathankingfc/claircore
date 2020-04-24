package libindex

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/remind101/migrate"

	"github.com/quay/claircore/internal/indexer"
	"github.com/quay/claircore/internal/indexer/postgres"
	"github.com/quay/claircore/libindex/migrations"
)

// initialize a indexer.Store given libindex.Opts
func initStore(ctx context.Context, opts *Opts) (*pgxpool.Pool, indexer.Store, error) {
	// we are going to use pgx for more control over connection pool and
	// and a cleaner api around bulk inserts
	cfg, err := pgxpool.ParseConfig(opts.ConnString)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse ConnString: %v", err)
	}
	cfg.MaxConns = 30
	pool, err := pgxpool.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create ConnPool: %v", err)
	}

	// do migrations if requested
	if opts.Migrations {
		db, err := sql.Open("pgx", opts.ConnString)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to open db: %v", err)
		}
		defer db.Close()

		migrator := migrate.NewPostgresMigrator(db)
		migrator.Table = migrations.MigrationTable
		if err := migrator.Exec(migrate.Up, migrations.Migrations...); err != nil {
			return nil, nil, fmt.Errorf("failed to perform migrations: %w", err)
		}
	}

	store := postgres.NewStore(pool)
	return pool, store, nil
}
