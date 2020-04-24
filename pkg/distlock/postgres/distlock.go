package postgres

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

const (
	manifestAdvisoryLock = `SELECT pg_try_advisory_xact_lock($1);`
)

// Lock implements the distlock.Locker interface. Locker utilizes
// postgres transaction advisor locks to implement a distributed lock.
type lock struct {
	// a database instance where we can create pg_advisory locks
	pool *pgxpool.Pool
	// the frequency we would like to attempt a lock acquisition
	retry time.Duration
	// mu ensures TryLock() and Unlock() have exclusive access to the locked bool.
	mu sync.Mutex
	// whether a lock has been acquired or not
	locked bool
	// the key used to identify this unique lock
	key string
	// The transaction in which the lock is being held. Committing this
	// transaction will release the advisory lock.
	tx pgx.Tx
}

func NewLock(pool *pgxpool.Pool, retry time.Duration) *lock {
	return &lock{
		pool:  pool,
		retry: retry,
	}
}

// Lock will immediately attempt to obtain a lock with the key
// being the provided hash. on failure of initial attempt a new attempt
// will be made every l.retry interval. Once lock is acquired the
// method unblocks
func (l *lock) Lock(ctx context.Context, key string) error {
	if l.locked {
		return fmt.Errorf("attempt to lock while lock held")
	}

	// attempt initial lock acquisition. we throw away the bool and prefer
	// checking l.locked bool which l.TryLock flips under mu lock
	_, err := l.TryLock(ctx, key)
	if err != nil {
		return fmt.Errorf("failed at attempting initial lock acquisition: %v", err)
	}
	if l.locked {
		return nil
	}

	// if initial attempt failed begin retry loop.
	t := time.NewTicker(l.retry)
	defer t.Stop()

	for !l.locked {
		select {
		case <-t.C:
			_, err := l.TryLock(ctx, key)
			if err != nil {
				return fmt.Errorf("failed at attempting initial lock acquisition: %v", err)
			}
		case <-ctx.Done():
			return fmt.Errorf("context canceled: %v", ctx.Err())
		}
	}

	return nil
}

// Unlock first checks if the scanLock is in a locked state, secondly checks to
// confirm the tx field is not nil, and lastly will commit the tx allowing
// other calls to Lock() to succeed and sets l.locked = false.
func (l *lock) Unlock(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.locked {
		return fmt.Errorf("attempted to unlock when no lock has been acquired")
	}
	if l.tx == nil {
		return fmt.Errorf("attempted to unlock but no transaction is populated in scanLock")
	}

	// Committing the transaction will free the pg_advisory lock allowing other
	// instances utilizing a lock to proceed.
	err := l.tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction and free lock: %v", err)
	}
	l.locked = false
	return nil
}

// TryLock will begin a transaction and then attempt to acquire a pg_advisory transaction
// lock. On success it will set sl.locked = true and populate sl.tx with the transaction
// holding the lock. on any errors we rollback the transcation (unlocking any other scanLocks)
// and return the error. if this process dies a TCP reset will be sent to postgres and the transaction
// will be closed allowing other scanLocks to acquire.
func (l *lock) TryLock(ctx context.Context, key string) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.locked {
		return false, nil
	}

	kh := fnv.New64a()
	_, _ = fmt.Fprint(kh, key) // Writing to a hash.Hash never errors.
	keyInt64 := int64(kh.Sum64())

	// start transaction
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return false, err
	}

	// attempt to acquire lock
	var acquired bool
	if err := tx.QueryRow(ctx, manifestAdvisoryLock, keyInt64).Scan(&acquired); err != nil {
		tx.Rollback(ctx)
		return false, fmt.Errorf("failed to issue pg_advisory lock query: %v", err)
	}

	// if acquired set lock status which unblocks caller waiting on Lock() method
	// and populate tx which will be Commited on Unlock()
	if acquired {
		l.locked = true
		// hold this tx until Unlock() is called!
		l.tx = tx
		return true, nil
	}

	// we did not acquire the lock
	tx.Rollback(ctx)
	return false, nil
}
