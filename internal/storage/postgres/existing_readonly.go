// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// OpenExistingReadOnly opens an existing database without migrations or project
// creation. Call ScopedExisting before reading project data and Close on exit.
func OpenExistingReadOnly(ctx context.Context, connString string) (*Store, error) {
	return openExisting(ctx, connString, true)
}

// OpenExisting opens an existing database for explicit commands without migrations
// or project creation. Call ScopedExisting before any project command.
func OpenExisting(ctx context.Context, connString string) (*Store, error) {
	return openExisting(ctx, connString, false)
}

func openExisting(ctx context.Context, connString string, readOnly bool) (*Store, error) {
	poolCfg, err := buildPoolConfig(connString)
	if err != nil {
		return nil, err
	}
	if readOnly {
		poolCfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: open existing database: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: verify existing database: %w", err)
	}
	var hasVersionTable bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('public.goose_db_version') IS NOT NULL`).Scan(&hasVersionTable); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: read business schema version table: %w", err)
	}
	var version int64
	if hasVersionTable {
		if err := pool.QueryRow(ctx, businessSchemaVersionSQL).Scan(&version); err != nil {
			pool.Close()
			return nil, fmt.Errorf("postgres: read business schema version: %w", err)
		}
	}
	if version != businessSchemaVersion {
		pool.Close()
		return nil, fmt.Errorf("%w (found %d, expected %d)", ErrSchemaVersionMismatch, version, businessSchemaVersion)
	}
	return &Store{pool: pool, nowFunc: time.Now, ownsPool: true, shared: &sharedState{}}, nil
}

// ExistingAuth shares this Store's pool without auth migrations. The caller owns
// its lifetime; read-only authentication callers must not invoke mutation methods.
func (s *Store) ExistingAuth() *AuthStore {
	return &AuthStore{pool: s.pool, nowFunc: s.nowFunc, genPrefix: defaultGenerateKeyPrefix}
}
