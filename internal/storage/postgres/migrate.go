// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	_ "github.com/jackc/pgx/v5/stdlib" // registers "pgx" driver for database/sql
	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var migrations embed.FS

// gooseMu serializes access to goose's package-global state
// (SetBaseFS / SetDialect / SetTableName). Both runMigrations and
// runAuthMigrations mutate that state, so they must never interleave.
var gooseMu sync.Mutex

const businessSchemaVersion int64 = 202609240001

// ErrSchemaVersionMismatch never authorizes an automatic database rewrite.
var ErrSchemaVersionMismatch = errors.New("business schema is not the current baseline; rebuild the development database explicitly")

// Only the current initialization is supported; a rollback marker is not current.
const businessSchemaVersionSQL = `SELECT COALESCE((SELECT CASE WHEN is_applied THEN version_id ELSE -1 END
	FROM public.goose_db_version ORDER BY id DESC LIMIT 1),0)`

func runMigrations(connString string) error {
	gooseMu.Lock()
	defer gooseMu.Unlock()

	db, err := sql.Open("pgx", connString)
	if err != nil {
		return fmt.Errorf("open migration connection: %w", err)
	}
	defer func() {
		if cErr := db.Close(); cErr != nil {
			slog.LogAttrs(context.Background(), slog.LevelError, "close migration connection",
				slog.Any("error", cErr))
		}
	}()

	var hasVersionTable bool
	if err := db.QueryRow(`SELECT to_regclass('public.goose_db_version') IS NOT NULL`).Scan(&hasVersionTable); err != nil {
		return fmt.Errorf("read business schema version table: %w", err)
	}
	var version int64
	if hasVersionTable {
		if err := db.QueryRow(businessSchemaVersionSQL).Scan(&version); err != nil {
			return fmt.Errorf("read business schema version: %w", err)
		}
	}
	if version == businessSchemaVersion {
		return nil
	}
	if version != 0 {
		return fmt.Errorf("%w (found %d, expected %d)", ErrSchemaVersionMismatch, version, businessSchemaVersion)
	}
	goose.SetBaseFS(migrations)
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("set goose dialect: %w", err)
	}
	if err := goose.Up(db, "migrations"); err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}
	return nil
}
