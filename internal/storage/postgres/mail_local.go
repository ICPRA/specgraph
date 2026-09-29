// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/storage"
)

// ProjectForLocalMailScope resolves only an existing unambiguous local run binding.
func (s *Store) ProjectForLocalMailScope(ctx context.Context, scope storage.MailScope) (string, error) {
	if !validMailScope(scope) {
		return "", storage.ErrMailInvalid
	}
	rows, err := s.query(ctx, `SELECT DISTINCT project_slug FROM run_bindings WHERE environment_id=$1 AND thread_ref=$2 AND state='bound'`, scope.EnvironmentID, scope.ThreadID)
	if err != nil {
		return "", err
	}
	projects, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return "", fmt.Errorf("postgres: local mail projects: %w", err)
	}
	if len(projects) != 1 {
		return "", storage.ErrMailForbidden
	}
	return projects[0], nil
}

// EnsureLocalMailBinding registers a bound local agent, never an arbitrary sender.
// Explicit revocation is preserved; the subsequent operation rechecks all locks.
func (s *Store) EnsureLocalMailBinding(ctx context.Context, subject, actor string, scope storage.MailScope) error {
	if !validMailScope(scope) {
		return storage.ErrMailInvalid
	}
	return s.RunInTransaction(ctx, func(ctx context.Context) error {
		rows, err := s.query(ctx, `SELECT id FROM run_bindings WHERE project_slug=$1 AND environment_id=$2 AND thread_ref=$3 AND state='bound'`, s.project, scope.EnvironmentID, scope.ThreadID)
		if err != nil {
			return err
		}
		runs, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return fmt.Errorf("postgres: local mail runs: %w", err)
		}
		if len(runs) != 1 {
			return storage.ErrMailForbidden
		}
		grant, err := scanMailGrant(s.queryRow(ctx, `SELECT `+mailGrantColumns+` FROM mail_grants WHERE project_slug=$1 AND environment_id=$2 AND bridge_subject=$3 ORDER BY granted_at DESC,id DESC LIMIT 1 FOR SHARE`, s.project, scope.EnvironmentID, subject))
		if errors.Is(err, pgx.ErrNoRows) {
			grant, err = s.GrantMailBridge(ctx, scope.EnvironmentID, subject, actor)
		}
		if err != nil {
			return err
		}
		if grant.RevokedAt != nil {
			return storage.ErrMailForbidden
		}
		binding, err := scanMailBinding(s.queryRow(ctx, `SELECT `+mailBindingColumns+` FROM mail_bindings WHERE project_slug=$1 AND run_id=$2 ORDER BY approved_at DESC,id DESC LIMIT 1 FOR UPDATE`, s.project, runs[0]))
		if err == nil && binding.RevokedAt != nil {
			return storage.ErrMailForbidden
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if err == nil && binding.MailScope != scope {
			if binding.GrantID != grant.ID || binding.EnvironmentID != scope.EnvironmentID || binding.ThreadID != scope.ThreadID {
				return storage.ErrMailForbidden
			}
			if _, revokeErr := s.RevokeMailBinding(ctx, binding.ID, actor); revokeErr != nil {
				return revokeErr
			}
		}
		_, err = s.ApproveMailBinding(ctx, grant.ID, runs[0], scope, actor)
		return err
	})
}
