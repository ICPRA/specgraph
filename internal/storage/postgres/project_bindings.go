// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/storage"
)

// ProjectBinding records the human-confirmed native project target of one
// SpecGraph project. Revoked rows are retained history; dispatch preparation
// rejects targets outside the active binding once one exists.
type ProjectBinding struct {
	ID              string     `json:"id"`
	ProjectSlug     string     `json:"projectSlug" db:"project_slug"`
	EnvironmentID   string     `json:"environmentId" db:"environment_id"`
	NativeProjectID string     `json:"nativeProjectId" db:"native_project_id"`
	WorkspaceRoot   *string    `json:"workspaceRoot" db:"workspace_root"`
	Reason          string     `json:"reason"`
	Actor           string     `json:"actor"`
	CreatedAt       time.Time  `json:"createdAt" db:"created_at"`
	RevokedAt       *time.Time `json:"revokedAt" db:"revoked_at"`
	RevokeReason    *string    `json:"revokeReason" db:"revoke_reason"`
	RevokedBy       *string    `json:"revokedBy" db:"revoked_by"`
}

// ProjectBindingHistory pages immutable binding records by their decimal identity cursor.
type ProjectBindingHistory struct {
	Items      []ProjectBinding `json:"items"`
	HasMore    bool             `json:"hasMore"`
	NextCursor string           `json:"nextCursor"`
}

const projectBindingColumns = `id::text,project_slug,environment_id,native_project_id,workspace_root,reason,actor,created_at,revoked_at,revoke_reason,revoked_by`

func scanProjectBinding(row pgx.Row) (ProjectBinding, error) {
	var binding ProjectBinding
	err := row.Scan(&binding.ID, &binding.ProjectSlug, &binding.EnvironmentID, &binding.NativeProjectID,
		&binding.WorkspaceRoot, &binding.Reason, &binding.Actor, &binding.CreatedAt, &binding.RevokedAt,
		&binding.RevokeReason, &binding.RevokedBy)
	return binding, err
}

func validProjectBindingRequest(projectSlug, environmentID, nativeProjectID, workspaceRoot, reason, actor string) bool {
	return strings.TrimSpace(projectSlug) != "" &&
		strings.TrimSpace(environmentID) != "" && utf8.RuneCountInString(environmentID) <= 256 &&
		strings.TrimSpace(nativeProjectID) != "" && utf8.RuneCountInString(nativeProjectID) <= 256 &&
		len(workspaceRoot) <= 4096 &&
		strings.TrimSpace(reason) != "" && utf8.RuneCountInString(reason) <= 4000 &&
		strings.TrimSpace(actor) != ""
}

// BindProject revokes the current active binding and records the confirmed
// replacement in one transaction. The project row lock serializes rebinds.
// The project is an explicit argument: bindings are a global table, not rows
// under the caller's scoped project.
func (s *Store) BindProject(ctx context.Context, projectSlug, environmentID, nativeProjectID, workspaceRoot, reason, actor string) (*ProjectBinding, error) {
	if !validProjectBindingRequest(projectSlug, environmentID, nativeProjectID, workspaceRoot, reason, actor) {
		return nil, storage.ErrInvalidProjectBinding
	}
	var binding ProjectBinding
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		var project string
		if err := s.queryRow(txCtx, `SELECT slug FROM projects WHERE slug=$1 FOR NO KEY UPDATE`, projectSlug).Scan(&project); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return storage.ErrProjectNotFound
			}
			return fmt.Errorf("postgres: lock project binding scope: %w", err)
		}
		if _, err := s.exec(txCtx, `UPDATE project_bindings SET revoked_at=$2 WHERE project_slug=$1 AND revoked_at IS NULL`, projectSlug, s.now()); err != nil {
			return fmt.Errorf("postgres: revoke prior project binding: %w", err)
		}
		recorded, err := scanProjectBinding(s.queryRow(txCtx, `INSERT INTO project_bindings(project_slug,environment_id,native_project_id,workspace_root,reason,actor,created_at)
			VALUES($1,$2,$3,NULLIF($4,''),$5,$6,$7) RETURNING `+projectBindingColumns,
			projectSlug, environmentID, nativeProjectID, workspaceRoot, reason, actor, s.now()))
		if err != nil {
			return fmt.Errorf("postgres: record project binding: %w", err)
		}
		binding = recorded
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &binding, nil
}

// RevokeProjectBinding ends the active binding without deleting its history.
// The revocation reason and actor are recorded on the revoked row for audit.
func (s *Store) RevokeProjectBinding(ctx context.Context, projectSlug, reason, actor string) (*ProjectBinding, error) {
	if strings.TrimSpace(projectSlug) == "" || strings.TrimSpace(reason) == "" || utf8.RuneCountInString(reason) > 4000 || strings.TrimSpace(actor) == "" {
		return nil, storage.ErrInvalidProjectBinding
	}
	binding, err := scanProjectBinding(s.queryRow(ctx, `UPDATE project_bindings SET revoked_at=$2,revoke_reason=$3,revoked_by=$4
		WHERE project_slug=$1 AND revoked_at IS NULL RETURNING `+projectBindingColumns, projectSlug, s.now(), reason, actor))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, storage.ErrProjectBindingNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: revoke project binding: %w", err)
	}
	return &binding, nil
}

// ActiveProjectBinding returns the current binding or nil when none is active.
func (s *Store) ActiveProjectBinding(ctx context.Context, projectSlug string) (*ProjectBinding, error) {
	binding, err := scanProjectBinding(s.queryRow(ctx, `SELECT `+projectBindingColumns+` FROM project_bindings
		WHERE project_slug=$1 AND revoked_at IS NULL`, projectSlug))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: active project binding: %w", err)
	}
	return &binding, nil
}

// ProjectBindingHistory reads up to 50 binding records before a validated
// positive cursor, newest first. A zero cursor starts at the newest record.
func (s *Store) ProjectBindingHistory(ctx context.Context, projectSlug string, beforeID int64) (*ProjectBindingHistory, error) {
	var exists bool
	if err := s.queryRow(ctx, `SELECT EXISTS(SELECT 1 FROM projects WHERE slug=$1)`, projectSlug).Scan(&exists); err != nil {
		return nil, fmt.Errorf("postgres: ProjectBindingHistory: %w", err)
	}
	if !exists {
		return nil, storage.ErrProjectNotFound
	}
	rows, err := s.query(ctx, `SELECT `+projectBindingColumns+` FROM project_bindings
		WHERE project_slug=$1 AND ($2::bigint=0 OR id<$2)
		ORDER BY id DESC LIMIT 51`, projectSlug, beforeID)
	if err != nil {
		return nil, fmt.Errorf("postgres: read project binding history: %w", err)
	}
	items, err := pgx.CollectRows(rows, pgx.RowToStructByName[ProjectBinding])
	if err != nil {
		return nil, fmt.Errorf("postgres: ProjectBindingHistory: %w", err)
	}
	page := &ProjectBindingHistory{Items: items, HasMore: len(items) > 50}
	if page.HasMore {
		page.Items = items[:50]
		page.NextCursor = page.Items[49].ID
	}
	return page, nil
}

// checkDispatchTargetBinding rejects a dispatch target outside the confirmed
// active binding. A project without any binding is not yet constrained: the
// first confirmed dispatch establishes the binding.
func (s *Store) checkDispatchTargetBinding(ctx context.Context, target json.RawMessage) error {
	var fields struct {
		EnvironmentID string `json:"environmentId"`
		ProjectID     string `json:"projectId"`
	}
	if err := json.Unmarshal(target, &fields); err != nil {
		return storage.ErrInvalidRunPreparation
	}
	binding, err := s.ActiveProjectBinding(ctx, s.project)
	if err != nil {
		return fmt.Errorf("postgres: check dispatch target binding: %w", err)
	}
	if binding == nil {
		return nil
	}
	if fields.EnvironmentID != binding.EnvironmentID || fields.ProjectID != binding.NativeProjectID {
		return fmt.Errorf("dispatch target environment %q project %q does not match the active project binding %q/%q: %w",
			fields.EnvironmentID, fields.ProjectID, binding.EnvironmentID, binding.NativeProjectID, storage.ErrProjectBindingMismatch)
	}
	return nil
}
