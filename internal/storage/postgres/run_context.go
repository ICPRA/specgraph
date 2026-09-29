// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/storage"
)

// ReadRunContext returns exactly what preparation recorded, never a regenerated
// bundle. Reading it does not claim that the run is still eligible to execute.
func (s *Store) ReadRunContext(ctx context.Context, runID string) (storage.RunContext, error) {
	var result storage.RunContext
	err := s.queryRow(ctx, `SELECT rb.id,rb.task_spec_slug,cp.id,cp.body,cp.created_at
		FROM run_bindings rb JOIN context_packages cp ON cp.project_slug=rb.project_slug AND cp.id=rb.package_id AND cp.task_spec_slug=rb.task_spec_slug
		WHERE rb.project_slug=$1 AND rb.id=$2`, s.project, runID).
		Scan(&result.RunID, &result.TaskSlug, &result.PackageID, &result.Body, &result.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.RunContext{}, storage.ErrRunContextNotFound
	}
	if err != nil {
		return storage.RunContext{}, fmt.Errorf("postgres: read run context: %w", err)
	}
	return result, nil
}
