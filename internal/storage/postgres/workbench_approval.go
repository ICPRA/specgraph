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

// ApproveWorkbenchNode records an operator's execution approval, not authoring
// discussions or execution evidence. The caller authenticates and validates input.
func (s *Store) ApproveWorkbenchNode(ctx context.Context, slug, actor string, expectedVersion int32, basis string) (version int32, err error) {
	err = s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if lockErr := s.lockDependencyState(txCtx); lockErr != nil {
			return lockErr
		}
		var stage storage.SpecStage
		var current int32
		var role storage.SpecRole
		if scanErr := s.queryRow(txCtx, `SELECT stage, version, role FROM specs WHERE project_slug=$1 AND slug=$2 FOR UPDATE`, s.project, slug).Scan(&stage, &current, &role); scanErr != nil {
			if errors.Is(scanErr, pgx.ErrNoRows) {
				return storage.ErrSpecNotFound
			}
			return fmt.Errorf("postgres: workbench approval lock: %w", scanErr)
		}
		if current != expectedVersion {
			return storage.ErrConcurrentModification
		}
		if stage.IsFullyTerminal() {
			return storage.ErrSpecTerminal
		}
		if !stage.IsValidReEntryStage() {
			return storage.ErrSpecIneligibleStage
		}
		if role == storage.SpecRoleSummary {
			return storage.ErrSummaryNotExecutable
		}
		if held, hasDispatchResponsibilityErr := s.hasDispatchResponsibility(txCtx, slug, ""); hasDispatchResponsibilityErr != nil {
			return hasDispatchResponsibilityErr
		} else if held {
			return storage.ErrDispatchResponsibilityHeld
		}
		var active bool
		if scanErr := s.queryRow(txCtx, `SELECT lease_expires > $3 FROM claims WHERE project_slug=$1 AND spec_slug=$2 FOR UPDATE`, s.project, slug, s.now()).Scan(&active); scanErr != nil && !errors.Is(scanErr, pgx.ErrNoRows) {
			return fmt.Errorf("postgres: workbench approval claim: %w", scanErr)
		}
		if active {
			return storage.ErrSpecAlreadyClaimed
		}
		now := s.now()
		tag, execErr := s.exec(txCtx, `UPDATE specs SET stage='approved', version=version+1, updated_at=$3 WHERE project_slug=$1 AND slug=$2 AND version=$4`, s.project, slug, now, current)
		if execErr != nil {
			return fmt.Errorf("postgres: workbench approval update: %w", execErr)
		}
		if tag.RowsAffected() != 1 {
			return storage.ErrConcurrentModification
		}
		if recomputeContentHashErr := s.recomputeContentHash(txCtx, slug); recomputeContentHashErr != nil {
			return recomputeContentHashErr
		}
		spec, execErr := s.GetSpec(txCtx, slug)
		if execErr != nil {
			return execErr
		}
		version = spec.Version
		return s.createChangeLog(txCtx, slug, &storage.ChangeLogEntry{
			Version: version, Stage: "approved", ContentHash: spec.ContentHash, Checkpoint: true,
			Summary: fmt.Sprintf("Workbench execution approval by %s (version %d -> %d)", actor, current, version),
			Reason:  basis, Date: now,
		}, storage.ComputeFieldDeltas(&storage.SpecFields{Stage: string(stage)}, &storage.SpecFields{Stage: "approved"}))
	})
	return version, err
}
