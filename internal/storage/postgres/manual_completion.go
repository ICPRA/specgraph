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

// ManualComplete records an authorized human assertion without creating AI evidence.
// The caller owns authentication, authorization and input validation.
func (s *Store) ManualComplete(ctx context.Context, slug, actor string, expectedVersion int32, key, note string) (version int32, replayed bool, err error) {
	err = s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if lockErr := s.lockDependencyState(txCtx); lockErr != nil {
			return lockErr
		}
		var stage string
		var current int32
		var role string
		if scanErr := s.queryRow(txCtx, `SELECT stage, version, role FROM specs WHERE project_slug = $1 AND slug = $2 FOR UPDATE`, s.project, slug).Scan(&stage, &current, &role); scanErr != nil {
			if errors.Is(scanErr, pgx.ErrNoRows) {
				return storage.ErrSpecNotFound
			}
			return fmt.Errorf("postgres: manual completion lock: %w", scanErr)
		}
		var previousActor, previousNote string
		var source int32
		replayErr := s.queryRow(txCtx, `SELECT actor, note, source_version, result_version FROM manual_completions WHERE project_slug = $1 AND spec_slug = $2 AND idempotency_key = $3`, s.project, slug, key).Scan(&previousActor, &previousNote, &source, &version)
		if replayErr == nil {
			if previousActor != actor || previousNote != note || source != expectedVersion {
				return storage.ErrManualCompletionConflict
			}
			replayed = true
			return nil
		}
		if !errors.Is(replayErr, pgx.ErrNoRows) {
			return fmt.Errorf("postgres: manual completion replay: %w", replayErr)
		}
		if storage.SpecStage(stage).IsFullyTerminal() {
			return storage.ErrSpecTerminal
		}
		if role == string(storage.SpecRoleSummary) {
			return storage.ErrSummaryNotExecutable
		}
		if stage == "done" {
			return storage.ErrManualCompletionConflict
		}
		if current != expectedVersion {
			return storage.ErrConcurrentModification
		}
		if err := s.rejectMergedSourceExecution(txCtx, slug); err != nil {
			return err
		}
		review, readReviewStatusErr := s.ReadReviewStatus(txCtx, slug)
		if readReviewStatusErr != nil {
			return readReviewStatusErr
		}
		for _, state := range review.Reviews {
			if state.HumanHold {
				return storage.ErrReviewHumanHold
			}
		}
		if held, hasDispatchResponsibilityErr := s.hasDispatchResponsibility(txCtx, slug, ""); hasDispatchResponsibilityErr != nil {
			return hasDispatchResponsibilityErr
		} else if held {
			return storage.ErrDispatchResponsibilityHeld
		}
		// Lock an existing lease as well, so renewal cannot race the expiry check.
		var active bool
		if scanErr := s.queryRow(txCtx, `SELECT lease_expires > $3 FROM claims WHERE project_slug = $1 AND spec_slug = $2 FOR UPDATE`, s.project, slug, s.now()).Scan(&active); scanErr != nil && !errors.Is(scanErr, pgx.ErrNoRows) {
			return fmt.Errorf("postgres: manual completion claim: %w", scanErr)
		}
		if active {
			return storage.ErrSpecAlreadyClaimed
		}
		now := s.now()
		tag, readReviewStatusErr := s.exec(txCtx, `UPDATE specs SET stage = 'done', version = version + 1, updated_at = $1 WHERE project_slug = $2 AND slug = $3 AND version = $4`, now, s.project, slug, expectedVersion)
		if readReviewStatusErr != nil {
			return fmt.Errorf("postgres: manual completion update: %w", readReviewStatusErr)
		}
		if tag.RowsAffected() != 1 {
			return storage.ErrConcurrentModification
		}
		version = current + 1
		if recomputeContentHashErr := s.recomputeContentHash(txCtx, slug); recomputeContentHashErr != nil {
			return recomputeContentHashErr
		}
		spec, readReviewStatusErr := s.GetSpec(txCtx, slug)
		if readReviewStatusErr != nil {
			return readReviewStatusErr
		}
		deltas := storage.ComputeFieldDeltas(&storage.SpecFields{Stage: stage}, &storage.SpecFields{Stage: "done"})
		if err := s.createChangeLog(txCtx, slug, &storage.ChangeLogEntry{Version: version, Stage: "done", ContentHash: spec.ContentHash, Checkpoint: true, Summary: "Manually completed by operator (not technical verification)", Date: now}, deltas); err != nil {
			return err
		}
		if err := s.RefreshDependencyHashes(txCtx, slug); err != nil {
			return err
		}
		id := newID("mc")
		if _, err := s.exec(txCtx, `INSERT INTO manual_completions (id, project_slug, spec_slug, actor, source_version, result_version, note, idempotency_key, created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, id, s.project, slug, actor, current, version, note, key, now); err != nil {
			return fmt.Errorf("postgres: manual completion audit: %w", err)
		}
		return s.triggerCompletionHooks(txCtx, slug, "manual", id)
	})
	return version, replayed, err
}

// ListManualCompletions reads human assertions separately from AI execution events.
func (s *Store) ListManualCompletions(ctx context.Context, slug string) ([]storage.ManualCompletion, error) {
	rows, err := s.query(ctx, `SELECT id, spec_slug, actor, source_version, result_version, note, idempotency_key, created_at FROM manual_completions WHERE project_slug = $1 AND spec_slug = $2 ORDER BY created_at DESC, id DESC`, s.project, slug)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]storage.ManualCompletion, 0)
	for rows.Next() {
		var item storage.ManualCompletion
		if err := rows.Scan(&item.ID, &item.Slug, &item.Actor, &item.SourceVersion, &item.ResultVersion, &item.Note, &item.IdempotencyKey, &item.CreatedAt); err != nil {
			return nil, fmt.Errorf("postgres: manual completion audit scan: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: manual completion audit rows: %w", err)
	}
	return result, nil
}
