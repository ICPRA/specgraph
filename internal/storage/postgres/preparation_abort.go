// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/storage"
)

// AbortPreparation fences the request key even if its original response was
// lost. Publication and abort share the project lock, so absence is not guessed.
func (s *Store) AbortPreparation(ctx context.Context, actor string, request *storage.PreparationAbortRequest) (storage.PreparationAbortResult, error) {
	var format struct {
		Version int    `json:"version"`
		Prompt  string `json:"promptFormatVersion"`
	}
	if strings.TrimSpace(actor) == "" || !subdivisionKeyPattern.MatchString(request.IdempotencyKey) || !subdivisionSlugPattern.MatchString(request.TaskSlug) || len(request.TaskSlug) > 256 || strings.TrimSpace(request.Workspace) == "" || len(request.Workspace) > 4096 || strings.TrimSpace(request.Note) == "" || utf8.RuneCountInString(request.Note) > 4000 || len(request.Target) > 24<<10 || json.Unmarshal(request.Target, &format) != nil || format.Version != 1 || format.Prompt != "vacpms-run-v2" {
		return storage.PreparationAbortResult{}, storage.ErrInvalidRunPreparation
	}
	result := storage.PreparationAbortResult{IdempotencyKey: request.IdempotencyKey}
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		var same bool
		err := s.queryRow(txCtx, `SELECT run_id,actor=$3 AND task_slug=$4 AND workspace=$5 AND dispatch_target=$6::jsonb AND note=$7
			FROM preparation_aborts WHERE project_slug=$1 AND idempotency_key=$2`, s.project, request.IdempotencyKey, actor, request.TaskSlug, request.Workspace, request.Target, request.Note).Scan(&result.RunID, &same)
		if err == nil {
			if !same {
				return storage.ErrRunBindingConflict
			}
			result.Replayed = true
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("postgres: AbortPreparation: %w", err)
		}
		var runID, packageID string
		var alreadyCancelled bool
		err = s.queryRow(txCtx, `SELECT p.run_id,r.package_id,COALESCE(p.task_slug=$3 AND p.workspace=$4 AND p.dispatch_target=$5::jsonb,false),
			(EXISTS(SELECT 1 FROM run_preparation_cancellations c WHERE c.project_slug=p.project_slug AND c.run_id=p.run_id)
			 OR EXISTS(SELECT 1 FROM run_dispatches d WHERE d.project_slug=p.project_slug AND d.run_id=p.run_id AND d.released_at IS NOT NULL))
			FROM run_preparations p JOIN run_bindings r ON r.project_slug=p.project_slug AND r.id=p.run_id
			WHERE p.project_slug=$1 AND p.idempotency_key=$2`, s.project, request.IdempotencyKey, request.TaskSlug, request.Workspace, request.Target).Scan(&runID, &packageID, &same, &alreadyCancelled)
		if err == nil {
			if !same {
				return storage.ErrRunBindingConflict
			}
			result.RunID = &runID
			if !alreadyCancelled {
				if cancelRunPreparationErr := s.CancelRunPreparation(txCtx, runID, packageID, actor, request.Note); cancelRunPreparationErr != nil {
					return cancelRunPreparationErr
				}
			}
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("postgres: AbortPreparation: %w", err)
		}
		_, err = s.exec(txCtx, `INSERT INTO preparation_aborts(project_slug,idempotency_key,task_slug,workspace,dispatch_target,actor,note,run_id,created_at)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, s.project, request.IdempotencyKey, request.TaskSlug, request.Workspace, request.Target, actor, request.Note, result.RunID, s.now())
		return err
	})
	if err != nil {
		return result, err
	}
	return result, nil
}
