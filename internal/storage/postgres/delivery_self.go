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

func (s *Store) readOwnDeliveryContext(ctx context.Context, scope storage.MailScope) (storage.DeliverySelfContext, json.RawMessage, error) {
	run, _, err := s.reviewActorRun(ctx, scope)
	if err != nil {
		return storage.DeliverySelfContext{}, nil, err
	}
	recorded, err := s.ReadRunContext(ctx, run)
	if err != nil {
		return storage.DeliverySelfContext{}, nil, err
	}
	var body struct {
		Workspace string `json:"workspace"`
		Target    struct {
			Baseline json.RawMessage `json:"gitBaseline"`
		} `json:"dispatch_target"`
	}
	if json.Unmarshal(recorded.Body, &body) != nil || strings.TrimSpace(body.Workspace) == "" {
		return storage.DeliverySelfContext{}, nil, storage.ErrInvalidRunPreparation
	}
	result := storage.DeliverySelfContext{RunID: run, TaskSlug: recorded.TaskSlug, Workspace: body.Workspace}
	var attemptID string
	err = s.queryRow(ctx, `SELECT id FROM candidate_attempts WHERE project_slug=$1 AND run_id=$2 ORDER BY ordinal DESC LIMIT 1`, s.project, run).Scan(&attemptID)
	if err == nil {
		result.CurrentAttemptID = &attemptID
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return storage.DeliverySelfContext{}, nil, fmt.Errorf("postgres: read own candidate attempt: %w", err)
	}
	return result, body.Target.Baseline, nil
}

// ReadOwnDeliveryContext returns the authenticated bound run's recorded workspace and task.
func (s *Store) ReadOwnDeliveryContext(ctx context.Context, scope storage.MailScope) (storage.DeliverySelfContext, error) {
	var result storage.DeliverySelfContext
	err := s.RunReadSnapshot(ctx, func(snapshotCtx context.Context) error {
		var err error
		result, _, err = s.readOwnDeliveryContext(snapshotCtx, scope)
		return err
	})
	return result, err
}

// SubmitOwnDelivery records the host-observed head without asserting clean files, tests or completion.
func (s *Store) SubmitOwnDelivery(ctx context.Context, scope storage.MailScope, expectedRunID, expectedAttemptID, summary string, head storage.DeliveryHead) (storage.DeliverySelfReceipt, error) {
	if strings.TrimSpace(expectedRunID) == "" || strings.TrimSpace(summary) == "" || utf8.RuneCountInString(summary) > 4000 || (!head.IsRepo && head.CommitSHA != nil) || (head.CommitSHA != nil && !reviewCommitPattern.MatchString(*head.CommitSHA)) {
		return storage.DeliverySelfReceipt{}, storage.ErrInvalidDeliverySubmission
	}
	var result storage.DeliverySelfReceipt
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		recorded, baseline, err := s.readOwnDeliveryContext(txCtx, scope)
		if err != nil {
			return err
		}
		if recorded.RunID != expectedRunID {
			return storage.ErrRunBindingConflict
		}
		if (recorded.CurrentAttemptID == nil && expectedAttemptID != "") || (recorded.CurrentAttemptID != nil && *recorded.CurrentAttemptID != expectedAttemptID) {
			return storage.ErrCandidateLoopConflict
		}
		snapshot, err := json.Marshal(map[string]any{"summary": summary, "git": map[string]any{"workspace": recorded.Workspace, "baseline": baseline, "head": head, "uncommittedContentIncluded": false}})
		if err != nil {
			return fmt.Errorf("postgres: SubmitOwnDelivery: %w", err)
		}
		delivery, err := s.CreateDelivery(txCtx, recorded.RunID, snapshot, recorded.RunID, expectedAttemptID)
		if err != nil {
			return err
		}
		result = storage.DeliverySelfReceipt{DeliveryID: delivery, RunID: recorded.RunID, TaskSlug: recorded.TaskSlug, AttemptID: recorded.CurrentAttemptID}
		return nil
	})
	return result, err
}
