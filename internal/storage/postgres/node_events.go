// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
)

const nodeEventJSON = `jsonb_build_object('id',id,'taskSlug',task_slug,'kind',kind,'reason',reason,'actor',actor,
 'previousRunId',previous_run_id,'runId',run_id,'deliveryId',delivery_id,'gitUndo',git_undo,'recordedAt',recorded_at)`

// RecordNodeEvent records the operator's declaration without changing any node, run or Git state.
// A retry may be another attempt in the same run; run counts and thread creation do not prove retries.
func (s *Store) RecordNodeEvent(ctx context.Context, slug string, req storage.RecordNodeEventRequest) (*storage.NodeExecutionEvent, error) {
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" {
		return nil, auth.ErrUnauthenticated
	}
	return s.recordNodeEvent(ctx, slug, req, identity.UserID)
}

// RecordOwnNodeEvent derives the task and actor from one trusted bound run.
func (s *Store) RecordOwnNodeEvent(ctx context.Context, scope storage.MailScope, req storage.RecordOwnNodeEventRequest) (*storage.NodeExecutionEvent, error) {
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" {
		return nil, auth.ErrUnauthenticated
	}
	var result *storage.NodeExecutionEvent
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		run, _, err := s.reviewActorRun(txCtx, scope)
		if err != nil {
			return err
		}
		task, err := s.RunBindingTask(txCtx, run)
		if err != nil {
			return err
		}
		original := storage.RecordNodeEventRequest{EventID: req.EventID, Kind: req.Kind, Reason: req.Reason,
			PreviousRunID: req.PreviousRunID, DeliveryID: req.DeliveryID, GitUndo: req.GitUndo}
		if req.Kind == "retry" {
			if req.ExpectedRunID != "" {
				return storage.ErrInvalidNodeEvent
			}
			if original.PreviousRunID == nil {
				original.PreviousRunID = &run
			}
			original.RunID = &run
		} else if req.Kind == "git_undo" {
			if req.ExpectedRunID != run {
				return storage.ErrRunBindingConflict
			}
			original.RunID = &run
		} else if req.ExpectedRunID != "" {
			return storage.ErrInvalidNodeEvent
		}
		result, err = s.recordNodeEvent(txCtx, task, original, run)
		return err
	})
	return result, err
}

func (s *Store) recordNodeEvent(ctx context.Context, slug string, req storage.RecordNodeEventRequest, actor string) (*storage.NodeExecutionEvent, error) {
	if !subdivisionKeyPattern.MatchString(req.EventID) || strings.TrimSpace(req.Reason) == "" || utf8.RuneCountInString(req.Reason) > 4000 {
		return nil, storage.ErrInvalidNodeEvent
	}
	switch req.Kind {
	case "retry":
		if req.PreviousRunID == nil || req.DeliveryID != nil || req.GitUndo != nil {
			return nil, storage.ErrInvalidNodeEvent
		}
	case "rework":
		if req.DeliveryID == nil || req.PreviousRunID != nil || req.RunID != nil || req.GitUndo != nil {
			return nil, storage.ErrInvalidNodeEvent
		}
	case "git_undo":
		if req.RunID == nil || req.PreviousRunID != nil || req.DeliveryID != nil || req.GitUndo == nil ||
			(req.GitUndo.Operation != "revert" && req.GitUndo.Operation != "reset") ||
			!reviewCommitPattern.MatchString(req.GitUndo.SourceCommit) || !reviewCommitPattern.MatchString(req.GitUndo.ResultCommit) ||
			(req.GitUndo.Operation == "revert" && strings.EqualFold(req.GitUndo.SourceCommit, req.GitUndo.ResultCommit)) {
			return nil, storage.ErrInvalidNodeEvent
		}
	default:
		return nil, storage.ErrInvalidNodeEvent
	}
	for _, reference := range []*string{req.PreviousRunID, req.RunID, req.DeliveryID} {
		if reference != nil && strings.TrimSpace(*reference) == "" {
			return nil, storage.ErrInvalidNodeEvent
		}
	}
	var gitUndo []byte
	if req.GitUndo != nil {
		var err error
		gitUndo, err = json.Marshal(req.GitUndo)
		if err != nil {
			return nil, fmt.Errorf("postgres: encode node Git undo: %w", err)
		}
	}
	var encoded []byte
	err := s.queryRow(ctx, `INSERT INTO node_execution_events(id,project_slug,task_slug,kind,reason,actor,previous_run_id,run_id,delivery_id,git_undo,recorded_at)
 SELECT $3,s.project_slug,s.slug,$4,$5,$6,$7::text,$8::text,$9::text,$10::jsonb,$11 FROM specs s WHERE s.project_slug=$1 AND s.slug=$2
 AND (($4='retry' AND EXISTS(SELECT 1 FROM run_bindings r WHERE r.project_slug=s.project_slug AND r.task_spec_slug=s.slug AND r.id=$7)
   AND ($8::text IS NULL OR EXISTS(SELECT 1 FROM run_bindings r WHERE r.project_slug=s.project_slug AND r.task_spec_slug=s.slug AND r.id=$8)))
 OR ($4='rework' AND EXISTS(SELECT 1 FROM deliveries d JOIN run_bindings r ON r.project_slug=d.project_slug AND r.id=d.run_binding_id
   WHERE d.project_slug=s.project_slug AND r.task_spec_slug=s.slug AND d.id=$9))
 OR ($4='git_undo' AND EXISTS(SELECT 1 FROM run_bindings r WHERE r.project_slug=s.project_slug AND r.task_spec_slug=s.slug AND r.id=$8))) RETURNING `+nodeEventJSON,
		s.project, slug, req.EventID, req.Kind, req.Reason, actor, req.PreviousRunID, req.RunID, req.DeliveryID, gitUndo, s.now()).Scan(&encoded)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, storage.ErrInvalidNodeEvent
	}
	var databaseError *pgconn.PgError
	if errors.As(err, &databaseError) && databaseError.Code == "23505" {
		return nil, storage.ErrNodeEventConflict
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: RecordNodeEvent: %w", err)
	}
	var event storage.NodeExecutionEvent
	if decodeErr := json.Unmarshal(encoded, &event); decodeErr != nil {
		return nil, fmt.Errorf("postgres: RecordNodeEvent: %w", decodeErr)
	}
	return &event, nil
}

// ReadNodeEvents pages attributed retry and rework declarations without inferring execution state.
func (s *Store) ReadNodeEvents(ctx context.Context, slug, cursor string) (*storage.NodeEventPage, error) {
	var before int64
	if cursor != "" {
		var err error
		before, err = strconv.ParseInt(cursor, 10, 64)
		if err != nil || before < 1 || strconv.FormatInt(before, 10) != cursor {
			return nil, storage.ErrInvalidNodeEvent
		}
	}
	var exists bool
	if err := s.queryRow(ctx, `SELECT EXISTS(SELECT 1 FROM specs WHERE project_slug=$1 AND slug=$2)`, s.project, slug).Scan(&exists); err != nil {
		return nil, fmt.Errorf("postgres: ReadNodeEvents: %w", err)
	}
	if !exists {
		return nil, storage.ErrSpecNotFound
	}
	rows, err := s.query(ctx, `SELECT event_order,`+nodeEventJSON+` AS event FROM node_execution_events WHERE project_slug=$1 AND task_slug=$2
 AND ($3::bigint=0 OR event_order<$3) ORDER BY event_order DESC LIMIT 51`, s.project, slug, before)
	if err != nil {
		return nil, err
	}
	type eventRow struct {
		Order int64           `db:"event_order"`
		Event json.RawMessage `db:"event"`
	}
	records, err := pgx.CollectRows(rows, pgx.RowToStructByName[eventRow])
	if err != nil {
		return nil, fmt.Errorf("postgres: ReadNodeEvents: %w", err)
	}
	page := &storage.NodeEventPage{TaskSlug: slug, Events: []storage.NodeExecutionEvent{}, HasMore: len(records) > 50}
	if page.HasMore {
		records = records[:50]
		cursor := strconv.FormatInt(records[49].Order, 10)
		page.NextCursor = &cursor
	}
	for _, record := range records {
		var event storage.NodeExecutionEvent
		if err := json.Unmarshal(record.Event, &event); err != nil {
			return nil, fmt.Errorf("postgres: ReadNodeEvents: %w", err)
		}
		page.Events = append(page.Events, event)
	}
	return page, nil
}

func (s *Store) readNodeEventCounts(ctx context.Context) ([]storage.NodeEventCount, error) {
	rows, err := s.query(ctx, `SELECT task_slug,count(*) FILTER (WHERE kind='retry') AS retries,count(*) FILTER (WHERE kind='rework') AS reworks,
 count(*) FILTER (WHERE kind='git_undo') AS git_undos
 FROM node_execution_events WHERE project_slug = $1 GROUP BY task_slug ORDER BY task_slug`, s.project)
	if err != nil {
		return nil, err
	}
	result, collectErr := pgx.CollectRows(rows, pgx.RowToStructByName[storage.NodeEventCount])
	if collectErr != nil {
		return nil, fmt.Errorf("postgres: readNodeEventCounts: %w", collectErr)
	}
	return result, nil
}
