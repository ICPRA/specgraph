// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
)

// CancelRunPreparation fences a never-admitted VACPMS preparation. It does not
// delete a native conversation or claim that independent host activity stopped.
func (s *Store) CancelRunPreparation(ctx context.Context, runID, packageID, actor, note string, rawHandoff ...*storage.RawPreparationHandoff) error {
	if packageID == "" || strings.TrimSpace(actor) == "" || strings.TrimSpace(note) == "" || utf8.RuneCountInString(note) > 4000 || len(rawHandoff) > 1 {
		return storage.ErrInvalidRunPreparation
	}
	var raw *storage.RawPreparationHandoff
	if len(rawHandoff) == 1 {
		raw = rawHandoff[0]
	}
	if raw != nil && (!validMailText(raw.EnvironmentID, 256) || !validMailText(raw.ThreadID, 256) || !validNodeHandoffSummary(raw.Summary) || raw.Summary.Kind == "program_result") {
		return storage.ErrInvalidRunPreparation
	}
	return s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		slug, err := s.RunBindingTask(txCtx, runID)
		if err != nil {
			return err
		}
		if scanErr := s.queryRow(txCtx, `SELECT slug FROM specs WHERE project_slug=$1 AND slug=$2 FOR UPDATE`, s.project, slug).Scan(&slug); scanErr != nil {
			return fmt.Errorf("postgres: CancelRunPreparation: %w", scanErr)
		}
		var state, storedPackage, executor, environment, thread string
		if scanErr := s.queryRow(txCtx, `SELECT state,package_id,executor_kind,environment_id,thread_ref FROM run_bindings WHERE project_slug=$1 AND id=$2 FOR UPDATE`, s.project, runID).Scan(&state, &storedPackage, &executor, &environment, &thread); scanErr != nil {
			return fmt.Errorf("postgres: CancelRunPreparation: %w", scanErr)
		}
		if storedPackage != packageID {
			return storage.ErrRunBindingConflict
		}
		if executor == "program" {
			identity, ok := auth.IdentityFromContext(txCtx)
			if !ok || identity.UserKind != storage.KindHuman || identity.UserID != actor {
				return storage.ErrProgramRunForbidden
			}
		}
		if raw != nil {
			human, err := s.currentHumanOperator(txCtx)
			if err != nil || human != actor {
				return storage.ErrProgramRunForbidden
			}
		}
		var priorActor, priorNote string
		var priorRaw []byte
		err = s.queryRow(txCtx, `SELECT actor,note,raw_handoff FROM run_preparation_cancellations WHERE project_slug=$1 AND run_id=$2`, s.project, runID).Scan(&priorActor, &priorNote, &priorRaw)
		if err == nil {
			var prior *storage.RawPreparationHandoff
			if len(priorRaw) != 0 {
				prior = &storage.RawPreparationHandoff{}
				if err := json.Unmarshal(priorRaw, prior); err != nil {
					return fmt.Errorf("postgres: decode prior raw handoff: %w", err)
				}
			}
			if priorActor != actor || priorNote != note || !reflect.DeepEqual(prior, raw) {
				return storage.ErrRunBindingConflict
			}
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("postgres: CancelRunPreparation: %w", err)
		}
		if raw != nil {
			return s.handoffRawBoundPreparation(txCtx, slug,
				storage.NodeOwnershipPreparation{RunID: runID, PackageID: packageID, ExecutorKind: executor, State: state, EnvironmentID: raw.EnvironmentID, ThreadID: raw.ThreadID, Raw: true},
				storage.NodeOwnershipPreparedHandoff{RunID: runID, PackageID: packageID, Summary: raw.Summary, StopNote: note}, actor)
		}
		var admitted, owned bool
		if scanErr := s.queryRow(txCtx, `SELECT EXISTS(SELECT 1 FROM run_dispatches WHERE project_slug=$1 AND run_id=$2),
			EXISTS(SELECT 1 FROM run_preparations p JOIN context_packages c ON c.project_slug=p.project_slug AND c.id=$3
			WHERE p.project_slug=$1 AND p.run_id=$2 AND
			(($4='agent' AND c.body->'dispatch_target'->'version'='1'::jsonb
			AND c.body->'dispatch_target'->>'promptFormatVersion'='vacpms-run-v2') OR
			($4='program' AND jsonb_typeof(c.body->'program_target')='object'
			AND c.body->'program_target'->>'authorizedByUserId'=p.actor
			AND c.body->'program_target'->>'cwd'=p.workspace
			AND c.task_spec_slug=p.task_slug AND p.task_slug=$5)))`, s.project, runID, packageID, executor, slug).Scan(&admitted, &owned); scanErr != nil {
			return fmt.Errorf("postgres: CancelRunPreparation: %w", scanErr)
		}
		if admitted {
			return storage.ErrDispatchResponsibilityHeld
		}
		if !owned {
			var raw bool
			if scanErr := s.queryRow(txCtx, `SELECT cp.body->'dispatch_target' IS NULL AND cp.body->'program_target' IS NULL
 AND NOT EXISTS(SELECT 1 FROM run_preparations p WHERE p.project_slug=$1 AND p.run_id=$2 AND p.dispatch_target IS NOT NULL)
 FROM context_packages cp WHERE cp.project_slug=$1 AND cp.id=$3`, s.project, runID, packageID).Scan(&raw); scanErr != nil {
				return fmt.Errorf("postgres: check raw preparation: %w", scanErr)
			}
			owned = raw && executor == "agent" && state == "prepared" && environment == "" && thread == ""
		}
		if !owned || (state != "prepared" && state != "bound") {
			return storage.ErrRunBindingConflict
		}
		return s.cancelPreparationLocked(txCtx, slug, runID, packageID, actor, note, nil)
	})
}

func (s *Store) cancelPreparationLocked(ctx context.Context, slug, runID, packageID, actor, note string, raw *storage.RawPreparationHandoff) error {
	var evidence []byte
	if raw != nil {
		var err error
		evidence, err = json.Marshal(raw)
		if err != nil {
			return err
		}
	}
	if _, err := s.exec(ctx, `INSERT INTO run_preparation_cancellations(run_id,project_slug,package_id,actor,note,cancelled_at,raw_handoff) VALUES($1,$2,$3,$4,$5,$6,$7)`, runID, s.project, packageID, actor, note, s.now(), evidence); err != nil {
		return err
	}
	if _, err := s.exec(ctx, `UPDATE run_bindings SET state='preparation_cancelled',updated_at=$3 WHERE project_slug=$1 AND id=$2`, s.project, runID, s.now()); err != nil {
		return err
	}
	if _, err := s.exec(ctx, `DELETE FROM claims WHERE project_slug=$1 AND spec_slug=$2 AND agent=$3`, s.project, slug, runID); err != nil {
		return err
	}
	_, err := s.exec(ctx, `DELETE FROM edges WHERE project_slug=$1 AND from_slug=$2 AND to_slug=$3 AND edge_type='CLAIMED_BY'`, s.project, slug, runID)
	return err
}
