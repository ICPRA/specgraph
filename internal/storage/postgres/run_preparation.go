// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/storage"
)

// ReadRunPreparation recovers the original package, including its immutable target.
// PM callers hold the planning transaction/project lock and select exactly one
// of run ID and original request key. This is not a global history snapshot.
func (s *Store) ReadRunPreparation(ctx context.Context, runID, key string) (storage.RunContext, storage.RunDispatchStatus, error) {
	var prepared storage.RunContext
	var dispatch storage.RunDispatchStatus
	if _, ok := txFromContext(ctx); !ok {
		return prepared, dispatch, errors.New("postgres: preparation management read requires the planning transaction")
	}
	if (runID == "") == (key == "") || (key != "" && !subdivisionKeyPattern.MatchString(key)) {
		return prepared, dispatch, storage.ErrInvalidRunPreparation
	}
	var original string
	err := s.queryRow(ctx, `SELECT run_id FROM run_preparations WHERE project_slug=$1 AND (($2<>'' AND run_id=$2) OR ($3<>'' AND idempotency_key=$3))`, s.project, runID, key).Scan(&original)
	if errors.Is(err, pgx.ErrNoRows) {
		return prepared, dispatch, storage.ErrRunContextNotFound
	}
	if err != nil {
		return prepared, dispatch, fmt.Errorf("postgres: ReadRunPreparation: %w", err)
	}
	prepared, err = s.ReadRunContext(ctx, original)
	if err != nil {
		return prepared, dispatch, err
	}
	dispatch, err = s.readRunDispatch(ctx, original)
	return prepared, dispatch, err
}

// PrepareRunForOperator binds an authenticated request to the original result.
// A retry never renews the old lease or creates a replacement execution.
func (s *Store) PrepareRunForOperator(ctx context.Context, taskSlug, workspace, actor, key string, target json.RawMessage) (runID string, replayed bool, err error) {
	if strings.TrimSpace(actor) == "" || !subdivisionKeyPattern.MatchString(key) || strings.TrimSpace(workspace) == "" || len(workspace) > 4096 {
		return "", false, storage.ErrInvalidRunPreparation
	}
	if target != nil {
		if checkDispatchPurposeErr := checkDispatchPurpose(target); checkDispatchPurposeErr != nil {
			return "", false, checkDispatchPurposeErr
		}
	}
	err = s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if lockErr := s.lockDependencyState(txCtx); lockErr != nil {
			return lockErr
		}
		var priorActor, priorTask, priorWorkspace string
		var sameTarget bool
		rowErr := s.queryRow(txCtx, `SELECT actor,task_slug,workspace,run_id,dispatch_target IS NOT DISTINCT FROM $3::jsonb FROM run_preparations WHERE project_slug=$1 AND idempotency_key=$2`, s.project, key, target).
			Scan(&priorActor, &priorTask, &priorWorkspace, &runID, &sameTarget)
		if rowErr == nil {
			var kind string
			if scanErr := s.queryRow(txCtx, `SELECT executor_kind FROM run_bindings WHERE project_slug=$1 AND id=$2`, s.project, runID).Scan(&kind); scanErr != nil {
				return fmt.Errorf("postgres: PrepareRunForOperator: %w", scanErr)
			}
			if kind != "agent" {
				return storage.ErrRunBindingConflict
			}
			if priorActor != actor || priorTask != taskSlug || priorWorkspace != workspace || !sameTarget {
				return storage.ErrRunBindingConflict
			}
			if target != nil {
				if checkDispatchQABasisErr := s.checkDispatchQABasis(txCtx, target); checkDispatchQABasisErr != nil {
					return checkDispatchQABasisErr
				}
			}
			replayed = true
			return nil
		}
		if !errors.Is(rowErr, pgx.ErrNoRows) {
			return fmt.Errorf("postgres: run preparation replay: %w", rowErr)
		}
		var cancelled bool
		if scanErr := s.queryRow(txCtx, `SELECT EXISTS(SELECT 1 FROM preparation_aborts WHERE project_slug=$1 AND idempotency_key=$2)`, s.project, key).Scan(&cancelled); scanErr != nil {
			return fmt.Errorf("postgres: PrepareRunForOperator: %w", scanErr)
		}
		if cancelled {
			return storage.ErrPreparationCancelled
		}
		if target != nil {
			if checkDispatchQABasisErr := s.checkDispatchQABasis(txCtx, target); checkDispatchQABasisErr != nil {
				return checkDispatchQABasisErr
			}
		}
		runID, rowErr = s.PrepareRun(txCtx, taskSlug, workspace)
		if rowErr != nil {
			return rowErr
		}
		if target != nil {
			// Complete the unpublished package in the same preparation transaction.
			// Native host contracts validate the target before executing it.
			if _, rowErr = s.exec(txCtx, `UPDATE context_packages cp SET body=body || jsonb_build_object('dispatch_target',$3::jsonb)
				FROM run_bindings rb WHERE rb.project_slug=$1 AND rb.id=$2 AND cp.project_slug=rb.project_slug AND cp.id=rb.package_id`, s.project, runID, target); rowErr != nil {
				return rowErr
			}
		}
		_, rowErr = s.exec(txCtx, `INSERT INTO run_preparations(project_slug,idempotency_key,actor,task_slug,workspace,run_id,created_at,dispatch_target) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, s.project, key, actor, taskSlug, workspace, runID, s.now(), target)
		if rowErr != nil {
			return fmt.Errorf("postgres: run preparation receipt: %w", rowErr)
		}
		return nil
	})
	if err != nil {
		return "", false, err
	}
	return runID, replayed, nil
}

// The role names who is responsible; purpose identifies the work being assigned.
// Missing historical purpose stays unknown, never inferred from a role or prompt.
func checkDispatchPurpose(target json.RawMessage) error {
	var object map[string]json.RawMessage
	if len(target) > 24<<10 || json.Unmarshal(target, &object) != nil || object == nil {
		return storage.ErrInvalidRunPreparation
	}
	purposeBody, hasPurpose := object["workPurpose"]
	guidanceBody, hasGuidance := object["purposeGuidance"]
	var purpose, guidance string
	if !hasPurpose || !hasGuidance || json.Unmarshal(purposeBody, &purpose) != nil || json.Unmarshal(guidanceBody, &guidance) != nil || strings.TrimSpace(guidance) == "" || utf8.RuneCountInString(guidance) > 4000 {
		return storage.ErrInvalidRunPreparation
	}
	return checkRunWorkPurpose(purpose)
}

// Work purpose is shared by agents and programs; only agents need prompt guidance.
func checkRunWorkPurpose(purpose string) error {
	switch purpose {
	case "requirements", "requirements_review", "design", "design_review", "test_design", "implementation", "test_execution", "coordination", "knowledge", "investigation":
		return nil
	default:
		return storage.ErrInvalidRunPreparation
	}
}

// Checks source identity and recorded authority, not semantic coverage or test success.
func (s *Store) checkDispatchQABasis(ctx context.Context, target json.RawMessage) error {
	var fields struct {
		WorkPurpose string          `json:"workPurpose"`
		QABasis     json.RawMessage `json:"qaBasis"`
	}
	if err := json.Unmarshal(target, &fields); err != nil {
		return storage.ErrInvalidRunPreparation
	}
	var basis storage.DispatchQABasis
	if len(fields.QABasis) > 0 {
		decoder := json.NewDecoder(bytes.NewReader(fields.QABasis))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&basis) != nil || basis.RequirementDecisionIDs == nil || basis.DesignDecisionIDs == nil || basis.DesignSources == nil || basis.TestPlanSources == nil || utf8.RuneCountInString(basis.Applicability) > 4000 {
			return fmt.Errorf("qaBasis requires four reference arrays and applicability of at most 4000 characters: %w", storage.ErrInvalidRunPreparation)
		}
	}
	switch fields.WorkPurpose {
	case "design":
		if len(basis.RequirementDecisionIDs) == 0 {
			return fmt.Errorf("qaBasis for design requires an effective requirements approval: %w", storage.ErrInvalidRunPreparation)
		}
	case "test_design":
		if len(basis.RequirementDecisionIDs) == 0 || len(basis.DesignSources) == 0 {
			return fmt.Errorf("qaBasis for test design requires requirements approval and fixed design sources: %w", storage.ErrInvalidRunPreparation)
		}
	case "implementation":
		if len(basis.RequirementDecisionIDs) == 0 || len(basis.DesignDecisionIDs) == 0 || len(basis.TestPlanSources) == 0 {
			return fmt.Errorf("qaBasis for implementation requires requirements approval, design approval, and fixed test plan sources: %w", storage.ErrInvalidRunPreparation)
		}
	case "test_execution":
		if len(basis.TestPlanSources) == 0 {
			return fmt.Errorf("qaBasis for test execution requires fixed test plan sources: %w", storage.ErrInvalidRunPreparation)
		}
	}
	switch fields.WorkPurpose {
	case "design", "test_design", "implementation", "test_execution":
		if strings.TrimSpace(basis.Applicability) == "" {
			return fmt.Errorf("qaBasis requires an explicit applicability explanation for this work purpose: %w", storage.ErrInvalidRunPreparation)
		}
	}
	requirements := make(map[string]bool, len(basis.RequirementDecisionIDs))
	for _, id := range basis.RequirementDecisionIDs {
		if _, err := s.readEffectiveReviewDecision(ctx, id, "requirements"); err != nil {
			return fmt.Errorf("qaBasis must reference effective requirements approvals in this project; check for an explicit later rejection: %w", err)
		}
		requirements[id] = true
	}
	for _, id := range basis.DesignDecisionIDs {
		required, err := s.readEffectiveReviewDecision(ctx, id, "design")
		if err != nil {
			return fmt.Errorf("qaBasis must reference effective design approvals in this project; check for an explicit later rejection: %w", err)
		}
		if fields.WorkPurpose == "implementation" {
			for _, requirement := range required {
				if !requirements[requirement] {
					return fmt.Errorf("qaBasis requirements must include every requirements approval used by the selected design: %w", storage.ErrInvalidRunPreparation)
				}
			}
		}
	}
	for i := range basis.DesignSources {
		if err := s.validateReviewSource(ctx, &basis.DesignSources[i]); err != nil {
			return fmt.Errorf("qaBasis design source must identify an existing field change or registered fixed Git source in this project: %w", err)
		}
	}
	for i := range basis.TestPlanSources {
		if err := s.validateReviewSource(ctx, &basis.TestPlanSources[i]); err != nil {
			return fmt.Errorf("qaBasis test plan source must identify an existing field change or registered fixed Git source in this project: %w", err)
		}
	}
	return nil
}
