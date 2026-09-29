// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
)

func (s *Store) completionHookHostRecord(ctx context.Context, scope storage.DeliveryHookHostScope, id string) (*storage.CompletionProgramHook, *programRunRecord, error) {
	if !validMailText(id, 256) || !validMailText(scope.EnvironmentID, 256) || !validMailText(scope.NativeProjectID, 256) {
		return nil, nil, storage.ErrInvalidCompletionHook
	}
	hook, err := s.completionHook(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	r, err := s.programRun(ctx, hook.TargetRunID)
	if err != nil {
		return nil, nil, err
	}
	if err := checkProgramHost(ctx, storage.ProgramHostScope(scope), &r.target); err != nil {
		return nil, nil, err
	}
	if hook.TargetPackageID != r.Context.PackageID || hook.HostConsumerUserID != r.target.HostConsumerUserID {
		return nil, nil, storage.ErrCompletionHookConflict
	}
	return hook, r, nil
}

func (s *Store) checkCompletionHookReady(ctx context.Context, hook *storage.CompletionProgramHook, r *programRunRecord) error {
	if hook.CancelledAt != nil || hook.Fact == nil || hook.TargetPackageID != r.Context.PackageID || hook.HostConsumerUserID != r.target.HostConsumerUserID || r.State != "prepared" || r.Dispatch.Cancellation != nil || r.Dispatch.Admission != nil || r.Result != nil {
		return storage.ErrCompletionHookConflict
	}
	user, err := s.ExistingAuth().GetUserByID(ctx, hook.ConfiguredByUserID)
	if err != nil {
		return err
	}
	if user.Kind != storage.KindHuman || user.DeletedAt != nil {
		return storage.ErrProgramRunForbidden
	}
	if err := s.checkCompletionHookSource(ctx, hook); err != nil {
		return err
	}
	if err := s.checkCompletionHookDeadlock(ctx, hook.SourceTaskSlug, r.Context.TaskSlug); err != nil {
		return err
	}
	if err := s.checkPreparedContract(ctx, r.Context.TaskSlug, r.Context.RunID); err != nil {
		return err
	}
	return s.checkDispatchQABasis(ctx, r.targetBody)
}

func completionHookReadiness(err error) (*storage.DeliveryHookReadinessError, error) {
	switch {
	case errors.Is(err, storage.ErrCompletionHookConflict), errors.Is(err, storage.ErrSummaryConflict), errors.Is(err, storage.ErrSummaryNotAcceptable):
		return &storage.DeliveryHookReadinessError{Code: "conflict", Message: "The fixed completion fact or program preparation is no longer eligible; cancel and explicitly configure a new trigger"}, nil
	case errors.Is(err, storage.ErrInvalidCompletionHook):
		return &storage.DeliveryHookReadinessError{Code: "invalid_argument", Message: "The fixed completion trigger is invalid"}, nil
	case errors.Is(err, storage.ErrProgramRunForbidden), errors.Is(err, storage.ErrUserNotFound):
		return &storage.DeliveryHookReadinessError{Code: "forbidden", Message: "The recorded human program grant is not eligible"}, nil
	case errors.Is(err, storage.ErrSummaryAcceptanceNotFound):
		return &storage.DeliveryHookReadinessError{Code: "not_found", Message: "The original completion fact is unavailable"}, nil
	default:
		return hookReadinessError(err)
	}
}

// ReadHostCompletionHook returns the fixed hook and its current readiness to the recorded host.
func (s *Store) ReadHostCompletionHook(ctx context.Context, scope storage.DeliveryHookHostScope, id string) (*storage.CompletionHookPreparation, error) {
	var result *storage.CompletionHookPreparation
	err := s.RunReadSnapshot(ctx, func(txCtx context.Context) error {
		hook, r, err := s.completionHookHostRecord(txCtx, scope, id)
		if err != nil {
			return err
		}
		readiness, err := completionHookReadiness(s.checkCompletionHookReady(txCtx, hook, r))
		if err != nil {
			return err
		}
		result = &storage.CompletionHookPreparation{Hook: hook, Run: &r.ProgramRun, ReadinessError: readiness}
		return nil
	})
	return result, err
}

// HostAuthorizeCompletionHook admits the original program without rechecking an existing admission.
func (s *Store) HostAuthorizeCompletionHook(ctx context.Context, scope storage.DeliveryHookHostScope, id string) (*storage.ProgramAdmission, error) {
	var result *storage.ProgramAdmission
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		hook, r, err := s.completionHookHostRecord(txCtx, scope, id)
		if err != nil {
			return err
		}
		if hook.CancelledAt != nil {
			return storage.ErrCompletionHookConflict
		}
		if r.Dispatch.Admission != nil && hook.Admission == nil {
			return storage.ErrCompletionHookConflict
		}
		if r.Dispatch.Admission == nil {
			if hook.State != "pending" {
				return storage.ErrCompletionHookConflict
			}
			if checkCompletionHookReadyErr := s.checkCompletionHookReady(txCtx, hook, r); checkCompletionHookReadyErr != nil {
				return checkCompletionHookReadyErr
			}
			// Recover only this existing owner's short lease; never take an absent or replaced claim.
			var owned bool
			if scanErr := s.queryRow(txCtx, `SELECT EXISTS(SELECT 1 FROM claims WHERE project_slug=$1 AND spec_slug=$2 AND agent=$3)`, s.project, r.Context.TaskSlug, r.Context.RunID).Scan(&owned); scanErr != nil {
				return fmt.Errorf("postgres: HostAuthorizeCompletionHook: %w", scanErr)
			}
			if !owned {
				return storage.ErrCompletionHookConflict
			}
			if _, claimSpecErr := s.ClaimSpec(txCtx, r.Context.TaskSlug, r.Context.RunID, defaultLeaseDuration); claimSpecErr != nil {
				return claimSpecErr
			}
		}
		result, err = s.authorizeProgramRun(txCtx, storage.ProgramHostScope(scope), r.Context.RunID, id)
		if err != nil {
			return err
		}
		if hook.ProgramAdmissionID == nil {
			_, err = s.exec(txCtx, `UPDATE delivery_test_hooks SET program_admission_id=$3 WHERE hook_kind='completion_program' AND project_slug=$1 AND id=$2`, s.project, id, result.Run.Dispatch.Admission.ID)
		}
		return err
	})
	return result, err
}

// RecordCompletionHookHostResult records a blocked or unconfirmed observation, not a program result.
func (s *Store) RecordCompletionHookHostResult(ctx context.Context, scope storage.DeliveryHookHostScope, id string, req storage.DeliveryHookHostResult) (*storage.CompletionProgramHook, error) {
	if (req.Status != "blocked" && req.Status != "unconfirmed") || req.Phase != nil || req.Detail == nil || !validMailText(*req.Detail, 4000) {
		return nil, storage.ErrInvalidCompletionHook
	}
	var result *storage.CompletionProgramHook
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		hook, r, err := s.completionHookHostRecord(txCtx, scope, id)
		if err != nil {
			return err
		}
		if hook.CancelledAt != nil || hook.Fact == nil || (r.Dispatch.Admission != nil && (hook.Admission == nil || req.Status != "unconfirmed")) {
			return storage.ErrCompletionHookConflict
		}
		if hook.DispatchStatus != nil {
			if *hook.DispatchStatus != req.Status || hook.DispatchDetail == nil || *hook.DispatchDetail != *req.Detail {
				return storage.ErrCompletionHookConflict
			}
			result = hook
			return nil
		}
		if _, execErr := s.exec(txCtx, `UPDATE delivery_test_hooks SET dispatch_status=$3,dispatch_detail=$4,dispatch_recorded_at=$5 WHERE hook_kind='completion_program' AND project_slug=$1 AND id=$2`, s.project, id, req.Status, req.Detail, s.now()); execErr != nil {
			return execErr
		}
		result, err = s.completionHook(txCtx, id)
		return err
	})
	return result, err
}

// ListHostHooks pages eligible hooks and unfinished loops for their recorded host consumer.
func (s *Store) ListHostHooks(ctx context.Context, scope storage.DeliveryHookHostScope, limit int, cursor string) (storage.HostHookPage, error) {
	page := storage.HostHookPage{Hooks: []storage.HostHook{}}
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" || identity.UserKind != storage.KindServiceAccount || identity.Source != "apikey" {
		return page, storage.ErrProgramRunForbidden
	}
	if !validMailText(scope.EnvironmentID, 256) || !validMailText(scope.NativeProjectID, 256) || !validMailPage(limit, cursor) {
		return page, storage.ErrInvalidCompletionHook
	}
	rows, err := s.query(ctx, `SELECT kind,body FROM (
 SELECT id,hook_kind AS kind,CASE WHEN hook_kind='delivery_test' THEN `+deliveryHookJSON+` ELSE `+completionHookJSON+` END AS body
 FROM (SELECT h.* FROM delivery_test_hooks h JOIN context_packages p ON p.project_slug=h.project_slug AND p.id=h.target_package_id
 WHERE h.project_slug=$1 AND h.host_consumer_user_id=$2 AND h.cancelled_at IS NULL AND ($5='' OR h.id<$5)
 AND ((h.hook_kind='delivery_test' AND h.delivery_id IS NOT NULL AND (h.dispatch_status IS NULL OR h.dispatch_status='unconfirmed')
 AND p.body->'dispatch_target'->>'environmentId'=$3 AND p.body->'dispatch_target'->>'projectId'=$4)
 OR (h.hook_kind='completion_program' AND h.fact_id IS NOT NULL AND h.dispatch_status IS NULL
 AND p.body->'program_target'->>'environmentId'=$3 AND p.body->'program_target'->>'nativeProjectId'=$4
 AND NOT EXISTS(SELECT 1 FROM run_dispatches d WHERE d.project_slug=h.project_slug AND d.run_id=h.target_run_id)))) hook_candidates
 UNION ALL
 SELECT b.id,'program_loop',jsonb_build_object('runId',b.id,'project',b.project_slug)
 FROM run_bindings b JOIN context_packages p ON p.project_slug=b.project_slug AND p.id=b.package_id
 JOIN run_dispatches d ON d.project_slug=b.project_slug AND d.run_id=b.id
 WHERE b.project_slug=$1 AND b.executor_kind='program' AND b.state<>'completed' AND d.released_at IS NULL
 AND p.body->'program_target'->>'hostConsumerUserId'=$2 AND p.body->'program_target'->>'environmentId'=$3
 AND p.body->'program_target'->>'nativeProjectId'=$4 AND p.body->'program_target'->'loop' IS NOT NULL
 AND ($5='' OR b.id<$5) AND NOT EXISTS(SELECT 1 FROM program_loop_events e WHERE e.project_slug=b.project_slug AND e.run_id=b.id AND e.kind='stop')
 ) eligible ORDER BY id DESC LIMIT $6`, s.project, identity.UserID, scope.EnvironmentID, scope.NativeProjectID, cursor, limit+1)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	var last string
	for rows.Next() {
		var hook storage.HostHook
		var body []byte
		if err := rows.Scan(&hook.Kind, &body); err != nil {
			return page, fmt.Errorf("postgres: ListHostHooks: %w", err)
		}
		switch hook.Kind {
		case "delivery_test":
			hook.DeliveryTest = &storage.DeliveryTestHook{}
			if err := json.Unmarshal(body, hook.DeliveryTest); err != nil {
				return page, fmt.Errorf("postgres: ListHostHooks: %w", err)
			}
			last = hook.DeliveryTest.ID
		case "program_loop":
			hook.ProgramLoop = &storage.ProgramLoopReference{}
			if err := json.Unmarshal(body, hook.ProgramLoop); err != nil {
				return page, fmt.Errorf("postgres: ListHostHooks: %w", err)
			}
			last = hook.ProgramLoop.RunID
		default:
			hook.CompletionProgram = &storage.CompletionProgramHook{}
			if err := json.Unmarshal(body, hook.CompletionProgram); err != nil {
				return page, fmt.Errorf("postgres: ListHostHooks: %w", err)
			}
			last = hook.CompletionProgram.ID
		}
		page.Hooks = append(page.Hooks, hook)
		if len(page.Hooks) == limit {
			page.NextCursor = last
		}
	}
	if err := rows.Err(); err != nil {
		return page, fmt.Errorf("postgres: ListHostHooks: %w", err)
	}
	if len(page.Hooks) > limit {
		page.Hooks = page.Hooks[:limit]
	} else {
		page.NextCursor = ""
	}
	return page, nil
}
