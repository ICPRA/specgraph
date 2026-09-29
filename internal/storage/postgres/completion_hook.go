// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
)

const completionHookJSON = `jsonb_build_object('id',id,'project',project_slug,'sourceTaskSlug',source_task_slug,'sourceSpecId',source_spec_id,'sourceRole',source_role,
 'targetRunId',target_run_id,'targetPackageId',target_package_id,'idempotencyKey',idempotency_key,
 'requestedFact',CASE WHEN requested_fact_id IS NULL THEN NULL ELSE jsonb_build_object('kind',requested_fact_kind,'id',requested_fact_id) END,
 'configuredByUserId',configured_by_user_id,'hostConsumerUserId',host_consumer_user_id,'createdAt',created_at,
 'fact',CASE WHEN fact_id IS NULL THEN NULL ELSE jsonb_build_object('kind',fact_kind,'id',fact_id) END,'triggeredAt',triggered_at,
 'cancelledAt',cancelled_at,'cancelledByUserId',cancelled_by_user_id,'cancellationReason',cancellation_reason,
 'dispatchStatus',dispatch_status,'dispatchDetail',dispatch_detail,'dispatchRecordedAt',dispatch_recorded_at,'retriedAt',retried_at,'retriedByUserId',retried_by_user_id,'programAdmissionId',program_admission_id,
 'state',CASE WHEN cancelled_at IS NOT NULL THEN 'cancelled' WHEN dispatch_status IS NOT NULL THEN dispatch_status WHEN fact_id IS NOT NULL THEN 'pending' ELSE 'armed' END)`

// WorkbenchCompletionHook discovers original hooks without their execution context or actor text.
type WorkbenchCompletionHook struct {
	ID                 string                     `json:"id"`
	SourceTaskSlug     string                     `json:"sourceTaskSlug"`
	SourceSpecID       string                     `json:"sourceSpecId"`
	TargetRunID        string                     `json:"targetRunId"`
	TargetPackageID    string                     `json:"targetPackageId"`
	IdempotencyKey     string                     `json:"idempotencyKey"`
	RequestedFact      *storage.CompletionFactRef `json:"requestedFact"`
	TargetTaskSlug     string                     `json:"targetTaskSlug"`
	CreatedAt          time.Time                  `json:"createdAt"`
	TriggeredAt        *time.Time                 `json:"triggeredAt"`
	CancelledAt        *time.Time                 `json:"cancelledAt"`
	Fact               *storage.CompletionFactRef `json:"fact"`
	State              string                     `json:"state"`
	DispatchStatus     *string                    `json:"dispatchStatus"`
	ProgramAdmissionID *string                    `json:"programAdmissionId"`
}

// ListWorkbenchCompletionHooks includes cancelled and ineligible history for this source node.
func (s *Store) ListWorkbenchCompletionHooks(ctx context.Context, slug string) ([]WorkbenchCompletionHook, error) {
	rows, err := s.query(ctx, `SELECT h.id,target.task_spec_slug FROM delivery_test_hooks h
 JOIN run_bindings target ON target.project_slug=h.project_slug AND target.id=h.target_run_id
 WHERE h.hook_kind='completion_program' AND h.project_slug=$1 AND h.source_task_slug=$2
 ORDER BY h.created_at,h.id`, s.project, slug)
	if err != nil {
		return nil, err
	}
	type hookTarget struct{ ID, TaskSlug string }
	targets, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (hookTarget, error) {
		var target hookTarget
		scanErr := row.Scan(&target.ID, &target.TaskSlug)
		if scanErr != nil {
			return target, fmt.Errorf("postgres: ListWorkbenchCompletionHooks: %w", scanErr)
		}
		return target, nil
	})
	if err != nil {
		return nil, fmt.Errorf("postgres: ListWorkbenchCompletionHooks: %w", err)
	}
	result := make([]WorkbenchCompletionHook, 0, len(targets))
	// ponytail: N authoritative reads per source; batch at this owner if large hook histories become a bottleneck.
	for _, target := range targets {
		hook, err := s.completionHook(ctx, target.ID)
		if err != nil {
			return nil, err
		}
		result = append(result, WorkbenchCompletionHook{
			ID: hook.ID, SourceTaskSlug: hook.SourceTaskSlug, SourceSpecID: hook.SourceSpecID,
			TargetRunID: hook.TargetRunID, TargetPackageID: hook.TargetPackageID, TargetTaskSlug: target.TaskSlug,
			IdempotencyKey: hook.IdempotencyKey, RequestedFact: hook.RequestedFact,
			CreatedAt: hook.CreatedAt, TriggeredAt: hook.TriggeredAt, CancelledAt: hook.CancelledAt,
			Fact: hook.Fact, State: hook.State, DispatchStatus: hook.DispatchStatus, ProgramAdmissionID: hook.ProgramAdmissionID,
		})
	}
	return result, nil
}

func (s *Store) completionHook(ctx context.Context, id string) (*storage.CompletionProgramHook, error) {
	var body []byte
	err := s.queryRow(ctx, `SELECT `+completionHookJSON+` FROM delivery_test_hooks WHERE hook_kind='completion_program' AND project_slug=$1 AND id=$2`, s.project, id).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, storage.ErrCompletionHookNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: completionHook: %w", err)
	}
	var hook storage.CompletionProgramHook
	if decodeErr := json.Unmarshal(body, &hook); decodeErr != nil {
		return nil, fmt.Errorf("postgres: completionHook: %w", decodeErr)
	}
	dispatch, err := s.readRunDispatch(ctx, hook.TargetRunID)
	if err != nil {
		return nil, err
	}
	if hook.ProgramAdmissionID != nil {
		if dispatch.Admission == nil || dispatch.Admission.ID != *hook.ProgramAdmissionID || dispatch.Admission.RunID != hook.TargetRunID || dispatch.Admission.PackageID != hook.TargetPackageID {
			return nil, storage.ErrCompletionHookConflict
		}
		hook.Admission = dispatch.Admission
	}
	if hook.Admission != nil && hook.CancelledAt == nil {
		hook.State = "admitted"
	}
	return &hook, nil
}

// ArmHumanCompletionProgramHook records a human grant for the original completion fact and prepared program.
func (s *Store) ArmHumanCompletionProgramHook(ctx context.Context, req storage.ArmCompletionHookRequest, consumer *auth.Identity) (*storage.CompletionProgramHook, error) {
	if !req.ConfirmTrigger || !validMailText(req.SourceTaskSlug, 256) || !validMailText(req.SourceSpecID, 256) || !validMailText(req.TargetRunID, 256) || !validMailText(req.TargetPackageID, 256) || !subdivisionKeyPattern.MatchString(req.IdempotencyKey) {
		return nil, storage.ErrInvalidCompletionHook
	}
	if req.Fact != nil && (!validMailText(req.Fact.ID, 256) || (req.Fact.Kind != "execution" && req.Fact.Kind != "manual" && req.Fact.Kind != "summary")) {
		return nil, storage.ErrInvalidCompletionHook
	}
	if consumer == nil || consumer.Source != "apikey" || consumer.UserKind != storage.KindServiceAccount || consumer.UserID == "" {
		return nil, storage.ErrProgramRunForbidden
	}
	var result *storage.CompletionProgramHook
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		_, user, err := s.deliveryHookActor(txCtx, nil)
		if err != nil {
			return err
		}
		for _, actor := range []struct {
			id   string
			kind storage.Kind
		}{{user, storage.KindHuman}, {consumer.UserID, storage.KindServiceAccount}} {
			account, getUserByIDErr := s.ExistingAuth().GetUserByID(txCtx, actor.id)
			if getUserByIDErr != nil {
				return getUserByIDErr
			}
			if account.Kind != actor.kind || account.DeletedAt != nil {
				return storage.ErrProgramRunForbidden
			}
		}
		var factKind, factID *string
		if req.Fact != nil {
			factKind, factID = &req.Fact.Kind, &req.Fact.ID
		}
		var prior string
		var same bool
		err = s.queryRow(txCtx, `SELECT id,hook_kind='completion_program' AND source_task_slug=$4 AND source_spec_id=$5 AND target_run_id=$6 AND target_package_id=$7
 AND requested_fact_kind IS NOT DISTINCT FROM $8::text AND requested_fact_id IS NOT DISTINCT FROM $9::text AND host_consumer_user_id=$10
 FROM delivery_test_hooks WHERE project_slug=$1 AND configured_by_user_id=$2 AND configured_by_run_id IS NULL AND idempotency_key=$3`, s.project, user, req.IdempotencyKey, req.SourceTaskSlug, req.SourceSpecID, req.TargetRunID, req.TargetPackageID, factKind, factID, consumer.UserID).Scan(&prior, &same)
		if err == nil {
			if !same {
				return storage.ErrCompletionHookConflict
			}
			result, err = s.completionHook(txCtx, prior)
			return err
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		r, err := s.programRun(txCtx, req.TargetRunID)
		if err != nil {
			return err
		}
		if r.Context.PackageID != req.TargetPackageID || r.target.HostConsumerUserID != consumer.UserID || r.State != "prepared" || r.Dispatch.Admission != nil || r.Dispatch.Cancellation != nil || r.Result != nil {
			return storage.ErrCompletionHookConflict
		}
		var sourceID, role string
		if scanErr := s.queryRow(txCtx, `SELECT id,role FROM specs WHERE project_slug=$1 AND slug=$2`, s.project, req.SourceTaskSlug).Scan(&sourceID, &role); scanErr != nil {
			return fmt.Errorf("postgres: ArmHumanCompletionProgramHook: %w", scanErr)
		}
		if sourceID != req.SourceSpecID || (role != "work" && role != "summary") {
			return storage.ErrCompletionHookConflict
		}
		var used bool
		if scanErr := s.queryRow(txCtx, `SELECT EXISTS(SELECT 1 FROM delivery_test_hooks WHERE project_slug=$1 AND target_run_id=$2 AND (hook_kind='delivery_test' OR cancelled_at IS NULL))`, s.project, req.TargetRunID).Scan(&used); scanErr != nil {
			return fmt.Errorf("postgres: ArmHumanCompletionProgramHook: %w", scanErr)
		}
		if used {
			return storage.ErrCompletionHookConflict
		}
		if checkPreparedContractErr := s.checkPreparedContract(txCtx, r.Context.TaskSlug, r.Context.RunID); checkPreparedContractErr != nil {
			return checkPreparedContractErr
		}
		if checkDispatchQABasisErr := s.checkDispatchQABasis(txCtx, r.targetBody); checkDispatchQABasisErr != nil {
			return checkDispatchQABasisErr
		}
		if checkCompletionHookDeadlockErr := s.checkCompletionHookDeadlock(txCtx, req.SourceTaskSlug, r.Context.TaskSlug); checkCompletionHookDeadlockErr != nil {
			return checkCompletionHookDeadlockErr
		}
		if req.Fact != nil {
			if checkCompletionHookSourceErr := s.checkCompletionHookSource(txCtx, &storage.CompletionProgramHook{SourceTaskSlug: req.SourceTaskSlug, SourceSpecID: sourceID, SourceRole: role, Fact: req.Fact}); checkCompletionHookSourceErr != nil {
				return checkCompletionHookSourceErr
			}
		}
		id := newID("dhk")
		_, err = s.exec(txCtx, `INSERT INTO delivery_test_hooks(project_slug,id,hook_kind,source_task_slug,source_spec_id,source_role,target_run_id,target_package_id,configured_by_user_id,host_consumer_user_id,idempotency_key,requested_fact_kind,requested_fact_id,fact_kind,fact_id,created_at,triggered_at)
 VALUES($1,$2,'completion_program',$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$11,$12,$13,CASE WHEN $12::text IS NULL THEN NULL ELSE $13::timestamptz END)`, s.project, id, req.SourceTaskSlug, sourceID, role, req.TargetRunID, req.TargetPackageID, user, consumer.UserID, req.IdempotencyKey, factKind, factID, s.now())
		if err != nil {
			return err
		}
		result, err = s.completionHook(txCtx, id)
		return err
	})
	return result, err
}

// ReadHumanCompletionProgramHook reads the recorded hook for an authenticated human.
func (s *Store) ReadHumanCompletionProgramHook(ctx context.Context, id string) (*storage.CompletionProgramHook, error) {
	if !validMailText(id, 256) {
		return nil, storage.ErrInvalidCompletionHook
	}
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" || identity.UserKind != storage.KindHuman {
		return nil, storage.ErrPlanningForbidden
	}
	var result *storage.CompletionProgramHook
	err := s.RunReadSnapshot(ctx, func(txCtx context.Context) error {
		var err error
		result, err = s.completionHook(txCtx, id)
		return err
	})
	return result, err
}

// CancelHumanCompletionProgramHook cancels a hook only before its program admission.
func (s *Store) CancelHumanCompletionProgramHook(ctx context.Context, id, reason string) (*storage.CompletionProgramHook, error) {
	if !validMailText(id, 256) || !validMailText(reason, 4000) {
		return nil, storage.ErrInvalidCompletionHook
	}
	var result *storage.CompletionProgramHook
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		_, user, err := s.deliveryHookActor(txCtx, nil)
		if err != nil {
			return err
		}
		hook, err := s.completionHook(txCtx, id)
		if err != nil {
			return err
		}
		if hook.CancelledAt != nil {
			if *hook.CancelledByUserID != user || *hook.CancellationReason != reason {
				return storage.ErrCompletionHookConflict
			}
			result = hook
			return nil
		}
		dispatch, err := s.readRunDispatch(txCtx, hook.TargetRunID)
		if err != nil {
			return err
		}
		if dispatch.Admission != nil {
			return storage.ErrCompletionHookConflict
		}
		if _, execErr := s.exec(txCtx, `UPDATE delivery_test_hooks SET cancelled_at=$3,cancelled_by_user_id=$4,cancellation_reason=$5 WHERE hook_kind='completion_program' AND project_slug=$1 AND id=$2`, s.project, id, s.now(), user, reason); execErr != nil {
			return execErr
		}
		result, err = s.completionHook(txCtx, id)
		return err
	})
	return result, err
}

// RetryHumanCompletionProgramHook rechecks the fixed trigger without replacing its fact or preparation.
func (s *Store) RetryHumanCompletionProgramHook(ctx context.Context, id string) (*storage.CompletionProgramHook, error) {
	if !validMailText(id, 256) {
		return nil, storage.ErrInvalidCompletionHook
	}
	var result *storage.CompletionProgramHook
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		_, user, err := s.deliveryHookActor(txCtx, nil)
		if err != nil {
			return err
		}
		hook, err := s.completionHook(txCtx, id)
		if err != nil {
			return err
		}
		if hook.Admission != nil || (hook.State != "blocked" && hook.State != "unconfirmed") {
			return storage.ErrCompletionHookConflict
		}
		r, err := s.programRun(txCtx, hook.TargetRunID)
		if err != nil {
			return err
		}
		if checkCompletionHookReadyErr := s.checkCompletionHookReady(txCtx, hook, r); checkCompletionHookReadyErr != nil {
			return checkCompletionHookReadyErr
		}
		if _, execErr := s.exec(txCtx, `UPDATE delivery_test_hooks SET dispatch_status=NULL,dispatch_phase=NULL,dispatch_detail=NULL,dispatch_recorded_at=NULL,retried_at=$3,retried_by_user_id=$4 WHERE hook_kind='completion_program' AND project_slug=$1 AND id=$2`, s.project, id, s.now(), user); execErr != nil {
			return execErr
		}
		result, err = s.completionHook(txCtx, id)
		return err
	})
	return result, err
}
