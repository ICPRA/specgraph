// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
)

type deliveryHookHostRecord struct {
	storage.DeliveryHookPreparation
	target                   deliveryHookTarget
	targetBody               json.RawMessage
	parentState              string
	parentRole               string
	parentPackageEnvironment string
}

// Historical reads and factual receipts use saved identities, not a fabricated MCP scope.
func (s *Store) deliveryHookHostRecord(ctx context.Context, scope storage.DeliveryHookHostScope, id string) (*deliveryHookHostRecord, error) {
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" {
		return nil, storage.ErrPlanningForbidden
	}
	if !validMailText(scope.EnvironmentID, 256) || !validMailText(scope.NativeProjectID, 256) || !validMailText(id, 256) {
		return nil, storage.ErrInvalidDeliveryHook
	}
	record, err := s.deliveryHookRecord(ctx, id)
	if err != nil {
		return nil, err
	}
	if record.Hook.HostConsumerUserID != identity.UserID {
		return nil, storage.ErrPlanningForbidden
	}
	if record.target.EnvironmentID != scope.EnvironmentID || record.target.ProjectID != scope.NativeProjectID {
		return nil, storage.ErrPlanningForbidden
	}
	return record, nil
}

// Callers authorize either management or the recorded host consumer before use.
func (s *Store) deliveryHookRecord(ctx context.Context, id string) (*deliveryHookHostRecord, error) {
	hook, err := s.deliveryHook(ctx, id)
	if err != nil {
		return nil, err
	}
	record := &deliveryHookHostRecord{}
	record.Hook = hook
	record.Context, record.Dispatch, err = s.ReadRunPreparation(ctx, hook.TargetRunID, "")
	if err != nil {
		return nil, err
	}
	if record.Context.PackageID != hook.TargetPackageID {
		return nil, storage.ErrDeliveryHookConflict
	}
	var body struct {
		Target json.RawMessage `json:"dispatch_target"`
	}
	if json.Unmarshal(record.Context.Body, &body) != nil || json.Unmarshal(body.Target, &record.target) != nil {
		return nil, storage.ErrInvalidDeliveryHook
	}
	record.targetBody = body.Target
	if record.target.ThreadID == "" || record.target.StartCommandID == "" || record.target.CreateCommandID == "" || record.target.MessageID == "" || record.target.StartCommandID == record.target.CreateCommandID || record.target.WorkPurpose != "test_execution" || record.target.GitBaseline.CommitSHA == nil || *record.target.GitBaseline.CommitSHA != hook.CommitSHA {
		return nil, storage.ErrInvalidDeliveryHook
	}
	if hook.ConfiguredByRunID == nil {
		if record.target.EnvironmentID == "" || record.target.ProjectID == "" || record.target.RuntimeMode == "" {
			return nil, storage.ErrInvalidDeliveryHook
		}
		return record, nil
	}
	record.Parent = &storage.DeliveryHookParent{RunID: *hook.ConfiguredByRunID}
	err = s.queryRow(ctx, `SELECT b.environment_id,b.thread_ref,b.state,
	 COALESCE(p.body->'dispatch_target'->>'assignmentRole',''),COALESCE(p.body->'dispatch_target'->>'projectId',''),COALESCE(p.body->'dispatch_target'->>'environmentId','')
	 FROM run_bindings b JOIN context_packages p ON p.project_slug=b.project_slug AND p.id=b.package_id AND p.task_spec_slug=b.task_spec_slug
	 WHERE b.project_slug=$1 AND b.id=$2`, s.project, hook.ConfiguredByRunID).Scan(&record.Parent.EnvironmentID, &record.Parent.ThreadID, &record.parentState, &record.parentRole, &record.Parent.ProjectID, &record.parentPackageEnvironment)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, storage.ErrRunContextNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: deliveryHookRecord: %w", err)
	}
	if record.Parent.EnvironmentID != record.target.EnvironmentID || record.Parent.ProjectID != record.target.ProjectID || record.parentPackageEnvironment != record.target.EnvironmentID {
		return nil, storage.ErrPlanningForbidden
	}
	return record, nil
}

// Same live bound-manager predicate as PlanningManagerRun, using the recorded
// configurer run instead of inventing unavailable provider session/instance IDs.
func (s *Store) checkDeliveryHookReady(ctx context.Context, r *deliveryHookHostRecord) error {
	if r.Hook.ConfiguredByRunID == nil {
		// The durable human grant survives signout/key rotation, not account removal.
		user, err := s.ExistingAuth().GetUserByID(ctx, r.Hook.ConfiguredByUserID)
		if errors.Is(err, storage.ErrUserNotFound) {
			return storage.ErrPlanningForbidden
		}
		if err != nil {
			return err
		}
		if user.Kind != storage.KindHuman || user.DeletedAt != nil {
			return storage.ErrPlanningForbidden
		}
	} else if r.parentState != "bound" || r.parentRole != "manager" || r.Parent.ThreadID == "" {
		return storage.ErrPlanningForbidden
	}
	if r.Hook.CancelledAt != nil || r.Hook.DeliveryID == nil || r.Dispatch.Cancellation != nil || r.Dispatch.Resolution != nil {
		return storage.ErrDeliveryHookConflict
	}
	var state string
	if err := s.queryRow(ctx, `SELECT state FROM run_bindings WHERE project_slug=$1 AND id=$2`, s.project, r.Hook.TargetRunID).Scan(&state); err != nil {
		return fmt.Errorf("postgres: checkDeliveryHookReady: %w", err)
	}
	if state != "prepared" && state != "bound" {
		return storage.ErrDeliveryHookConflict
	}
	if _, _, err := s.checkDeliveryHookInputs(ctx, r.Hook.SourceRunID, r.Hook.CommitSHA, &r.Context); err != nil {
		return err
	}
	var source string
	var snapshot []byte
	err := s.queryRow(ctx, `SELECT run_binding_id,snapshot FROM deliveries WHERE project_slug=$1 AND id=$2`, s.project, *r.Hook.DeliveryID).Scan(&source, &snapshot)
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.ErrDeliveryNotFound
	}
	if err != nil {
		return fmt.Errorf("postgres: checkDeliveryHookReady: %w", err)
	}
	commit, err := testDeliveryCommit(snapshot)
	if err != nil || source != r.Hook.SourceRunID || commit != r.Hook.CommitSHA {
		return storage.ErrDeliveryHookConflict
	}
	return nil
}

func hookReadinessError(err error) (*storage.DeliveryHookReadinessError, error) {
	switch {
	case err == nil:
		return nil, nil
	case errors.Is(err, storage.ErrPlanningForbidden), errors.Is(err, storage.ErrReviewForbidden):
		return &storage.DeliveryHookReadinessError{Code: "forbidden", Message: "Original configuring identity is not currently eligible"}, nil
	case errors.Is(err, storage.ErrDeliveryHookConflict), errors.Is(err, storage.ErrRunBindingConflict), errors.Is(err, storage.ErrExecutionDependenciesChanged), errors.Is(err, storage.ErrCompletionRequiresRequirementReview), errors.Is(err, storage.ErrDependenciesNotReady), errors.Is(err, storage.ErrPreparationCancelled), errors.Is(err, storage.ErrDispatchResolved), errors.Is(err, storage.ErrSpecNotApproved), errors.Is(err, storage.ErrReviewHumanHold):
		return &storage.DeliveryHookReadinessError{Code: "conflict", Message: "Original prepared target or its current inputs are not eligible"}, nil
	case errors.Is(err, storage.ErrInvalidDeliveryHook), errors.Is(err, storage.ErrInvalidRunPreparation), errors.Is(err, storage.ErrInvalidReview):
		return &storage.DeliveryHookReadinessError{Code: "invalid_argument", Message: "Original fixed target or QA basis no longer validates"}, nil
	case errors.Is(err, storage.ErrRunContextNotFound), errors.Is(err, storage.ErrDeliveryNotFound), errors.Is(err, storage.ErrRunBindingNotFound), errors.Is(err, storage.ErrSpecNotFound):
		return &storage.DeliveryHookReadinessError{Code: "not_found", Message: "Original hook source or preparation is unavailable"}, nil
	default:
		return nil, err
	}
}

// PrepareDeliveryHookDispatch never accepts a replacement actor, run, package or target.
func (s *Store) PrepareDeliveryHookDispatch(ctx context.Context, scope storage.DeliveryHookHostScope, id, action string) (*storage.DeliveryHookPreparation, error) {
	if action != "read" && action != "bind" && action != "authorize" {
		return nil, storage.ErrInvalidDeliveryHook
	}
	var result *storage.DeliveryHookPreparation
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		r, err := s.deliveryHookHostRecord(txCtx, scope, id)
		if err != nil {
			return err
		}
		readyErr := s.checkDeliveryHookReady(txCtx, r)
		if action == "read" {
			r.ReadinessError, err = hookReadinessError(readyErr)
			if err != nil {
				return err
			}
			result = &r.DeliveryHookPreparation
			return nil
		}
		if readyErr != nil {
			return readyErr
		}
		if r.Hook.State != "pending" {
			return storage.ErrDeliveryHookConflict
		}
		if action == "bind" {
			if err := s.BindRunThreadInEnvironment(txCtx, r.Hook.TargetRunID, r.target.EnvironmentID, r.target.ThreadID); err != nil {
				return err
			}
		} else {
			admission, err := s.AuthorizeRunDispatch(txCtx, r.Hook.TargetRunID, deliveryHookDispatchActor(r.Hook), r.Hook.TargetPackageID, r.targetBody)
			if err != nil {
				return err
			}
			r.Dispatch.Admission = &admission
		}
		result = &r.DeliveryHookPreparation
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ListHostDeliveryTestHooks pages pending hooks owned by the recorded host consumer.
func (s *Store) ListHostDeliveryTestHooks(ctx context.Context, scope storage.DeliveryHookHostScope, limit int, cursor string) (storage.DeliveryTestHookPage, error) {
	page := storage.DeliveryTestHookPage{Hooks: []storage.DeliveryTestHook{}}
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" {
		return page, storage.ErrPlanningForbidden
	}
	if !validMailText(scope.EnvironmentID, 256) || !validMailText(scope.NativeProjectID, 256) || !validMailPage(limit, cursor) {
		return page, storage.ErrInvalidDeliveryHook
	}
	rows, err := s.query(ctx, `SELECT `+deliveryHookJSON+` FROM (SELECT h.* FROM delivery_test_hooks h
	 JOIN context_packages p ON p.project_slug=h.project_slug AND p.id=h.target_package_id
	 WHERE h.hook_kind='delivery_test' AND h.project_slug=$1 AND h.host_consumer_user_id=$2 AND h.delivery_id IS NOT NULL AND h.cancelled_at IS NULL
	 AND (h.dispatch_status IS NULL OR h.dispatch_status='unconfirmed')
	 AND p.body->'dispatch_target'->>'environmentId'=$3 AND p.body->'dispatch_target'->>'projectId'=$4
	 AND ($5='' OR h.id<$5) ORDER BY h.id DESC LIMIT $6) eligible ORDER BY id DESC`, s.project, identity.UserID, scope.EnvironmentID, scope.NativeProjectID, cursor, limit+1)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	for rows.Next() {
		var encoded []byte
		var hook storage.DeliveryTestHook
		if err := rows.Scan(&encoded); err != nil {
			return page, fmt.Errorf("postgres: ListHostDeliveryTestHooks: %w", err)
		}
		if err := json.Unmarshal(encoded, &hook); err != nil {
			return page, fmt.Errorf("postgres: ListHostDeliveryTestHooks: %w", err)
		}
		page.Hooks = append(page.Hooks, hook)
	}
	if err := rows.Err(); err != nil {
		return page, fmt.Errorf("postgres: ListHostDeliveryTestHooks: %w", err)
	}
	if len(page.Hooks) > limit {
		page.NextCursor = page.Hooks[limit-1].ID
		page.Hooks = page.Hooks[:limit]
	}
	return page, nil
}

// RecordDeliveryHookHostResult records a host dispatch observation without asserting execution success.
func (s *Store) RecordDeliveryHookHostResult(ctx context.Context, scope storage.DeliveryHookHostScope, id string, req storage.DeliveryHookHostResult) (*storage.DeliveryTestHook, error) {
	if (req.Status != "commandAccepted" && req.Status != "rejected" && req.Status != "unconfirmed" && req.Status != "blocked") ||
		(req.Phase != nil && *req.Phase != "create" && *req.Phase != "start") || (req.Detail != nil && !validMailText(*req.Detail, 16000)) || (req.Detail != nil && utf8.RuneCountInString(*req.Detail) > 4000) ||
		(req.Status == "commandAccepted" && (req.Phase == nil || *req.Phase != "start")) || (req.Status == "rejected" && req.Phase == nil) || (req.Status != "commandAccepted" && req.Detail == nil) {
		return nil, storage.ErrInvalidDeliveryHook
	}
	var result *storage.DeliveryTestHook
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		r, err := s.deliveryHookHostRecord(txCtx, scope, id)
		if err != nil {
			return err
		}
		if r.Hook.CancelledAt != nil || r.Hook.DeliveryID == nil {
			return storage.ErrDeliveryHookConflict
		}
		if req.Phase != nil && *req.Phase == "start" && (req.Status == "commandAccepted" || req.Status == "rejected") {
			a := r.Dispatch.Admission
			if a == nil || a.RunID != r.Hook.TargetRunID || a.PackageID != r.Hook.TargetPackageID || a.Actor != deliveryHookDispatchActor(r.Hook) {
				return storage.ErrDeliveryHookConflict
			}
		}
		if r.Hook.DispatchStatus != nil {
			var same bool
			if scanErr := s.queryRow(txCtx, `SELECT dispatch_status=$3 AND dispatch_phase IS NOT DISTINCT FROM $4::text AND dispatch_detail IS NOT DISTINCT FROM $5::text FROM delivery_test_hooks WHERE hook_kind='delivery_test' AND project_slug=$1 AND id=$2`, s.project, id, req.Status, req.Phase, req.Detail).Scan(&same); scanErr != nil {
				return fmt.Errorf("postgres: RecordDeliveryHookHostResult: %w", scanErr)
			}
			if same {
				result = r.Hook
				return nil
			}
			if *r.Hook.DispatchStatus != "unconfirmed" || (req.Status != "commandAccepted" && req.Status != "rejected") {
				return storage.ErrDeliveryHookConflict
			}
		}
		if _, execErr := s.exec(txCtx, `UPDATE delivery_test_hooks SET dispatch_status=$3,dispatch_phase=$4,dispatch_detail=$5,dispatch_recorded_at=$6 WHERE hook_kind='delivery_test' AND project_slug=$1 AND id=$2`, s.project, id, req.Status, req.Phase, req.Detail, s.now()); execErr != nil {
			return execErr
		}
		result, err = s.deliveryHook(txCtx, id)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// RetryDeliveryTestHook rechecks the fixed trigger under manager authority.
func (s *Store) RetryDeliveryTestHook(ctx context.Context, scope storage.MailScope, id string) (*storage.DeliveryTestHook, error) {
	return s.retryDeliveryTestHook(ctx, &scope, id)
}

// RetryHumanDeliveryTestHook rechecks the fixed trigger under human authority.
func (s *Store) RetryHumanDeliveryTestHook(ctx context.Context, id string) (*storage.DeliveryTestHook, error) {
	return s.retryDeliveryTestHook(ctx, nil, id)
}

func deliveryHookDispatchActor(hook *storage.DeliveryTestHook) string {
	if hook.ConfiguredByRunID != nil {
		return *hook.ConfiguredByRunID
	}
	return hook.ConfiguredByUserID
}

func (s *Store) retryDeliveryTestHook(ctx context.Context, scope *storage.MailScope, id string) (*storage.DeliveryTestHook, error) {
	if !validMailText(id, 256) {
		return nil, storage.ErrInvalidDeliveryHook
	}
	var result *storage.DeliveryTestHook
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		actor, user, err := s.deliveryHookActor(txCtx, scope)
		if err != nil {
			return err
		}
		hook, err := s.deliveryHook(txCtx, id)
		if err != nil {
			return err
		}
		if hook.State != "blocked" && hook.State != "unconfirmed" {
			return storage.ErrDeliveryHookConflict
		}
		r, err := s.deliveryHookRecord(txCtx, id)
		if err != nil {
			return err
		}
		if actor != nil {
			manager, readRunContextErr := s.ReadRunContext(txCtx, *actor)
			if readRunContextErr != nil {
				return readRunContextErr
			}
			var body struct {
				Target struct {
					ProjectID string `json:"projectId"`
				} `json:"dispatch_target"`
			}
			if json.Unmarshal(manager.Body, &body) != nil {
				return storage.ErrInvalidDeliveryHook
			}
			if r.target.EnvironmentID != scope.EnvironmentID || r.target.ProjectID != body.Target.ProjectID {
				return storage.ErrPlanningForbidden
			}
		}
		if checkDeliveryHookReadyErr := s.checkDeliveryHookReady(txCtx, r); checkDeliveryHookReadyErr != nil {
			return checkDeliveryHookReadyErr
		}
		if _, execErr := s.exec(txCtx, `UPDATE delivery_test_hooks SET dispatch_status=NULL,dispatch_phase=NULL,dispatch_detail=NULL,dispatch_recorded_at=NULL,retried_at=$3,retried_by_run_id=$4,retried_by_user_id=$5 WHERE hook_kind='delivery_test' AND project_slug=$1 AND id=$2`, s.project, id, s.now(), actor, user); execErr != nil {
			return execErr
		}
		result, err = s.deliveryHook(txCtx, id)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ReadOwnDeliveryHookContext returns the original delivery reference for the caller's bound test run.
func (s *Store) ReadOwnDeliveryHookContext(ctx context.Context, scope storage.MailScope) (*storage.DeliveryHookContext, error) {
	var result storage.DeliveryHookContext
	err := s.RunReadSnapshot(ctx, func(txCtx context.Context) error {
		run, _, err := s.reviewActorRun(txCtx, scope)
		if err != nil {
			return err
		}
		var purpose string
		if scanErr := s.queryRow(txCtx, `SELECT p.body->'dispatch_target'->>'workPurpose' FROM run_bindings b JOIN context_packages p ON p.project_slug=b.project_slug AND p.id=b.package_id WHERE b.project_slug=$1 AND b.id=$2`, s.project, run).Scan(&purpose); scanErr != nil {
			return fmt.Errorf("postgres: ReadOwnDeliveryHookContext: %w", scanErr)
		}
		if purpose != "test_execution" {
			return storage.ErrPlanningForbidden
		}
		err = s.queryRow(txCtx, `SELECT id,delivery_id,source_run_id,target_run_id,commit_sha FROM delivery_test_hooks WHERE hook_kind='delivery_test' AND project_slug=$1 AND target_run_id=$2 AND delivery_id IS NOT NULL`, s.project, run).Scan(&result.HookID, &result.DeliveryID, &result.SourceRunID, &result.TargetRunID, &result.CommitSHA)
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrDeliveryHookNotFound
		}
		if err != nil {
			return fmt.Errorf("postgres: ReadOwnDeliveryHookContext: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}
