// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
)

const deliveryHookJSON = `jsonb_build_object('id',id,'project',project_slug,'sourceRunId',source_run_id,
 'targetRunId',target_run_id,'targetPackageId',target_package_id,'commitSha',commit_sha,
 'configuredByRunId',configured_by_run_id,'configuredByUserId',configured_by_user_id,'hostConsumerUserId',host_consumer_user_id,'createdAt',created_at,
 'deliveryId',delivery_id,'triggeredAt',triggered_at,'cancelledAt',cancelled_at,
 'cancelledByRunId',cancelled_by_run_id,'cancelledByUserId',cancelled_by_user_id,'cancellationReason',cancellation_reason,
 'dispatchStatus',dispatch_status,'dispatchPhase',dispatch_phase,'dispatchDetail',dispatch_detail,'dispatchRecordedAt',dispatch_recorded_at,
 'retriedAt',retried_at,'retriedByRunId',retried_by_run_id,'retriedByUserId',retried_by_user_id,
 'state',CASE WHEN cancelled_at IS NOT NULL THEN 'cancelled' WHEN dispatch_status IS NOT NULL THEN dispatch_status WHEN delivery_id IS NOT NULL THEN 'pending' ELSE 'armed' END)`

// Each public operation calls this inside its transaction, before any row locks.
func (s *Store) deliveryHookActor(ctx context.Context, scope *storage.MailScope) (runID *string, userID string, actorErr error) {
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" {
		return nil, "", storage.ErrPlanningForbidden
	}
	if scope == nil {
		if identity.UserKind != storage.KindHuman {
			return nil, "", storage.ErrPlanningForbidden
		}
		if err := s.lockDependencyState(ctx); err != nil {
			return nil, "", err
		}
		return nil, identity.UserID, nil
	}
	run, err := s.PlanningManagerRun(ctx, *scope)
	return &run, identity.UserID, err
}

func (s *Store) deliveryHook(ctx context.Context, id string) (*storage.DeliveryTestHook, error) {
	var encoded []byte
	err := s.queryRow(ctx, `SELECT `+deliveryHookJSON+` FROM delivery_test_hooks WHERE hook_kind='delivery_test' AND project_slug=$1 AND id=$2`, s.project, id).Scan(&encoded)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, storage.ErrDeliveryHookNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: deliveryHook: %w", err)
	}
	var hook storage.DeliveryTestHook
	if err := json.Unmarshal(encoded, &hook); err != nil {
		return nil, fmt.Errorf("postgres: deliveryHook: %w", err)
	}
	return &hook, nil
}

type deliveryHookTarget struct {
	testReportTarget
	EnvironmentID   string `json:"environmentId"`
	ProjectID       string `json:"projectId"`
	ThreadID        string `json:"threadId"`
	StartCommandID  string `json:"startCommandId"`
	CreateCommandID string `json:"createCommandId"`
	MessageID       string `json:"messageId"`
	RuntimeMode     string `json:"runtimeMode"`
}

// Configuration and host dispatch share the original source/QA/commit checks.
func (s *Store) checkDeliveryHookInputs(ctx context.Context, sourceRun, commit string, prepared *storage.RunContext) (deliveryHookTarget, json.RawMessage, error) {
	var target deliveryHookTarget
	source, err := s.ReadRunContext(ctx, sourceRun)
	if err != nil {
		return target, nil, err
	}
	var sourceBody struct {
		Target testReportTarget `json:"dispatch_target"`
	}
	if json.Unmarshal(source.Body, &sourceBody) != nil || sourceBody.Target.WorkPurpose != "implementation" || sourceBody.Target.QABasis == nil {
		return target, nil, storage.ErrInvalidDeliveryHook
	}
	var body struct {
		Target json.RawMessage `json:"dispatch_target"`
	}
	if json.Unmarshal(prepared.Body, &body) != nil || json.Unmarshal(body.Target, &target) != nil || target.WorkPurpose != "test_execution" || !target.GitBaseline.IsRepo || target.GitBaseline.CommitSHA == nil || *target.GitBaseline.CommitSHA != commit {
		return target, nil, storage.ErrInvalidDeliveryHook
	}
	if err := checkDispatchPurpose(body.Target); err != nil {
		return target, nil, err
	}
	if err := s.checkDispatchQABasis(ctx, body.Target); err != nil {
		return target, nil, err
	}
	for i := range target.QABasis.TestPlanSources {
		if !slices.Contains(sourceBody.Target.QABasis.TestPlanSources, target.QABasis.TestPlanSources[i]) {
			return target, nil, storage.ErrDeliveryHookConflict
		}
	}
	var task string
	if err := s.queryRow(ctx, `SELECT slug FROM specs WHERE project_slug=$1 AND slug=$2 FOR UPDATE`, s.project, prepared.TaskSlug).Scan(&task); err != nil {
		return target, nil, fmt.Errorf("postgres: checkDeliveryHookInputs: %w", err)
	}
	if err := s.checkPreparedContract(ctx, prepared.TaskSlug, prepared.RunID); err != nil {
		return target, nil, err
	}
	return target, body.Target, nil
}

// ArmDeliveryTestHook fixes a delivery trigger under the authenticated manager's authority.
func (s *Store) ArmDeliveryTestHook(ctx context.Context, scope storage.MailScope, req storage.ArmDeliveryHookRequest) (*storage.DeliveryTestHook, error) {
	return s.armDeliveryTestHook(ctx, &scope, req, "")
}

// ArmHumanDeliveryTestHook fixes a delivery trigger for a verified existing API-key consumer, never a request userId.
func (s *Store) ArmHumanDeliveryTestHook(ctx context.Context, req storage.ArmDeliveryHookRequest, consumer *auth.Identity) (*storage.DeliveryTestHook, error) {
	if consumer == nil || consumer.UserID == "" || consumer.UserKind != storage.KindServiceAccount || consumer.Source != "apikey" {
		return nil, storage.ErrPlanningForbidden
	}
	account, err := s.ExistingAuth().GetUserByID(ctx, consumer.UserID)
	if err != nil {
		return nil, err
	}
	if account.Kind != storage.KindServiceAccount || account.DeletedAt != nil {
		return nil, storage.ErrPlanningForbidden
	}
	return s.armDeliveryTestHook(ctx, nil, req, consumer.UserID)
}

func (s *Store) armDeliveryTestHook(ctx context.Context, scope *storage.MailScope, req storage.ArmDeliveryHookRequest, consumerUser string) (*storage.DeliveryTestHook, error) {
	if !validMailText(req.SourceRunID, 256) || !validMailText(req.TargetRunID, 256) || !reviewCommitPattern.MatchString(req.CommitSHA) || !subdivisionKeyPattern.MatchString(req.IdempotencyKey) ||
		(req.DeliveryID != nil && !validMailText(*req.DeliveryID, 256)) || req.SourceRunID == req.TargetRunID {
		return nil, storage.ErrInvalidDeliveryHook
	}
	var result *storage.DeliveryTestHook
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		actor, user, err := s.deliveryHookActor(txCtx, scope)
		if err != nil {
			return err
		}
		if scope != nil {
			consumerUser = user
		}
		var priorID string
		var same bool
		err = s.queryRow(txCtx, `SELECT id,source_run_id=$3 AND target_run_id=$4 AND commit_sha=$5 AND
		 requested_delivery_id IS NOT DISTINCT FROM $6::text AND configured_by_user_id=$7 AND host_consumer_user_id=$9
		 FROM delivery_test_hooks WHERE hook_kind='delivery_test' AND project_slug=$1 AND configured_by_run_id IS NOT DISTINCT FROM $2::text AND idempotency_key=$8
		 AND ($2::text IS NOT NULL OR configured_by_user_id=$7)`,
			s.project, actor, req.SourceRunID, req.TargetRunID, req.CommitSHA, req.DeliveryID, user, req.IdempotencyKey, consumerUser).Scan(&priorID, &same)
		if err == nil {
			if !same {
				return storage.ErrDeliveryHookConflict
			}
			result, err = s.deliveryHook(txCtx, priorID)
			return err
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		var used bool
		if scanErr := s.queryRow(txCtx, `SELECT EXISTS(SELECT 1 FROM delivery_test_hooks WHERE hook_kind='delivery_test' AND project_slug=$1 AND target_run_id=$2)`, s.project, req.TargetRunID).Scan(&used); scanErr != nil {
			return fmt.Errorf("postgres: armDeliveryTestHook: %w", scanErr)
		}
		if used {
			return storage.ErrDeliveryHookConflict
		}
		prepared, dispatch, err := s.ReadRunPreparation(txCtx, req.TargetRunID, "")
		if err != nil {
			return err
		}
		var state string
		if scanErr := s.queryRow(txCtx, `SELECT state FROM run_bindings WHERE project_slug=$1 AND id=$2`, s.project, req.TargetRunID).Scan(&state); scanErr != nil {
			return fmt.Errorf("postgres: armDeliveryTestHook: %w", scanErr)
		}
		if state != "prepared" || dispatch.Admission != nil || dispatch.Cancellation != nil {
			return storage.ErrDeliveryHookConflict
		}
		target, _, err := s.checkDeliveryHookInputs(txCtx, req.SourceRunID, req.CommitSHA, &prepared)
		if err != nil {
			return err
		}
		if actor != nil {
			manager, readRunContextErr := s.ReadRunContext(txCtx, *actor)
			if readRunContextErr != nil {
				return readRunContextErr
			}
			var managerBody struct {
				Target struct {
					EnvironmentID string `json:"environmentId"`
					ProjectID     string `json:"projectId"`
				} `json:"dispatch_target"`
			}
			if json.Unmarshal(manager.Body, &managerBody) != nil || managerBody.Target.ProjectID == "" || managerBody.Target.EnvironmentID != scope.EnvironmentID ||
				target.EnvironmentID != managerBody.Target.EnvironmentID || target.ProjectID != managerBody.Target.ProjectID {
				return storage.ErrPlanningForbidden
			}
		} else if target.EnvironmentID == "" || target.ProjectID == "" || target.RuntimeMode == "" {
			return storage.ErrInvalidDeliveryHook
		}
		if req.DeliveryID != nil {
			var snapshot []byte
			var sourceRun string
			rowErr := s.queryRow(txCtx, `SELECT run_binding_id,snapshot FROM deliveries WHERE project_slug=$1 AND id=$2`, s.project, *req.DeliveryID).Scan(&sourceRun, &snapshot)
			if errors.Is(rowErr, pgx.ErrNoRows) {
				return storage.ErrDeliveryNotFound
			}
			if rowErr != nil {
				return fmt.Errorf("postgres: armDeliveryTestHook: %w", rowErr)
			}
			commit, rowErr := testDeliveryCommit(snapshot)
			if rowErr != nil || commit != req.CommitSHA || sourceRun != req.SourceRunID {
				return storage.ErrDeliveryHookConflict
			}
		}
		id := newID("dhk")
		_, err = s.exec(txCtx, `INSERT INTO delivery_test_hooks(project_slug,id,source_run_id,target_run_id,target_package_id,commit_sha,
		 configured_by_run_id,configured_by_user_id,host_consumer_user_id,idempotency_key,requested_delivery_id,delivery_id,created_at,triggered_at)
		 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$11,$12,CASE WHEN $11::text IS NULL THEN NULL ELSE $12::timestamptz END)`,
			s.project, id, req.SourceRunID, req.TargetRunID, prepared.PackageID, req.CommitSHA, actor, user, consumerUser, req.IdempotencyKey, req.DeliveryID, s.now())
		if err != nil {
			return err
		}
		result, err = s.deliveryHook(txCtx, id)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ReadDeliveryTestHook reads a hook under the authenticated manager's authority.
func (s *Store) ReadDeliveryTestHook(ctx context.Context, scope storage.MailScope, id string) (*storage.DeliveryTestHook, error) {
	if !validMailText(id, 256) {
		return nil, storage.ErrInvalidDeliveryHook
	}
	var result *storage.DeliveryTestHook
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if _, _, err := s.deliveryHookActor(txCtx, &scope); err != nil {
			return err
		}
		var err error
		result, err = s.deliveryHook(txCtx, id)
		return err
	})
	return result, err
}

// ListDeliveryTestHooks pages project hooks under the authenticated manager's authority.
func (s *Store) ListDeliveryTestHooks(ctx context.Context, scope storage.MailScope, limit int, cursor string) (storage.DeliveryTestHookPage, error) {
	page := storage.DeliveryTestHookPage{Hooks: []storage.DeliveryTestHook{}}
	if !validMailPage(limit, cursor) {
		return page, storage.ErrInvalidDeliveryHook
	}
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if _, _, err := s.deliveryHookActor(txCtx, &scope); err != nil {
			return err
		}
		rows, err := s.query(txCtx, `SELECT `+deliveryHookJSON+` FROM delivery_test_hooks WHERE hook_kind='delivery_test' AND project_slug=$1 AND ($2='' OR id<$2) ORDER BY id DESC LIMIT $3`, s.project, cursor, limit+1)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var encoded []byte
			var hook storage.DeliveryTestHook
			if err := rows.Scan(&encoded); err != nil {
				return fmt.Errorf("postgres: ListDeliveryTestHooks: %w", err)
			}
			if err := json.Unmarshal(encoded, &hook); err != nil {
				return fmt.Errorf("postgres: ListDeliveryTestHooks: %w", err)
			}
			page.Hooks = append(page.Hooks, hook)
		}
		return rows.Err()
	})
	if len(page.Hooks) > limit {
		page.NextCursor = page.Hooks[limit-1].ID
		page.Hooks = page.Hooks[:limit]
	}
	return page, err
}

// CancelDeliveryTestHook cancels an unadmitted hook under manager authority.
func (s *Store) CancelDeliveryTestHook(ctx context.Context, scope storage.MailScope, id, reason string) (*storage.DeliveryTestHook, error) {
	return s.cancelDeliveryTestHook(ctx, &scope, id, reason)
}

// CancelHumanDeliveryTestHook cancels an unadmitted hook under human authority.
func (s *Store) CancelHumanDeliveryTestHook(ctx context.Context, id, reason string) (*storage.DeliveryTestHook, error) {
	return s.cancelDeliveryTestHook(ctx, nil, id, reason)
}

func (s *Store) cancelDeliveryTestHook(ctx context.Context, scope *storage.MailScope, id, reason string) (*storage.DeliveryTestHook, error) {
	if !validMailText(id, 256) || strings.TrimSpace(reason) == "" || utf8.RuneCountInString(reason) > 4000 {
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
		if hook.CancelledAt != nil {
			sameRun := hook.CancelledByRunID == nil && actor == nil
			if hook.CancelledByRunID != nil && actor != nil {
				sameRun = *hook.CancelledByRunID == *actor
			}
			if !sameRun || *hook.CancelledByUserID != user || *hook.CancellationReason != reason {
				return storage.ErrDeliveryHookConflict
			}
			result = hook
			return nil
		}
		dispatch, err := s.readRunDispatch(txCtx, hook.TargetRunID)
		if err != nil {
			return err
		}
		var state string
		if scanErr := s.queryRow(txCtx, `SELECT state FROM run_bindings WHERE project_slug=$1 AND id=$2`, s.project, hook.TargetRunID).Scan(&state); scanErr != nil {
			return fmt.Errorf("postgres: cancelDeliveryTestHook: %w", scanErr)
		}
		if dispatch.Admission != nil || dispatch.Resolution != nil || (state != "prepared" && state != "bound") {
			return storage.ErrDeliveryHookConflict
		}
		if _, execErr := s.exec(txCtx, `UPDATE delivery_test_hooks SET cancelled_at=$3,cancelled_by_run_id=$4,cancelled_by_user_id=$5,cancellation_reason=$6 WHERE hook_kind='delivery_test' AND project_slug=$1 AND id=$2`, s.project, id, s.now(), actor, user, reason); execErr != nil {
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
