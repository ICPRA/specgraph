// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
)

const nodeOwnershipColumns = `id,task_slug,idempotency_key,action,status,before_owner_user_id,after_owner_user_id,
 from_version,to_version,actor_user_id,reason,dispatches,preparations,handoffs,prepared_handoffs,frozen_claim,claim_handoff,created_at,finished_at,cancel_actor_user_id,cancel_reason`

func scanNodeOwnershipOperation(row pgx.Row) (storage.NodeOwnershipOperation, error) {
	var op storage.NodeOwnershipOperation
	var dispatches, preparations, handoffs, preparedHandoffs, frozenClaim, claimHandoff []byte
	err := row.Scan(&op.ID, &op.TaskSlug, &op.IdempotencyKey, &op.Action, &op.Status, &op.BeforeOwnerUserID,
		&op.AfterOwnerUserID, &op.FromVersion, &op.ToVersion, &op.ActorUserID, &op.Reason,
		&dispatches, &preparations, &handoffs, &preparedHandoffs, &frozenClaim, &claimHandoff, &op.CreatedAt, &op.FinishedAt, &op.CancelActorUserID, &op.CancelReason)
	if err != nil {
		return op, err
	}
	if err := json.Unmarshal(dispatches, &op.Dispatches); err != nil {
		return op, fmt.Errorf("postgres: decode frozen ownership dispatches: %w", err)
	}
	if err := json.Unmarshal(preparations, &op.Preparations); err != nil {
		return op, fmt.Errorf("postgres: decode frozen ownership preparations: %w", err)
	}
	if err := json.Unmarshal(handoffs, &op.Handoffs); err != nil {
		return op, fmt.Errorf("postgres: decode ownership handoffs: %w", err)
	}
	if err := json.Unmarshal(preparedHandoffs, &op.PreparedHandoffs); err != nil {
		return op, fmt.Errorf("postgres: decode prepared ownership handoffs: %w", err)
	}
	if len(frozenClaim) != 0 {
		var fact storage.NodeOwnershipClaim
		if err := json.Unmarshal(frozenClaim, &fact); err != nil {
			return op, fmt.Errorf("postgres: decode frozen claim: %w", err)
		}
		op.FrozenClaim = &fact
	}
	if len(claimHandoff) != 0 {
		var fact storage.NodeOwnershipClaimHandoff
		if err := json.Unmarshal(claimHandoff, &fact); err != nil {
			return op, fmt.Errorf("postgres: decode claim handoff: %w", err)
		}
		op.ClaimHandoff = &fact
	}
	return op, nil
}

func (s *Store) currentHumanOperator(ctx context.Context) (string, error) {
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" || identity.UserKind != storage.KindHuman {
		return "", storage.ErrPlanningForbidden
	}
	user, err := s.ExistingAuth().GetUserByID(ctx, identity.UserID)
	if err != nil {
		return "", err
	}
	if user.Kind != storage.KindHuman || user.DeletedAt != nil {
		return "", storage.ErrPlanningForbidden
	}
	return identity.UserID, nil
}

func validNodeOwnershipReason(reason string) bool {
	return strings.TrimSpace(reason) != "" && utf8.RuneCountInString(reason) <= 4000
}

func sameOwner(a, b *string) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && *a == *b)
}

func (s *Store) nodeOwnershipOperation(ctx context.Context, id string) (*storage.NodeOwnershipOperation, error) {
	op, err := scanNodeOwnershipOperation(s.queryRow(ctx, `SELECT `+nodeOwnershipColumns+` FROM node_ownership_operations WHERE project_slug=$1 AND id=$2`, s.project, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, storage.ErrNodeOwnershipNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: read node ownership operation: %w", err)
	}
	return &op, nil
}

func (s *Store) pendingNodeOwnership(ctx context.Context, slug string) (*storage.NodeOwnershipOperation, error) {
	op, err := scanNodeOwnershipOperation(s.queryRow(ctx, `SELECT `+nodeOwnershipColumns+` FROM node_ownership_operations WHERE project_slug=$1 AND task_slug=$2 AND status='pending'`, s.project, slug))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: read pending node ownership: %w", err)
	}
	return &op, nil
}

func (s *Store) rejectNewNodeExecution(ctx context.Context, slug string) error {
	var owner *string
	var pending bool
	err := s.queryRow(ctx, `SELECT human_owner_user_id,
 EXISTS(SELECT 1 FROM node_ownership_operations WHERE project_slug=$1 AND task_slug=$2 AND status='pending')
 FROM specs WHERE project_slug=$1 AND slug=$2`, s.project, slug).Scan(&owner, &pending)
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.ErrSpecNotFound
	}
	if err != nil {
		return fmt.Errorf("postgres: read node execution ownership: %w", err)
	}
	if owner != nil {
		return storage.ErrNodeOwnershipConflict
	}
	if pending {
		return storage.ErrNodeOwnershipPending
	}
	return s.rejectMergedSourceExecution(ctx, slug)
}

func (s *Store) nodeClaimAllowed(ctx context.Context, slug, runID string) error {
	if err := s.rejectHumanOwnedNode(ctx, slug); err != nil {
		return err
	}
	pending, err := s.pendingNodeOwnership(ctx, slug)
	if err != nil {
		return err
	}
	if pending == nil {
		return nil
	}
	if pending.FrozenClaim == nil || pending.FrozenClaim.Agent != runID {
		return storage.ErrNodeOwnershipPending
	}
	current, err := s.readNodeClaim(ctx, slug)
	if err != nil {
		return err
	}
	if current == nil || current.Agent != runID || !current.ClaimedAt.Equal(pending.FrozenClaim.ClaimedAt) || current.LeaseExpires.Before(s.now()) {
		return storage.ErrNodeOwnershipPending
	}
	return nil
}

func (s *Store) rejectHumanOwnedNode(ctx context.Context, slug string) error {
	var owner *string
	err := s.queryRow(ctx, `SELECT human_owner_user_id FROM specs WHERE project_slug=$1 AND slug=$2`, s.project, slug).Scan(&owner)
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.ErrSpecNotFound
	}
	if err != nil {
		return fmt.Errorf("postgres: read node owner: %w", err)
	}
	if owner != nil {
		return storage.ErrNodeOwnershipConflict
	}
	return s.rejectMergedSourceExecution(ctx, slug)
}

func (s *Store) ReadNodeOwnership(ctx context.Context, slug string) (*storage.NodeOwnershipState, error) {
	if !validMailText(slug, 256) {
		return nil, storage.ErrInvalidNodeOwnership
	}
	var state *storage.NodeOwnershipState
	err := s.RunReadSnapshot(ctx, func(snapshotCtx context.Context) error {
		result := &storage.NodeOwnershipState{TaskSlug: slug, DispatchObservations: []storage.NodeOwnershipDispatchObservation{}, PreparationObservations: []storage.NodeOwnershipPreparationObservation{}}
		if err := s.queryRow(snapshotCtx, `SELECT version,human_owner_user_id FROM specs WHERE project_slug=$1 AND slug=$2`, s.project, slug).Scan(&result.Version, &result.HumanOwnerUserID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return storage.ErrSpecNotFound
			}
			return fmt.Errorf("postgres: read node ownership: %w", err)
		}
		pending, err := s.pendingNodeOwnership(snapshotCtx, slug)
		if err != nil {
			return err
		}
		result.Pending = pending
		claim, err := s.readNodeClaim(snapshotCtx, slug)
		if err != nil {
			return err
		}
		result.CurrentClaim = claim
		if pending != nil {
			for _, frozen := range pending.Dispatches {
				var observation storage.NodeOwnershipDispatchObservation
				var resultBody []byte
				observation.RunID, observation.AdmissionID = frozen.RunID, frozen.AdmissionID
				if err := s.queryRow(snapshotCtx, `SELECT d.stop_confirmed_at,d.release_kind,
	 a.id,a.result
 FROM run_dispatches d LEFT JOIN LATERAL (SELECT id,result FROM program_attempts WHERE project_slug=d.project_slug AND run_id=d.run_id ORDER BY ordinal DESC LIMIT 1) a ON true
 WHERE d.project_slug=$1 AND d.run_id=$2 AND d.id=$3`, s.project, frozen.RunID, frozen.AdmissionID).Scan(&observation.StopConfirmedAt, &observation.ReleaseKind, &observation.CurrentAttemptID, &resultBody); err != nil {
					return fmt.Errorf("postgres: read ownership dispatch observation: %w", err)
				}
				if len(resultBody) != 0 {
					var result storage.ProgramRunResult
					if err := json.Unmarshal(resultBody, &result); err != nil {
						return fmt.Errorf("postgres: decode ownership program result: %w", err)
					}
					observation.ProgramResult = &result
				}
				result.DispatchObservations = append(result.DispatchObservations, observation)
			}
			for _, frozen := range pending.Preparations {
				var observation storage.NodeOwnershipPreparationObservation
				observation.RunID, observation.PackageID = frozen.RunID, frozen.PackageID
				if err := s.queryRow(snapshotCtx, `SELECT cancelled_at FROM run_preparation_cancellations WHERE project_slug=$1 AND run_id=$2 AND package_id=$3`, s.project, frozen.RunID, frozen.PackageID).Scan(&observation.CancelledAt); err != nil && !errors.Is(err, pgx.ErrNoRows) {
					return fmt.Errorf("postgres: read ownership preparation observation: %w", err)
				}
				result.PreparationObservations = append(result.PreparationObservations, observation)
			}
		}
		state = result
		return nil
	})
	return state, err
}

func (s *Store) ReadNodeOwnershipHistory(ctx context.Context, slug, cursor string) (*storage.NodeOwnershipPage, error) {
	if !validMailText(slug, 256) || (cursor != "" && !validMailText(cursor, 256)) {
		return nil, storage.ErrInvalidNodeOwnership
	}
	page := &storage.NodeOwnershipPage{TaskSlug: slug, Operations: []storage.NodeOwnershipOperation{}}
	err := s.RunReadSnapshot(ctx, func(snapshotCtx context.Context) error {
		var before *time.Time
		if cursor != "" {
			var created time.Time
			if err := s.queryRow(snapshotCtx, `SELECT created_at FROM node_ownership_operations WHERE project_slug=$1 AND task_slug=$2 AND id=$3`, s.project, slug, cursor).Scan(&created); err != nil {
				return storage.ErrInvalidNodeOwnership
			}
			before = &created
		}
		rows, err := s.query(snapshotCtx, `SELECT `+nodeOwnershipColumns+` FROM node_ownership_operations
 WHERE project_slug=$1 AND task_slug=$2 AND ($3::timestamptz IS NULL OR (created_at,id)<($3::timestamptz,$4::text))
 ORDER BY created_at DESC,id DESC LIMIT 51`, s.project, slug, before, cursor)
		if err != nil {
			return fmt.Errorf("postgres: read ownership history: %w", err)
		}
		for rows.Next() {
			op, err := scanNodeOwnershipOperation(rows)
			if err != nil {
				rows.Close()
				return err
			}
			page.Operations = append(page.Operations, op)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		if len(page.Operations) > 50 {
			page.Operations = page.Operations[:50]
			page.HasMore = true
			id := page.Operations[49].ID
			page.NextCursor = &id
		}
		return nil
	})
	return page, err
}

func sortOwnershipHandoffs(items []storage.NodeOwnershipHandoff) {
	sort.Slice(items, func(i, j int) bool { return items[i].RunID < items[j].RunID })
}

func sameOwnershipHandoffs(a, b []storage.NodeOwnershipHandoff) bool {
	left, right := slices.Clone(a), slices.Clone(b)
	sortOwnershipHandoffs(left)
	sortOwnershipHandoffs(right)
	return reflect.DeepEqual(left, right)
}

func samePreparedHandoffs(a, b []storage.NodeOwnershipPreparedHandoff) bool {
	left, right := slices.Clone(a), slices.Clone(b)
	sortPreparedHandoffs(left)
	sortPreparedHandoffs(right)
	return reflect.DeepEqual(left, right)
}

func (s *Store) nodeOwnershipSnapshot(ctx context.Context, slug string) ([]storage.NodeOwnershipDispatch, []storage.NodeOwnershipPreparation, error) {
	dispatches := []storage.NodeOwnershipDispatch{}
	rows, err := s.query(ctx, `SELECT d.run_id,d.id,b.executor_kind,b.environment_id,
 CASE WHEN b.executor_kind='program' THEN cp.body->'program_target'->>'nativeProjectId' ELSE cp.body->'dispatch_target'->>'projectId' END,
 (SELECT a.id FROM program_attempts a WHERE a.project_slug=d.project_slug AND a.run_id=d.run_id ORDER BY a.ordinal DESC LIMIT 1)
 FROM run_dispatches d JOIN run_bindings b ON b.project_slug=d.project_slug AND b.id=d.run_id
 JOIN context_packages cp ON cp.project_slug=b.project_slug AND cp.id=b.package_id
 WHERE d.project_slug=$1 AND d.task_slug=$2 AND d.stop_confirmed_at IS NULL ORDER BY d.run_id`, s.project, slug)
	if err != nil {
		return nil, nil, fmt.Errorf("postgres: freeze node dispatches: %w", err)
	}
	for rows.Next() {
		var item storage.NodeOwnershipDispatch
		if err := rows.Scan(&item.RunID, &item.AdmissionID, &item.ExecutorKind, &item.EnvironmentID, &item.NativeProjectID, &item.AttemptID); err != nil {
			rows.Close()
			return nil, nil, err
		}
		dispatches = append(dispatches, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, nil, err
	}
	rows.Close()
	preparations := []storage.NodeOwnershipPreparation{}
	rows, err = s.query(ctx, `SELECT b.id,b.package_id,b.executor_kind,b.state,b.environment_id,b.thread_ref,
 NOT EXISTS(SELECT 1 FROM run_preparations p WHERE p.project_slug=b.project_slug AND p.run_id=b.id)
 AND cp.body->'dispatch_target' IS NULL AND cp.body->'program_target' IS NULL AS raw
 FROM run_bindings b JOIN context_packages cp ON cp.project_slug=b.project_slug AND cp.id=b.package_id
 WHERE b.project_slug=$1 AND b.task_spec_slug=$2 AND b.state IN ('prepared','bound')
 AND NOT EXISTS(SELECT 1 FROM run_dispatches d WHERE d.project_slug=b.project_slug AND d.run_id=b.id)
 AND NOT EXISTS(SELECT 1 FROM run_preparation_cancellations c WHERE c.project_slug=b.project_slug AND c.run_id=b.id)
 ORDER BY b.id`, s.project, slug)
	if err != nil {
		return nil, nil, fmt.Errorf("postgres: freeze node preparations: %w", err)
	}
	for rows.Next() {
		var item storage.NodeOwnershipPreparation
		if err := rows.Scan(&item.RunID, &item.PackageID, &item.ExecutorKind, &item.State, &item.EnvironmentID, &item.ThreadID, &item.Raw); err != nil {
			rows.Close()
			return nil, nil, err
		}
		preparations = append(preparations, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, nil, err
	}
	rows.Close()
	return dispatches, preparations, nil
}

func (s *Store) readNodeClaim(ctx context.Context, slug string) (*storage.NodeOwnershipClaim, error) {
	claim := &storage.NodeOwnershipClaim{}
	err := s.queryRow(ctx, `SELECT agent,claimed_at,lease_expires FROM claims WHERE project_slug=$1 AND spec_slug=$2`, s.project, slug).
		Scan(&claim.Agent, &claim.ClaimedAt, &claim.LeaseExpires)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: read node claim: %w", err)
	}
	return claim, nil
}

func (s *Store) nodeOwnerSpec(ctx context.Context, slug string) (int32, *string, string, string, string, error) {
	var version int32
	var owner *string
	var stage, role, hash string
	err := s.queryRow(ctx, `SELECT version,human_owner_user_id,stage,role,content_hash FROM specs WHERE project_slug=$1 AND slug=$2 FOR UPDATE`, s.project, slug).Scan(&version, &owner, &stage, &role, &hash)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil, "", "", "", storage.ErrSpecNotFound
	}
	if err != nil {
		return 0, nil, "", "", "", fmt.Errorf("postgres: read node owner spec: %w", err)
	}
	return version, owner, stage, role, hash, nil
}

func (s *Store) changeNodeOwner(ctx context.Context, slug string, from, to *string, version int32, stage, hash, reason string) (int32, error) {
	now := s.now()
	var next int32
	if err := s.queryRow(ctx, `UPDATE specs SET human_owner_user_id=$4,version=version+1,updated_at=$5
 WHERE project_slug=$1 AND slug=$2 AND version=$3 RETURNING version`, s.project, slug, version, to, now).Scan(&next); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, storage.ErrNodeOwnershipConflict
		}
		return 0, fmt.Errorf("postgres: change node owner: %w", err)
	}
	oldValue, newValue := "", ""
	if from != nil {
		oldValue = *from
	}
	if to != nil {
		newValue = *to
	}
	if err := s.createChangeLog(ctx, slug, &storage.ChangeLogEntry{Version: next, Stage: stage, ContentHash: hash,
		Summary: "Human node responsibility changed", Reason: reason, Date: now},
		[]storage.FieldChange{{Field: "human_owner_user_id", OldValue: oldValue, NewValue: newValue}}); err != nil {
		return 0, err
	}
	return next, nil
}

func (s *Store) ownershipReplay(ctx context.Context, key string) (*storage.NodeOwnershipOperation, error) {
	op, err := scanNodeOwnershipOperation(s.queryRow(ctx, `SELECT `+nodeOwnershipColumns+` FROM node_ownership_operations WHERE project_slug=$1 AND idempotency_key=$2`, s.project, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: read node owner replay: %w", err)
	}
	return &op, nil
}

func validNodeOwnershipBegin(slug string, version int32, key, reason string) bool {
	return validMailText(slug, 256) && version > 0 && subdivisionKeyPattern.MatchString(key) && validNodeOwnershipReason(reason)
}

// BeginNodeOwnershipTake freezes the exact work that must hand off before the owner changes.
func (s *Store) BeginNodeOwnershipTake(ctx context.Context, req storage.BeginNodeOwnershipTakeRequest) (*storage.NodeOwnershipOperation, error) {
	if !validNodeOwnershipBegin(req.TaskSlug, req.ExpectedVersion, req.IdempotencyKey, req.Reason) {
		return nil, storage.ErrInvalidNodeOwnership
	}
	var result *storage.NodeOwnershipOperation
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		actor, err := s.currentHumanOperator(txCtx)
		if err != nil {
			return err
		}
		prior, err := s.ownershipReplay(txCtx, req.IdempotencyKey)
		if err != nil {
			return err
		}
		if prior != nil {
			if prior.Action != "take" || prior.TaskSlug != req.TaskSlug || prior.ActorUserID != actor || prior.FromVersion != req.ExpectedVersion || !sameOwner(prior.BeforeOwnerUserID, req.ExpectedOwnerUserID) || prior.Reason != req.Reason {
				return storage.ErrNodeOwnershipConflict
			}
			prior.Replayed = true
			result = prior
			return nil
		}
		version, owner, stage, role, hash, err := s.nodeOwnerSpec(txCtx, req.TaskSlug)
		if err != nil {
			return err
		}
		if role != string(storage.SpecRoleWork) || stage == "done" || storage.SpecStage(stage).IsFullyTerminal() {
			return storage.ErrSpecTerminal
		}
		if version != req.ExpectedVersion || !sameOwner(owner, req.ExpectedOwnerUserID) || (owner != nil && *owner == actor) {
			return storage.ErrNodeOwnershipConflict
		}
		pending, err := s.pendingNodeOwnership(txCtx, req.TaskSlug)
		if err != nil {
			return err
		}
		if pending != nil {
			return storage.ErrNodeOwnershipPending
		}
		dispatches, preparations, err := s.nodeOwnershipSnapshot(txCtx, req.TaskSlug)
		if err != nil {
			return err
		}
		claim, err := s.readNodeClaim(txCtx, req.TaskSlug)
		if err != nil {
			return err
		}
		now := s.now()
		status := "pending"
		var toVersion *int32
		var finished *time.Time
		if len(dispatches) == 0 && len(preparations) == 0 && claim == nil {
			status = "committed"
			next, err := s.changeNodeOwner(txCtx, req.TaskSlug, owner, &actor, version, stage, hash, req.Reason)
			if err != nil {
				return err
			}
			toVersion, finished = &next, &now
		}
		encodedDispatches, err := json.Marshal(dispatches)
		if err != nil {
			return err
		}
		encodedPreparations, err := json.Marshal(preparations)
		if err != nil {
			return err
		}
		var encodedClaim []byte
		if claim != nil {
			encodedClaim, err = json.Marshal(claim)
			if err != nil {
				return err
			}
		}
		id := newID("own")
		_, err = s.exec(txCtx, `INSERT INTO node_ownership_operations(id,project_slug,task_slug,idempotency_key,action,status,
 before_owner_user_id,after_owner_user_id,from_version,to_version,actor_user_id,reason,dispatches,preparations,handoffs,frozen_claim,created_at,finished_at)
 VALUES($1,$2,$3,$4,'take',$5,$6,$7,$8,$9,$10,$11,$12,$13,'[]'::jsonb,$14,$15,$16)`, id, s.project, req.TaskSlug, req.IdempotencyKey,
			status, owner, actor, version, toVersion, actor, req.Reason, encodedDispatches, encodedPreparations, encodedClaim, now, finished)
		if err != nil {
			return fmt.Errorf("postgres: begin node owner take: %w", err)
		}
		result, err = s.nodeOwnershipOperation(txCtx, id)
		return err
	})
	return result, err
}

// ReturnNodeOwnership clears a named human owner but grants no Agent execution.
func (s *Store) ReturnNodeOwnership(ctx context.Context, req storage.ReturnNodeOwnershipRequest) (*storage.NodeOwnershipOperation, error) {
	if !validNodeOwnershipBegin(req.TaskSlug, req.ExpectedVersion, req.IdempotencyKey, req.Reason) {
		return nil, storage.ErrInvalidNodeOwnership
	}
	var result *storage.NodeOwnershipOperation
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		actor, err := s.currentHumanOperator(txCtx)
		if err != nil {
			return err
		}
		prior, err := s.ownershipReplay(txCtx, req.IdempotencyKey)
		if err != nil {
			return err
		}
		if prior != nil {
			if prior.Action != "return" || prior.TaskSlug != req.TaskSlug || prior.ActorUserID != actor || prior.FromVersion != req.ExpectedVersion || !sameOwner(prior.BeforeOwnerUserID, req.ExpectedOwnerUserID) || prior.Reason != req.Reason {
				return storage.ErrNodeOwnershipConflict
			}
			prior.Replayed = true
			result = prior
			return nil
		}
		version, owner, stage, _, hash, err := s.nodeOwnerSpec(txCtx, req.TaskSlug)
		if err != nil {
			return err
		}
		if version != req.ExpectedVersion || owner == nil || !sameOwner(owner, req.ExpectedOwnerUserID) {
			return storage.ErrNodeOwnershipConflict
		}
		pending, err := s.pendingNodeOwnership(txCtx, req.TaskSlug)
		if err != nil {
			return err
		}
		if pending != nil {
			return storage.ErrNodeOwnershipPending
		}
		next, err := s.changeNodeOwner(txCtx, req.TaskSlug, owner, nil, version, stage, hash, req.Reason)
		if err != nil {
			return err
		}
		id, now := newID("own"), s.now()
		_, err = s.exec(txCtx, `INSERT INTO node_ownership_operations(id,project_slug,task_slug,idempotency_key,action,status,
 before_owner_user_id,after_owner_user_id,from_version,to_version,actor_user_id,reason,created_at,finished_at)
 VALUES($1,$2,$3,$4,'return','committed',$5,NULL,$6,$7,$8,$9,$10,$10)`, id, s.project, req.TaskSlug, req.IdempotencyKey, owner, version, next, actor, req.Reason, now)
		if err != nil {
			return fmt.Errorf("postgres: return node owner: %w", err)
		}
		result, err = s.nodeOwnershipOperation(txCtx, id)
		return err
	})
	return result, err
}

func validNodeHandoffSummary(summary storage.NodeOwnershipSummary) bool {
	switch summary.Kind {
	case "progress":
		return summary.ProgressEventID != nil && validMailText(*summary.ProgressEventID, 256) && summary.ProgramAttemptID == nil && summary.HumanSubstitute == nil
	case "program_result":
		return summary.ProgramAttemptID != nil && validMailText(*summary.ProgramAttemptID, 256) && summary.ProgressEventID == nil && summary.HumanSubstitute == nil
	case "human_substitute":
		if summary.ProgressEventID != nil || summary.ProgramAttemptID != nil || summary.HumanSubstitute == nil {
			return false
		}
		human := summary.HumanSubstitute
		if !validNodeOwnershipReason(human.Statement) || len(human.SourceRefs) == 0 || len(human.MissingInfo) == 0 {
			return false
		}
		for _, ref := range human.SourceRefs {
			if !validMailText(ref, 4096) {
				return false
			}
		}
		for _, missing := range human.MissingInfo {
			if !validNodeOwnershipReason(missing) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func sortPreparedHandoffs(items []storage.NodeOwnershipPreparedHandoff) {
	sort.Slice(items, func(i, j int) bool { return items[i].RunID < items[j].RunID })
}

func (s *Store) handoffRawBoundPreparation(ctx context.Context, task string, frozen storage.NodeOwnershipPreparation, handoff storage.NodeOwnershipPreparedHandoff, actor string) error {
	if !frozen.Raw || frozen.State != "bound" || handoff.RunID != frozen.RunID || handoff.PackageID != frozen.PackageID ||
		!validNodeOwnershipReason(handoff.StopNote) || !validNodeHandoffSummary(handoff.Summary) || handoff.Summary.Kind == "program_result" {
		return storage.ErrInvalidNodeOwnership
	}
	var state, environment, thread, executor, packageID string
	var raw, admitted bool
	err := s.queryRow(ctx, `SELECT b.state,b.environment_id,b.thread_ref,b.executor_kind,b.package_id,
 cp.body->'dispatch_target' IS NULL AND cp.body->'program_target' IS NULL
 AND NOT EXISTS(SELECT 1 FROM run_preparations p WHERE p.project_slug=b.project_slug AND p.run_id=b.id AND p.dispatch_target IS NOT NULL),
 EXISTS(SELECT 1 FROM run_dispatches d WHERE d.project_slug=b.project_slug AND d.run_id=b.id)
 FROM run_bindings b JOIN context_packages cp ON cp.project_slug=b.project_slug AND cp.id=b.package_id
 WHERE b.project_slug=$1 AND b.task_spec_slug=$2 AND b.id=$3`, s.project, task, frozen.RunID).
		Scan(&state, &environment, &thread, &executor, &packageID, &raw, &admitted)
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.ErrNodeOwnershipConflict
	}
	if err != nil {
		return fmt.Errorf("postgres: verify bound raw preparation: %w", err)
	}
	if !raw || admitted || executor != "agent" || state != frozen.State || environment != frozen.EnvironmentID ||
		thread != frozen.ThreadID || packageID != frozen.PackageID {
		return storage.ErrNodeOwnershipHandoffRequired
	}
	if handoff.Summary.Kind == "progress" {
		var found bool
		if err := s.queryRow(ctx, `SELECT EXISTS(SELECT 1 FROM execution_events WHERE project_slug=$1 AND spec_slug=$2 AND agent=$3 AND id=$4 AND event_type='progress')`,
			s.project, task, frozen.RunID, *handoff.Summary.ProgressEventID).Scan(&found); err != nil {
			return err
		}
		if !found {
			return storage.ErrNodeOwnershipHandoffRequired
		}
	}
	return s.cancelPreparationLocked(ctx, task, frozen.RunID, frozen.PackageID, actor, handoff.StopNote,
		&storage.RawPreparationHandoff{EnvironmentID: frozen.EnvironmentID, ThreadID: frozen.ThreadID, Summary: handoff.Summary})
}

func claimCapturedByRun(claim *storage.NodeOwnershipClaim, dispatches []storage.NodeOwnershipDispatch, preparations []storage.NodeOwnershipPreparation) bool {
	if claim == nil {
		return false
	}
	for _, item := range dispatches {
		if item.RunID == claim.Agent {
			return true
		}
	}
	for _, item := range preparations {
		if item.RunID == claim.Agent {
			return true
		}
	}
	return false
}

func (s *Store) finishClaimOnlyHandoff(ctx context.Context, task string, frozen *storage.NodeOwnershipClaim, handoff *storage.NodeOwnershipClaimHandoff) error {
	if frozen == nil || handoff == nil || handoff.Agent != frozen.Agent || !handoff.ClaimedAt.Equal(frozen.ClaimedAt) ||
		!validNodeOwnershipReason(handoff.StopNote) || !validNodeHandoffSummary(handoff.Summary) || handoff.Summary.Kind == "program_result" {
		return storage.ErrInvalidNodeOwnership
	}
	if handoff.Summary.Kind == "progress" {
		var found bool
		if err := s.queryRow(ctx, `SELECT EXISTS(SELECT 1 FROM execution_events WHERE project_slug=$1 AND spec_slug=$2 AND agent=$3 AND id=$4 AND event_type='progress')`,
			s.project, task, frozen.Agent, *handoff.Summary.ProgressEventID).Scan(&found); err != nil {
			return err
		}
		if !found {
			return storage.ErrNodeOwnershipHandoffRequired
		}
	}
	current, err := s.readNodeClaim(ctx, task)
	if err != nil {
		return err
	}
	if current != nil {
		if current.Agent != frozen.Agent || !current.ClaimedAt.Equal(frozen.ClaimedAt) {
			return storage.ErrNodeOwnershipConflict
		}
		return s.UnclaimSpec(ctx, task, frozen.Agent)
	}
	return nil
}

func (s *Store) validateNodeHandoff(ctx context.Context, task string, frozen storage.NodeOwnershipDispatch, handoff storage.NodeOwnershipHandoff) error {
	if handoff.RunID != frozen.RunID || handoff.AdmissionID != frozen.AdmissionID || !validNodeHandoffSummary(handoff.Summary) {
		return storage.ErrInvalidNodeOwnership
	}
	var stopped, released *time.Time
	var executor, environment string
	var nativeProject, currentAttempt *string
	err := s.queryRow(ctx, `SELECT d.stop_confirmed_at,d.released_at,b.executor_kind,b.environment_id,
 CASE WHEN b.executor_kind='program' THEN cp.body->'program_target'->>'nativeProjectId' ELSE cp.body->'dispatch_target'->>'projectId' END,
 (SELECT a.id FROM program_attempts a WHERE a.project_slug=d.project_slug AND a.run_id=d.run_id ORDER BY a.ordinal DESC LIMIT 1)
 FROM run_dispatches d JOIN run_bindings b ON b.project_slug=d.project_slug AND b.id=d.run_id
 JOIN context_packages cp ON cp.project_slug=b.project_slug AND cp.id=b.package_id
 WHERE d.project_slug=$1 AND d.task_slug=$2 AND d.run_id=$3 AND d.id=$4`, s.project, task, frozen.RunID, frozen.AdmissionID).
		Scan(&stopped, &released, &executor, &environment, &nativeProject, &currentAttempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.ErrNodeOwnershipConflict
	}
	if err != nil {
		return fmt.Errorf("postgres: verify old run handoff: %w", err)
	}
	if stopped == nil || released == nil || executor != frozen.ExecutorKind || environment != frozen.EnvironmentID ||
		!sameOwner(nativeProject, frozen.NativeProjectID) || !sameOwner(currentAttempt, frozen.AttemptID) {
		return storage.ErrNodeOwnershipHandoffRequired
	}
	switch handoff.Summary.Kind {
	case "progress":
		if executor != "agent" {
			return storage.ErrInvalidNodeOwnership
		}
		var found bool
		if err := s.queryRow(ctx, `SELECT EXISTS(SELECT 1 FROM execution_events WHERE project_slug=$1 AND spec_slug=$2 AND agent=$3 AND id=$4 AND event_type='progress')`,
			s.project, task, frozen.RunID, *handoff.Summary.ProgressEventID).Scan(&found); err != nil {
			return err
		}
		if !found {
			return storage.ErrNodeOwnershipHandoffRequired
		}
	case "program_result":
		if executor != "program" || frozen.AttemptID == nil || *handoff.Summary.ProgramAttemptID != *frozen.AttemptID {
			return storage.ErrInvalidNodeOwnership
		}
		var found bool
		if err := s.queryRow(ctx, `SELECT EXISTS(SELECT 1 FROM program_attempts WHERE project_slug=$1 AND run_id=$2 AND id=$3 AND result IS NOT NULL)`,
			s.project, frozen.RunID, *frozen.AttemptID).Scan(&found); err != nil {
			return err
		}
		if !found {
			return storage.ErrNodeOwnershipHandoffRequired
		}
	}
	return nil
}

// CommitNodeOwnershipTake consumes only the exact run identities frozen at begin.
func (s *Store) CommitNodeOwnershipTake(ctx context.Context, req storage.CommitNodeOwnershipTakeRequest) (*storage.NodeOwnershipOperation, error) {
	if !validMailText(req.OperationID, 256) || req.ExpectedVersion < 1 || req.Handoffs == nil || req.PreparedHandoffs == nil {
		return nil, storage.ErrInvalidNodeOwnership
	}
	for _, handoff := range req.Handoffs {
		if !validMailText(handoff.RunID, 256) || !validMailText(handoff.AdmissionID, 256) || !validNodeHandoffSummary(handoff.Summary) {
			return nil, storage.ErrInvalidNodeOwnership
		}
	}
	for _, handoff := range req.PreparedHandoffs {
		if !validMailText(handoff.RunID, 256) || !validMailText(handoff.PackageID, 256) || !validNodeOwnershipReason(handoff.StopNote) || !validNodeHandoffSummary(handoff.Summary) {
			return nil, storage.ErrInvalidNodeOwnership
		}
	}
	if req.ClaimHandoff != nil {
		if !validMailText(req.ClaimHandoff.Agent, 256) || req.ClaimHandoff.ClaimedAt.IsZero() ||
			!validNodeOwnershipReason(req.ClaimHandoff.StopNote) || !validNodeHandoffSummary(req.ClaimHandoff.Summary) {
			return nil, storage.ErrInvalidNodeOwnership
		}
	}
	var result *storage.NodeOwnershipOperation
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		actor, err := s.currentHumanOperator(txCtx)
		if err != nil {
			return err
		}
		op, err := s.nodeOwnershipOperation(txCtx, req.OperationID)
		if err != nil {
			return err
		}
		if op.Action != "take" || op.ActorUserID != actor {
			return storage.ErrNodeOwnershipConflict
		}
		if op.Status == "committed" {
			if op.ToVersion == nil || req.ExpectedVersion != *op.ToVersion-1 || !sameOwnershipHandoffs(op.Handoffs, req.Handoffs) || !samePreparedHandoffs(op.PreparedHandoffs, req.PreparedHandoffs) || !reflect.DeepEqual(op.ClaimHandoff, req.ClaimHandoff) {
				return storage.ErrNodeOwnershipConflict
			}
			op.Replayed = true
			result = op
			return nil
		}
		if op.Status != "pending" || len(req.Handoffs) != len(op.Dispatches) {
			return storage.ErrNodeOwnershipConflict
		}
		version, owner, stage, role, hash, err := s.nodeOwnerSpec(txCtx, op.TaskSlug)
		if err != nil {
			return err
		}
		if role != string(storage.SpecRoleWork) || stage == "done" || storage.SpecStage(stage).IsFullyTerminal() {
			return storage.ErrSpecTerminal
		}
		if version != req.ExpectedVersion || !sameOwner(owner, op.BeforeOwnerUserID) {
			return storage.ErrNodeOwnershipConflict
		}
		handoffs := slices.Clone(req.Handoffs)
		sortOwnershipHandoffs(handoffs)
		for i, frozen := range op.Dispatches {
			if handoffs[i].RunID != frozen.RunID {
				return storage.ErrNodeOwnershipConflict
			}
			if err := s.validateNodeHandoff(txCtx, op.TaskSlug, frozen, handoffs[i]); err != nil {
				return err
			}
		}
		preparedHandoffs := slices.Clone(req.PreparedHandoffs)
		sortPreparedHandoffs(preparedHandoffs)
		preparedIndex := 0
		for _, prepared := range op.Preparations {
			if prepared.Raw && prepared.State == "bound" {
				if preparedIndex >= len(preparedHandoffs) || preparedHandoffs[preparedIndex].RunID != prepared.RunID {
					return storage.ErrNodeOwnershipHandoffRequired
				}
				if err := s.handoffRawBoundPreparation(txCtx, op.TaskSlug, prepared, preparedHandoffs[preparedIndex], actor); err != nil {
					return err
				}
				preparedIndex++
				continue
			}
			var cancelled bool
			if err := s.queryRow(txCtx, `SELECT EXISTS(SELECT 1 FROM run_preparation_cancellations WHERE project_slug=$1 AND run_id=$2 AND package_id=$3)`,
				s.project, prepared.RunID, prepared.PackageID).Scan(&cancelled); err != nil {
				return err
			}
			if !cancelled {
				return storage.ErrNodeOwnershipHandoffRequired
			}
		}
		if preparedIndex != len(preparedHandoffs) {
			return storage.ErrNodeOwnershipConflict
		}
		claimOnly := op.FrozenClaim != nil && !claimCapturedByRun(op.FrozenClaim, op.Dispatches, op.Preparations)
		if claimOnly {
			if err := s.finishClaimOnlyHandoff(txCtx, op.TaskSlug, op.FrozenClaim, req.ClaimHandoff); err != nil {
				return err
			}
		} else if req.ClaimHandoff != nil {
			return storage.ErrNodeOwnershipConflict
		}
		currentDispatches, currentPreparations, err := s.nodeOwnershipSnapshot(txCtx, op.TaskSlug)
		if err != nil {
			return err
		}
		if len(currentDispatches) != 0 || len(currentPreparations) != 0 {
			return storage.ErrNodeOwnershipHandoffRequired
		}
		claim, err := s.readNodeClaim(txCtx, op.TaskSlug)
		if err != nil {
			return err
		}
		if claim != nil {
			return storage.ErrNodeOwnershipHandoffRequired
		}
		next, err := s.changeNodeOwner(txCtx, op.TaskSlug, owner, op.AfterOwnerUserID, version, stage, hash, op.Reason)
		if err != nil {
			return err
		}
		encoded, err := json.Marshal(handoffs)
		if err != nil {
			return err
		}
		encodedPrepared, err := json.Marshal(preparedHandoffs)
		if err != nil {
			return err
		}
		var encodedClaimHandoff []byte
		if req.ClaimHandoff != nil {
			encodedClaimHandoff, err = json.Marshal(req.ClaimHandoff)
			if err != nil {
				return err
			}
		}
		now := s.now()
		_, err = s.exec(txCtx, `UPDATE node_ownership_operations SET status='committed',to_version=$3,handoffs=$4,prepared_handoffs=$5,claim_handoff=$6,finished_at=$7 WHERE project_slug=$1 AND id=$2 AND status='pending'`,
			s.project, op.ID, next, encoded, encodedPrepared, encodedClaimHandoff, now)
		if err != nil {
			return fmt.Errorf("postgres: commit node owner take: %w", err)
		}
		result, err = s.nodeOwnershipOperation(txCtx, op.ID)
		return err
	})
	return result, err
}

// CancelNodeOwnershipTake leaves the current owner unchanged and preserves the pending receipt.
func (s *Store) CancelNodeOwnershipTake(ctx context.Context, req storage.CancelNodeOwnershipTakeRequest) (*storage.NodeOwnershipOperation, error) {
	if !validMailText(req.OperationID, 256) || !validNodeOwnershipReason(req.Reason) {
		return nil, storage.ErrInvalidNodeOwnership
	}
	var result *storage.NodeOwnershipOperation
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		actor, err := s.currentHumanOperator(txCtx)
		if err != nil {
			return err
		}
		op, err := s.nodeOwnershipOperation(txCtx, req.OperationID)
		if err != nil {
			return err
		}
		if op.Status == "cancelled" {
			if op.CancelActorUserID == nil || *op.CancelActorUserID != actor || op.CancelReason == nil || *op.CancelReason != req.Reason {
				return storage.ErrNodeOwnershipConflict
			}
			op.Replayed = true
			result = op
			return nil
		}
		if op.Status != "pending" {
			return storage.ErrNodeOwnershipConflict
		}
		now := s.now()
		_, err = s.exec(txCtx, `UPDATE node_ownership_operations SET status='cancelled',finished_at=$3,cancel_actor_user_id=$4,cancel_reason=$5
 WHERE project_slug=$1 AND id=$2 AND status='pending'`, s.project, op.ID, now, actor, req.Reason)
		if err != nil {
			return fmt.Errorf("postgres: cancel node owner take: %w", err)
		}
		result, err = s.nodeOwnershipOperation(txCtx, op.ID)
		return err
	})
	return result, err
}

// ProjectForOwnHandoffSummary selects a current bound project or an exact historical event for replay only.
func (s *Store) ProjectForOwnHandoffSummary(ctx context.Context, scope storage.MailScope, nativeProjectID string, req storage.RecordOwnNodeHandoffSummaryRequest) (string, error) {
	if !validMailScope(scope) || !validMailText(nativeProjectID, 256) || !validMailText(req.EventID, 256) || !validNodeOwnershipReason(req.Message) {
		return "", storage.ErrInvalidNodeOwnership
	}
	rows, err := s.query(ctx, `SELECT e.project_slug,e.event_type,e.message,b.environment_id,b.thread_ref,
 cp.body->'dispatch_target'->>'projectId' FROM execution_events e
 JOIN run_bindings b ON b.project_slug=e.project_slug AND b.id=e.agent AND b.task_spec_slug=e.spec_slug
 JOIN context_packages cp ON cp.project_slug=b.project_slug AND cp.id=b.package_id
 WHERE e.id=$1 LIMIT 2`, req.EventID)
	if err != nil {
		return "", err
	}
	type prior struct {
		Project, Kind, Message, Environment, Thread string
		Native                                      *string
	}
	var found []prior
	for rows.Next() {
		var item prior
		if err := rows.Scan(&item.Project, &item.Kind, &item.Message, &item.Environment, &item.Thread, &item.Native); err != nil {
			rows.Close()
			return "", err
		}
		found = append(found, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return "", err
	}
	rows.Close()
	if len(found) != 0 {
		if len(found) != 1 || found[0].Kind != "progress" || found[0].Message != req.Message || found[0].Environment != scope.EnvironmentID ||
			found[0].Thread != scope.ThreadID || found[0].Native == nil || *found[0].Native != nativeProjectID {
			return "", storage.ErrNodeOwnershipConflict
		}
		return found[0].Project, nil
	}
	project, err := s.ProjectForLocalMailScope(ctx, scope)
	if err != nil {
		return "", err
	}
	var targetNative *string
	if err := s.queryRow(ctx, `SELECT cp.body->'dispatch_target'->>'projectId' FROM run_bindings b
 JOIN context_packages cp ON cp.project_slug=b.project_slug AND cp.id=b.package_id
 WHERE b.project_slug=$1 AND b.environment_id=$2 AND b.thread_ref=$3 AND b.state='bound'`, project, scope.EnvironmentID, scope.ThreadID).Scan(&targetNative); err != nil {
		return "", err
	}
	if targetNative == nil || *targetNative != nativeProjectID {
		return "", storage.ErrNodeOwnershipConflict
	}
	return project, nil
}

// RecordOwnNodeHandoffSummary returns the real progress event ID, including an exact stopped-run replay.
func (s *Store) RecordOwnNodeHandoffSummary(ctx context.Context, scope storage.MailScope, nativeProjectID string, req storage.RecordOwnNodeHandoffSummaryRequest) (*storage.NodeHandoffSummaryReceipt, error) {
	if !validMailScope(scope) || !validMailText(nativeProjectID, 256) || !validMailText(req.EventID, 256) || !validNodeOwnershipReason(req.Message) {
		return nil, storage.ErrInvalidNodeOwnership
	}
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" {
		return nil, auth.ErrUnauthenticated
	}
	var result *storage.NodeHandoffSummaryReceipt
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		var prior storage.NodeHandoffSummaryReceipt
		var environment, thread string
		var native *string
		err := s.queryRow(txCtx, `SELECT e.id,e.agent,e.spec_slug,e.message,e.created_at,b.environment_id,b.thread_ref,
 cp.body->'dispatch_target'->>'projectId' FROM execution_events e
 JOIN run_bindings b ON b.project_slug=e.project_slug AND b.id=e.agent AND b.task_spec_slug=e.spec_slug
 JOIN context_packages cp ON cp.project_slug=b.project_slug AND cp.id=b.package_id
 WHERE e.project_slug=$1 AND e.id=$2 AND e.event_type='progress'`, s.project, req.EventID).
			Scan(&prior.EventID, &prior.RunID, &prior.TaskSlug, &prior.Message, &prior.RecordedAt, &environment, &thread, &native)
		if err == nil {
			if prior.Message != req.Message || environment != scope.EnvironmentID || thread != scope.ThreadID || native == nil || *native != nativeProjectID {
				return storage.ErrNodeOwnershipConflict
			}
			prior.Replayed = true
			result = &prior
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("postgres: read own summary replay: %w", err)
		}
		run, _, err := s.reviewActorRun(txCtx, scope)
		if err != nil {
			return err
		}
		task, err := s.RunBindingTask(txCtx, run)
		if err != nil {
			return err
		}
		var targetNative *string
		if err := s.queryRow(txCtx, `SELECT cp.body->'dispatch_target'->>'projectId' FROM run_bindings b
 JOIN context_packages cp ON cp.project_slug=b.project_slug AND cp.id=b.package_id
 WHERE b.project_slug=$1 AND b.id=$2 AND b.executor_kind='agent'`, s.project, run).Scan(&targetNative); err != nil {
			return err
		}
		if targetNative == nil || *targetNative != nativeProjectID {
			return storage.ErrNodeOwnershipConflict
		}
		if _, err := s.recordClaimedEvent(txCtx, task, run, "progress", req.Message, req.EventID); err != nil {
			return err
		}
		var recorded time.Time
		if err := s.queryRow(txCtx, `SELECT created_at FROM execution_events WHERE project_slug=$1 AND id=$2`, s.project, req.EventID).Scan(&recorded); err != nil {
			return err
		}
		result = &storage.NodeHandoffSummaryReceipt{EventID: req.EventID, RunID: run, TaskSlug: task, Message: req.Message, RecordedAt: recorded}
		return nil
	})
	return result, err
}
