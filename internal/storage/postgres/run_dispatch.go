// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/storage"
)

// ReadRunDispatch returns recorded authorization and human resolution separately.
// Neither an absent record nor a release is proof of a provider process state.
func (s *Store) ReadRunDispatch(ctx context.Context, runID string) (storage.RunDispatchStatus, error) {
	var result storage.RunDispatchStatus
	err := s.RunReadSnapshot(ctx, func(snapshotCtx context.Context) error {
		var err error
		result, err = s.readRunDispatch(snapshotCtx, runID)
		return err
	})
	if err != nil {
		return storage.RunDispatchStatus{}, err
	}
	return result, err
}

func (s *Store) readRunDispatch(ctx context.Context, runID string) (storage.RunDispatchStatus, error) {
	var result storage.RunDispatchStatus
	err := func() error {
		if _, err := s.RunBindingTask(ctx, runID); err != nil {
			return err
		}
		var cancellation storage.RunPreparationCancellation
		var rawHandoff []byte
		err := s.queryRow(ctx, `SELECT actor,note,cancelled_at,raw_handoff FROM run_preparation_cancellations WHERE project_slug=$1 AND run_id=$2`, s.project, runID).
			Scan(&cancellation.Actor, &cancellation.Note, &cancellation.CancelledAt, &rawHandoff)
		if err == nil {
			if len(rawHandoff) != 0 {
				var handoff storage.RawPreparationHandoff
				if err := json.Unmarshal(rawHandoff, &handoff); err != nil {
					return fmt.Errorf("postgres: decode raw preparation handoff: %w", err)
				}
				cancellation.RawHandoff = &handoff
			}
			result.Cancellation = &cancellation
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("postgres: readRunDispatch: %w", err)
		}
		var admission storage.RunDispatch
		var released *time.Time
		var actor, kind, note *string
		var stopped *time.Time
		var stopActor, stopNote, stopProgramAttempt *string
		var flowBasis []byte
		var joinBasis []byte
		err = s.queryRow(ctx, `SELECT id,run_id,task_slug,package_id,actor,authorized_at,released_at,release_actor,release_kind,release_note,stop_confirmed_at,stop_actor,stop_note,stop_program_attempt_id,report_flow_basis,report_join_basis
			FROM run_dispatches WHERE project_slug=$1 AND run_id=$2`, s.project, runID).
			Scan(&admission.ID, &admission.RunID, &admission.TaskSlug, &admission.PackageID, &admission.Actor, &admission.AuthorizedAt, &released, &actor, &kind, &note, &stopped, &stopActor, &stopNote, &stopProgramAttempt, &flowBasis, &joinBasis)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("postgres: readRunDispatch: %w", err)
		}
		if len(flowBasis) != 0 {
			var proof storage.ReportBranchProof
			if err := json.Unmarshal(flowBasis, &proof); err != nil {
				return fmt.Errorf("postgres: decode report branch proof: %w", err)
			}
			admission.ReportFlowBasis = &proof
		}
		if len(joinBasis) != 0 {
			var proof storage.ReportFlowJoinProof
			if err := json.Unmarshal(joinBasis, &proof); err != nil {
				return fmt.Errorf("postgres: decode report join proof: %w", err)
			}
			admission.ReportJoinBasis = &proof
		}
		admission.CandidateAttemptID, err = s.firstCandidateAttemptID(ctx, runID)
		if err != nil {
			return err
		}
		result.Admission = &admission
		if released != nil {
			result.Resolution = &storage.RunDispatchResolution{Actor: *actor, Kind: *kind, Note: *note, ReleasedAt: *released}
		}
		if stopped != nil {
			result.StopConfirmation = &storage.RunStopConfirmation{Actor: *stopActor, Note: *stopNote, ConfirmedAt: *stopped, ProgramAttemptID: stopProgramAttempt}
		}
		return nil
	}()
	if err != nil {
		return storage.RunDispatchStatus{}, err
	}
	return result, nil
}

// AuthorizeRunDispatch is for an explicitly authorized operator action.
// The host must separately validate its native target and report actual startup.
func (s *Store) AuthorizeRunDispatch(ctx context.Context, runID, actor, packageID string, expectedTarget json.RawMessage) (storage.RunDispatch, error) {
	return s.authorizeRunDispatch(ctx, runID, actor, packageID, expectedTarget, "agent")
}

func (s *Store) authorizeRunDispatch(ctx context.Context, runID, actor, packageID string, expectedTarget json.RawMessage, executor string) (storage.RunDispatch, error) {
	if strings.TrimSpace(actor) == "" || packageID == "" || len(expectedTarget) == 0 || !json.Valid(expectedTarget) {
		return storage.RunDispatch{}, storage.ErrInvalidRunPreparation
	}
	if executor == "agent" {
		if err := checkDispatchPurpose(expectedTarget); err != nil {
			return storage.RunDispatch{}, err
		}
	}
	var result storage.RunDispatch
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		slug, err := s.RunBindingTask(txCtx, runID)
		if err != nil {
			return err
		}
		var role, stage string
		if scanErr := s.queryRow(txCtx, `SELECT role,stage FROM specs WHERE project_slug=$1 AND slug=$2 FOR UPDATE`, s.project, slug).Scan(&role, &stage); scanErr != nil {
			return fmt.Errorf("postgres: authorizeRunDispatch: %w", scanErr)
		}
		var environment, thread, state, storedPackage, workspace, kind string
		if scanErr := s.queryRow(txCtx, `SELECT environment_id,thread_ref,state,package_id,workspace,executor_kind FROM run_bindings WHERE project_slug=$1 AND id=$2 FOR UPDATE`, s.project, runID).Scan(&environment, &thread, &state, &storedPackage, &workspace, &kind); scanErr != nil {
			return fmt.Errorf("postgres: authorizeRunDispatch: %w", scanErr)
		}
		if kind != executor {
			return storage.ErrRunBindingConflict
		}
		if storedPackage != packageID {
			return storage.ErrRunBindingConflict
		}
		var targetBody []byte
		var same bool
		if scanErr := s.queryRow(txCtx, `SELECT body->(CASE WHEN $4='program' THEN 'program_target' ELSE 'dispatch_target' END),COALESCE(body->(CASE WHEN $4='program' THEN 'program_target' ELSE 'dispatch_target' END)=$3::jsonb,false) FROM context_packages WHERE project_slug=$1 AND id=$2`, s.project, packageID, expectedTarget, executor).Scan(&targetBody, &same); scanErr != nil {
			return fmt.Errorf("postgres: authorizeRunDispatch: %w", scanErr)
		}
		if !same {
			return storage.ErrRunBindingConflict
		}
		if executor == "agent" {
			if checkDispatchPurposeErr := checkDispatchPurpose(targetBody); checkDispatchPurposeErr != nil {
				return checkDispatchPurposeErr
			}
		} else {
			var target storage.ProgramTarget
			if json.Unmarshal(targetBody, &target) != nil {
				return storage.ErrInvalidProgramRun
			}
			if checkProgramCommandErr := checkProgramCommand(&target.ProgramCommand); checkProgramCommandErr != nil {
				return checkProgramCommandErr
			}
			if state != "prepared" || thread != "" || environment != target.EnvironmentID || workspace != target.Cwd {
				return storage.ErrRunBindingConflict
			}
		}
		if checkDispatchQABasisErr := s.checkDispatchQABasis(txCtx, targetBody); checkDispatchQABasisErr != nil {
			return checkDispatchQABasisErr
		}
		if executor == "agent" {
			var target struct {
				EnvironmentID   string `json:"environmentId"`
				ThreadID        string `json:"threadId"`
				Workspace       string `json:"workspace"`
				CreateCommandID string `json:"createCommandId"`
				StartCommandID  string `json:"startCommandId"`
				MessageID       string `json:"messageId"`
			}
			if json.Unmarshal(targetBody, &target) != nil || target.EnvironmentID == "" || target.ThreadID == "" || target.Workspace == "" || target.CreateCommandID == "" || target.StartCommandID == "" || target.MessageID == "" || target.CreateCommandID == target.StartCommandID {
				return storage.ErrInvalidRunPreparation
			}
			if environment != target.EnvironmentID || thread != target.ThreadID || workspace != target.Workspace {
				return storage.ErrRunBindingConflict
			}
		}
		var released *time.Time
		var flowBasis []byte
		var joinBasis []byte
		err = s.queryRow(txCtx, `SELECT id,actor,authorized_at,released_at,report_flow_basis,report_join_basis FROM run_dispatches WHERE project_slug=$1 AND run_id=$2`, s.project, runID).Scan(&result.ID, &result.Actor, &result.AuthorizedAt, &released, &flowBasis, &joinBasis)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("postgres: authorizeRunDispatch: %w", err)
		}
		existing := err == nil
		if existing && released != nil {
			return storage.ErrDispatchResolved
		}
		if existing && result.Actor != actor {
			return storage.ErrRunBindingConflict
		}
		if executor == "agent" && state != "bound" {
			return storage.ErrRunBindingConflict
		}
		if role == string(storage.SpecRoleSummary) {
			return storage.ErrSummaryNotExecutable
		}
		if stage != "approved" && stage != "in_progress" {
			return storage.ErrSpecNotApproved
		}
		candidateLoop := false
		if !existing {
			candidateLoop, err = s.candidateFirstAdmissionAllowed(txCtx, runID, slug)
			if err != nil {
				return err
			}
			if candidateLoop {
				if err := s.checkCandidateHumanHold(txCtx, slug); err != nil {
					return err
				}
				if err := s.checkCandidateScopeCurrent(txCtx, slug, packageID); err != nil {
					return err
				}
			}
		}
		if assertActiveClaimErr := s.assertActiveClaim(txCtx, slug, runID); assertActiveClaimErr != nil {
			return assertActiveClaimErr
		}
		if checkPreparedContractErr := s.checkPreparedContract(txCtx, slug, runID); checkPreparedContractErr != nil {
			return checkPreparedContractErr
		}
		result.RunID = runID
		result.TaskSlug = slug
		result.PackageID = packageID
		if existing {
			if len(flowBasis) != 0 {
				var proof storage.ReportBranchProof
				if err := json.Unmarshal(flowBasis, &proof); err != nil {
					return fmt.Errorf("postgres: decode report branch replay: %w", err)
				}
				result.ReportFlowBasis = &proof
			}
			if len(joinBasis) != 0 {
				var proof storage.ReportFlowJoinProof
				if err := json.Unmarshal(joinBasis, &proof); err != nil {
					return fmt.Errorf("postgres: decode report join replay: %w", err)
				}
				result.ReportJoinBasis = &proof
			}
			result.CandidateAttemptID, err = s.firstCandidateAttemptID(txCtx, runID)
			if err != nil {
				return err
			}
			result.Replayed = true
			return nil
		}
		if err := s.rejectNewNodeExecution(txCtx, slug); err != nil {
			return err
		}
		proof, err := s.reportBranchAdmissionBasis(txCtx, runID, packageID)
		if err != nil {
			return err
		}
		result.ReportFlowBasis = proof
		joinProof, err := s.reportJoinAdmissionBasis(txCtx, runID, packageID)
		if err != nil {
			return err
		}
		result.ReportJoinBasis = joinProof
		var held bool
		if scanErr := s.queryRow(txCtx, `SELECT EXISTS(SELECT 1 FROM run_dispatches WHERE project_slug=$1 AND task_slug=$2 AND released_at IS NULL)`, s.project, slug).Scan(&held); scanErr != nil {
			return fmt.Errorf("postgres: authorizeRunDispatch: %w", scanErr)
		}
		if held {
			return storage.ErrDispatchResponsibilityHeld
		}
		result.ID = newID("dsp")
		result.Actor = actor
		result.AuthorizedAt = s.now()
		var encodedProof []byte
		if proof != nil {
			encodedProof, err = json.Marshal(proof)
			if err != nil {
				return fmt.Errorf("postgres: encode report branch proof: %w", err)
			}
		}
		var encodedJoinProof []byte
		if joinProof != nil {
			encodedJoinProof, err = json.Marshal(joinProof)
			if err != nil {
				return fmt.Errorf("postgres: encode report join proof: %w", err)
			}
		}
		_, err = s.exec(txCtx, `INSERT INTO run_dispatches(id,project_slug,task_slug,run_id,package_id,actor,authorized_at,report_flow_basis,report_join_basis) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, result.ID, s.project, slug, runID, packageID, actor, result.AuthorizedAt, encodedProof, encodedJoinProof)
		if err != nil {
			return err
		}
		if candidateLoop {
			id := newID("cat")
			_, err = s.exec(txCtx, `INSERT INTO candidate_attempts(project_slug,id,run_id,ordinal,granted_at) VALUES($1,$2,$3,1,$4)`, s.project, id, runID, result.AuthorizedAt)
			if err != nil {
				return fmt.Errorf("postgres: grant first candidate attempt: %w", err)
			}
			result.CandidateAttemptID = &id
		}
		return nil
	})
	if err != nil {
		return storage.RunDispatch{}, err
	}
	return result, nil
}

// ResolveRunDispatch records an operator assertion, never fabricated host proof.
func (s *Store) ResolveRunDispatch(ctx context.Context, runID, admissionID, actor, kind, note string) error {
	if strings.TrimSpace(actor) == "" || strings.TrimSpace(note) == "" || utf8.RuneCountInString(note) > 4000 || (kind != "stopped_writing" && kind != "isolated_workspace") {
		return storage.ErrInvalidRunPreparation
	}
	return s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		var id, slug string
		var released *time.Time
		var priorActor, priorKind, priorNote *string
		err := s.queryRow(txCtx, `SELECT id,task_slug,released_at,release_actor,release_kind,release_note FROM run_dispatches WHERE project_slug=$1 AND run_id=$2`, s.project, runID).Scan(&id, &slug, &released, &priorActor, &priorKind, &priorNote)
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrRunBindingNotFound
		}
		if err != nil {
			return fmt.Errorf("postgres: ResolveRunDispatch: %w", err)
		}
		if id != admissionID {
			return storage.ErrRunBindingConflict
		}
		if released != nil {
			if *priorActor != actor || *priorKind != kind || *priorNote != note {
				return storage.ErrRunBindingConflict
			}
			return nil
		}
		if _, err = s.exec(txCtx, `UPDATE run_dispatches SET released_at=$3,release_actor=$4,release_kind=$5,release_note=$6 WHERE project_slug=$1 AND run_id=$2`, s.project, runID, s.now(), actor, kind, note); err != nil {
			return err
		}
		if _, err = s.exec(txCtx, `DELETE FROM claims WHERE project_slug=$1 AND spec_slug=$2 AND agent=$3`, s.project, slug, runID); err != nil {
			return err
		}
		if _, err = s.exec(txCtx, `DELETE FROM edges WHERE project_slug=$1 AND from_slug=$2 AND to_slug=$3 AND edge_type='CLAIMED_BY'`, s.project, slug, runID); err != nil {
			return err
		}
		_, err = s.exec(txCtx, `UPDATE run_bindings SET state=CASE WHEN state IN ('prepared','bound') THEN 'handed_off' ELSE state END,updated_at=$3 WHERE project_slug=$1 AND id=$2`, s.project, runID, s.now())
		return err
	})
}

// ConfirmRunStopped records an operator confirmation tied to the original host target.
// It neither stops a process nor substitutes workspace isolation for a stop observation.
func (s *Store) ConfirmRunStopped(ctx context.Context, runID, admissionID, environmentID, threadID, actor, note string) error {
	if admissionID == "" || !validMailText(environmentID, 256) || !validMailText(threadID, 256) || strings.TrimSpace(actor) == "" || strings.TrimSpace(note) == "" || utf8.RuneCountInString(note) > 4000 {
		return storage.ErrInvalidRunPreparation
	}
	return s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		var environment, thread string
		err := s.queryRow(txCtx, `SELECT environment_id,thread_ref FROM run_bindings WHERE project_slug=$1 AND id=$2 FOR UPDATE`, s.project, runID).Scan(&environment, &thread)
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrRunBindingNotFound
		}
		if err != nil {
			return fmt.Errorf("postgres: ConfirmRunStopped: %w", err)
		}
		if environment != environmentID || thread != threadID {
			return storage.ErrRunBindingConflict
		}
		var id string
		var released, stopped *time.Time
		var priorActor, priorNote *string
		err = s.queryRow(txCtx, `SELECT id,released_at,stop_confirmed_at,stop_actor,stop_note FROM run_dispatches WHERE project_slug=$1 AND run_id=$2`, s.project, runID).Scan(&id, &released, &stopped, &priorActor, &priorNote)
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrRunBindingNotFound
		}
		if err != nil {
			return fmt.Errorf("postgres: ConfirmRunStopped: %w", err)
		}
		if id != admissionID {
			return storage.ErrRunBindingConflict
		}
		if stopped != nil {
			if *priorActor != actor || *priorNote != note {
				return storage.ErrRunBindingConflict
			}
			return nil
		}
		if released == nil {
			// Nested storage calls join this transaction and retain the project lock.
			if resolveRunDispatchErr := s.ResolveRunDispatch(txCtx, runID, admissionID, actor, "stopped_writing", note); resolveRunDispatchErr != nil {
				return resolveRunDispatchErr
			}
		}
		_, err = s.exec(txCtx, `UPDATE run_dispatches SET stop_confirmed_at=$3,stop_actor=$4,stop_note=$5 WHERE project_slug=$1 AND run_id=$2`, s.project, runID, s.now(), actor, note)
		return err
	})
}

// ConfirmProgramRunStopped records a human's observed stop for one exact current program attempt.
func (s *Store) ConfirmProgramRunStopped(ctx context.Context, req storage.ConfirmProgramRunStoppedRequest) (storage.RunDispatchStatus, error) {
	if !validMailText(req.RunID, 256) || !validMailText(req.AdmissionID, 256) || !validMailText(req.AttemptID, 256) ||
		!validMailText(req.EnvironmentID, 256) || !validMailText(req.NativeProjectID, 256) || strings.TrimSpace(req.Note) == "" || utf8.RuneCountInString(req.Note) > 4000 {
		return storage.RunDispatchStatus{}, storage.ErrInvalidRunPreparation
	}
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		actor, err := s.currentHumanOperator(txCtx)
		if err != nil {
			return err
		}
		var executor, environment, nativeProject, attempt, admission string
		var stopped, released *time.Time
		var priorActor, priorNote, priorAttempt *string
		err = s.queryRow(txCtx, `SELECT b.executor_kind,b.environment_id,cp.body->'program_target'->>'nativeProjectId',a.id,d.id,
 d.stop_confirmed_at,d.released_at,d.stop_actor,d.stop_note,d.stop_program_attempt_id FROM run_bindings b
 JOIN context_packages cp ON cp.project_slug=b.project_slug AND cp.id=b.package_id
 JOIN run_dispatches d ON d.project_slug=b.project_slug AND d.run_id=b.id
 LEFT JOIN LATERAL (SELECT id FROM program_attempts WHERE project_slug=b.project_slug AND run_id=b.id ORDER BY ordinal DESC LIMIT 1) a ON true
 WHERE b.project_slug=$1 AND b.id=$2 FOR UPDATE OF b`, s.project, req.RunID).
			Scan(&executor, &environment, &nativeProject, &attempt, &admission, &stopped, &released, &priorActor, &priorNote, &priorAttempt)
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrRunBindingNotFound
		}
		if err != nil {
			return fmt.Errorf("postgres: read exact program stop: %w", err)
		}
		if executor != "program" || environment != req.EnvironmentID || nativeProject != req.NativeProjectID || attempt != req.AttemptID || admission != req.AdmissionID {
			return storage.ErrRunBindingConflict
		}
		if stopped != nil {
			if priorActor == nil || *priorActor != actor || priorNote == nil || *priorNote != req.Note || priorAttempt == nil || *priorAttempt != req.AttemptID {
				return storage.ErrRunBindingConflict
			}
			return nil
		}
		if released == nil {
			if err := s.ResolveRunDispatch(txCtx, req.RunID, req.AdmissionID, actor, "stopped_writing", req.Note); err != nil {
				return err
			}
		}
		_, err = s.exec(txCtx, `UPDATE run_dispatches SET stop_confirmed_at=$3,stop_actor=$4,stop_note=$5,stop_program_attempt_id=$6 WHERE project_slug=$1 AND run_id=$2`,
			s.project, req.RunID, s.now(), actor, req.Note, req.AttemptID)
		return err
	})
	if err != nil {
		return storage.RunDispatchStatus{}, err
	}
	return s.ReadRunDispatch(ctx, req.RunID)
}

func (s *Store) hasDispatchResponsibility(ctx context.Context, slug, exceptRun string) (bool, error) {
	var held bool
	err := s.queryRow(ctx, `SELECT EXISTS(SELECT 1 FROM run_dispatches WHERE project_slug=$1 AND task_slug=$2 AND run_id<>$3 AND released_at IS NULL)`, s.project, slug, exceptRun).Scan(&held)
	if err != nil {
		return false, fmt.Errorf("postgres: dispatch responsibility: %w", err)
	}
	return held, nil
}
