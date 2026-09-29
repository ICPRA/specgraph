// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
)

func checkProgramLoop(loop *storage.ProgramLoopConfig) error {
	if loop == nil {
		return nil
	}
	if (loop.Kind != "mechanical" && loop.Kind != "innovative") || !validMailText(loop.SynchronousCompletionBasis, 4000) ||
		(loop.MaxAttempts != nil && *loop.MaxAttempts <= 0) || (loop.Kind == "innovative" && loop.MaxAttempts == nil) {
		return storage.ErrInvalidProgramLoop
	}
	t := loop.Termination
	switch t.Kind {
	case "exit_code":
		if t.ExitCode == nil || t.Judgment != nil || t.ExitCode.ContinueCodes == nil {
			return storage.ErrInvalidProgramLoop
		}
		seen := map[int64]bool{t.ExitCode.StopCode: true}
		for _, code := range append([]int64{t.ExitCode.StopCode}, t.ExitCode.ContinueCodes...) {
			if code < -2147483648 || code > 4294967295 {
				return storage.ErrInvalidProgramLoop
			}
		}
		for _, code := range t.ExitCode.ContinueCodes {
			if seen[code] {
				return storage.ErrInvalidProgramLoop
			}
			seen[code] = true
		}
	case "judgment":
		if t.Judgment == nil || t.ExitCode != nil || !validMailText(t.Judgment.Criterion, 4000) {
			return storage.ErrInvalidProgramLoop
		}
	default:
		return storage.ErrInvalidProgramLoop
	}
	return nil
}

const programLoopEventColumns = `id,run_id,COALESCE(attempt_id,''),kind,COALESCE(value,''),actor_user_id,COALESCE(actor_run_id,''),reason,recorded_at,predecessor_id`

func scanProgramLoopEvent(row pgx.Row) (*storage.ProgramLoopEvent, error) {
	e := &storage.ProgramLoopEvent{}
	err := row.Scan(&e.ID, &e.RunID, &e.AttemptID, &e.Kind, &e.Value, &e.ActorUserID, &e.ActorRunID, &e.Reason, &e.RecordedAt, &e.PredecessorID)
	if err != nil {
		return e, fmt.Errorf("scan program loop event: %w", err)
	}
	return e, nil
}

func normalProgramExit(result *storage.ProgramRunResult) bool {
	return result != nil && result.Outcome == "exited" && result.ExitCode != nil && result.FinishedAt != nil && result.TimedOutAt == nil && result.CancelRequestedAt == nil
}

// This is a projection of grants and observed facts, never a claim that a process is running.
func deriveProgramLoopState(loop *storage.ProgramLoopConfig, attempt *storage.ProgramAttemptMetadata, result *storage.ProgramRunResult, stop, judgment *storage.ProgramLoopEvent, completion *storage.ProgramCompletion) *storage.ProgramLoopState {
	if loop == nil {
		return nil
	}
	state := &storage.ProgramLoopState{Status: "prepared", Completion: completion}
	if stop != nil {
		state.Stop = stop
		state.Status = "cancelled"
	}
	if attempt == nil {
		if completion != nil {
			state.Status = "completed"
		}
		return state
	}
	condition := &storage.ProgramLoopCondition{Value: "unknown"}
	state.Condition = condition
	if loop.Termination.Kind == "exit_code" {
		if normalProgramExit(result) {
			code := *result.ExitCode
			if code == loop.Termination.ExitCode.StopCode {
				condition.Value = "true"
			} else {
				for _, next := range loop.Termination.ExitCode.ContinueCodes {
					if code == next {
						condition.Value = "false"
						break
					}
				}
			}
		}
	} else if judgment != nil {
		condition.Value = judgment.Value
		condition.JudgmentID = judgment.ID
	}
	if state.Stop != nil {
		if completion != nil {
			state.Status = "completed"
		}
		return state
	}
	switch {
	case condition.Value == "true":
		state.Status = "stopped"
	case loop.MaxAttempts != nil && attempt.Ordinal >= *loop.MaxAttempts && result != nil:
		state.Status = "needs_human"
	case result == nil:
		state.Status = "admitted"
	case !normalProgramExit(result) || condition.Value == "unknown":
		state.Status = "waiting"
	default:
		state.Status = "ready"
	}
	if completion != nil {
		state.Status = "completed"
	}
	return state
}

func (s *Store) projectProgramLoop(ctx context.Context, r *programRunRecord) error {
	if r.target.Loop == nil {
		return nil
	}
	stop, err := scanProgramLoopEvent(s.queryRow(ctx, `SELECT `+programLoopEventColumns+` FROM program_loop_events WHERE project_slug=$1 AND run_id=$2 AND kind='stop' ORDER BY sequence LIMIT 1`, s.project, r.Context.RunID))
	if errors.Is(err, pgx.ErrNoRows) {
		stop = nil
	} else if err != nil {
		return err
	}
	var judgment *storage.ProgramLoopEvent
	if r.CurrentAttempt != nil && r.target.Loop.Termination.Kind == "judgment" {
		judgment, err = scanProgramLoopEvent(s.queryRow(ctx, `SELECT `+programLoopEventColumns+` FROM program_loop_events WHERE project_slug=$1 AND run_id=$2 AND attempt_id=$3 AND kind='condition' ORDER BY sequence DESC LIMIT 1`, s.project, r.Context.RunID, r.CurrentAttempt.ID))
		if errors.Is(err, pgx.ErrNoRows) {
			judgment = nil
		} else if err != nil {
			return err
		}
	}
	r.LoopState = deriveProgramLoopState(r.target.Loop, r.CurrentAttempt, r.Result, stop, judgment, r.Completion)
	return nil
}

// ReadProgramLoop returns the current run and its derived loop state in one snapshot.
func (s *Store) ReadProgramLoop(ctx context.Context, runID string) (*storage.ProgramRun, error) {
	if !validMailText(runID, 256) {
		return nil, storage.ErrInvalidProgramLoop
	}
	var result *storage.ProgramRun
	err := s.RunReadSnapshot(ctx, func(txCtx context.Context) error {
		r, err := s.programRun(txCtx, runID)
		if err != nil {
			return err
		}
		result = &r.ProgramRun
		return nil
	})
	return result, err
}

func (s *Store) checkProgramContinuation(ctx context.Context, r *programRunRecord) error {
	a := r.Dispatch.Admission
	if a == nil || a.PackageID != r.Context.PackageID || a.Actor != r.target.AuthorizedByUserID || r.Dispatch.Resolution != nil || r.Dispatch.Cancellation != nil || r.State == "completed" {
		return storage.ErrRunBindingConflict
	}
	user, err := s.ExistingAuth().GetUserByID(ctx, r.target.AuthorizedByUserID)
	if err != nil {
		return err
	}
	if user.Kind != storage.KindHuman || user.DeletedAt != nil {
		return storage.ErrProgramRunForbidden
	}
	if commandErr := checkProgramCommand(&r.target.ProgramCommand); commandErr != nil {
		return commandErr
	}
	var stage, role, environment, workspace, thread string
	if bindingErr := s.queryRow(ctx, `SELECT s.stage,s.role,b.environment_id,b.workspace,b.thread_ref FROM specs s JOIN run_bindings b ON b.project_slug=s.project_slug AND b.task_spec_slug=s.slug WHERE b.project_slug=$1 AND b.id=$2`, s.project, r.Context.RunID).Scan(&stage, &role, &environment, &workspace, &thread); bindingErr != nil {
		return fmt.Errorf("read program run %s continuation binding: %w", r.Context.RunID, bindingErr)
	}
	if role != "work" || (stage != "approved" && stage != "in_progress") || environment != r.target.EnvironmentID || workspace != r.target.Cwd || thread != "" {
		return storage.ErrRunBindingConflict
	}
	var competing, completed bool
	if ownershipErr := s.queryRow(ctx, `SELECT EXISTS(SELECT 1 FROM claims WHERE project_slug=$1 AND spec_slug=$2 AND agent<>$3) OR EXISTS(SELECT 1 FROM run_dispatches WHERE project_slug=$1 AND task_slug=$2 AND run_id<>$3 AND released_at IS NULL), EXISTS(SELECT 1 FROM execution_events WHERE project_slug=$1 AND spec_slug=$2 AND agent=$3 AND event_type='completion')`, s.project, r.Context.TaskSlug, r.Context.RunID).Scan(&competing, &completed); ownershipErr != nil {
		return fmt.Errorf("check program run %s continuation ownership: %w", r.Context.RunID, ownershipErr)
	}
	if competing || completed {
		return storage.ErrRunBindingConflict
	}
	if contractErr := s.checkPreparedContract(ctx, r.Context.TaskSlug, r.Context.RunID); contractErr != nil {
		return contractErr
	}
	if scopeErr := s.checkProgramScopeCurrent(ctx, r); scopeErr != nil {
		return scopeErr
	}
	if qaErr := s.checkDispatchQABasis(ctx, r.targetBody); qaErr != nil {
		return qaErr
	}
	review, err := s.ReadReviewStatus(ctx, r.Context.TaskSlug)
	if err != nil {
		return err
	}
	for _, state := range review.Reviews {
		if state.HumanHold {
			return storage.ErrReviewHumanHold
		}
	}
	var hookID string
	if hookErr := s.queryRow(ctx, `SELECT COALESCE((SELECT id FROM delivery_test_hooks WHERE project_slug=$1 AND target_run_id=$2 AND hook_kind='completion_program' AND program_admission_id=$3),'')`, s.project, r.Context.RunID, a.ID).Scan(&hookID); hookErr != nil {
		return fmt.Errorf("read program run %s continuation hook: %w", r.Context.RunID, hookErr)
	}
	if hookID != "" {
		hook, err := s.completionHook(ctx, hookID)
		if err != nil {
			return err
		}
		if hook.CancelledAt != nil {
			return storage.ErrCompletionHookConflict
		}
		if err := s.checkCompletionHookSource(ctx, hook); err != nil {
			return err
		}
		if err := s.checkCompletionHookDeadlock(ctx, hook.SourceTaskSlug, r.Context.TaskSlug); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) checkProgramScopeCurrent(ctx context.Context, r *programRunRecord) error {
	var prepared struct {
		Scope json.RawMessage `json:"program_loop_scope"`
	}
	if err := json.Unmarshal(r.Context.Body, &prepared); err != nil {
		return fmt.Errorf("decode program run %s prepared loop scope: %w", r.Context.RunID, err)
	}
	current, err := s.workCompletionScopeReferences(ctx, r.Context.TaskSlug)
	if err != nil {
		return err
	}
	var same bool
	if scopeErr := s.queryRow(ctx, `SELECT COALESCE($1::jsonb=$2::jsonb,false)`, prepared.Scope, current).Scan(&same); scopeErr != nil {
		return fmt.Errorf("compare program run %s loop scope: %w", r.Context.RunID, scopeErr)
	}
	if !same {
		return storage.ErrCompletionRequiresRequirementReview
	}
	return nil
}

// AuthorizeNextProgramAttempt grants the successor of an eligible exact attempt;
// replay observes the existing successor without permitting another execution.
func (s *Store) AuthorizeNextProgramAttempt(ctx context.Context, scope storage.ProgramHostScope, runID, expectedPreviousAttemptID string) (*storage.ProgramAdmission, error) {
	if !validMailText(runID, 256) || !validMailText(expectedPreviousAttemptID, 256) {
		return nil, storage.ErrInvalidProgramLoop
	}
	var result *storage.ProgramAdmission
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		r, err := s.programRun(txCtx, runID)
		if err != nil {
			return err
		}
		if hostErr := checkProgramHost(txCtx, scope, &r.target); hostErr != nil {
			return hostErr
		}
		if r.target.Loop == nil || r.Dispatch.Admission == nil || r.Dispatch.Resolution != nil {
			return storage.ErrProgramLoopConflict
		}
		var successor string
		err = s.queryRow(txCtx, `SELECT id FROM program_attempts WHERE project_slug=$1 AND run_id=$2 AND predecessor_id=$3`, s.project, runID, expectedPreviousAttemptID).Scan(&successor)
		if err == nil {
			if projectionErr := s.projectProgramAttempt(txCtx, r, successor); projectionErr != nil {
				return projectionErr
			}
			r.Replayed = true
			result = &storage.ProgramAdmission{Run: &r.ProgramRun, MayStart: false}
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("read program run %s attempt %s successor: %w", runID, expectedPreviousAttemptID, err)
		}
		if r.CurrentAttempt == nil || r.CurrentAttempt.ID != expectedPreviousAttemptID {
			return storage.ErrProgramLoopConflict
		}
		if err := s.rejectNewNodeExecution(txCtx, r.Context.TaskSlug); err != nil {
			return err
		}
		switch r.LoopState.Status {
		case "cancelled", "stopped", "completed":
			return storage.ErrProgramLoopStopped
		case "needs_human":
			return storage.ErrProgramLoopNeedsHuman
		case "ready":
		default:
			return storage.ErrProgramLoopWaiting
		}
		if err := s.checkProgramContinuation(txCtx, r); err != nil {
			return err
		}
		meta := storage.ProgramAttemptMetadata{ID: newID("pat"), Ordinal: r.CurrentAttempt.Ordinal + 1, GrantedAt: s.now(), PredecessorID: &expectedPreviousAttemptID}
		if r.LoopState.Condition.JudgmentID != "" {
			value := r.LoopState.Condition.JudgmentID
			meta.ConsumedJudgmentID = &value
		}
		if _, err := s.exec(txCtx, `INSERT INTO program_attempts(id,project_slug,run_id,admission_id,ordinal,granted_at,predecessor_id,consumed_judgment_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, meta.ID, s.project, runID, r.Dispatch.Admission.ID, meta.Ordinal, meta.GrantedAt, meta.PredecessorID, meta.ConsumedJudgmentID); err != nil {
			return err
		}
		if _, err := s.exec(txCtx, `UPDATE run_bindings SET state='admitted',updated_at=$3 WHERE project_slug=$1 AND id=$2`, s.project, runID, meta.GrantedAt); err != nil {
			return err
		}
		r.CurrentAttempt = &meta
		r.attemptID = meta.ID
		r.Result = nil
		r.State = "admitted"
		if err := s.projectProgramLoop(txCtx, r); err != nil {
			return err
		}
		result = &storage.ProgramAdmission{Run: &r.ProgramRun, MayStart: true}
		return nil
	})
	return result, err
}

func (s *Store) projectProgramAttempt(ctx context.Context, r *programRunRecord, id string) error {
	meta := &storage.ProgramAttemptMetadata{}
	var body []byte
	if err := s.queryRow(ctx, `SELECT id,ordinal,granted_at,predecessor_id,consumed_judgment_id,result FROM program_attempts WHERE project_slug=$1 AND run_id=$2 AND id=$3`, s.project, r.Context.RunID, id).Scan(&meta.ID, &meta.Ordinal, &meta.GrantedAt, &meta.PredecessorID, &meta.ConsumedJudgmentID, &body); err != nil {
		return fmt.Errorf("read program run %s attempt %s: %w", r.Context.RunID, id, err)
	}
	r.CurrentAttempt = meta
	r.attemptID = id
	r.Result = nil
	if len(body) > 0 {
		if err := json.Unmarshal(body, &r.Result); err != nil {
			return fmt.Errorf("decode program run %s attempt %s result: %w", r.Context.RunID, id, err)
		}
	}
	return s.projectProgramLoop(ctx, r)
}

// RecordProgramLoopJudgment records the authorizing human's judgment for an exact attempt.
func (s *Store) RecordProgramLoopJudgment(ctx context.Context, runID, attemptID, value, reason string, expectedJudgmentID *string) (*storage.ProgramLoopEvent, error) {
	return s.recordProgramLoopEvent(ctx, nil, nil, runID, attemptID, value, reason, expectedJudgmentID)
}

// PlanningRecordProgramLoopJudgment records a scoped planning manager's judgment for an exact attempt.
func (s *Store) PlanningRecordProgramLoopJudgment(ctx context.Context, scope storage.MailScope, runID, attemptID, value, reason string, expectedJudgmentID *string) (*storage.ProgramLoopEvent, error) {
	return s.recordProgramLoopEvent(ctx, &scope, nil, runID, attemptID, value, reason, expectedJudgmentID)
}

// StopProgramLoop persists the authorizing human's stop request for the run.
func (s *Store) StopProgramLoop(ctx context.Context, runID, reason string) (*storage.ProgramLoopEvent, error) {
	return s.recordProgramLoopEvent(ctx, nil, nil, runID, "", "", reason, nil)
}

// PlanningStopProgramLoop persists a scoped planning manager's stop request for the run.
func (s *Store) PlanningStopProgramLoop(ctx context.Context, scope storage.MailScope, runID, reason string) (*storage.ProgramLoopEvent, error) {
	return s.recordProgramLoopEvent(ctx, &scope, nil, runID, "", "", reason, nil)
}

// HostStopProgramLoop persists a stop request after checking the program host scope.
func (s *Store) HostStopProgramLoop(ctx context.Context, scope storage.ProgramHostScope, runID, reason string) (*storage.ProgramLoopEvent, error) {
	return s.recordProgramLoopEvent(ctx, nil, &scope, runID, "", "", reason, nil)
}

func (s *Store) recordProgramLoopEvent(ctx context.Context, managerScope *storage.MailScope, hostScope *storage.ProgramHostScope, runID, attemptID, value, reason string, expectedJudgmentID *string) (*storage.ProgramLoopEvent, error) {
	if !validMailText(runID, 256) || !validMailText(reason, 4000) || (attemptID != "" && (!validMailText(attemptID, 256) || (value != "true" && value != "false" && value != "unknown") || (expectedJudgmentID != nil && !validMailText(*expectedJudgmentID, 256)))) {
		return nil, storage.ErrInvalidProgramLoop
	}
	var result *storage.ProgramLoopEvent
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		r, err := s.programRun(txCtx, runID)
		if err != nil {
			return err
		}
		if r.target.Loop == nil {
			return storage.ErrProgramLoopConflict
		}
		identity, ok := auth.IdentityFromContext(txCtx)
		if !ok || identity.UserID == "" {
			return storage.ErrProgramRunForbidden
		}
		actorRun := ""
		switch {
		case managerScope != nil:
			actorRun, err = s.PlanningManagerRun(txCtx, *managerScope)
			if err != nil {
				return err
			}
		case hostScope != nil:
			if attemptID != "" {
				return storage.ErrProgramRunForbidden
			}
			if hostErr := checkProgramHost(txCtx, *hostScope, &r.target); hostErr != nil {
				return hostErr
			}
		case identity.UserKind != storage.KindHuman || identity.UserID != r.target.AuthorizedByUserID:
			return storage.ErrProgramRunForbidden
		}
		account, err := s.ExistingAuth().GetUserByID(txCtx, identity.UserID)
		if err != nil {
			return err
		}
		if account.DeletedAt != nil {
			return storage.ErrProgramRunForbidden
		}
		kind := "stop"
		if attemptID != "" {
			kind = "condition"
			if r.target.Loop.Termination.Kind != "judgment" || r.Dispatch.Admission == nil {
				return storage.ErrProgramLoopConflict
			}
			var exists bool
			if attemptErr := s.queryRow(txCtx, `SELECT EXISTS(SELECT 1 FROM program_attempts WHERE project_slug=$1 AND run_id=$2 AND id=$3)`, s.project, runID, attemptID).Scan(&exists); attemptErr != nil {
				return fmt.Errorf("check program run %s judgment attempt %s: %w", runID, attemptID, attemptErr)
			}
			if !exists {
				return storage.ErrProgramLoopConflict
			}
		}
		if kind == "stop" {
			prior, err := scanProgramLoopEvent(s.queryRow(txCtx, `SELECT `+programLoopEventColumns+` FROM program_loop_events WHERE project_slug=$1 AND run_id=$2 AND kind='stop' ORDER BY sequence LIMIT 1`, s.project, runID))
			if err == nil {
				result = prior
				return nil
			}
			if !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
		} else {
			prior, err := scanProgramLoopEvent(s.queryRow(txCtx, `SELECT `+programLoopEventColumns+` FROM program_loop_events
 WHERE project_slug=$1 AND run_id=$2 AND attempt_id=$3 AND kind='condition' AND predecessor_id IS NOT DISTINCT FROM $4::text`, s.project, runID, attemptID, expectedJudgmentID))
			if err == nil {
				if prior.Value != value || prior.Reason != reason || prior.ActorUserID != identity.UserID || prior.ActorRunID != actorRun {
					return storage.ErrProgramLoopConflict
				}
				result = prior
				return nil
			}
			if !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
			var latest string
			err = s.queryRow(txCtx, `SELECT id FROM program_loop_events WHERE project_slug=$1 AND run_id=$2 AND attempt_id=$3 AND kind='condition' ORDER BY sequence DESC LIMIT 1`, s.project, runID, attemptID).Scan(&latest)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("read program run %s latest judgment: %w", runID, err)
			}
			if (err == nil && (expectedJudgmentID == nil || *expectedJudgmentID != latest)) || (errors.Is(err, pgx.ErrNoRows) && expectedJudgmentID != nil) {
				return storage.ErrProgramLoopConflict
			}
		}
		result = &storage.ProgramLoopEvent{ID: newID("ple"), RunID: runID, AttemptID: attemptID, Kind: kind, Value: value, ActorUserID: identity.UserID, ActorRunID: actorRun, Reason: reason, RecordedAt: s.now(), PredecessorID: expectedJudgmentID}
		_, err = s.exec(txCtx, `INSERT INTO program_loop_events(id,project_slug,run_id,attempt_id,kind,value,actor_user_id,actor_run_id,reason,recorded_at,predecessor_id)
 VALUES($1,$2,$3,NULLIF($4,''),$5,NULLIF($6,''),$7,NULLIF($8,''),$9,$10,$11)`, result.ID, s.project, runID, attemptID, kind, value, identity.UserID, actorRun, reason, result.RecordedAt, expectedJudgmentID)
		return err
	})
	return result, err
}

// ReadProgramLoopHistory returns independently paged attempts and loop events for a run.
func (s *Store) ReadProgramLoopHistory(ctx context.Context, runID, attemptCursor, eventCursor string) (storage.ProgramLoopHistoryPage, error) {
	return s.readProgramLoopHistory(ctx, nil, runID, attemptCursor, eventCursor)
}

// HostReadProgramLoopHistory reads paged history after checking the program host scope.
func (s *Store) HostReadProgramLoopHistory(ctx context.Context, scope storage.ProgramHostScope, runID, attemptCursor, eventCursor string) (storage.ProgramLoopHistoryPage, error) {
	return s.readProgramLoopHistory(ctx, &scope, runID, attemptCursor, eventCursor)
}

func (s *Store) readProgramLoopHistory(ctx context.Context, scope *storage.ProgramHostScope, runID, attemptCursor, eventCursor string) (storage.ProgramLoopHistoryPage, error) {
	page := storage.ProgramLoopHistoryPage{Attempts: []storage.ProgramAttempt{}, Events: []storage.ProgramLoopEvent{}}
	ordinal := 0
	if attemptCursor != "" {
		value, err := strconv.Atoi(attemptCursor)
		if err != nil || value < 1 {
			return page, storage.ErrInvalidProgramLoop
		}
		ordinal = value
	}
	if !validMailText(runID, 256) || (eventCursor != "" && !validMailText(eventCursor, 256)) {
		return page, storage.ErrInvalidProgramLoop
	}
	err := s.RunReadSnapshot(ctx, func(txCtx context.Context) error {
		r, err := s.programRun(txCtx, runID)
		if err != nil {
			return err
		}
		if scope != nil {
			if hostErr := checkProgramHost(txCtx, *scope, &r.target); hostErr != nil {
				return hostErr
			}
		}
		rows, err := s.query(txCtx, `SELECT id,ordinal,granted_at,predecessor_id,consumed_judgment_id,result FROM program_attempts WHERE project_slug=$1 AND run_id=$2 AND ordinal>$3 ORDER BY ordinal LIMIT 51`, s.project, runID, ordinal)
		if err != nil {
			return err
		}
		for rows.Next() {
			var a storage.ProgramAttempt
			var body []byte
			if scanErr := rows.Scan(&a.ID, &a.Ordinal, &a.GrantedAt, &a.PredecessorID, &a.ConsumedJudgmentID, &body); scanErr != nil {
				rows.Close()
				return fmt.Errorf("scan program run %s attempt history: %w", runID, scanErr)
			}
			if len(body) > 0 {
				if decodeErr := json.Unmarshal(body, &a.Result); decodeErr != nil {
					rows.Close()
					return fmt.Errorf("decode program run %s attempt %s history result: %w", runID, a.ID, decodeErr)
				}
			}
			page.Attempts = append(page.Attempts, a)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return fmt.Errorf("iterate program run %s attempt history: %w", runID, err)
		}
		if len(page.Attempts) > 50 {
			page.NextAttemptCursor = strconv.Itoa(page.Attempts[49].Ordinal)
			page.Attempts = page.Attempts[:50]
		}
		if eventCursor != "" {
			var exists bool
			if cursorErr := s.queryRow(txCtx, `SELECT EXISTS(SELECT 1 FROM program_loop_events WHERE project_slug=$1 AND run_id=$2 AND id=$3)`, s.project, runID, eventCursor).Scan(&exists); cursorErr != nil {
				return fmt.Errorf("check program run %s event history cursor %s: %w", runID, eventCursor, cursorErr)
			}
			if !exists {
				return storage.ErrInvalidProgramLoop
			}
		}
		rows, err = s.query(txCtx, `SELECT `+programLoopEventColumns+` FROM program_loop_events WHERE project_slug=$1 AND run_id=$2 AND sequence>COALESCE((SELECT sequence FROM program_loop_events WHERE project_slug=$1 AND run_id=$2 AND id=$3),0) ORDER BY sequence LIMIT 51`, s.project, runID, eventCursor)
		if err != nil {
			return err
		}
		for rows.Next() {
			e, scanErr := scanProgramLoopEvent(rows)
			if scanErr != nil {
				rows.Close()
				return scanErr
			}
			page.Events = append(page.Events, *e)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return fmt.Errorf("iterate program run %s event history: %w", runID, err)
		}
		if len(page.Events) > 50 {
			page.NextEventCursor = page.Events[49].ID
			page.Events = page.Events[:50]
		}
		return nil
	})
	return page, err
}
