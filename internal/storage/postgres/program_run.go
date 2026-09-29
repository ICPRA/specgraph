// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
)

func checkProgramCommand(command *storage.ProgramCommand) error {
	if err := checkProgramLoop(command.Loop); err != nil {
		return err
	}
	if !validMailText(command.Executable, 4096) || !reviewRootPattern.MatchString(command.Executable) || strings.ContainsAny(command.Executable, "\r\n") ||
		!validMailText(command.Cwd, 4096) || !reviewRootPattern.MatchString(command.Cwd) || strings.ContainsAny(command.Cwd, "\r\n") || command.Args == nil ||
		!validMailText(command.EnvironmentID, 256) || !validMailText(command.NativeProjectID, 256) || command.TimeoutMS <= 0 {
		return storage.ErrInvalidProgramRun
	}
	for _, arg := range command.Args {
		if !utf8.ValidString(arg) || strings.ContainsRune(arg, 0) {
			return storage.ErrInvalidProgramRun
		}
	}
	if err := checkRunWorkPurpose(command.WorkPurpose); err != nil {
		return err
	}
	if command.CompleteOnSuccess {
		switch command.WorkPurpose {
		case "test_design", "test_execution", "requirements_review", "design_review", "investigation", "coordination", "knowledge":
		default:
			return storage.ErrProgramCompletionUnavailable
		}
	}
	encoded, err := json.Marshal(command)
	if err != nil || len(encoded) > 24<<10 {
		return storage.ErrInvalidProgramRun
	}
	return nil
}

type programRunRecord struct {
	storage.ProgramRun
	target     storage.ProgramTarget
	targetBody json.RawMessage
	attemptID  string
}

func (s *Store) programRun(ctx context.Context, id string) (*programRunRecord, error) {
	r := &programRunRecord{}
	var err error
	r.Context, r.Dispatch, err = s.ReadRunPreparation(ctx, id, "")
	if err != nil {
		return nil, err
	}
	var encoded []byte
	var ordinal *int
	var granted *time.Time
	var predecessor, consumed *string
	err = s.queryRow(ctx, `SELECT b.executor_kind,b.state,COALESCE(a.id,''),a.result,a.ordinal,a.granted_at,a.predecessor_id,a.consumed_judgment_id
 FROM run_bindings b LEFT JOIN LATERAL (SELECT * FROM program_attempts WHERE project_slug=b.project_slug AND run_id=b.id ORDER BY ordinal DESC LIMIT 1) a ON true
 WHERE b.project_slug=$1 AND b.id=$2`, s.project, id).Scan(&r.ExecutorKind, &r.State, &r.attemptID, &encoded, &ordinal, &granted, &predecessor, &consumed)
	if err != nil {
		return nil, fmt.Errorf("read program run %s latest attempt: %w", id, err)
	}
	if r.ExecutorKind != "program" {
		return nil, storage.ErrInvalidProgramRun
	}
	if r.Dispatch.Admission != nil && r.attemptID == "" {
		return nil, storage.ErrRunBindingConflict
	}
	var body struct {
		Target json.RawMessage `json:"program_target"`
	}
	if json.Unmarshal(r.Context.Body, &body) != nil || json.Unmarshal(body.Target, &r.target) != nil {
		return nil, storage.ErrInvalidProgramRun
	}
	r.targetBody = body.Target
	if len(encoded) != 0 {
		if decodeErr := json.Unmarshal(encoded, &r.Result); decodeErr != nil {
			return nil, fmt.Errorf("decode program run %s latest attempt result: %w", id, decodeErr)
		}
	}
	if ordinal != nil {
		r.CurrentAttempt = &storage.ProgramAttemptMetadata{ID: r.attemptID, Ordinal: *ordinal, GrantedAt: *granted, PredecessorID: predecessor, ConsumedJudgmentID: consumed}
	}
	var completionID string
	var completionBasis []byte
	err = s.queryRow(ctx, `SELECT id,program_completion_basis FROM execution_events WHERE project_slug=$1 AND spec_slug=$2 AND agent=$3 AND event_type='completion' ORDER BY created_at DESC,id DESC LIMIT 1`, s.project, r.Context.TaskSlug, id).Scan(&completionID, &completionBasis)
	if err == nil {
		r.Completion = &storage.ProgramCompletion{EventID: completionID}
		if len(completionBasis) != 0 {
			if err := json.Unmarshal(completionBasis, r.Completion); err != nil {
				return nil, fmt.Errorf("decode program run %s completion basis: %w", id, err)
			}
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("read program run %s completion: %w", id, err)
	}
	if err := s.projectProgramLoop(ctx, r); err != nil {
		return nil, err
	}
	return r, nil
}

// PrepareProgramRun requires the caller's transport to authorize human program preparation. No route or
// physical program execution is enabled by this storage operation.
func (s *Store) PrepareProgramRun(ctx context.Context, req *storage.PrepareProgramRunRequest, consumer *auth.Identity) (*storage.ProgramRun, error) {
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" || identity.UserKind != storage.KindHuman || consumer == nil || consumer.UserID == "" || consumer.Source != "apikey" || consumer.UserKind != storage.KindServiceAccount {
		return nil, storage.ErrProgramRunForbidden
	}
	if !validMailText(req.TaskSlug, 256) || !subdivisionKeyPattern.MatchString(req.IdempotencyKey) {
		return nil, storage.ErrInvalidProgramRun
	}
	if err := checkProgramCommand(&req.Command); err != nil {
		return nil, err
	}
	for _, actor := range []struct {
		id   string
		kind storage.Kind
	}{{identity.UserID, storage.KindHuman}, {consumer.UserID, storage.KindServiceAccount}} {
		user, err := s.ExistingAuth().GetUserByID(ctx, actor.id)
		if err != nil {
			return nil, err
		}
		if user.Kind != actor.kind || user.DeletedAt != nil {
			return nil, storage.ErrProgramRunForbidden
		}
	}
	target := storage.ProgramTarget{ProgramCommand: req.Command, AuthorizedByUserID: identity.UserID, HostConsumerUserID: consumer.UserID}
	encoded, encodeErr := json.Marshal(target)
	if encodeErr != nil || len(encoded) > 24<<10 {
		return nil, storage.ErrInvalidProgramRun
	}
	var result *storage.ProgramRun
	transactionErr := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		var run, actor, task, workspace, kind string
		var same bool
		lookupErr := s.queryRow(txCtx, `SELECT p.run_id,p.actor,p.task_slug,p.workspace,b.executor_kind,COALESCE(c.body->'program_target'=$3::jsonb,false)
		 FROM run_preparations p JOIN run_bindings b ON b.project_slug=p.project_slug AND b.id=p.run_id
		 JOIN context_packages c ON c.project_slug=b.project_slug AND c.id=b.package_id
		 WHERE p.project_slug=$1 AND p.idempotency_key=$2`, s.project, req.IdempotencyKey, encoded).Scan(&run, &actor, &task, &workspace, &kind, &same)
		if lookupErr == nil {
			if kind != "program" || actor != identity.UserID || task != req.TaskSlug || workspace != req.Command.Cwd || !same {
				return storage.ErrRunBindingConflict
			}
			r, err := s.programRun(txCtx, run)
			if err != nil {
				return err
			}
			r.Replayed = true
			result = &r.ProgramRun
			return nil
		}
		if !errors.Is(lookupErr, pgx.ErrNoRows) {
			return fmt.Errorf("look up program preparation %s: %w", req.IdempotencyKey, lookupErr)
		}
		var aborted bool
		if abortErr := s.queryRow(txCtx, `SELECT EXISTS(SELECT 1 FROM preparation_aborts WHERE project_slug=$1 AND idempotency_key=$2)`, s.project, req.IdempotencyKey).Scan(&aborted); abortErr != nil {
			return fmt.Errorf("check program preparation %s abort: %w", req.IdempotencyKey, abortErr)
		}
		if aborted {
			return storage.ErrPreparationCancelled
		}
		if err := s.checkDispatchQABasis(txCtx, encoded); err != nil {
			return err
		}
		preparedRun, prepareErr := s.PrepareRun(txCtx, req.TaskSlug, req.Command.Cwd)
		if prepareErr != nil {
			return prepareErr
		}
		run = preparedRun
		if _, err := s.exec(txCtx, `UPDATE run_bindings SET executor_kind='program',environment_id=$3 WHERE project_slug=$1 AND id=$2`, s.project, run, req.Command.EnvironmentID); err != nil {
			return err
		}
		if _, err := s.exec(txCtx, `UPDATE context_packages c SET body=body || jsonb_build_object('program_target',$3::jsonb)
		 FROM run_bindings b WHERE b.project_slug=$1 AND b.id=$2 AND c.project_slug=b.project_slug AND c.id=b.package_id`, s.project, run, encoded); err != nil {
			return err
		}
		if req.Command.Loop != nil {
			refs, err := s.workCompletionScopeReferences(txCtx, req.TaskSlug)
			if err != nil {
				return err
			}
			if _, err := s.exec(txCtx, `UPDATE context_packages c SET body=body || jsonb_build_object('program_loop_scope',$3::jsonb) FROM run_bindings b WHERE b.project_slug=$1 AND b.id=$2 AND c.project_slug=b.project_slug AND c.id=b.package_id`, s.project, run, refs); err != nil {
				return err
			}
		}
		if _, err := s.exec(txCtx, `INSERT INTO run_preparations(project_slug,idempotency_key,actor,task_slug,workspace,run_id,created_at)
		 VALUES($1,$2,$3,$4,$5,$6,$7)`, s.project, req.IdempotencyKey, identity.UserID, req.TaskSlug, req.Command.Cwd, run, s.now()); err != nil {
			return err
		}
		r, err := s.programRun(txCtx, run)
		if err != nil {
			return err
		}
		result = &r.ProgramRun
		return nil
	})
	if transactionErr != nil {
		return nil, transactionErr
	}
	return result, nil
}

func checkProgramHost(ctx context.Context, scope storage.ProgramHostScope, target *storage.ProgramTarget) error {
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.Source != "apikey" || identity.UserKind != storage.KindServiceAccount || identity.UserID != target.HostConsumerUserID {
		return storage.ErrProgramRunForbidden
	}
	if scope.EnvironmentID != target.EnvironmentID || scope.NativeProjectID != target.NativeProjectID {
		return storage.ErrProgramRunForbidden
	}
	return nil
}

// ReadProgramRun returns the current program attempt after checking the host scope.
func (s *Store) ReadProgramRun(ctx context.Context, scope storage.ProgramHostScope, id string) (*storage.ProgramRun, error) {
	if !validMailText(id, 256) {
		return nil, storage.ErrInvalidProgramRun
	}
	var result *storage.ProgramRun
	err := s.RunReadSnapshot(ctx, func(txCtx context.Context) error {
		r, err := s.programRun(txCtx, id)
		if err != nil {
			return err
		}
		if hostErr := checkProgramHost(ctx, scope, &r.target); hostErr != nil {
			return hostErr
		}
		result = &r.ProgramRun
		return nil
	})
	return result, err
}

// CompleteProgramRun completes an eligible run separately from result recording,
// so a rejected completion never loses an exit observation.
func (s *Store) CompleteProgramRun(ctx context.Context, scope storage.ProgramHostScope, id string) (storage.RunSelfCompletion, error) {
	var result storage.RunSelfCompletion
	if !validMailText(id, 256) {
		return result, storage.ErrInvalidProgramRun
	}
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		r, err := s.programRun(txCtx, id)
		if err != nil {
			return err
		}
		if hostErr := checkProgramHost(txCtx, scope, &r.target); hostErr != nil {
			return hostErr
		}
		slug, err := s.RunBindingTask(txCtx, id)
		if err != nil {
			return err
		}
		if completionErr := s.RecordCompletion(txCtx, slug, id); completionErr != nil {
			return completionErr
		}
		result = storage.RunSelfCompletion{RunID: id, Completed: slug}
		return nil
	})
	return result, err
}

func (s *Store) checkProgramCompletion(ctx context.Context, slug, id, stage string) (*storage.ProgramCompletion, error) {
	r, err := s.programRun(ctx, id)
	if err != nil {
		return nil, err
	}
	if hostErr := checkProgramHost(ctx, storage.ProgramHostScope{EnvironmentID: r.target.EnvironmentID, NativeProjectID: r.target.NativeProjectID}, &r.target); hostErr != nil {
		return nil, hostErr
	}
	if !r.target.CompleteOnSuccess || checkProgramCommand(&r.target.ProgramCommand) != nil || r.Result == nil ||
		r.Result.Outcome != "exited" || r.Result.ExitCode == nil || *r.Result.ExitCode != 0 || r.Result.FinishedAt == nil ||
		r.Result.TimedOutAt != nil || r.Result.CancelRequestedAt != nil {
		return nil, storage.ErrProgramCompletionUnavailable
	}
	if r.target.Loop != nil && r.Completion == nil && (r.LoopState.Condition == nil || r.LoopState.Condition.Value != "true" || r.LoopState.Stop != nil) {
		return nil, storage.ErrProgramCompletionUnavailable
	}
	if r.target.Loop != nil {
		if scopeErr := s.checkProgramScopeCurrent(ctx, r); scopeErr != nil {
			return nil, scopeErr
		}
	}
	a := r.Dispatch.Admission
	if a == nil || a.RunID != id || a.TaskSlug != slug || a.PackageID != r.Context.PackageID || a.Actor != r.target.AuthorizedByUserID ||
		r.Dispatch.Resolution != nil || r.Dispatch.Cancellation != nil || (r.State != "exited" && r.State != "completed") {
		return nil, storage.ErrRunBindingConflict
	}
	// Admission retains execution responsibility after the preparation lease expires.
	var competing, completed bool
	err = s.queryRow(ctx, `SELECT
 EXISTS(SELECT 1 FROM claims WHERE project_slug=$1 AND spec_slug=$2 AND agent<>$3) OR
 EXISTS(SELECT 1 FROM run_dispatches WHERE project_slug=$1 AND task_slug=$2 AND run_id<>$3 AND released_at IS NULL),
 EXISTS(SELECT 1 FROM execution_events WHERE project_slug=$1 AND spec_slug=$2 AND agent=$3 AND event_type='completion')`, s.project, slug, id).Scan(&competing, &completed)
	if err != nil {
		return nil, fmt.Errorf("check program run %s completion ownership: %w", id, err)
	}
	if competing || (stage == "done" && !completed) || (stage != "done" && (completed || r.State == "completed")) {
		return nil, storage.ErrRunBindingConflict
	}
	if r.Completion != nil {
		return r.Completion, nil
	}
	if r.CurrentAttempt == nil {
		return nil, storage.ErrProgramCompletionUnavailable
	}
	attemptID := r.CurrentAttempt.ID
	basis := &storage.ProgramCompletion{AttemptID: &attemptID}
	if r.target.Loop != nil && r.target.Loop.Termination.Kind == "judgment" && r.LoopState.Condition != nil && r.LoopState.Condition.JudgmentID != "" {
		judgmentID := r.LoopState.Condition.JudgmentID
		basis.JudgmentID = &judgmentID
	}
	return basis, nil
}

// AuthorizeProgramRun grants the first program attempt. Admission replay is
// observation only: a lost host must not spawn a second attempt.
func (s *Store) AuthorizeProgramRun(ctx context.Context, scope storage.ProgramHostScope, id string) (*storage.ProgramAdmission, error) {
	return s.authorizeProgramRun(ctx, scope, id, "")
}

func (s *Store) authorizeProgramRun(ctx context.Context, scope storage.ProgramHostScope, id, hookID string) (*storage.ProgramAdmission, error) {
	if !validMailText(id, 256) {
		return nil, storage.ErrInvalidProgramRun
	}
	var result *storage.ProgramAdmission
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		r, err := s.programRun(txCtx, id)
		if err != nil {
			return err
		}
		if hostErr := checkProgramHost(ctx, scope, &r.target); hostErr != nil {
			return hostErr
		}
		var activeHook string
		if hookErr := s.queryRow(txCtx, `SELECT COALESCE((SELECT id FROM delivery_test_hooks WHERE hook_kind='completion_program' AND project_slug=$1 AND target_run_id=$2 AND cancelled_at IS NULL),'')`, s.project, id).Scan(&activeHook); hookErr != nil {
			return fmt.Errorf("read program run %s active completion hook: %w", id, hookErr)
		}
		if activeHook != hookID {
			return storage.ErrProgramHookRequired
		}
		if r.Dispatch.Admission != nil {
			a := r.Dispatch.Admission
			if a.Actor != r.target.AuthorizedByUserID || a.PackageID != r.Context.PackageID {
				return storage.ErrRunBindingConflict
			}
			r.Replayed = true
			a.Replayed = true
			result = &storage.ProgramAdmission{Run: &r.ProgramRun, MayStart: false}
			return nil
		}
		user, err := s.ExistingAuth().GetUserByID(txCtx, r.target.AuthorizedByUserID)
		if err != nil {
			return err
		}
		if user.Kind != storage.KindHuman || user.DeletedAt != nil {
			return storage.ErrProgramRunForbidden
		}
		if r.Dispatch.Cancellation != nil || r.Result != nil {
			return storage.ErrRunBindingConflict
		}
		if r.target.Loop != nil {
			if scopeErr := s.checkProgramScopeCurrent(txCtx, r); scopeErr != nil {
				return scopeErr
			}
		}
		admission, err := s.authorizeRunDispatch(txCtx, id, r.target.AuthorizedByUserID, r.Context.PackageID, r.targetBody, "program")
		if err != nil {
			return err
		}
		r.attemptID = newID("pat")
		if r.LoopState != nil && r.LoopState.Stop != nil {
			return storage.ErrProgramLoopStopped
		}
		granted := s.now()
		if _, err := s.exec(txCtx, `INSERT INTO program_attempts(id,project_slug,run_id,admission_id,ordinal,granted_at) VALUES($1,$2,$3,$4,1,$5)`, r.attemptID, s.project, id, admission.ID, granted); err != nil {
			return err
		}
		if _, err := s.exec(txCtx, `UPDATE run_bindings SET state='admitted',updated_at=$3 WHERE project_slug=$1 AND id=$2`, s.project, id, s.now()); err != nil {
			return err
		}
		r.State = "admitted"
		r.Dispatch.Admission = &admission
		r.CurrentAttempt = &storage.ProgramAttemptMetadata{ID: r.attemptID, Ordinal: 1, GrantedAt: granted}
		if err := s.projectProgramLoop(txCtx, r); err != nil {
			return err
		}
		result = &storage.ProgramAdmission{Run: &r.ProgramRun, MayStart: !admission.Replayed}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func preserveProgramTime(prior, next *time.Time) (*time.Time, error) {
	if prior == nil {
		return next, nil
	}
	if next != nil && !prior.Equal(*next) {
		return nil, storage.ErrProgramResultConflict
	}
	return prior, nil
}

// RecordProgramResult records an observation for the current non-loop attempt.
func (s *Store) RecordProgramResult(ctx context.Context, scope storage.ProgramHostScope, id string, observation *storage.ProgramObservation) (*storage.ProgramRunResult, error) {
	localObservation := *observation
	return s.recordProgramAttemptResult(ctx, scope, id, "", &localObservation)
}

// RecordProgramAttemptResult records an observation against the exact granted attempt.
func (s *Store) RecordProgramAttemptResult(ctx context.Context, scope storage.ProgramHostScope, id, attemptID string, observation *storage.ProgramObservation) (*storage.ProgramRunResult, error) {
	if !validMailText(attemptID, 256) {
		return nil, storage.ErrInvalidProgramRun
	}
	localObservation := *observation
	return s.recordProgramAttemptResult(ctx, scope, id, attemptID, &localObservation)
}

func (s *Store) recordProgramAttemptResult(ctx context.Context, scope storage.ProgramHostScope, id, attemptID string, observation *storage.ProgramObservation) (*storage.ProgramRunResult, error) {
	if !validMailText(id, 256) || observation.ObservedAt.IsZero() ||
		(observation.ExitCode != nil && (*observation.ExitCode < -2147483648 || *observation.ExitCode > 4294967295)) ||
		(observation.Summary != nil && (!utf8.ValidString(*observation.Summary) || strings.ContainsRune(*observation.Summary, 0) || utf8.RuneCountInString(*observation.Summary) > 4000)) {
		return nil, storage.ErrInvalidProgramRun
	}
	for _, value := range []*time.Time{observation.StartedAt, observation.FinishedAt, observation.TimedOutAt, observation.CancelRequestedAt} {
		if value != nil && value.IsZero() {
			return nil, storage.ErrInvalidProgramRun
		}
	}
	switch observation.Outcome {
	case "exited":
		if observation.ExitCode == nil || observation.FinishedAt == nil {
			return nil, storage.ErrInvalidProgramRun
		}
	case "timed_out", "cancel_requested", "unconfirmed":
		if observation.ExitCode != nil || observation.FinishedAt != nil {
			return nil, storage.ErrInvalidProgramRun
		}
	default:
		return nil, storage.ErrInvalidProgramRun
	}
	if observation.Outcome == "timed_out" && observation.TimedOutAt == nil {
		stamp := observation.ObservedAt
		observation.TimedOutAt = &stamp
	}
	if observation.Outcome == "cancel_requested" && observation.CancelRequestedAt == nil {
		stamp := observation.ObservedAt
		observation.CancelRequestedAt = &stamp
	}
	var result *storage.ProgramRunResult
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		r, err := s.programRun(txCtx, id)
		if err != nil {
			return err
		}
		if hostErr := checkProgramHost(ctx, scope, &r.target); hostErr != nil {
			return hostErr
		}
		if r.Dispatch.Admission == nil || r.Dispatch.Admission.PackageID != r.Context.PackageID || r.Dispatch.Admission.Actor != r.target.AuthorizedByUserID {
			return storage.ErrRunBindingConflict
		}
		currentID := r.attemptID
		if attemptID == "" {
			if r.target.Loop != nil {
				return storage.ErrProgramLoopConflict
			}
			attemptID = r.attemptID
		}
		var prior []byte
		if attemptErr := s.queryRow(txCtx, `SELECT result FROM program_attempts WHERE project_slug=$1 AND run_id=$2 AND id=$3`, s.project, id, attemptID).Scan(&prior); attemptErr != nil {
			if errors.Is(attemptErr, pgx.ErrNoRows) {
				return storage.ErrProgramLoopConflict
			}
			return fmt.Errorf("read program run %s attempt %s prior result: %w", id, attemptID, attemptErr)
		}
		r.Result = nil
		if len(prior) > 0 {
			if decodeErr := json.Unmarshal(prior, &r.Result); decodeErr != nil {
				return fmt.Errorf("decode program run %s attempt %s prior result: %w", id, attemptID, decodeErr)
			}
		}
		if r.Result != nil {
			observation.StartedAt, err = preserveProgramTime(r.Result.StartedAt, observation.StartedAt)
			if err != nil {
				return err
			}
			observation.TimedOutAt, err = preserveProgramTime(r.Result.TimedOutAt, observation.TimedOutAt)
			if err != nil {
				return err
			}
			observation.CancelRequestedAt, err = preserveProgramTime(r.Result.CancelRequestedAt, observation.CancelRequestedAt)
			if err != nil {
				return err
			}
			before, encodeBeforeErr := json.Marshal(r.Result.ProgramObservation)
			if encodeBeforeErr != nil {
				return fmt.Errorf("encode program run %s attempt %s prior observation: %w", id, attemptID, encodeBeforeErr)
			}
			after, encodeAfterErr := json.Marshal(observation)
			if encodeAfterErr != nil {
				return fmt.Errorf("encode program run %s attempt %s next observation: %w", id, attemptID, encodeAfterErr)
			}
			if bytes.Equal(before, after) {
				result = r.Result
				return nil
			}
			if r.Result.Outcome == "exited" {
				return storage.ErrProgramResultConflict
			}
		}
		identity, _ := auth.IdentityFromContext(ctx)
		result = &storage.ProgramRunResult{ProgramObservation: *observation, ReporterUserID: identity.UserID, RecordedAt: s.now()}
		encoded, err := json.Marshal(result)
		if err != nil {
			return storage.ErrInvalidProgramRun
		}
		if _, updateErr := s.exec(txCtx, `UPDATE program_attempts SET result=$4 WHERE project_slug=$1 AND run_id=$2 AND id=$3`, s.project, id, attemptID, encoded); updateErr != nil {
			return updateErr
		}
		if currentID != attemptID {
			return nil
		}
		_, err = s.exec(txCtx, `UPDATE run_bindings SET state=CASE WHEN state='completed' THEN state ELSE $3 END,updated_at=$4 WHERE project_slug=$1 AND id=$2`, s.project, id, observation.Outcome, s.now())
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
