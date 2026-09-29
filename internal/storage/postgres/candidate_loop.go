// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
)

// ArmCandidateLoop fixes a formal candidate budget before the original first dispatch.
func (s *Store) ArmCandidateLoop(ctx context.Context, req storage.ArmCandidateLoopRequest, scope *storage.MailScope) (*storage.CandidateLoop, error) {
	if !validMailText(req.RunID, 256) || req.MaxAttempts < 1 || len(req.PlanSources) == 0 {
		return nil, storage.ErrInvalidCandidateLoop
	}
	if (req.TerminationKind != "report" && req.TerminationKind != "satisfaction") ||
		(req.TerminationKind == "report" && req.SatisfactionCriterion != nil) ||
		(req.TerminationKind == "satisfaction" && (req.SatisfactionCriterion == nil || strings.TrimSpace(*req.SatisfactionCriterion) == "" || utf8.RuneCountInString(*req.SatisfactionCriterion) > 4000)) {
		return nil, storage.ErrInvalidCandidateLoop
	}
	var joinMode *string
	var joinSkip *bool
	if req.Join != nil {
		if req.Join.Mode != "all" && req.Join.Mode != "any" {
			return nil, storage.ErrInvalidCandidateLoop
		}
		joinMode, joinSkip = &req.Join.Mode, &req.Join.AllowEmptySkip
	}
	req.PlanSources = slices.Clone(req.PlanSources)
	seen := make(map[storage.ReviewSource]bool, len(req.PlanSources))
	for _, source := range req.PlanSources {
		if seen[source] {
			return nil, storage.ErrInvalidCandidateLoop
		}
		seen[source] = true
	}
	sort.Slice(req.PlanSources, func(i, j int) bool {
		left, _ := json.Marshal(req.PlanSources[i])
		right, _ := json.Marshal(req.PlanSources[j])
		return bytes.Compare(left, right) < 0
	})
	plans, err := json.Marshal(req.PlanSources)
	if err != nil {
		return nil, fmt.Errorf("postgres: encode candidate plans: %w", err)
	}
	var result *storage.CandidateLoop
	err = s.RunInTransaction(ctx, func(txCtx context.Context) error {
		run, user, err := s.deliveryHookActor(txCtx, scope)
		if err != nil {
			return err
		}
		actor := "user:" + user
		if run != nil {
			actor = "run:" + *run
		}
		var same bool
		err = s.queryRow(txCtx, `SELECT max_attempts=$3 AND plan_sources=$4::jsonb AND join_mode IS NOT DISTINCT FROM $5::text
 AND join_allow_empty_skip IS NOT DISTINCT FROM $6::boolean AND termination_kind=$7
 AND satisfaction_criterion IS NOT DISTINCT FROM $8::text FROM candidate_loops
 WHERE project_slug=$1 AND run_id=$2`, s.project, req.RunID, req.MaxAttempts, plans, joinMode, joinSkip, req.TerminationKind, req.SatisfactionCriterion).Scan(&same)
		if err == nil {
			if !same {
				return storage.ErrCandidateLoopConflict
			}
			result, err = s.readCandidateLoop(txCtx, req.RunID)
			return err
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("postgres: read candidate loop replay: %w", err)
		}
		var slug, packageID, kind, state string
		var target []byte
		var dispatched, cancelled, priorDelivery bool
		err = s.queryRow(txCtx, `SELECT b.task_spec_slug,b.package_id,b.executor_kind,b.state,p.body->'dispatch_target',
 EXISTS(SELECT 1 FROM run_dispatches d WHERE d.project_slug=b.project_slug AND d.run_id=b.id),
 EXISTS(SELECT 1 FROM run_preparation_cancellations c WHERE c.project_slug=b.project_slug AND c.run_id=b.id),
 EXISTS(SELECT 1 FROM deliveries d WHERE d.project_slug=b.project_slug AND d.run_binding_id=b.id)
 FROM run_bindings b JOIN context_packages p ON p.project_slug=b.project_slug AND p.id=b.package_id
 WHERE b.project_slug=$1 AND b.id=$2`, s.project, req.RunID).Scan(&slug, &packageID, &kind, &state, &target, &dispatched, &cancelled, &priorDelivery)
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrCandidateLoopConflict
		}
		if err != nil {
			return fmt.Errorf("postgres: read candidate run: %w", err)
		}
		if kind != "agent" || (state != "prepared" && state != "bound") || dispatched || cancelled || priorDelivery {
			return storage.ErrCandidateLoopConflict
		}
		var fixed testReportTarget
		if json.Unmarshal(target, &fixed) != nil || fixed.WorkPurpose != "implementation" || fixed.QABasis == nil {
			return storage.ErrInvalidCandidateLoop
		}
		if err := s.checkDispatchQABasis(txCtx, target); err != nil {
			return err
		}
		if err := s.checkCandidateHumanHold(txCtx, slug); err != nil {
			return err
		}
		if err := s.checkCandidateScopeCurrent(txCtx, slug, packageID); err != nil {
			return err
		}
		for _, plan := range req.PlanSources {
			if !slices.Contains(fixed.QABasis.TestPlanSources, plan) {
				return storage.ErrInvalidCandidateLoop
			}
		}
		var held bool
		if err := s.queryRow(txCtx, `SELECT EXISTS(SELECT 1 FROM candidate_loops WHERE project_slug=$1 AND task_slug=$2
 AND resolved_at IS NULL AND abandoned_at IS NULL)`, s.project, slug).Scan(&held); err != nil {
			return fmt.Errorf("postgres: check candidate budget: %w", err)
		}
		if held {
			return storage.ErrCandidateBudgetHeld
		}
		_, err = s.exec(txCtx, `INSERT INTO candidate_loops(project_slug,run_id,task_slug,package_id,max_attempts,plan_sources,join_mode,join_allow_empty_skip,termination_kind,satisfaction_criterion,configured_by,configured_at)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, s.project, req.RunID, slug, packageID, req.MaxAttempts, plans, joinMode, joinSkip, req.TerminationKind, req.SatisfactionCriterion, actor, s.now())
		if err != nil {
			return fmt.Errorf("postgres: insert candidate loop: %w", err)
		}
		result, err = s.readCandidateLoop(txCtx, req.RunID)
		return err
	})
	return result, err
}

// ReadCandidateLoop projects persisted grants and the current exact-delivery report condition.
func (s *Store) ReadCandidateLoop(ctx context.Context, runID string) (*storage.CandidateLoop, error) {
	if !validMailText(runID, 256) {
		return nil, storage.ErrInvalidCandidateLoop
	}
	var result *storage.CandidateLoop
	err := s.RunReadSnapshot(ctx, func(snapshotCtx context.Context) error {
		var err error
		result, err = s.readCandidateLoop(snapshotCtx, runID)
		return err
	})
	return result, err
}

func (s *Store) readCandidateLoop(ctx context.Context, runID string) (*storage.CandidateLoop, error) {
	var result storage.CandidateLoop
	var plans []byte
	var joinMode *string
	var joinSkip *bool
	var completedJoin []byte
	var criterion *string
	var completedCondition, completedSatisfaction []byte
	var stopUser, stopRun, stopReason, abandonUser, abandonReason *string
	var stoppedAt, abandonedAt, resolvedAt *time.Time
	err := s.queryRow(ctx, `SELECT run_id,task_slug,package_id,max_attempts,plan_sources,join_mode,join_allow_empty_skip,termination_kind,satisfaction_criterion,configured_by,configured_at,
 stop_actor_user_id,stop_actor_run_id,stop_reason,stopped_at,abandon_actor_user_id,abandon_reason,abandoned_at,resolved_at,completed_join,completed_condition,completed_satisfaction
 FROM candidate_loops WHERE project_slug=$1 AND run_id=$2`, s.project, runID).Scan(&result.RunID, &result.TaskSlug,
		&result.PackageID, &result.MaxAttempts, &plans, &joinMode, &joinSkip, &result.TerminationKind, &criterion, &result.ConfiguredBy, &result.ConfiguredAt,
		&stopUser, &stopRun, &stopReason, &stoppedAt, &abandonUser, &abandonReason, &abandonedAt, &resolvedAt, &completedJoin, &completedCondition, &completedSatisfaction)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, storage.ErrCandidateLoopNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: read candidate loop: %w", err)
	}
	if err := json.Unmarshal(plans, &result.PlanSources); err != nil {
		return nil, fmt.Errorf("postgres: decode candidate plans: %w", err)
	}
	if joinMode != nil {
		result.Join = &storage.CandidateJoinState{CandidateJoinConfig: storage.CandidateJoinConfig{Mode: *joinMode, AllowEmptySkip: *joinSkip}}
	}
	if criterion != nil {
		result.Satisfaction = &storage.CandidateSatisfactionState{Criterion: *criterion, Value: "unknown", Reason: "judgment_missing"}
	}
	if len(completedCondition) != 0 {
		var condition storage.CandidateCondition
		if err := json.Unmarshal(completedCondition, &condition); err != nil {
			return nil, fmt.Errorf("postgres: decode completed candidate condition: %w", err)
		}
		result.CompletedCondition = &condition
	}
	if len(completedSatisfaction) != 0 {
		var basis storage.CandidateSatisfactionBasis
		if err := json.Unmarshal(completedSatisfaction, &basis); err != nil {
			return nil, fmt.Errorf("postgres: decode completed candidate satisfaction: %w", err)
		}
		result.CompletedSatisfaction = &basis
	}
	if len(completedJoin) != 0 {
		var proof storage.CandidateJoinProof
		if err := json.Unmarshal(completedJoin, &proof); err != nil {
			return nil, fmt.Errorf("postgres: decode completed candidate join: %w", err)
		}
		result.CompletedJoin = &proof
	}
	if stoppedAt != nil {
		result.Stop = &storage.CandidateLoopEvent{ActorUserID: *stopUser, ActorRunID: stopRun, Reason: *stopReason, RecordedAt: *stoppedAt}
	}
	if abandonedAt != nil {
		result.Abandon = &storage.CandidateLoopEvent{ActorUserID: *abandonUser, Reason: *abandonReason, RecordedAt: *abandonedAt}
	}
	result.CompletedAt = resolvedAt
	rows, err := s.query(ctx, `SELECT id,ordinal,granted_at,predecessor_id,delivery_id,flow_id,consumed_condition,consumed_join,consumed_satisfaction
 FROM candidate_attempts WHERE project_slug=$1 AND run_id=$2 ORDER BY ordinal`, s.project, runID)
	if err != nil {
		return nil, fmt.Errorf("postgres: read candidate attempts: %w", err)
	}
	result.Attempts = []storage.CandidateAttempt{}
	for rows.Next() {
		var attempt storage.CandidateAttempt
		var consumed, consumedJoin, consumedSatisfaction []byte
		if err := rows.Scan(&attempt.ID, &attempt.Number, &attempt.GrantedAt, &attempt.PredecessorID, &attempt.DeliveryID, &attempt.FlowID, &consumed, &consumedJoin, &consumedSatisfaction); err != nil {
			rows.Close()
			return nil, fmt.Errorf("postgres: scan candidate attempt: %w", err)
		}
		if len(consumed) != 0 {
			var condition storage.CandidateCondition
			if err := json.Unmarshal(consumed, &condition); err != nil {
				rows.Close()
				return nil, fmt.Errorf("postgres: decode consumed candidate condition: %w", err)
			}
			attempt.ConsumedCondition = &condition
		}
		if len(consumedJoin) != 0 {
			var proof storage.CandidateJoinProof
			if err := json.Unmarshal(consumedJoin, &proof); err != nil {
				rows.Close()
				return nil, fmt.Errorf("postgres: decode consumed candidate join: %w", err)
			}
			attempt.ConsumedJoin = &proof
		}
		if len(consumedSatisfaction) != 0 {
			var basis storage.CandidateSatisfactionBasis
			if err := json.Unmarshal(consumedSatisfaction, &basis); err != nil {
				rows.Close()
				return nil, fmt.Errorf("postgres: decode consumed candidate satisfaction: %w", err)
			}
			attempt.ConsumedSatisfaction = &basis
		}
		result.Attempts = append(result.Attempts, attempt)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("postgres: collect candidate attempts: %w", err)
	}
	rows.Close()
	if result.Join != nil {
		result.Join.Reason = "flow_missing"
		if len(result.Attempts) != 0 {
			current := &result.Attempts[len(result.Attempts)-1]
			if current.FlowID != nil {
				if err := s.readCandidateJoin(ctx, result.Join, *current.FlowID); err != nil {
					return nil, err
				}
			}
		}
	}
	if len(result.Attempts) == 0 {
		result.Status = candidateLoopStatus(result.MaxAttempts, 0, false, nil, result.Satisfaction, result.Stop != nil, result.Abandon != nil, result.CompletedAt != nil)
		return &result, nil
	}
	current := &result.Attempts[len(result.Attempts)-1]
	result.CurrentAttempt = current
	if result.Abandon != nil {
		result.Status = candidateLoopStatus(result.MaxAttempts, current.Number, current.DeliveryID != nil, nil, result.Satisfaction, result.Stop != nil, true, result.CompletedAt != nil)
		return &result, nil
	}
	if current.DeliveryID == nil {
		result.Status = candidateLoopStatus(result.MaxAttempts, current.Number, false, nil, result.Satisfaction, result.Stop != nil, false, result.CompletedAt != nil)
		return &result, nil
	}
	condition, err := s.evaluateCandidateCondition(ctx, *current.DeliveryID, result.PlanSources)
	if err != nil {
		return nil, err
	}
	result.Condition = &condition
	if result.Satisfaction != nil {
		if err := s.readCandidateSatisfactionState(ctx, result.Satisfaction, current); err != nil {
			return nil, err
		}
	}
	result.Status = candidateLoopStatus(result.MaxAttempts, current.Number, true, &condition, result.Satisfaction, result.Stop != nil, false, result.CompletedAt != nil)
	return &result, nil
}

func (s *Store) readCandidateJoin(ctx context.Context, state *storage.CandidateJoinState, flowID string) error {
	state.FlowID = &flowID
	var cancelled bool
	if err := s.queryRow(ctx, `SELECT cancelled_at IS NOT NULL FROM report_branch_flows WHERE project_slug=$1 AND id=$2`, s.project, flowID).Scan(&cancelled); err != nil {
		return fmt.Errorf("postgres: read candidate join flow: %w", err)
	}
	if cancelled {
		state.Reason = "flow_cancelled"
		state.Evaluation = &storage.ReportFlowJoinEvaluation{Participants: []storage.ReportFlowJoinMember{}, Unknown: []storage.ReportFlowJoinMember{}, Conditions: []storage.ReportFlowJoinCondition{}, Reason: "flow_cancelled"}
		return nil
	}
	evaluation, err := s.evaluateReportFlowJoin(ctx, storage.ReportFlowJoinConfig{FlowID: flowID, Mode: state.Mode, AllowEmptySkip: state.AllowEmptySkip})
	if err != nil {
		return err
	}
	state.Evaluation = &evaluation
	state.Reason = evaluation.Reason
	return nil
}

func candidateJoinProof(loop *storage.CandidateLoop) (*storage.CandidateJoinProof, error) {
	if loop.Join == nil {
		return nil, nil
	}
	if loop.CurrentAttempt == nil || loop.Join.FlowID == nil || loop.Join.Evaluation == nil || !loop.Join.Evaluation.Satisfied {
		return nil, storage.ErrReportFlowJoinUnsatisfied
	}
	return &storage.CandidateJoinProof{SourceAttemptID: loop.CurrentAttempt.ID, FlowID: *loop.Join.FlowID,
		Mode: loop.Join.Mode, Evaluation: *loop.Join.Evaluation}, nil
}

func candidateLoopStatus(maxAttempts, ordinal int, hasDelivery bool, condition *storage.CandidateCondition, satisfaction *storage.CandidateSatisfactionState, stopped, abandoned, completed bool) string {
	switch {
	case completed:
		return "completed"
	case abandoned:
		return "abandoned"
	case satisfaction != nil && satisfaction.Intervention != nil && satisfaction.Intervention.ResolvedAt == nil:
		return "needs_human"
	case satisfaction != nil:
		if stopped && !(condition != nil && condition.Value == "true" && satisfaction.Value == "true") {
			return "stopped"
		}
		if ordinal == 0 {
			return "waiting"
		}
		if !hasDelivery {
			return "candidate"
		}
		if condition == nil || condition.Reason == "report_missing" {
			return "waiting"
		}
		if satisfaction.Judgment == nil {
			return "waiting"
		}
		if condition.Value == "true" && satisfaction.Value == "true" {
			return "condition_met"
		}
		if condition.Value == "false" && satisfaction.Value == "false" {
			if ordinal >= maxAttempts {
				return "needs_human"
			}
			return "ready_next"
		}
		if condition.Value == "unknown" || satisfaction.Value == "unknown" {
			if ordinal >= maxAttempts {
				return "needs_human"
			}
			return "waiting"
		}
		return "needs_human"
	case condition != nil && condition.Value == "true":
		return "condition_met"
	case stopped:
		return "stopped"
	case ordinal == 0:
		return "waiting"
	case !hasDelivery:
		return "candidate"
	case condition == nil || condition.Reason == "report_missing":
		return "waiting"
	case ordinal >= maxAttempts:
		return "needs_human"
	case condition.Value == "false":
		return "ready_next"
	default:
		return "waiting"
	}
}

func candidateReportValue(statuses []string) (value, reason string) {
	failed, missing, unknown := false, false, false
	for _, status := range statuses {
		switch status {
		case "failed":
			failed = true
		case "passed":
		case "":
			missing = true
		default:
			unknown = true
		}
	}
	switch {
	case failed:
		return "false", ""
	case missing:
		return "unknown", "report_missing"
	case unknown:
		return "unknown", "report_unknown"
	default:
		return "true", ""
	}
}

func (s *Store) evaluateCandidateCondition(ctx context.Context, deliveryID string, plans []storage.ReviewSource) (storage.CandidateCondition, error) {
	result := storage.CandidateCondition{Value: "unknown", Reports: []storage.ReportBranchReport{}, MissingPlans: []storage.ReviewSource{}}
	var snapshot []byte
	err := s.queryRow(ctx, `SELECT snapshot FROM deliveries WHERE project_slug=$1 AND id=$2`, s.project, deliveryID).Scan(&snapshot)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, storage.ErrCandidateLoopConflict
	}
	if err != nil {
		return result, fmt.Errorf("postgres: read candidate delivery: %w", err)
	}
	commit, err := testDeliveryCommit(snapshot)
	if err != nil {
		return result, fmt.Errorf("postgres: decode candidate delivery commit: %w", err)
	}
	statuses := make([]string, 0, len(plans))
	for _, plan := range plans {
		report, err := s.latestRegisteredTestReport(ctx, deliveryID, commit, plan)
		if err != nil {
			return result, err
		}
		if report == nil {
			result.MissingPlans = append(result.MissingPlans, plan)
			statuses = append(statuses, "")
			continue
		}
		result.Reports = append(result.Reports, *report)
		statuses = append(statuses, report.Status)
	}
	result.Value, result.Reason = candidateReportValue(statuses)
	return result, nil
}

func (s *Store) candidateFirstAdmissionAllowed(ctx context.Context, runID, slug string) (bool, error) {
	var otherBudget bool
	if err := s.queryRow(ctx, `SELECT EXISTS(SELECT 1 FROM candidate_loops WHERE project_slug=$1 AND task_slug=$2
 AND run_id<>$3 AND resolved_at IS NULL AND abandoned_at IS NULL)`, s.project, slug, runID).Scan(&otherBudget); err != nil {
		return false, fmt.Errorf("postgres: check candidate budget owner: %w", err)
	}
	if otherBudget {
		return false, storage.ErrCandidateBudgetHeld
	}
	var stopped, abandoned bool
	err := s.queryRow(ctx, `SELECT stopped_at IS NOT NULL,abandoned_at IS NOT NULL FROM candidate_loops WHERE project_slug=$1 AND run_id=$2`, s.project, runID).Scan(&stopped, &abandoned)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("postgres: read candidate first grant: %w", err)
	}
	if stopped || abandoned {
		return false, storage.ErrCandidateLoopStopped
	}
	return true, nil
}

func (s *Store) firstCandidateAttemptID(ctx context.Context, runID string) (*string, error) {
	var id string
	err := s.queryRow(ctx, `SELECT id FROM candidate_attempts WHERE project_slug=$1 AND run_id=$2 AND ordinal=1`, s.project, runID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: read initial candidate attempt: %w", err)
	}
	return &id, nil
}

func (s *Store) checkCandidateHumanHold(ctx context.Context, slug string) error {
	review, err := s.ReadReviewStatus(ctx, slug)
	if err != nil {
		return err
	}
	for _, state := range review.Reviews {
		if state.HumanHold {
			return storage.ErrReviewHumanHold
		}
	}
	return nil
}

func (s *Store) checkCandidateScopeCurrent(ctx context.Context, slug, packageID string) error {
	var body []byte
	if err := s.queryRow(ctx, `SELECT body FROM context_packages WHERE project_slug=$1 AND id=$2`, s.project, packageID).Scan(&body); err != nil {
		return fmt.Errorf("postgres: read candidate prepared scope: %w", err)
	}
	var prepared struct {
		Scope storage.ScopeContext `json:"scope_context"`
	}
	if err := json.Unmarshal(body, &prepared); err != nil {
		return fmt.Errorf("postgres: decode candidate prepared scope: %w", err)
	}
	contexts, err := s.readScopeContexts(ctx, []string{slug})
	if err != nil {
		return err
	}
	current := contexts[slug]
	// Scope revision and decision metadata are not substantive content changes.
	normalize := func(scope *storage.ScopeContext) {
		for i := range scope.Sources {
			scope.Sources[i].Revision = ""
		}
		for _, decision := range scope.Decisions {
			decision.Version = 0
			decision.ContentHash = ""
			decision.CreatedAt = time.Time{}
			decision.UpdatedAt = time.Time{}
		}
	}
	normalize(&prepared.Scope)
	normalize(&current)
	before, err := json.Marshal(prepared.Scope)
	if err != nil {
		return fmt.Errorf("postgres: encode candidate prepared scope: %w", err)
	}
	after, err := json.Marshal(current)
	if err != nil {
		return fmt.Errorf("postgres: encode candidate current scope: %w", err)
	}
	var same bool
	if err := s.queryRow(ctx, `SELECT $1::jsonb=$2::jsonb`, before, after).Scan(&same); err != nil {
		return fmt.Errorf("postgres: compare candidate scope: %w", err)
	}
	if !same {
		return storage.ErrCompletionRequiresRequirementReview
	}
	return nil
}

type candidateCompletionEvidence struct {
	DeliveryID   string
	Condition    storage.CandidateCondition
	Satisfaction *storage.CandidateSatisfactionBasis
	Join         *storage.CandidateJoinProof
}

func (s *Store) candidateCompletionDelivery(ctx context.Context, runID, slug string) (*candidateCompletionEvidence, error) {
	var packageID string
	var abandoned bool
	err := s.queryRow(ctx, `SELECT package_id,abandoned_at IS NOT NULL FROM candidate_loops
 WHERE project_slug=$1 AND run_id=$2 AND task_slug=$3`, s.project, runID, slug).Scan(&packageID, &abandoned)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: read candidate completion: %w", err)
	}
	if abandoned {
		return nil, storage.ErrCandidateLoopStopped
	}
	if err := s.checkCandidateScopeCurrent(ctx, slug, packageID); err != nil {
		return nil, err
	}
	loop, err := s.readCandidateLoop(ctx, runID)
	if err != nil {
		return nil, err
	}
	if loop.CurrentAttempt == nil || loop.CurrentAttempt.DeliveryID == nil || loop.Condition == nil {
		return nil, storage.ErrCandidateConditionNotMet
	}
	var satisfaction *storage.CandidateSatisfactionBasis
	if loop.Satisfaction != nil {
		satisfaction, err = candidateSatisfactionBasis(loop)
		if err != nil {
			return nil, err
		}
		if satisfaction.Value != "true" {
			return nil, storage.ErrCandidateConditionNotMet
		}
	} else if loop.Condition.Value != "true" {
		return nil, storage.ErrCandidateConditionNotMet
	}
	join, err := candidateJoinProof(loop)
	if err != nil {
		return nil, err
	}
	return &candidateCompletionEvidence{DeliveryID: *loop.CurrentAttempt.DeliveryID, Condition: *loop.Condition, Satisfaction: satisfaction, Join: join}, nil
}

// NextOwnCandidateAttempt grants one successor to the bound run, never another provider session.
func (s *Store) NextOwnCandidateAttempt(ctx context.Context, scope storage.MailScope, expectedPreviousAttemptID string) (*storage.CandidateLoop, error) {
	if !validMailText(expectedPreviousAttemptID, 256) {
		return nil, storage.ErrInvalidCandidateLoop
	}
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" {
		return nil, storage.ErrPlanningForbidden
	}
	var result *storage.CandidateLoop
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		runID, _, err := s.reviewActorRun(txCtx, scope)
		if err != nil {
			return err
		}
		var predecessorRun string
		if err := s.queryRow(txCtx, `SELECT run_id FROM candidate_attempts WHERE project_slug=$1 AND id=$2`, s.project, expectedPreviousAttemptID).Scan(&predecessorRun); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return storage.ErrCandidateLoopConflict
			}
			return fmt.Errorf("postgres: read candidate predecessor: %w", err)
		}
		if predecessorRun != runID {
			return storage.ErrCandidateLoopConflict
		}
		var successorID string
		err = s.queryRow(txCtx, `SELECT id FROM candidate_attempts WHERE project_slug=$1 AND run_id=$2 AND predecessor_id=$3`, s.project, runID, expectedPreviousAttemptID).Scan(&successorID)
		if err == nil {
			result, err = s.readCandidateLoop(txCtx, runID)
			if err != nil {
				return err
			}
			for i := range result.Attempts {
				if result.Attempts[i].ID == successorID {
					result.GrantedAttempt = &result.Attempts[i]
					break
				}
			}
			result.Replayed = true
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("postgres: read candidate successor: %w", err)
		}
		result, err = s.readCandidateLoop(txCtx, runID)
		if err != nil {
			return err
		}
		if result.Abandon != nil || result.Stop != nil {
			return storage.ErrCandidateLoopStopped
		}
		if result.CompletedAt != nil {
			return storage.ErrCandidateLoopCompleted
		}
		if result.CurrentAttempt == nil || result.CurrentAttempt.ID != expectedPreviousAttemptID {
			return storage.ErrCandidateLoopConflict
		}
		if err := s.rejectNewNodeExecution(txCtx, result.TaskSlug); err != nil {
			return err
		}
		var satisfactionBasis *storage.CandidateSatisfactionBasis
		if result.Satisfaction != nil {
			satisfactionBasis, err = candidateSatisfactionBasis(result)
			if err != nil {
				return err
			}
			if satisfactionBasis.Value == "true" {
				return storage.ErrCandidateConditionNotMet
			}
			if result.CurrentAttempt.Number >= result.MaxAttempts {
				return storage.ErrCandidateAttemptsExhausted
			}
		} else {
			if result.CurrentAttempt.DeliveryID == nil || result.Condition == nil || result.Condition.Reason == "report_missing" {
				return storage.ErrCandidateLoopWaiting
			}
			if result.Condition.Value == "true" {
				return storage.ErrCandidateConditionNotMet
			}
			if result.CurrentAttempt.Number >= result.MaxAttempts {
				return storage.ErrCandidateAttemptsExhausted
			}
			if result.Condition.Value != "false" {
				return storage.ErrCandidateLoopWaiting
			}
		}
		joinProof, err := candidateJoinProof(result)
		if err != nil {
			return err
		}
		dispatch, err := s.readRunDispatch(txCtx, runID)
		if err != nil {
			return err
		}
		if dispatch.Admission == nil || dispatch.Resolution != nil || dispatch.Cancellation != nil {
			return storage.ErrCandidateLoopConflict
		}
		if err := s.assertActiveClaim(txCtx, result.TaskSlug, runID); err != nil {
			return err
		}
		if err := s.checkPreparedContract(txCtx, result.TaskSlug, runID); err != nil {
			return err
		}
		if err := s.checkCandidateHumanHold(txCtx, result.TaskSlug); err != nil {
			return err
		}
		if err := s.checkCandidateScopeCurrent(txCtx, result.TaskSlug, result.PackageID); err != nil {
			return err
		}
		var stage string
		if err := s.queryRow(txCtx, `SELECT stage FROM specs WHERE project_slug=$1 AND slug=$2`, s.project, result.TaskSlug).Scan(&stage); err != nil {
			return fmt.Errorf("postgres: read candidate task: %w", err)
		}
		if stage != "approved" && stage != "in_progress" {
			return storage.ErrSpecNotApproved
		}
		var target []byte
		if err := s.queryRow(txCtx, `SELECT body->'dispatch_target' FROM context_packages WHERE project_slug=$1 AND id=$2`, s.project, result.PackageID).Scan(&target); err != nil {
			return fmt.Errorf("postgres: read candidate QA: %w", err)
		}
		if err := s.checkDispatchQABasis(txCtx, target); err != nil {
			return err
		}
		consumed, err := json.Marshal(result.Condition)
		if err != nil {
			return fmt.Errorf("postgres: encode consumed candidate result: %w", err)
		}
		var consumedJoin []byte
		if joinProof != nil {
			consumedJoin, err = json.Marshal(joinProof)
			if err != nil {
				return fmt.Errorf("postgres: encode consumed candidate join: %w", err)
			}
		}
		var consumedSatisfaction []byte
		if satisfactionBasis != nil {
			consumedSatisfaction, err = json.Marshal(satisfactionBasis)
			if err != nil {
				return fmt.Errorf("postgres: encode consumed satisfaction: %w", err)
			}
		}
		successorID = newID("cat")
		_, err = s.exec(txCtx, `INSERT INTO candidate_attempts(project_slug,id,run_id,ordinal,predecessor_id,consumed_condition,consumed_join,consumed_satisfaction,granted_at)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, s.project, successorID, runID, result.CurrentAttempt.Number+1, expectedPreviousAttemptID, consumed, consumedJoin, consumedSatisfaction, s.now())
		if err != nil {
			return fmt.Errorf("postgres: grant next candidate attempt: %w", err)
		}
		result, err = s.readCandidateLoop(txCtx, runID)
		if err != nil {
			return err
		}
		result.GrantedAttempt = result.CurrentAttempt
		return nil
	})
	return result, err
}

// StopCandidateLoop prevents future grants without claiming the current work has stopped.
func (s *Store) StopCandidateLoop(ctx context.Context, runID, reason string, scope *storage.MailScope) (*storage.CandidateLoop, error) {
	if !validMailText(runID, 256) || strings.TrimSpace(reason) == "" || utf8.RuneCountInString(reason) > 4000 {
		return nil, storage.ErrInvalidCandidateLoop
	}
	var result *storage.CandidateLoop
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		run, user, err := s.deliveryHookActor(txCtx, scope)
		if err != nil {
			return err
		}
		var priorUser, priorRun, priorReason *string
		err = s.queryRow(txCtx, `SELECT stop_actor_user_id,stop_actor_run_id,stop_reason FROM candidate_loops WHERE project_slug=$1 AND run_id=$2 FOR UPDATE`, s.project, runID).Scan(&priorUser, &priorRun, &priorReason)
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrCandidateLoopNotFound
		}
		if err != nil {
			return fmt.Errorf("postgres: read candidate stop: %w", err)
		}
		if priorUser != nil {
			if *priorUser != user || (priorRun == nil) != (run == nil) || (run != nil && *priorRun != *run) || *priorReason != reason {
				return storage.ErrCandidateLoopConflict
			}
		} else if _, err := s.exec(txCtx, `UPDATE candidate_loops SET stop_actor_user_id=$3,stop_actor_run_id=$4,stop_reason=$5,stopped_at=$6 WHERE project_slug=$1 AND run_id=$2`, s.project, runID, user, run, reason, s.now()); err != nil {
			return fmt.Errorf("postgres: stop candidate loop: %w", err)
		}
		result, err = s.readCandidateLoop(txCtx, runID)
		return err
	})
	return result, err
}

// AbandonCandidateLoop is a named human budget decision, not completion or responsibility release.
func (s *Store) AbandonCandidateLoop(ctx context.Context, runID, reason string) (*storage.CandidateLoop, error) {
	if !validMailText(runID, 256) || strings.TrimSpace(reason) == "" || utf8.RuneCountInString(reason) > 4000 {
		return nil, storage.ErrInvalidCandidateLoop
	}
	var result *storage.CandidateLoop
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		identity, ok := auth.IdentityFromContext(txCtx)
		if !ok || identity.UserID == "" || identity.UserKind != storage.KindHuman {
			return storage.ErrPlanningForbidden
		}
		var priorUser, priorReason *string
		var completed bool
		err := s.queryRow(txCtx, `SELECT abandon_actor_user_id,abandon_reason,resolved_at IS NOT NULL FROM candidate_loops WHERE project_slug=$1 AND run_id=$2 FOR UPDATE`, s.project, runID).Scan(&priorUser, &priorReason, &completed)
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrCandidateLoopNotFound
		}
		if err != nil {
			return fmt.Errorf("postgres: read candidate abandonment: %w", err)
		}
		if completed {
			return storage.ErrCandidateLoopCompleted
		}
		if priorUser != nil {
			if *priorUser != identity.UserID || *priorReason != reason {
				return storage.ErrCandidateLoopConflict
			}
		} else if _, err := s.exec(txCtx, `UPDATE candidate_loops SET abandon_actor_user_id=$3,abandon_reason=$4,abandoned_at=$5 WHERE project_slug=$1 AND run_id=$2`, s.project, runID, identity.UserID, reason, s.now()); err != nil {
			return fmt.Errorf("postgres: abandon candidate loop: %w", err)
		}
		result, err = s.readCandidateLoop(txCtx, runID)
		return err
	})
	return result, err
}
