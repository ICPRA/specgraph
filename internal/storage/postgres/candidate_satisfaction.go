// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
)

const candidateJudgmentColumns = `id,run_id,attempt_id,delivery_id,value,reason,actor_user_id,actor_run_id,recorded_at,predecessor_id`
const candidateInterventionColumns = `id,run_id,attempt_id,judgment_id,report_ids,conflict_kind,reason,recorded_at,
 resolved_at,resolved_by_user_id,resolution_reason,resolution_judgment_id,resolution_report_ids`

func scanCandidateJudgment(row pgx.Row) (storage.CandidateSatisfactionJudgment, error) {
	var fact storage.CandidateSatisfactionJudgment
	err := row.Scan(&fact.ID, &fact.RunID, &fact.AttemptID, &fact.DeliveryID, &fact.Value, &fact.Reason,
		&fact.ActorUserID, &fact.ActorRunID, &fact.RecordedAt, &fact.PredecessorID)
	return fact, err
}

func scanCandidateIntervention(row pgx.Row) (storage.CandidateIntervention, error) {
	var fact storage.CandidateIntervention
	var reports, resolvedReports []byte
	err := row.Scan(&fact.ID, &fact.RunID, &fact.AttemptID, &fact.JudgmentID, &reports, &fact.ConflictKind, &fact.Reason,
		&fact.RecordedAt, &fact.ResolvedAt, &fact.ResolvedByUserID, &fact.ResolutionReason, &fact.ResolutionJudgmentID, &resolvedReports)
	if err != nil {
		return fact, err
	}
	if err := json.Unmarshal(reports, &fact.ReportIDs); err != nil {
		return fact, fmt.Errorf("postgres: decode intervention report IDs: %w", err)
	}
	if len(resolvedReports) != 0 {
		if err := json.Unmarshal(resolvedReports, &fact.ResolutionReportIDs); err != nil {
			return fact, fmt.Errorf("postgres: decode intervention resolution IDs: %w", err)
		}
	}
	return fact, nil
}

func candidateReportIDs(reports []storage.ReportBranchReport) []string {
	ids := make([]string, len(reports))
	for i := range reports {
		ids[i] = reports[i].ID
	}
	sort.Strings(ids)
	return ids
}

func (s *Store) latestCandidateJudgment(ctx context.Context, attemptID string) (*storage.CandidateSatisfactionJudgment, error) {
	fact, err := scanCandidateJudgment(s.queryRow(ctx, `SELECT `+candidateJudgmentColumns+` FROM candidate_satisfaction_judgments
 WHERE project_slug=$1 AND attempt_id=$2 ORDER BY event_order DESC LIMIT 1`, s.project, attemptID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: read candidate judgment: %w", err)
	}
	return &fact, nil
}

func (s *Store) latestCandidateIntervention(ctx context.Context, attemptID string) (*storage.CandidateIntervention, error) {
	fact, err := scanCandidateIntervention(s.queryRow(ctx, `SELECT `+candidateInterventionColumns+` FROM candidate_interventions
 WHERE project_slug=$1 AND attempt_id=$2 ORDER BY event_order DESC LIMIT 1`, s.project, attemptID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: read candidate intervention: %w", err)
	}
	return &fact, nil
}

func (s *Store) readCandidateSatisfactionState(ctx context.Context, state *storage.CandidateSatisfactionState, attempt *storage.CandidateAttempt) error {
	judgment, err := s.latestCandidateJudgment(ctx, attempt.ID)
	if err != nil {
		return err
	}
	state.Judgment = judgment
	if judgment != nil {
		if attempt.DeliveryID == nil || judgment.DeliveryID != *attempt.DeliveryID {
			return storage.ErrCandidateSatisfactionConflict
		}
		state.Value = judgment.Value
		state.Reason = ""
		if judgment.Value == "unknown" {
			state.Reason = "judgment_unknown"
		}
	}
	state.Intervention, err = s.latestCandidateIntervention(ctx, attempt.ID)
	return err
}

func candidateSatisfactionBasis(loop *storage.CandidateLoop) (*storage.CandidateSatisfactionBasis, error) {
	if loop.Satisfaction == nil {
		return nil, nil
	}
	if loop.Satisfaction.Intervention != nil && loop.Satisfaction.Intervention.ResolvedAt == nil {
		return nil, storage.ErrCandidateInterventionHeld
	}
	if loop.Condition == nil || loop.Satisfaction.Judgment == nil ||
		(loop.Condition.Value != "true" && loop.Condition.Value != "false") || loop.Condition.Value != loop.Satisfaction.Value {
		return nil, storage.ErrCandidateLoopWaiting
	}
	ids := candidateReportIDs(loop.Condition.Reports)
	basis := &storage.CandidateSatisfactionBasis{Value: loop.Condition.Value, JudgmentID: loop.Satisfaction.Judgment.ID, ReportIDs: ids}
	if intervention := loop.Satisfaction.Intervention; intervention != nil && intervention.ResolvedAt != nil {
		basis.InterventionID = &intervention.ID
	}
	return basis, nil
}

// Called in either original fact-producing transaction after its durable report or judgment insert.
func (s *Store) recordCandidateInterventionForDelivery(ctx context.Context, deliveryID string) error {
	var runID, attemptID string
	var plansBody []byte
	err := s.queryRow(ctx, `SELECT a.run_id,a.id,l.plan_sources FROM candidate_attempts a
 JOIN candidate_loops l ON l.project_slug=a.project_slug AND l.run_id=a.run_id
 WHERE a.project_slug=$1 AND a.delivery_id=$2 AND l.termination_kind='satisfaction'
 AND l.resolved_at IS NULL AND l.abandoned_at IS NULL
 AND NOT EXISTS(SELECT 1 FROM candidate_attempts later WHERE later.project_slug=a.project_slug AND later.run_id=a.run_id AND later.ordinal>a.ordinal)`, s.project, deliveryID).Scan(&runID, &attemptID, &plansBody)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("postgres: find candidate report intervention owner: %w", err)
	}
	var plans []storage.ReviewSource
	if err := json.Unmarshal(plansBody, &plans); err != nil {
		return fmt.Errorf("postgres: decode candidate intervention plans: %w", err)
	}
	reports, err := s.evaluateCandidateCondition(ctx, deliveryID, plans)
	if err != nil {
		return err
	}
	judgment, err := s.latestCandidateJudgment(ctx, attemptID)
	if err != nil {
		return err
	}
	if judgment == nil {
		return nil
	}
	conflict := ""
	if reports.Value == "true" && judgment.Value == "false" {
		conflict = "passed_false"
	}
	if reports.Value == "false" && judgment.Value == "true" {
		conflict = "failed_true"
	}
	if conflict == "" {
		return nil
	}
	var active bool
	if err := s.queryRow(ctx, `SELECT EXISTS(SELECT 1 FROM candidate_interventions WHERE project_slug=$1 AND attempt_id=$2 AND resolved_at IS NULL)`, s.project, attemptID).Scan(&active); err != nil {
		return fmt.Errorf("postgres: check active candidate intervention: %w", err)
	}
	if active {
		return nil
	}
	ids := candidateReportIDs(reports.Reports)
	encoded, err := json.Marshal(ids)
	if err != nil {
		return fmt.Errorf("postgres: encode intervention report IDs: %w", err)
	}
	reason := "Registered reports and satisfaction judgment disagree"
	_, err = s.exec(ctx, `INSERT INTO candidate_interventions(id,project_slug,run_id,attempt_id,judgment_id,report_ids,conflict_kind,reason,recorded_at)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, newID("civ"), s.project, runID, attemptID, judgment.ID, encoded, conflict, reason, s.now())
	if err != nil {
		return fmt.Errorf("postgres: record candidate intervention: %w", err)
	}
	return nil
}

// RecordCandidateSatisfactionJudgment uses the actual human or bound planning manager authority.
func (s *Store) RecordCandidateSatisfactionJudgment(ctx context.Context, req storage.RecordCandidateSatisfactionJudgmentRequest, scope *storage.MailScope) (*storage.CandidateSatisfactionJudgmentResult, error) {
	var result *storage.CandidateSatisfactionJudgmentResult
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		run, user, err := s.deliveryHookActor(txCtx, scope)
		if err != nil {
			return err
		}
		result, err = s.recordCandidateJudgment(txCtx, req, run, user, false)
		return err
	})
	return result, err
}

// RecordOwnCandidateSatisfactionJudgment resolves only the current bound run and attempt.
func (s *Store) RecordOwnCandidateSatisfactionJudgment(ctx context.Context, scope storage.MailScope, req storage.RecordOwnCandidateSatisfactionJudgmentRequest) (*storage.CandidateSatisfactionJudgmentResult, error) {
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" {
		return nil, auth.ErrUnauthenticated
	}
	var result *storage.CandidateSatisfactionJudgmentResult
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		run, _, err := s.reviewActorRun(txCtx, scope)
		if err != nil {
			return err
		}
		result, err = s.recordCandidateJudgment(txCtx, storage.RecordCandidateSatisfactionJudgmentRequest{
			RunID: run, AttemptID: req.AttemptID, ExpectedJudgmentID: req.ExpectedJudgmentID, Value: req.Value, Reason: req.Reason,
		}, &run, identity.UserID, true)
		return err
	})
	return result, err
}

func (s *Store) recordCandidateJudgment(ctx context.Context, req storage.RecordCandidateSatisfactionJudgmentRequest, actorRun *string, actorUser string, own bool) (*storage.CandidateSatisfactionJudgmentResult, error) {
	if !validMailText(req.RunID, 256) || !validMailText(req.AttemptID, 256) ||
		(req.ExpectedJudgmentID != nil && !validMailText(*req.ExpectedJudgmentID, 256)) ||
		(req.Value != "true" && req.Value != "false" && req.Value != "unknown") ||
		strings.TrimSpace(req.Reason) == "" || utf8.RuneCountInString(req.Reason) > 4000 {
		return nil, storage.ErrInvalidCandidateSatisfaction
	}
	var deliveryID *string
	var kind string
	var current bool
	err := s.queryRow(ctx, `SELECT a.delivery_id,l.termination_kind,
 NOT EXISTS(SELECT 1 FROM candidate_attempts later WHERE later.project_slug=a.project_slug AND later.run_id=a.run_id AND later.ordinal>a.ordinal)
 FROM candidate_attempts a JOIN candidate_loops l ON l.project_slug=a.project_slug AND l.run_id=a.run_id
 WHERE a.project_slug=$1 AND a.run_id=$2 AND a.id=$3`, s.project, req.RunID, req.AttemptID).Scan(&deliveryID, &kind, &current)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, storage.ErrCandidateSatisfactionConflict
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: read candidate judgment attempt: %w", err)
	}
	if kind != "satisfaction" || deliveryID == nil {
		return nil, storage.ErrCandidateSatisfactionConflict
	}
	prior, err := scanCandidateJudgment(s.queryRow(ctx, `SELECT `+candidateJudgmentColumns+` FROM candidate_satisfaction_judgments
 WHERE project_slug=$1 AND attempt_id=$2 AND predecessor_id IS NOT DISTINCT FROM $3::text`, s.project, req.AttemptID, req.ExpectedJudgmentID))
	if err == nil {
		if prior.RunID != req.RunID || prior.DeliveryID != *deliveryID || prior.Value != req.Value || prior.Reason != req.Reason ||
			prior.ActorUserID != actorUser || (prior.ActorRunID == nil) != (actorRun == nil) || (actorRun != nil && *prior.ActorRunID != *actorRun) {
			return nil, storage.ErrCandidateSatisfactionConflict
		}
		return &storage.CandidateSatisfactionJudgmentResult{Judgment: prior, Replayed: true}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("postgres: read candidate judgment successor: %w", err)
	}
	if own && !current {
		return nil, storage.ErrCandidateSatisfactionConflict
	}
	var latest string
	err = s.queryRow(ctx, `SELECT id FROM candidate_satisfaction_judgments WHERE project_slug=$1 AND attempt_id=$2 ORDER BY event_order DESC LIMIT 1`, s.project, req.AttemptID).Scan(&latest)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("postgres: read latest candidate judgment: %w", err)
	}
	if (err == nil && (req.ExpectedJudgmentID == nil || *req.ExpectedJudgmentID != latest)) || (errors.Is(err, pgx.ErrNoRows) && req.ExpectedJudgmentID != nil) {
		return nil, storage.ErrCandidateSatisfactionConflict
	}
	fact := storage.CandidateSatisfactionJudgment{ID: newID("csj"), RunID: req.RunID, AttemptID: req.AttemptID, DeliveryID: *deliveryID,
		Value: req.Value, Reason: req.Reason, ActorUserID: actorUser, ActorRunID: actorRun, RecordedAt: s.now(), PredecessorID: req.ExpectedJudgmentID}
	_, err = s.exec(ctx, `INSERT INTO candidate_satisfaction_judgments(id,project_slug,run_id,attempt_id,delivery_id,predecessor_id,value,reason,actor_user_id,actor_run_id,recorded_at)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, fact.ID, s.project, fact.RunID, fact.AttemptID, fact.DeliveryID, fact.PredecessorID,
		fact.Value, fact.Reason, fact.ActorUserID, fact.ActorRunID, fact.RecordedAt)
	if err != nil {
		return nil, fmt.Errorf("postgres: record candidate judgment: %w", err)
	}
	if err := s.recordCandidateInterventionForDelivery(ctx, *deliveryID); err != nil {
		return nil, err
	}
	return &storage.CandidateSatisfactionJudgmentResult{Judgment: fact}, nil
}

// ResolveCandidateIntervention names the exact current facts; agreement alone never auto-clears a hold.
func (s *Store) ResolveCandidateIntervention(ctx context.Context, req storage.ResolveCandidateInterventionRequest) (*storage.CandidateIntervention, error) {
	if !validMailText(req.RunID, 256) || !validMailText(req.AttemptID, 256) || !validMailText(req.InterventionID, 256) ||
		!validMailText(req.ExpectedJudgmentID, 256) || req.ExpectedReportIDs == nil ||
		strings.TrimSpace(req.Reason) == "" || utf8.RuneCountInString(req.Reason) > 4000 {
		return nil, storage.ErrInvalidCandidateSatisfaction
	}
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" || identity.UserKind != storage.KindHuman {
		return nil, storage.ErrPlanningForbidden
	}
	result := (*storage.CandidateIntervention)(nil)
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		user, err := s.ExistingAuth().GetUserByID(txCtx, identity.UserID)
		if err != nil {
			return err
		}
		if user.Kind != storage.KindHuman || user.DeletedAt != nil {
			return storage.ErrPlanningForbidden
		}
		intervention, err := scanCandidateIntervention(s.queryRow(txCtx, `SELECT `+candidateInterventionColumns+` FROM candidate_interventions WHERE project_slug=$1 AND run_id=$2 AND attempt_id=$3 AND id=$4`, s.project, req.RunID, req.AttemptID, req.InterventionID))
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrCandidateInterventionNotFound
		}
		if err != nil {
			return fmt.Errorf("postgres: read candidate intervention: %w", err)
		}
		ids := slices.Clone(req.ExpectedReportIDs)
		sort.Strings(ids)
		if intervention.ResolvedAt != nil {
			if intervention.ResolvedByUserID == nil || *intervention.ResolvedByUserID != identity.UserID ||
				intervention.ResolutionJudgmentID == nil || *intervention.ResolutionJudgmentID != req.ExpectedJudgmentID ||
				intervention.ResolutionReason == nil || *intervention.ResolutionReason != req.Reason || !slices.Equal(intervention.ResolutionReportIDs, ids) {
				return storage.ErrCandidateSatisfactionConflict
			}
			result = &intervention
			return nil
		}
		loop, err := s.readCandidateLoop(txCtx, req.RunID)
		if err != nil {
			return err
		}
		if loop.CurrentAttempt == nil || loop.CurrentAttempt.ID != req.AttemptID || loop.Satisfaction == nil || loop.Satisfaction.Intervention == nil || loop.Satisfaction.Intervention.ID != intervention.ID ||
			loop.Condition == nil || loop.Satisfaction.Judgment == nil ||
			loop.Condition.Value != loop.Satisfaction.Value || (loop.Condition.Value != "true" && loop.Condition.Value != "false") ||
			loop.Satisfaction.Judgment.ID != req.ExpectedJudgmentID || !slices.Equal(candidateReportIDs(loop.Condition.Reports), ids) {
			return storage.ErrCandidateSatisfactionConflict
		}
		reports, err := json.Marshal(ids)
		if err != nil {
			return fmt.Errorf("postgres: encode candidate resolution reports: %w", err)
		}
		_, err = s.exec(txCtx, `UPDATE candidate_interventions SET resolved_at=$3,resolved_by_user_id=$4,resolution_reason=$5,
 resolution_judgment_id=$6,resolution_report_ids=$7 WHERE project_slug=$1 AND id=$2 AND resolved_at IS NULL`, s.project, intervention.ID,
			s.now(), identity.UserID, req.Reason, req.ExpectedJudgmentID, reports)
		if err != nil {
			return fmt.Errorf("postgres: resolve candidate intervention: %w", err)
		}
		resolved, err := scanCandidateIntervention(s.queryRow(txCtx, `SELECT `+candidateInterventionColumns+` FROM candidate_interventions WHERE project_slug=$1 AND id=$2`, s.project, intervention.ID))
		if err != nil {
			return fmt.Errorf("postgres: read resolved candidate intervention: %w", err)
		}
		result = &resolved
		return nil
	})
	return result, err
}

// ReadCandidateSatisfactionHistory pages only one named run and attempt, with independent bounded cursors.
func (s *Store) ReadCandidateSatisfactionHistory(ctx context.Context, runID, attemptID, judgmentCursor, interventionCursor string) (*storage.CandidateSatisfactionHistoryPage, error) {
	if !validMailText(runID, 256) || !validMailText(attemptID, 256) {
		return nil, storage.ErrInvalidCandidateSatisfaction
	}
	parse := func(value string) (int64, error) {
		if value == "" {
			return 0, nil
		}
		order, err := strconv.ParseInt(value, 10, 64)
		if err != nil || order < 1 || strconv.FormatInt(order, 10) != value {
			return 0, storage.ErrInvalidCandidateSatisfaction
		}
		return order, nil
	}
	beforeJudgment, err := parse(judgmentCursor)
	if err != nil {
		return nil, err
	}
	beforeIntervention, err := parse(interventionCursor)
	if err != nil {
		return nil, err
	}
	var page *storage.CandidateSatisfactionHistoryPage
	err = s.RunReadSnapshot(ctx, func(snapshotCtx context.Context) error {
		var exists bool
		if err := s.queryRow(snapshotCtx, `SELECT EXISTS(SELECT 1 FROM candidate_attempts WHERE project_slug=$1 AND run_id=$2 AND id=$3)`, s.project, runID, attemptID).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return storage.ErrCandidateSatisfactionConflict
		}
		result := &storage.CandidateSatisfactionHistoryPage{RunID: runID, AttemptID: attemptID,
			Judgments: []storage.CandidateSatisfactionJudgment{}, Interventions: []storage.CandidateIntervention{}}
		rows, err := s.query(snapshotCtx, `SELECT event_order,`+candidateJudgmentColumns+` FROM candidate_satisfaction_judgments
 WHERE project_slug=$1 AND run_id=$2 AND attempt_id=$3 AND ($4::bigint=0 OR event_order<$4) ORDER BY event_order DESC LIMIT 51`, s.project, runID, attemptID, beforeJudgment)
		if err != nil {
			return err
		}
		var orders []int64
		for rows.Next() {
			var order int64
			var fact storage.CandidateSatisfactionJudgment
			if err := rows.Scan(&order, &fact.ID, &fact.RunID, &fact.AttemptID, &fact.DeliveryID, &fact.Value, &fact.Reason,
				&fact.ActorUserID, &fact.ActorRunID, &fact.RecordedAt, &fact.PredecessorID); err != nil {
				rows.Close()
				return err
			}
			result.Judgments = append(result.Judgments, fact)
			orders = append(orders, order)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		if len(result.Judgments) > 50 {
			result.Judgments = result.Judgments[:50]
			cursor := strconv.FormatInt(orders[49], 10)
			result.NextJudgmentCursor = &cursor
		}
		rows, err = s.query(snapshotCtx, `SELECT event_order,`+candidateInterventionColumns+` FROM candidate_interventions
 WHERE project_slug=$1 AND run_id=$2 AND attempt_id=$3 AND ($4::bigint=0 OR event_order<$4) ORDER BY event_order DESC LIMIT 51`, s.project, runID, attemptID, beforeIntervention)
		if err != nil {
			return err
		}
		orders = nil
		for rows.Next() {
			var order int64
			var fact storage.CandidateIntervention
			var reports, resolved []byte
			if err := rows.Scan(&order, &fact.ID, &fact.RunID, &fact.AttemptID, &fact.JudgmentID, &reports, &fact.ConflictKind, &fact.Reason,
				&fact.RecordedAt, &fact.ResolvedAt, &fact.ResolvedByUserID, &fact.ResolutionReason, &fact.ResolutionJudgmentID, &resolved); err != nil {
				rows.Close()
				return err
			}
			if err := json.Unmarshal(reports, &fact.ReportIDs); err != nil {
				rows.Close()
				return err
			}
			if len(resolved) > 0 {
				if err := json.Unmarshal(resolved, &fact.ResolutionReportIDs); err != nil {
					rows.Close()
					return err
				}
			}
			result.Interventions = append(result.Interventions, fact)
			orders = append(orders, order)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		if len(result.Interventions) > 50 {
			result.Interventions = result.Interventions[:50]
			cursor := strconv.FormatInt(orders[49], 10)
			result.NextInterventionCursor = &cursor
		}
		page = result
		return nil
	})
	return page, err
}
