// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/storage"
)

// ArmReportFlowJoin binds an original prepared target; it does not start that run.
func (s *Store) ArmReportFlowJoin(ctx context.Context, req storage.ArmReportFlowJoinRequest, scope *storage.MailScope) (*storage.ReportFlowJoin, error) {
	if !validMailText(req.FlowID, 256) || !validMailText(req.RunID, 256) || (req.Mode != "all" && req.Mode != "any") {
		return nil, storage.ErrInvalidReportFlowJoin
	}
	var result *storage.ReportFlowJoin
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		run, user, err := s.deliveryHookActor(txCtx, scope)
		if err != nil {
			return err
		}
		actor := "user:" + user
		if run != nil {
			actor = "run:" + *run
		}
		var priorFlow, priorMode string
		var priorSkip bool
		err = s.queryRow(txCtx, `SELECT flow_id,mode,allow_empty_skip FROM report_flow_joins WHERE project_slug=$1 AND run_id=$2`, s.project, req.RunID).Scan(&priorFlow, &priorMode, &priorSkip)
		if err == nil {
			if priorFlow != req.FlowID || priorMode != req.Mode || priorSkip != req.AllowEmptySkip {
				return storage.ErrReportFlowJoinConflict
			}
			result, err = s.readReportFlowJoin(txCtx, req.RunID)
			return err
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("postgres: read report join replay: %w", err)
		}
		var cancelled bool
		err = s.queryRow(txCtx, `SELECT cancelled_at IS NOT NULL FROM report_branch_flows WHERE project_slug=$1 AND id=$2`, s.project, req.FlowID).Scan(&cancelled)
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrReportBranchFlowNotFound
		}
		if err != nil {
			return fmt.Errorf("postgres: read report join flow: %w", err)
		}
		if cancelled {
			return storage.ErrReportFlowJoinConflict
		}
		var state, kind, packageID string
		var target []byte
		var dispatched, cancelledRun, selfMember bool
		err = s.queryRow(txCtx, `SELECT b.state,b.executor_kind,b.package_id,
 CASE WHEN b.executor_kind='program' THEN p.body->'program_target' ELSE p.body->'dispatch_target' END,
 EXISTS(SELECT 1 FROM run_dispatches d WHERE d.project_slug=b.project_slug AND d.run_id=b.id),
 EXISTS(SELECT 1 FROM run_preparation_cancellations c WHERE c.project_slug=b.project_slug AND c.run_id=b.id),
 EXISTS(SELECT 1 FROM report_branch_members m WHERE m.project_slug=b.project_slug AND m.run_id=b.id AND m.flow_id=$3)
 FROM run_bindings b JOIN context_packages p ON p.project_slug=b.project_slug AND p.id=b.package_id
 WHERE b.project_slug=$1 AND b.id=$2`, s.project, req.RunID, req.FlowID).Scan(&state, &kind, &packageID, &target, &dispatched, &cancelledRun, &selfMember)
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrReportFlowJoinConflict
		}
		if err != nil {
			return fmt.Errorf("postgres: read report join target: %w", err)
		}
		if dispatched || cancelledRun || selfMember || len(target) == 0 || string(target) == "null" ||
			(kind == "agent" && state != "bound" && state != "prepared") || (kind == "program" && state != "prepared") {
			return storage.ErrReportFlowJoinConflict
		}
		_, err = s.exec(txCtx, `INSERT INTO report_flow_joins(project_slug,flow_id,run_id,package_id,mode,allow_empty_skip,configured_by,configured_at)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, s.project, req.FlowID, req.RunID, packageID, req.Mode, req.AllowEmptySkip, actor, s.now())
		if err != nil {
			return fmt.Errorf("postgres: insert report join: %w", err)
		}
		result, err = s.readReportFlowJoin(txCtx, req.RunID)
		return err
	})
	return result, err
}

// ReadReportFlowJoin projects the current decision and immutable first-admission proof.
func (s *Store) ReadReportFlowJoin(ctx context.Context, runID string) (*storage.ReportFlowJoin, error) {
	if !validMailText(runID, 256) {
		return nil, storage.ErrInvalidReportFlowJoin
	}
	var result *storage.ReportFlowJoin
	err := s.RunReadSnapshot(ctx, func(snapshotCtx context.Context) error {
		var err error
		result, err = s.readReportFlowJoin(snapshotCtx, runID)
		return err
	})
	return result, err
}

func (s *Store) readReportFlowJoin(ctx context.Context, runID string) (*storage.ReportFlowJoin, error) {
	var result storage.ReportFlowJoin
	var admissionID *string
	var proofJSON []byte
	var cancelled bool
	err := s.queryRow(ctx, `SELECT j.flow_id,j.run_id,j.package_id,j.mode,j.allow_empty_skip,j.configured_by,j.configured_at,
 f.cancelled_at IS NOT NULL,d.id,d.report_join_basis
 FROM report_flow_joins j JOIN report_branch_flows f ON f.project_slug=j.project_slug AND f.id=j.flow_id
 LEFT JOIN run_dispatches d ON d.project_slug=j.project_slug AND d.run_id=j.run_id
 WHERE j.project_slug=$1 AND j.run_id=$2`, s.project, runID).Scan(&result.FlowID, &result.RunID, &result.PackageID,
		&result.Mode, &result.AllowEmptySkip, &result.ConfiguredBy, &result.ConfiguredAt, &cancelled, &admissionID, &proofJSON)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, storage.ErrReportFlowJoinNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: read report join: %w", err)
	}
	if admissionID != nil {
		var proof storage.ReportFlowJoinProof
		if err := json.Unmarshal(proofJSON, &proof); err != nil {
			return nil, fmt.Errorf("postgres: decode report join proof: %w", err)
		}
		result.Admission = &storage.ReportFlowJoinAdmission{ID: *admissionID, Proof: proof}
	}
	if cancelled {
		result.Evaluation = storage.ReportFlowJoinEvaluation{Participants: []storage.ReportFlowJoinMember{}, Unknown: []storage.ReportFlowJoinMember{}, Conditions: []storage.ReportFlowJoinCondition{}, Reason: "flow_cancelled"}
		return &result, nil
	}
	result.Evaluation, err = s.evaluateReportFlowJoin(ctx, result.ReportFlowJoinConfig)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// reportJoinAdmissionBasis is called under the original project lock before first grant.
func (s *Store) reportJoinAdmissionBasis(ctx context.Context, runID, packageID string) (*storage.ReportFlowJoinProof, error) {
	var cfg storage.ReportFlowJoinConfig
	var cancelled bool
	err := s.queryRow(ctx, `SELECT j.flow_id,j.run_id,j.package_id,j.mode,j.allow_empty_skip,f.cancelled_at IS NOT NULL
 FROM report_flow_joins j JOIN report_branch_flows f ON f.project_slug=j.project_slug AND f.id=j.flow_id
 WHERE j.project_slug=$1 AND j.run_id=$2`, s.project, runID).Scan(&cfg.FlowID, &cfg.RunID, &cfg.PackageID, &cfg.Mode, &cfg.AllowEmptySkip, &cancelled)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: read report join admission: %w", err)
	}
	if cfg.PackageID != packageID {
		return nil, storage.ErrReportFlowJoinConflict
	}
	if cancelled {
		return nil, storage.ErrReportFlowJoinUnsatisfied
	}
	evaluation, err := s.evaluateReportFlowJoin(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if !evaluation.Satisfied {
		return nil, storage.ErrReportFlowJoinUnsatisfied
	}
	return &storage.ReportFlowJoinProof{FlowID: cfg.FlowID, TargetRunID: runID, Mode: cfg.Mode, Evaluation: evaluation}, nil
}

func (s *Store) evaluateReportFlowJoin(ctx context.Context, cfg storage.ReportFlowJoinConfig) (storage.ReportFlowJoinEvaluation, error) {
	result := storage.ReportFlowJoinEvaluation{Participants: []storage.ReportFlowJoinMember{}, Unknown: []storage.ReportFlowJoinMember{}, Conditions: []storage.ReportFlowJoinCondition{}}
	var definition []byte
	if err := s.queryRow(ctx, `SELECT definition FROM report_branch_flows WHERE project_slug=$1 AND id=$2`, s.project, cfg.FlowID).Scan(&definition); err != nil {
		return result, fmt.Errorf("postgres: read report join definition: %w", err)
	}
	var flow storage.ArmReportBranchFlowRequest
	if err := json.Unmarshal(definition, &flow); err != nil {
		return result, fmt.Errorf("postgres: decode report join definition: %w", err)
	}
	conditions := make(map[string]storage.ArmReportBranchCondition, len(flow.Conditions))
	for _, c := range flow.Conditions {
		conditions[c.Key] = c
	}
	evaluations := make(map[string]*storage.ReportBranchEvaluation, len(flow.Conditions))
	rows, err := s.query(ctx, `SELECT m.run_id,m.package_id,m.condition_key,m.when_value,b.task_spec_slug,d.id,d.report_flow_basis
 FROM report_branch_members m JOIN run_bindings b ON b.project_slug=m.project_slug AND b.id=m.run_id
 LEFT JOIN run_dispatches d ON d.project_slug=m.project_slug AND d.run_id=m.run_id
 WHERE m.project_slug=$1 AND m.flow_id=$2 ORDER BY m.run_id`, s.project, cfg.FlowID)
	if err != nil {
		return result, fmt.Errorf("postgres: read report join members: %w", err)
	}
	type memberRow struct {
		Member    storage.ReportFlowJoinMember
		TaskSlug  string
		ProofJSON []byte
	}
	records := []memberRow{}
	for rows.Next() {
		var row memberRow
		if err := rows.Scan(&row.Member.RunID, &row.Member.PackageID, &row.Member.ConditionKey, &row.Member.When, &row.TaskSlug, &row.Member.AdmissionID, &row.ProofJSON); err != nil {
			rows.Close()
			return result, fmt.Errorf("postgres: scan report join member: %w", err)
		}
		records = append(records, row)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return result, fmt.Errorf("postgres: collect report join members: %w", err)
	}
	rows.Close()
	for _, row := range records {
		member := row.Member
		if member.AdmissionID != nil {
			var proof storage.ReportBranchProof
			if err := json.Unmarshal(row.ProofJSON, &proof); err != nil {
				return result, fmt.Errorf("postgres: decode activated branch proof: %w", err)
			}
			if proof.FlowID != cfg.FlowID || proof.ConditionKey != member.ConditionKey {
				return result, storage.ErrReportFlowJoinConflict
			}
			member.State = "admitted"
			member.Basis = &proof
			member.Completion, err = s.currentRunCompletion(ctx, member.RunID, row.TaskSlug)
			if err != nil {
				return result, err
			}
			result.Participants = append(result.Participants, member)
			continue
		}
		evaluation := evaluations[member.ConditionKey]
		if evaluation == nil {
			condition, ok := conditions[member.ConditionKey]
			if !ok {
				return result, storage.ErrReportFlowJoinConflict
			}
			evaluation, err = s.evaluateReportBranchCondition(ctx, cfg.FlowID, condition)
			if err != nil {
				return result, err
			}
			evaluations[member.ConditionKey] = evaluation
			result.Conditions = append(result.Conditions, storage.ReportFlowJoinCondition{Key: member.ConditionKey, Evaluation: *evaluation})
		}
		if evaluation.Value == "unknown" {
			member.State = "unknown"
			result.Unknown = append(result.Unknown, member)
		} else if evaluation.Value == member.When {
			member.State = "selected"
			result.Participants = append(result.Participants, member)
		}
	}
	if len(result.Participants) == 0 && len(result.Unknown) == 0 {
		if cfg.AllowEmptySkip {
			result.Satisfied, result.ExplicitSkip = true, true
		} else {
			result.Reason = "empty_path"
		}
		return result, nil
	}
	if cfg.Mode == "all" && len(result.Unknown) != 0 {
		result.Reason = "unknown_members"
		return result, nil
	}
	for _, member := range result.Participants {
		if member.Completion == nil {
			if cfg.Mode == "all" {
				result.Reason = "members_incomplete"
				return result, nil
			}
			continue
		}
		if result.Witness == nil {
			result.Witness = member.Completion
		}
		if cfg.Mode == "any" {
			result.Satisfied = true
			return result, nil
		}
	}
	if cfg.Mode == "all" {
		result.Satisfied = true
	} else if len(result.Unknown) != 0 {
		result.Reason = "unknown_members"
	} else {
		result.Reason = "members_incomplete"
	}
	return result, nil
}

// currentRunCompletion never substitutes a task-global done state or another run's event.
func (s *Store) currentRunCompletion(ctx context.Context, runID, slug string) (*storage.ReportFlowCompletionRef, error) {
	rows, err := s.query(ctx, `SELECT id FROM execution_events WHERE project_slug=$1 AND spec_slug=$2 AND agent=$3 AND event_type='completion'
 ORDER BY created_at DESC,id DESC`, s.project, slug, runID)
	if err != nil {
		return nil, fmt.Errorf("postgres: read report join completions: %w", err)
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, fmt.Errorf("postgres: scan report join completion: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("postgres: collect report join completions: %w", err)
	}
	rows.Close()
	for _, id := range ids {
		if err := s.checkWorkCompletionFact(ctx, slug, "execution", id); err != nil {
			if errors.Is(err, storage.ErrCompletionHookConflict) || errors.Is(err, storage.ErrReviewHumanHold) || errors.Is(err, storage.ErrSpecNotFound) {
				continue
			}
			return nil, err
		}
		return &storage.ReportFlowCompletionRef{ID: id, RunID: runID, TaskSlug: slug}, nil
	}
	return nil, nil
}
