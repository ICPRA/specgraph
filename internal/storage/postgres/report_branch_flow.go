// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/storage"
)

// ArmReportBranchFlow fixes one occurrence and its existing run/package branches.
func (s *Store) ArmReportBranchFlow(ctx context.Context, req storage.ArmReportBranchFlowRequest, scope *storage.MailScope) (*storage.ReportBranchFlow, error) {
	if !subdivisionKeyPattern.MatchString(req.IdempotencyKey) || len(req.Conditions) == 0 || len(req.Branches) == 0 || (req.CandidateAttemptID != "" && !validMailText(req.CandidateAttemptID, 256)) {
		return nil, storage.ErrInvalidReportBranchFlow
	}
	req.Conditions = slices.Clone(req.Conditions)
	req.Branches = slices.Clone(req.Branches)
	keys := make(map[string]bool, len(req.Conditions))
	for i := range req.Conditions {
		c := &req.Conditions[i]
		if !subdivisionKeyPattern.MatchString(c.Key) || keys[c.Key] {
			return nil, storage.ErrInvalidReportBranchFlow
		}
		keys[c.Key] = true
		switch c.Kind {
		case "report":
			if c.Report == nil || c.Judgment != nil || !validMailText(c.Report.DeliveryID, 256) {
				return nil, storage.ErrInvalidReportBranchFlow
			}
			copy := *c.Report
			if copy.InputSources == nil {
				copy.InputSources = []storage.ReviewSource{}
			}
			c.Report = &copy
		case "judgment":
			if c.Judgment == nil || c.Report != nil || !validMailText(c.Judgment.Criterion, 4000) || c.Judgment.InputSources == nil ||
				(c.Judgment.DeliveryID != nil && !validMailText(*c.Judgment.DeliveryID, 256)) {
				return nil, storage.ErrInvalidReportBranchFlow
			}
			copy := *c.Judgment
			copy.InputSources = slices.Clone(copy.InputSources)
			sort.Slice(copy.InputSources, func(i, j int) bool {
				left, _ := json.Marshal(copy.InputSources[i])
				right, _ := json.Marshal(copy.InputSources[j])
				return bytes.Compare(left, right) < 0
			})
			c.Judgment = &copy
		default:
			return nil, storage.ErrInvalidReportBranchFlow
		}
	}
	sort.Slice(req.Conditions, func(i, j int) bool { return req.Conditions[i].Key < req.Conditions[j].Key })
	runs := make(map[string]bool, len(req.Branches))
	for _, branch := range req.Branches {
		if !validMailText(branch.RunID, 256) || !keys[branch.ConditionKey] || (branch.When != "true" && branch.When != "false") || runs[branch.RunID] {
			return nil, storage.ErrInvalidReportBranchFlow
		}
		runs[branch.RunID] = true
	}
	sort.Slice(req.Branches, func(i, j int) bool { return req.Branches[i].RunID < req.Branches[j].RunID })
	definition, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("postgres: encode report branch definition: %w", err)
	}
	var result *storage.ReportBranchFlow
	err = s.RunInTransaction(ctx, func(txCtx context.Context) error {
		run, user, actorErr := s.deliveryHookActor(txCtx, scope)
		if actorErr != nil {
			return actorErr
		}
		actor := "user:" + user
		if run != nil {
			actor = "run:" + *run
		}
		var priorID string
		var same bool
		err := s.queryRow(txCtx, `SELECT id,definition=$4::jsonb FROM report_branch_flows
 WHERE project_slug=$1 AND configured_by=$2 AND idempotency_key=$3`, s.project, actor, req.IdempotencyKey, definition).Scan(&priorID, &same)
		if err == nil {
			if !same {
				return storage.ErrReportBranchFlowConflict
			}
			result, err = s.readReportBranchFlow(txCtx, priorID)
			return err
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("postgres: read report branch replay: %w", err)
		}
		if req.CandidateAttemptID != "" {
			var deliveryID, priorFlow *string
			var plansJSON []byte
			var hasJoin, completed, abandoned, current bool
			err := s.queryRow(txCtx, `SELECT a.delivery_id,a.flow_id,l.plan_sources,l.join_mode IS NOT NULL,
 l.resolved_at IS NOT NULL,l.abandoned_at IS NOT NULL,
 NOT EXISTS(SELECT 1 FROM candidate_attempts later WHERE later.project_slug=a.project_slug AND later.run_id=a.run_id AND later.ordinal>a.ordinal)
 FROM candidate_attempts a JOIN candidate_loops l ON l.project_slug=a.project_slug AND l.run_id=a.run_id
 WHERE a.project_slug=$1 AND a.id=$2`, s.project, req.CandidateAttemptID).
				Scan(&deliveryID, &priorFlow, &plansJSON, &hasJoin, &completed, &abandoned, &current)
			if errors.Is(err, pgx.ErrNoRows) {
				return storage.ErrInvalidReportBranchFlow
			}
			if err != nil {
				return fmt.Errorf("postgres: read candidate flow attempt: %w", err)
			}
			if deliveryID == nil || priorFlow != nil || !hasJoin || completed || abandoned || !current {
				return storage.ErrReportBranchFlowConflict
			}
			var plans []storage.ReviewSource
			if err := json.Unmarshal(plansJSON, &plans); err != nil {
				return fmt.Errorf("postgres: decode candidate flow plans: %w", err)
			}
			usedKeys := make(map[string]bool, len(req.Branches))
			for _, branch := range req.Branches {
				usedKeys[branch.ConditionKey] = true
			}
			matched := false
			for _, condition := range req.Conditions {
				if !usedKeys[condition.Key] {
					continue
				}
				if condition.Kind == "report" {
					matched = matched || (condition.Report.DeliveryID == *deliveryID && slices.Contains(plans, condition.Report.PlanSource))
				} else if condition.Judgment.DeliveryID != nil && *condition.Judgment.DeliveryID == *deliveryID {
					for _, source := range condition.Judgment.InputSources {
						matched = matched || slices.Contains(plans, source)
					}
				}
			}
			if !matched {
				return storage.ErrInvalidReportBranchFlow
			}
		}
		for _, c := range req.Conditions {
			if c.Kind == "judgment" {
				if c.Judgment.DeliveryID != nil {
					var exists bool
					if err := s.queryRow(txCtx, `SELECT EXISTS(SELECT 1 FROM deliveries WHERE project_slug=$1 AND id=$2)`, s.project, *c.Judgment.DeliveryID).Scan(&exists); err != nil {
						return fmt.Errorf("postgres: check judgment delivery: %w", err)
					}
					if !exists {
						return storage.ErrInvalidReportBranchFlow
					}
				}
				positions := make(map[storage.ReviewSource]bool, len(c.Judgment.InputSources))
				for _, source := range c.Judgment.InputSources {
					position := reportJudgmentSourcePosition(source)
					if positions[position] {
						return storage.ErrInvalidReportBranchFlow
					}
					positions[position] = true
					if err := s.validateReviewSource(txCtx, &source); err != nil {
						return err
					}
					if source.Kind == "specgraph" {
						if _, ok := reportBranchField(storage.SpecFields{}, source.Field); !ok {
							return storage.ErrInvalidReportBranchFlow
						}
					}
				}
				continue
			}
			var snapshot, targetBody []byte
			err := s.queryRow(txCtx, `SELECT d.snapshot,p.body->'dispatch_target' FROM deliveries d
 JOIN run_bindings b ON b.project_slug=d.project_slug AND b.id=d.run_binding_id
 JOIN context_packages p ON p.project_slug=b.project_slug AND p.id=b.package_id
 WHERE d.project_slug=$1 AND d.id=$2`, s.project, c.Report.DeliveryID).Scan(&snapshot, &targetBody)
			if errors.Is(err, pgx.ErrNoRows) {
				return storage.ErrInvalidReportBranchFlow
			}
			if err != nil {
				return fmt.Errorf("postgres: read report branch delivery: %w", err)
			}
			if _, err := testDeliveryCommit(snapshot); err != nil {
				return storage.ErrInvalidReportBranchFlow
			}
			var target testReportTarget
			if json.Unmarshal(targetBody, &target) != nil || target.WorkPurpose != "implementation" || target.QABasis == nil || !slices.Contains(target.QABasis.TestPlanSources, c.Report.PlanSource) {
				return storage.ErrInvalidReportBranchFlow
			}
			for _, source := range append([]storage.ReviewSource{c.Report.PlanSource}, c.Report.InputSources...) {
				if err := s.validateReviewSource(txCtx, &source); err != nil {
					return err
				}
				if source.Kind == "specgraph" {
					if _, ok := reportBranchField(storage.SpecFields{}, source.Field); !ok {
						return storage.ErrInvalidReportBranchFlow
					}
				}
			}
		}
		members := make([]storage.ReportBranchMember, len(req.Branches))
		for i, branch := range req.Branches {
			var state, kind, packageID string
			var dispatched, cancelled, alreadyMember bool
			var target []byte
			err := s.queryRow(txCtx, `SELECT b.state,b.executor_kind,b.package_id,
 CASE WHEN b.executor_kind='program' THEN p.body->'program_target' ELSE p.body->'dispatch_target' END,
 EXISTS(SELECT 1 FROM run_dispatches d WHERE d.project_slug=b.project_slug AND d.run_id=b.id),
 EXISTS(SELECT 1 FROM run_preparation_cancellations c WHERE c.project_slug=b.project_slug AND c.run_id=b.id),
 EXISTS(SELECT 1 FROM report_branch_members m WHERE m.project_slug=b.project_slug AND m.run_id=b.id)
 FROM run_bindings b JOIN context_packages p ON p.project_slug=b.project_slug AND p.id=b.package_id
 WHERE b.project_slug=$1 AND b.id=$2`, s.project, branch.RunID).Scan(&state, &kind, &packageID, &target, &dispatched, &cancelled, &alreadyMember)
			if errors.Is(err, pgx.ErrNoRows) {
				return storage.ErrReportBranchFlowConflict
			}
			if err != nil {
				return fmt.Errorf("postgres: read report branch run: %w", err)
			}
			if dispatched || cancelled || alreadyMember || len(target) == 0 || string(target) == "null" || (kind == "agent" && state != "bound" && state != "prepared") || (kind == "program" && state != "prepared") {
				return storage.ErrReportBranchFlowConflict
			}
			members[i] = storage.ReportBranchMember{RunID: branch.RunID, PackageID: packageID, ConditionKey: branch.ConditionKey, When: branch.When}
		}
		id := newID("rbf")
		_, err = s.exec(txCtx, `INSERT INTO report_branch_flows(id,project_slug,configured_by,idempotency_key,definition,created_at)
 VALUES($1,$2,$3,$4,$5,$6)`, id, s.project, actor, req.IdempotencyKey, definition, s.now())
		if err != nil {
			return fmt.Errorf("postgres: insert report branch flow: %w", err)
		}
		for _, member := range members {
			_, err = s.exec(txCtx, `INSERT INTO report_branch_members(project_slug,flow_id,run_id,package_id,condition_key,when_value)
 VALUES($1,$2,$3,$4,$5,$6)`, s.project, id, member.RunID, member.PackageID, member.ConditionKey, member.When)
			if err != nil {
				return fmt.Errorf("postgres: insert report branch member: %w", err)
			}
		}
		if req.CandidateAttemptID != "" {
			tag, err := s.exec(txCtx, `UPDATE candidate_attempts SET flow_id=$3 WHERE project_slug=$1 AND id=$2 AND flow_id IS NULL`, s.project, req.CandidateAttemptID, id)
			if err != nil {
				return fmt.Errorf("postgres: bind candidate report flow: %w", err)
			}
			if tag.RowsAffected() != 1 {
				return storage.ErrReportBranchFlowConflict
			}
		}
		result, err = s.readReportBranchFlow(txCtx, id)
		return err
	})
	return result, err
}

// ReadReportBranchFlow projects current applicability without recording a new evaluation.
func (s *Store) ReadReportBranchFlow(ctx context.Context, id string) (*storage.ReportBranchFlow, error) {
	if !validMailText(id, 256) {
		return nil, storage.ErrInvalidReportBranchFlow
	}
	var result *storage.ReportBranchFlow
	err := s.RunReadSnapshot(ctx, func(snapshotCtx context.Context) error {
		var err error
		result, err = s.readReportBranchFlow(snapshotCtx, id)
		return err
	})
	return result, err
}

// ReadReportBranchFlowByRun recovers the unique occurrence from its original run.
func (s *Store) ReadReportBranchFlowByRun(ctx context.Context, runID string) (*storage.ReportBranchFlow, error) {
	if !validMailText(runID, 256) {
		return nil, storage.ErrInvalidReportBranchFlow
	}
	var result *storage.ReportBranchFlow
	err := s.RunReadSnapshot(ctx, func(snapshotCtx context.Context) error {
		var id string
		err := s.queryRow(snapshotCtx, `SELECT flow_id FROM report_branch_members WHERE project_slug=$1 AND run_id=$2`, s.project, runID).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrReportBranchFlowNotFound
		}
		if err != nil {
			return fmt.Errorf("postgres: lookup report branch run: %w", err)
		}
		result, err = s.readReportBranchFlow(snapshotCtx, id)
		return err
	})
	return result, err
}

func (s *Store) readReportBranchFlow(ctx context.Context, id string) (*storage.ReportBranchFlow, error) {
	var result storage.ReportBranchFlow
	var definition []byte
	err := s.queryRow(ctx, `SELECT id,project_slug,configured_by,idempotency_key,definition,created_at,cancelled_by,cancel_reason,cancelled_at
 FROM report_branch_flows WHERE project_slug=$1 AND id=$2`, s.project, id).Scan(&result.ID, &result.Project, &result.ConfiguredBy,
		&result.IdempotencyKey, &definition, &result.ConfiguredAt, &result.CancelledBy, &result.CancelReason, &result.CancelledAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, storage.ErrReportBranchFlowNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: read report branch flow: %w", err)
	}
	var attemptID string
	err = s.queryRow(ctx, `SELECT id FROM candidate_attempts WHERE project_slug=$1 AND flow_id=$2`, s.project, id).Scan(&attemptID)
	if err == nil {
		result.CandidateAttemptID = &attemptID
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("postgres: read candidate flow binding: %w", err)
	}
	var req storage.ArmReportBranchFlowRequest
	if err := json.Unmarshal(definition, &req); err != nil {
		return nil, fmt.Errorf("postgres: decode report branch definition: %w", err)
	}
	result.Conditions = make([]storage.ReportBranchCondition, len(req.Conditions))
	evaluations := make(map[string]*storage.ReportBranchEvaluation, len(req.Conditions))
	for i, c := range req.Conditions {
		evaluation := &storage.ReportBranchEvaluation{Value: "unknown", Reason: "flow_cancelled"}
		if result.CancelledAt == nil {
			var err error
			evaluation, err = s.evaluateReportBranchCondition(ctx, id, c)
			if err != nil {
				return nil, err
			}
		}
		result.Conditions[i] = storage.ReportBranchCondition{Key: c.Key, Kind: c.Kind, Report: c.Report, Judgment: c.Judgment, Evaluation: evaluation}
		evaluations[c.Key] = evaluation
	}
	rows, err := s.query(ctx, `SELECT m.run_id,m.package_id,m.condition_key,m.when_value,d.id,d.report_flow_basis
 FROM report_branch_members m LEFT JOIN run_dispatches d ON d.project_slug=m.project_slug AND d.run_id=m.run_id
 WHERE m.project_slug=$1 AND m.flow_id=$2 ORDER BY m.run_id`, s.project, id)
	if err != nil {
		return nil, fmt.Errorf("postgres: read report branch members: %w", err)
	}
	defer rows.Close()
	result.Branches = []storage.ReportBranchMember{}
	for rows.Next() {
		var member storage.ReportBranchMember
		var admissionID *string
		var proofJSON []byte
		if err := rows.Scan(&member.RunID, &member.PackageID, &member.ConditionKey, &member.When, &admissionID, &proofJSON); err != nil {
			return nil, fmt.Errorf("postgres: scan report branch member: %w", err)
		}
		switch {
		case admissionID != nil:
			var proof storage.ReportBranchProof
			if err := json.Unmarshal(proofJSON, &proof); err != nil {
				return nil, fmt.Errorf("postgres: decode report branch proof: %w", err)
			}
			member.Admission = &storage.ReportBranchAdmission{ID: *admissionID, Proof: proof}
			member.Eligibility = "admitted"
		case result.CancelledAt != nil:
			member.Eligibility = "cancelled"
		case evaluations[member.ConditionKey].Value == "unknown":
			member.Eligibility = "unknown"
		case evaluations[member.ConditionKey].Value == member.When:
			member.Eligibility = "selected"
		default:
			member.Eligibility = "unselected"
		}
		result.Branches = append(result.Branches, member)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: collect report branch members: %w", err)
	}
	rows.Close()
	joinRows, err := s.query(ctx, `SELECT flow_id,run_id,package_id,mode,allow_empty_skip,configured_by,configured_at
 FROM report_flow_joins WHERE project_slug=$1 AND flow_id=$2 ORDER BY run_id`, s.project, id)
	if err != nil {
		return nil, fmt.Errorf("postgres: read report flow joins: %w", err)
	}
	defer joinRows.Close()
	result.Joins = []storage.ReportFlowJoinConfig{}
	for joinRows.Next() {
		var join storage.ReportFlowJoinConfig
		if err := joinRows.Scan(&join.FlowID, &join.RunID, &join.PackageID, &join.Mode, &join.AllowEmptySkip, &join.ConfiguredBy, &join.ConfiguredAt); err != nil {
			return nil, fmt.Errorf("postgres: scan report flow join: %w", err)
		}
		result.Joins = append(result.Joins, join)
	}
	if err := joinRows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: collect report flow joins: %w", err)
	}
	return &result, nil
}

// CancelReportBranchFlow only stops future admissions; existing proof remains intact.
func (s *Store) CancelReportBranchFlow(ctx context.Context, id, reason string, scope *storage.MailScope) (*storage.ReportBranchFlow, error) {
	if !validMailText(id, 256) || strings.TrimSpace(reason) == "" || utf8.RuneCountInString(reason) > 4000 {
		return nil, storage.ErrInvalidReportBranchFlow
	}
	var result *storage.ReportBranchFlow
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		run, user, actorErr := s.deliveryHookActor(txCtx, scope)
		if actorErr != nil {
			return actorErr
		}
		actor := "user:" + user
		if run != nil {
			actor = "run:" + *run
		}
		var priorActor, priorReason *string
		err := s.queryRow(txCtx, `SELECT cancelled_by,cancel_reason FROM report_branch_flows WHERE project_slug=$1 AND id=$2 FOR UPDATE`, s.project, id).Scan(&priorActor, &priorReason)
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrReportBranchFlowNotFound
		}
		if err != nil {
			return fmt.Errorf("postgres: read report branch cancellation: %w", err)
		}
		if priorActor != nil {
			if *priorActor != actor || *priorReason != reason {
				return storage.ErrReportBranchFlowConflict
			}
		} else if _, err := s.exec(txCtx, `UPDATE report_branch_flows SET cancelled_by=$3,cancel_reason=$4,cancelled_at=$5
 WHERE project_slug=$1 AND id=$2`, s.project, id, actor, reason, s.now()); err != nil {
			return fmt.Errorf("postgres: cancel report branch flow: %w", err)
		}
		result, err = s.readReportBranchFlow(txCtx, id)
		return err
	})
	return result, err
}

func (s *Store) evaluateReportBranchCondition(ctx context.Context, flowID string, c storage.ArmReportBranchCondition) (*storage.ReportBranchEvaluation, error) {
	if c.Kind == "judgment" {
		return s.evaluateReportBranchJudgment(ctx, flowID, c.Key)
	}
	if c.Kind != "report" || c.Report == nil {
		return nil, storage.ErrInvalidReportBranchFlow
	}
	evaluation := &storage.ReportBranchEvaluation{Value: "unknown"}
	var snapshot []byte
	err := s.queryRow(ctx, `SELECT snapshot FROM deliveries WHERE project_slug=$1 AND id=$2`, s.project, c.Report.DeliveryID).Scan(&snapshot)
	if errors.Is(err, pgx.ErrNoRows) {
		evaluation.Reason = "delivery_missing"
		return evaluation, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: read report branch delivery: %w", err)
	}
	commit, err := testDeliveryCommit(snapshot)
	if err != nil {
		return nil, fmt.Errorf("postgres: decode report branch delivery commit: %w", err)
	}
	for _, source := range append([]storage.ReviewSource{c.Report.PlanSource}, c.Report.InputSources...) {
		current, err := s.reportBranchSourceCurrent(ctx, source)
		if err != nil {
			return nil, err
		}
		if !current {
			evaluation.Reason = "input_changed"
			return evaluation, nil
		}
	}
	report, err := s.latestRegisteredTestReport(ctx, c.Report.DeliveryID, commit, c.Report.PlanSource)
	if err != nil {
		return nil, err
	}
	evaluation.Report = report
	if report == nil {
		evaluation.Reason = "report_missing"
		return evaluation, nil
	}
	switch report.Status {
	case "passed":
		evaluation.Value = "true"
	case "failed":
		evaluation.Value = "false"
	case "not_run", "environment_blocked":
		evaluation.Reason = report.Status
	default:
		evaluation.Reason = "report_status_unknown"
	}
	return evaluation, nil
}

func (s *Store) reportBranchSourceCurrent(ctx context.Context, source storage.ReviewSource) (bool, error) {
	if source.Kind == "git" {
		err := s.validateReviewSource(ctx, &source)
		if errors.Is(err, storage.ErrInvalidReview) {
			return false, nil
		}
		return err == nil, err // A registered fixed commit is not invalidated by workspace HEAD.
	}
	record, err := s.ReadWorkbenchKnowledgeRecord(ctx, "change", source.ChangeID)
	if errors.Is(err, ErrWorkbenchChangeNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if record.Slug != source.SpecSlug {
		return false, nil
	}
	original := ""
	found := false
	for _, change := range record.Record.(*storage.ChangeLogEntry).Changes { //nolint:errcheck // The fixed change read returns this type.
		if change.Field == source.Field {
			original, found = change.NewValue, true
			break
		}
	}
	if !found {
		return false, nil
	}
	fields, _, err := s.readSpecFields(ctx, source.SpecSlug)
	if errors.Is(err, storage.ErrSpecNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	current, ok := reportBranchField(fields, source.Field)
	if !ok {
		return false, nil
	}
	if source.Field != "spark_output" && source.Field != "shape_output" && source.Field != "specify_output" && source.Field != "decompose_output" {
		return original == current, nil
	}
	if original == current {
		return true, nil
	}
	if original == "" || current == "" {
		return false, nil
	}
	var left, right any
	for i, raw := range []string{original, current} {
		decoder := json.NewDecoder(bytes.NewReader([]byte(raw)))
		decoder.UseNumber()
		if i == 0 {
			if err := decoder.Decode(&left); err != nil {
				return false, err
			}
		} else if err := decoder.Decode(&right); err != nil {
			return false, err
		}
	}
	return reflect.DeepEqual(left, right), nil
}

func reportBranchField(fields storage.SpecFields, field string) (string, bool) {
	switch field {
	case "intent":
		return fields.Intent, true
	case "stage":
		return fields.Stage, true
	case "priority":
		return fields.Priority, true
	case "complexity":
		return fields.Complexity, true
	case "notes":
		return fields.Notes, true
	case "spark_output":
		return fields.SparkOutput, true
	case "shape_output":
		return fields.ShapeOutput, true
	case "specify_output":
		return fields.SpecifyOutput, true
	case "decompose_output":
		return fields.DecomposeOutput, true
	default:
		return "", false
	}
}

// reportBranchAdmissionBasis inspects only the condition bound to this run.
func (s *Store) reportBranchAdmissionBasis(ctx context.Context, runID, packageID string) (*storage.ReportBranchProof, error) {
	var flowID, conditionKey, when, storedPackage string
	var definition []byte
	var cancelled bool
	err := s.queryRow(ctx, `SELECT f.id,m.condition_key,m.when_value,m.package_id,f.definition,f.cancelled_at IS NOT NULL
 FROM report_branch_members m JOIN report_branch_flows f ON f.project_slug=m.project_slug AND f.id=m.flow_id
 WHERE m.project_slug=$1 AND m.run_id=$2`, s.project, runID).Scan(&flowID, &conditionKey, &when, &storedPackage, &definition, &cancelled)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: read report branch admission: %w", err)
	}
	if storedPackage != packageID {
		return nil, storage.ErrReportBranchFlowConflict
	}
	if cancelled {
		return nil, storage.ErrReportBranchFlowNotSelected
	}
	var req storage.ArmReportBranchFlowRequest
	if err := json.Unmarshal(definition, &req); err != nil {
		return nil, fmt.Errorf("postgres: decode report branch definition: %w", err)
	}
	for _, c := range req.Conditions {
		if c.Key != conditionKey {
			continue
		}
		evaluation, err := s.evaluateReportBranchCondition(ctx, flowID, c)
		if err != nil {
			return nil, err
		}
		if evaluation.Value != when {
			return nil, storage.ErrReportBranchFlowNotSelected
		}
		return &storage.ReportBranchProof{FlowID: flowID, ConditionKey: conditionKey, Value: evaluation.Value, Report: evaluation.Report, Judgment: evaluation.Judgment}, nil
	}
	return nil, storage.ErrReportBranchFlowConflict
}
