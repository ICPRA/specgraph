// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
)

type testReportTarget struct {
	WorkPurpose string                   `json:"workPurpose"`
	QABasis     *storage.DispatchQABasis `json:"qaBasis"`
	GitBaseline struct {
		IsRepo    bool    `json:"isRepo"`
		CommitSHA *string `json:"commitSha"`
	} `json:"gitBaseline"`
}

const testReportJSON = `jsonb_build_object('id',id,'deliveryId',delivery_id,'testRunId',environment->'testRunId',
 'commitSha',result->>'commitSha','planSources',result->'planSources','status',result->>'status',
 'command',command,'exitCode',exit_code,'summary',result->>'summary','outputRefs',artifact_refs,
 'reporter',verifier,'createdAt',created_at)`

// RecordTestReport validates declared execution facts. It neither executes commands nor completes work.
// A scope always identifies the Agent route, irrespective of the credential account's kind.
func (s *Store) RecordTestReport(ctx context.Context, input *storage.RecordTestReportRequest, scope *storage.MailScope) (*storage.TestReport, error) {
	req := *input
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" {
		return nil, storage.ErrTestReportForbidden
	}
	if strings.TrimSpace(req.DeliveryID) == "" || !reviewCommitPattern.MatchString(req.CommitSHA) || len(req.PlanSources) == 0 || req.OutputRefs == nil || utf8.RuneCountInString(req.Summary) > 4000 || utf8.RuneCountInString(req.Command) > 4000 {
		return nil, storage.ErrInvalidTestReport
	}
	if req.ExitCode != nil && (*req.ExitCode < -2147483648 || *req.ExitCode > 4294967295) {
		return nil, storage.ErrInvalidTestReport
	}
	switch req.Status {
	case "passed":
		if strings.TrimSpace(req.Command) == "" || len(req.OutputRefs) == 0 || (req.ExitCode != nil && *req.ExitCode != 0) {
			return nil, storage.ErrInvalidTestReport
		}
	case "failed", "not_run", "environment_blocked":
		if strings.TrimSpace(req.Summary) == "" {
			return nil, storage.ErrInvalidTestReport
		}
	default:
		return nil, storage.ErrInvalidTestReport
	}
	for _, ref := range req.OutputRefs {
		if strings.TrimSpace(ref) == "" || strings.ContainsRune(ref, 0) {
			return nil, storage.ErrInvalidTestReport
		}
	}
	if scope != nil && req.TestRunID != nil {
		return nil, storage.ErrTestReportForbidden
	}
	if req.TestRunID != nil && strings.TrimSpace(*req.TestRunID) == "" {
		return nil, storage.ErrInvalidTestReport
	}
	if scope == nil && req.TestRunID == nil {
		if identity.UserKind != storage.KindHuman {
			return nil, storage.ErrTestReportForbidden
		}
		user, err := s.ExistingAuth().GetUserByID(ctx, identity.UserID)
		if err != nil {
			return nil, err
		}
		if user.Kind != storage.KindHuman || user.DeletedAt != nil {
			return nil, storage.ErrTestReportForbidden
		}
	}
	var report storage.TestReport
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if lockErr := s.lockDependencyState(txCtx); lockErr != nil {
			return lockErr
		}
		reporter, reporterKind := identity.UserID, "operator"
		if scope != nil {
			run, _, err := s.reviewActorRun(txCtx, *scope)
			if err != nil {
				if errors.Is(err, storage.ErrReviewForbidden) || errors.Is(err, storage.ErrInvalidReview) {
					return storage.ErrTestReportForbidden
				}
				return err
			}
			req.TestRunID = &run
			reporter = run
			reporterKind = "agent"
		}
		var snapshot, targetBody []byte
		var implementationRun string
		err := s.queryRow(txCtx, `SELECT d.snapshot,b.id,p.body->'dispatch_target' FROM deliveries d
   JOIN run_bindings b ON b.project_slug=d.project_slug AND b.id=d.run_binding_id
   JOIN context_packages p ON p.project_slug=b.project_slug AND p.id=b.package_id AND p.task_spec_slug=b.task_spec_slug
   WHERE d.project_slug=$1 AND d.id=$2 FOR UPDATE OF d`, s.project, req.DeliveryID).Scan(&snapshot, &implementationRun, &targetBody)
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrDeliveryNotFound
		}
		if err != nil {
			return fmt.Errorf("postgres: read test delivery: %w", err)
		}
		commit, err := testDeliveryCommit(snapshot)
		if err != nil {
			return err
		}
		if commit != req.CommitSHA {
			return fmt.Errorf("reported commit must match the fixed delivery commit: %w", storage.ErrInvalidTestReport)
		}
		var target testReportTarget
		if json.Unmarshal(targetBody, &target) != nil || target.WorkPurpose != "implementation" || target.QABasis == nil {
			return storage.ErrInvalidTestReport
		}
		allowed := make(map[storage.ReviewSource]bool, len(target.QABasis.TestPlanSources))
		for i := range target.QABasis.TestPlanSources {
			source := &target.QABasis.TestPlanSources[i]
			allowed[*source] = true
		}
		var runPlans map[storage.ReviewSource]bool
		if req.TestRunID != nil {
			runContext, contextErr := s.ReadRunContext(txCtx, *req.TestRunID)
			if contextErr != nil {
				return contextErr
			}
			var body struct {
				Target testReportTarget `json:"dispatch_target"`
			}
			if json.Unmarshal(runContext.Body, &body) != nil || body.Target.QABasis == nil {
				return storage.ErrInvalidTestReport
			}
			switch body.Target.WorkPurpose {
			case "implementation":
				if *req.TestRunID != implementationRun {
					return storage.ErrTestReportForbidden
				}
			case "test_execution":
				if !body.Target.GitBaseline.IsRepo || body.Target.GitBaseline.CommitSHA == nil || *body.Target.GitBaseline.CommitSHA != commit {
					return fmt.Errorf("test execution baseline must match the fixed delivery commit: %w", storage.ErrInvalidTestReport)
				}
			default:
				return storage.ErrTestReportForbidden
			}
			runPlans = make(map[storage.ReviewSource]bool, len(body.Target.QABasis.TestPlanSources))
			for i := range body.Target.QABasis.TestPlanSources {
				source := &body.Target.QABasis.TestPlanSources[i]
				runPlans[*source] = true
			}
		}
		for i := range req.PlanSources {
			source := &req.PlanSources[i]
			if !allowed[*source] || (runPlans != nil && !runPlans[*source]) {
				return fmt.Errorf("report plan must belong to the frozen delivery and testing assignments: %w", storage.ErrInvalidTestReport)
			}
			if sourceErr := s.validateReviewSource(txCtx, source); sourceErr != nil {
				return sourceErr
			}
		}
		result, err := json.Marshal(map[string]any{"commitSha": commit, "planSources": req.PlanSources, "status": req.Status, "summary": req.Summary})
		if err != nil {
			return fmt.Errorf("postgres: encode test result: %w", err)
		}
		artifacts, err := json.Marshal(req.OutputRefs)
		if err != nil {
			return fmt.Errorf("postgres: encode test artifacts: %w", err)
		}
		environment, err := json.Marshal(map[string]any{"testRunId": req.TestRunID, "reporterKind": reporterKind})
		if err != nil {
			return fmt.Errorf("postgres: encode test environment: %w", err)
		}
		var encoded []byte
		err = s.queryRow(txCtx, `INSERT INTO evidence(id,project_slug,delivery_id,kind,command,exit_code,result,artifact_refs,environment,verifier,created_at)
   VALUES($1,$2,$3,'test_report',$4,$5,$6,$7,$8,$9,$10) RETURNING `+testReportJSON, newID("ev"), s.project, req.DeliveryID, req.Command, req.ExitCode, result, artifacts, environment, reporter, s.now()).Scan(&encoded)
		if err != nil {
			return fmt.Errorf("postgres: record test report: %w", err)
		}
		if decodeErr := json.Unmarshal(encoded, &report); decodeErr != nil {
			return fmt.Errorf("postgres: decode recorded test report: %w", decodeErr)
		}
		if err := s.recordCandidateInterventionForDelivery(txCtx, req.DeliveryID); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &report, nil
}

func testDeliveryCommit(snapshot []byte) (string, error) {
	var body struct {
		Git struct {
			Head struct {
				IsRepo    bool    `json:"isRepo"`
				CommitSHA *string `json:"commitSha"`
			} `json:"head"`
		} `json:"git"`
	}
	if json.Unmarshal(snapshot, &body) != nil || !body.Git.Head.IsRepo || body.Git.Head.CommitSHA == nil || !reviewCommitPattern.MatchString(*body.Git.Head.CommitSHA) {
		return "", storage.ErrInvalidTestReport
	}
	return *body.Git.Head.CommitSHA, nil
}

// latestRegisteredTestReport reads the newest assertion for one frozen delivery,
// commit and assigned plan. No test command is executed here.
func (s *Store) latestRegisteredTestReport(ctx context.Context, deliveryID, commit string, plan storage.ReviewSource) (*storage.ReportBranchReport, error) {
	source, err := json.Marshal([]storage.ReviewSource{plan})
	if err != nil {
		return nil, fmt.Errorf("postgres: encode test plan: %w", err)
	}
	var encoded []byte
	err = s.queryRow(ctx, `SELECT jsonb_build_object('id',id,'deliveryId',delivery_id,'commitSha',result->>'commitSha',
 'planSources',result->'planSources','status',result->>'status','createdAt',created_at)
 FROM evidence WHERE project_slug=$1 AND delivery_id=$2 AND kind='test_report'
 AND result->>'commitSha'=$3 AND result->'planSources' @> $4::jsonb ORDER BY record_order DESC LIMIT 1`,
		s.project, deliveryID, commit, source).Scan(&encoded)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: read latest test report: %w", err)
	}
	var report storage.ReportBranchReport
	if err := json.Unmarshal(encoded, &report); err != nil {
		return nil, fmt.Errorf("postgres: decode latest test report: %w", err)
	}
	return &report, nil
}

// ReadTestReports returns a delivery's test reports in descending record order.
func (s *Store) ReadTestReports(ctx context.Context, deliveryID, cursor string) (*storage.TestReportPage, error) {
	var before int64
	if cursor != "" {
		var err error
		before, err = strconv.ParseInt(cursor, 10, 64)
		if err != nil || before < 1 || strconv.FormatInt(before, 10) != cursor {
			return nil, storage.ErrInvalidTestReport
		}
	}
	var exists bool
	if scanErr := s.queryRow(ctx, `SELECT EXISTS(SELECT 1 FROM deliveries WHERE project_slug=$1 AND id=$2)`, s.project, deliveryID).Scan(&exists); scanErr != nil {
		return nil, fmt.Errorf("postgres: check test delivery: %w", scanErr)
	}
	if !exists {
		return nil, storage.ErrDeliveryNotFound
	}
	rows, err := s.query(ctx, `SELECT record_order,`+testReportJSON+` AS report FROM evidence WHERE project_slug=$1 AND delivery_id=$2 AND kind='test_report'
 AND ($3::bigint=0 OR record_order<$3) ORDER BY record_order DESC LIMIT 51`, s.project, deliveryID, before)
	if err != nil {
		return nil, err
	}
	type reportRow struct {
		Order  int64           `db:"record_order"`
		Report json.RawMessage `db:"report"`
	}
	records, err := pgx.CollectRows(rows, pgx.RowToStructByName[reportRow])
	if err != nil {
		return nil, fmt.Errorf("postgres: collect test reports: %w", err)
	}
	page := &storage.TestReportPage{DeliveryID: deliveryID, Reports: []storage.TestReport{}, HasMore: len(records) > 50}
	if page.HasMore {
		records = records[:50]
		cursor := strconv.FormatInt(records[49].Order, 10)
		page.NextCursor = &cursor
	}
	for _, row := range records {
		var report storage.TestReport
		if decodeErr := json.Unmarshal(row.Report, &report); decodeErr != nil {
			return nil, fmt.Errorf("postgres: decode test report: %w", decodeErr)
		}
		page.Reports = append(page.Reports, report)
	}
	return page, nil
}

func (s *Store) checkImplementationTests(ctx context.Context, slug, runID string) error {
	return s.checkImplementationTestsForDelivery(ctx, slug, runID, "")
}

// A nonempty deliveryID fixes the exact formal candidate being completed.
func (s *Store) checkImplementationTestsForDelivery(ctx context.Context, slug, runID, deliveryID string) error {
	runContext, err := s.ReadRunContext(ctx, runID)
	if err != nil {
		return err
	}
	var body struct {
		Target json.RawMessage `json:"dispatch_target"`
	}
	if decodeErr := json.Unmarshal(runContext.Body, &body); decodeErr != nil {
		return fmt.Errorf("postgres: decode implementation context: %w", decodeErr)
	}
	if qaErr := s.checkDispatchQABasis(ctx, body.Target); qaErr != nil {
		return qaErr
	}
	var target testReportTarget
	if json.Unmarshal(body.Target, &target) != nil || target.WorkPurpose != "implementation" || target.QABasis == nil || len(target.QABasis.TestPlanSources) == 0 {
		return storage.ErrImplementationTestsRequired
	}
	var snapshot []byte
	if deliveryID == "" {
		err = s.queryRow(ctx, `SELECT d.id,d.snapshot FROM deliveries d JOIN run_bindings b ON b.project_slug=d.project_slug AND b.id=d.run_binding_id
 WHERE b.project_slug=$1 AND b.id=$2 AND b.task_spec_slug=$3 ORDER BY d.submitted_at DESC,d.id DESC LIMIT 1`, s.project, runID, slug).Scan(&deliveryID, &snapshot)
	} else {
		err = s.queryRow(ctx, `SELECT d.snapshot FROM deliveries d JOIN run_bindings b ON b.project_slug=d.project_slug AND b.id=d.run_binding_id
 WHERE b.project_slug=$1 AND b.id=$2 AND b.task_spec_slug=$3 AND d.id=$4`, s.project, runID, slug, deliveryID).Scan(&snapshot)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.ErrImplementationTestsRequired
	}
	if err != nil {
		return fmt.Errorf("postgres: read implementation test delivery: %w", err)
	}
	commit, err := testDeliveryCommit(snapshot)
	if err != nil {
		return storage.ErrImplementationTestsRequired
	}
	for i := range target.QABasis.TestPlanSources {
		plan := &target.QABasis.TestPlanSources[i]
		report, err := s.latestRegisteredTestReport(ctx, deliveryID, commit, *plan)
		if err != nil {
			return err
		}
		if report == nil || report.Status != "passed" {
			return storage.ErrImplementationTestsRequired
		}
	}
	return nil
}
