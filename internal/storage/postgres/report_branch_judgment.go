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
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/storage"
)

const reportJudgmentColumns = `id,flow_id,condition_key,value,input_refs,reason,actor_user_id,actor_run_id,recorded_at,predecessor_id`

func reportJudgmentSourcePosition(source storage.ReviewSource) storage.ReviewSource {
	source.ChangeID = ""
	source.CommitSHA = ""
	return source
}

func scanReportBranchJudgment(row pgx.Row) (storage.ReportBranchJudgment, error) {
	var fact storage.ReportBranchJudgment
	var inputs []byte
	err := row.Scan(&fact.ID, &fact.FlowID, &fact.ConditionKey, &fact.Value, &inputs, &fact.Reason,
		&fact.ActorUserID, &fact.ActorRunID, &fact.RecordedAt, &fact.PredecessorID)
	if err != nil {
		return fact, err
	}
	if err := json.Unmarshal(inputs, &fact.InputRefs); err != nil {
		return fact, fmt.Errorf("postgres: decode report judgment inputs: %w", err)
	}
	return fact, nil
}

func (s *Store) reportJudgmentCondition(ctx context.Context, flowID, key string) (*storage.ReportBranchJudgmentInput, error) {
	var definition []byte
	err := s.queryRow(ctx, `SELECT definition FROM report_branch_flows WHERE project_slug=$1 AND id=$2`, s.project, flowID).Scan(&definition)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, storage.ErrReportBranchFlowNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: read judgment flow: %w", err)
	}
	var flow storage.ArmReportBranchFlowRequest
	if err := json.Unmarshal(definition, &flow); err != nil {
		return nil, fmt.Errorf("postgres: decode judgment flow: %w", err)
	}
	for _, condition := range flow.Conditions {
		if condition.Key == key && condition.Kind == "judgment" && condition.Judgment != nil {
			return condition.Judgment, nil
		}
	}
	return nil, storage.ErrReportBranchJudgmentNotFound
}

// RecordReportBranchJudgment appends one named judgment at an exact predecessor under the project lock.
func (s *Store) RecordReportBranchJudgment(ctx context.Context, req storage.RecordReportBranchJudgmentRequest, scope *storage.MailScope) (*storage.ReportBranchJudgmentResult, error) {
	if !validMailText(req.FlowID, 256) || !subdivisionKeyPattern.MatchString(req.ConditionKey) ||
		(req.Value != "true" && req.Value != "false" && req.Value != "unknown") || req.InputRefs == nil ||
		strings.TrimSpace(req.Reason) == "" || utf8.RuneCountInString(req.Reason) > 4000 ||
		(req.ExpectedJudgmentID != nil && !validMailText(*req.ExpectedJudgmentID, 256)) {
		return nil, storage.ErrInvalidReportBranchJudgment
	}
	req.InputRefs = slices.Clone(req.InputRefs)
	sort.Slice(req.InputRefs, func(i, j int) bool {
		left, _ := json.Marshal(req.InputRefs[i])
		right, _ := json.Marshal(req.InputRefs[j])
		return bytes.Compare(left, right) < 0
	})
	inputs, err := json.Marshal(req.InputRefs)
	if err != nil {
		return nil, fmt.Errorf("postgres: encode judgment inputs: %w", err)
	}
	var result storage.ReportBranchJudgmentResult
	err = s.RunInTransaction(ctx, func(txCtx context.Context) error {
		run, user, err := s.deliveryHookActor(txCtx, scope)
		if err != nil {
			return err
		}
		condition, err := s.reportJudgmentCondition(txCtx, req.FlowID, req.ConditionKey)
		if err != nil {
			return err
		}
		var priorID string
		var same bool
		err = s.queryRow(txCtx, `SELECT id,value=$5 AND input_refs=$6::jsonb AND reason=$7 AND actor_user_id=$8
 AND actor_run_id IS NOT DISTINCT FROM $9::text FROM report_branch_judgments
 WHERE project_slug=$1 AND flow_id=$2 AND condition_key=$3 AND predecessor_id IS NOT DISTINCT FROM $4::text`,
			s.project, req.FlowID, req.ConditionKey, req.ExpectedJudgmentID, req.Value, inputs, req.Reason, user, run).Scan(&priorID, &same)
		if err == nil {
			if !same {
				return storage.ErrReportBranchJudgmentConflict
			}
			result.Judgment, err = scanReportBranchJudgment(s.queryRow(txCtx, `SELECT `+reportJudgmentColumns+` FROM report_branch_judgments WHERE project_slug=$1 AND id=$2`, s.project, priorID))
			if err != nil {
				return fmt.Errorf("postgres: read judgment replay: %w", err)
			}
			result.Replayed = true
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("postgres: read judgment successor: %w", err)
		}
		var cancelled bool
		if err := s.queryRow(txCtx, `SELECT cancelled_at IS NOT NULL FROM report_branch_flows WHERE project_slug=$1 AND id=$2`, s.project, req.FlowID).Scan(&cancelled); err != nil {
			return fmt.Errorf("postgres: read judgment cancellation: %w", err)
		}
		if cancelled {
			return storage.ErrReportBranchJudgmentConflict
		}
		var latest string
		err = s.queryRow(txCtx, `SELECT id FROM report_branch_judgments WHERE project_slug=$1 AND flow_id=$2 AND condition_key=$3 ORDER BY record_order DESC LIMIT 1`, s.project, req.FlowID, req.ConditionKey).Scan(&latest)
		if !errors.Is(err, pgx.ErrNoRows) && err != nil {
			return fmt.Errorf("postgres: read latest judgment: %w", err)
		}
		if (err == nil && (req.ExpectedJudgmentID == nil || *req.ExpectedJudgmentID != latest)) || (errors.Is(err, pgx.ErrNoRows) && req.ExpectedJudgmentID != nil) {
			return storage.ErrReportBranchJudgmentConflict
		}
		if len(condition.InputSources) != len(req.InputRefs) {
			return storage.ErrInvalidReportBranchJudgment
		}
		positions := make(map[storage.ReviewSource]bool, len(condition.InputSources))
		for _, source := range condition.InputSources {
			positions[reportJudgmentSourcePosition(source)] = true
		}
		seen := make(map[storage.ReviewSource]bool, len(req.InputRefs))
		for _, source := range req.InputRefs {
			position := reportJudgmentSourcePosition(source)
			if !positions[position] || seen[position] {
				return storage.ErrInvalidReportBranchJudgment
			}
			seen[position] = true
			if err := s.validateReviewSource(txCtx, &source); err != nil {
				return err
			}
			current, err := s.reportBranchSourceCurrent(txCtx, source)
			if err != nil {
				return err
			}
			if !current {
				return storage.ErrReportBranchJudgmentConflict
			}
		}
		id := newID("rbj")
		result.Judgment = storage.ReportBranchJudgment{ID: id, FlowID: req.FlowID, ConditionKey: req.ConditionKey,
			Value: req.Value, InputRefs: req.InputRefs, Reason: req.Reason, ActorUserID: user,
			ActorRunID: run, RecordedAt: s.now(), PredecessorID: req.ExpectedJudgmentID}
		_, err = s.exec(txCtx, `INSERT INTO report_branch_judgments(id,project_slug,flow_id,condition_key,value,input_refs,reason,
 actor_user_id,actor_run_id,recorded_at,predecessor_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
			id, s.project, req.FlowID, req.ConditionKey, req.Value, inputs, req.Reason, user, run, result.Judgment.RecordedAt, req.ExpectedJudgmentID)
		if err != nil {
			return fmt.Errorf("postgres: record report judgment: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (s *Store) evaluateReportBranchJudgment(ctx context.Context, flowID, key string) (*storage.ReportBranchEvaluation, error) {
	evaluation := &storage.ReportBranchEvaluation{Value: "unknown"}
	fact, err := scanReportBranchJudgment(s.queryRow(ctx, `SELECT `+reportJudgmentColumns+` FROM report_branch_judgments
 WHERE project_slug=$1 AND flow_id=$2 AND condition_key=$3 ORDER BY record_order DESC LIMIT 1`, s.project, flowID, key))
	if errors.Is(err, pgx.ErrNoRows) {
		evaluation.Reason = "judgment_missing"
		return evaluation, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: read latest report judgment: %w", err)
	}
	evaluation.Judgment = &fact
	for _, source := range fact.InputRefs {
		current, err := s.reportBranchSourceCurrent(ctx, source)
		if err != nil {
			return nil, err
		}
		if !current {
			evaluation.Reason = "input_changed"
			return evaluation, nil
		}
	}
	if fact.Value == "unknown" {
		evaluation.Reason = "judgment_unknown"
	} else {
		evaluation.Value = fact.Value
	}
	return evaluation, nil
}

// ReadReportBranchJudgmentHistory pages exact immutable judgments without changing the current value.
func (s *Store) ReadReportBranchJudgmentHistory(ctx context.Context, flowID, key, cursor string) (*storage.ReportBranchJudgmentPage, error) {
	if !validMailText(flowID, 256) || !subdivisionKeyPattern.MatchString(key) {
		return nil, storage.ErrInvalidReportBranchJudgment
	}
	var before int64
	if cursor != "" {
		var err error
		before, err = strconv.ParseInt(cursor, 10, 64)
		if err != nil || before < 1 || strconv.FormatInt(before, 10) != cursor {
			return nil, storage.ErrInvalidReportBranchJudgment
		}
	}
	var result *storage.ReportBranchJudgmentPage
	err := s.RunReadSnapshot(ctx, func(snapshotCtx context.Context) error {
		if _, err := s.reportJudgmentCondition(snapshotCtx, flowID, key); err != nil {
			return err
		}
		page := &storage.ReportBranchJudgmentPage{FlowID: flowID, ConditionKey: key, Judgments: []storage.ReportBranchJudgment{}}
		rows, err := s.query(snapshotCtx, `SELECT record_order,`+reportJudgmentColumns+` FROM report_branch_judgments
 WHERE project_slug=$1 AND flow_id=$2 AND condition_key=$3 AND ($4::bigint=0 OR record_order<$4)
 ORDER BY record_order DESC LIMIT 51`, s.project, flowID, key, before)
		if err != nil {
			return fmt.Errorf("postgres: read judgment history: %w", err)
		}
		var orders []int64
		for rows.Next() {
			var order int64
			var fact storage.ReportBranchJudgment
			var inputs []byte
			if err := rows.Scan(&order, &fact.ID, &fact.FlowID, &fact.ConditionKey, &fact.Value, &inputs, &fact.Reason,
				&fact.ActorUserID, &fact.ActorRunID, &fact.RecordedAt, &fact.PredecessorID); err != nil {
				rows.Close()
				return fmt.Errorf("postgres: scan judgment history: %w", err)
			}
			if err := json.Unmarshal(inputs, &fact.InputRefs); err != nil {
				rows.Close()
				return fmt.Errorf("postgres: decode judgment history: %w", err)
			}
			page.Judgments = append(page.Judgments, fact)
			orders = append(orders, order)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("postgres: collect judgment history: %w", err)
		}
		rows.Close()
		page.HasMore = len(page.Judgments) > 50
		if page.HasMore {
			page.Judgments = page.Judgments[:50]
			cursor := strconv.FormatInt(orders[49], 10)
			page.NextCursor = &cursor
		}
		result = page
		return nil
	})
	return result, err
}
