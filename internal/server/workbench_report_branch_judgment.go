// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
)

// ExecuteReportBranchJudgmentRecord derives the writer from the human or current manager path.
func ExecuteReportBranchJudgmentRecord(ctx context.Context, root *postgres.Store, project, operation string, body io.Reader) (json.RawMessage, error) {
	var scope *storage.MailScope
	var request json.RawMessage
	if operation == "pm-report-judgment-record" {
		var envelope struct {
			Scope   storage.MailScope `json:"scope"`
			Request json.RawMessage   `json:"request"`
		}
		if decodeMailRequest(body, &envelope) != nil || len(envelope.Request) == 0 {
			return nil, storage.ErrInvalidReportBranchJudgment
		}
		scope, request = &envelope.Scope, envelope.Request
		if project == "" {
			var err error
			project, err = root.ProjectForLocalMailScope(ctx, *scope)
			if err != nil {
				return nil, fmt.Errorf("workbench judgment project: %w", err)
			}
		}
	} else if operation == "report-judgment-record" {
		var err error
		request, err = io.ReadAll(body)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, storage.ErrInvalidReportBranchJudgment
	}
	var req storage.RecordReportBranchJudgmentRequest
	if decodeMailRequest(bytes.NewReader(request), &req) != nil {
		return nil, storage.ErrInvalidReportBranchJudgment
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(request, &fields) != nil || len(fields["expectedJudgmentId"]) == 0 {
		return nil, storage.ErrInvalidReportBranchJudgment
	}
	if project == "" || project == "_server" {
		return nil, storage.ErrProjectNotFound
	}
	s, err := root.ScopedExisting(ctx, project)
	if err != nil {
		return nil, err
	}
	result, err := s.RecordReportBranchJudgment(ctx, req, scope)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("workbench judgment response: %w", err)
	}
	return encoded, nil
}

// ExecuteHumanReportJudgmentHistory reads one project without a live host association.
func ExecuteHumanReportJudgmentHistory(ctx context.Context, root *postgres.Store, project string, body io.Reader) (json.RawMessage, error) {
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" || identity.UserKind != storage.KindHuman {
		return nil, storage.ErrPlanningForbidden
	}
	var req struct {
		FlowID       string `json:"flowId"`
		ConditionKey string `json:"conditionKey"`
		Cursor       string `json:"cursor"`
	}
	if decodeMailRequest(body, &req) != nil || strings.TrimSpace(req.FlowID) == "" || strings.TrimSpace(req.ConditionKey) == "" {
		return nil, storage.ErrInvalidReportBranchJudgment
	}
	if project == "" || project == "_server" {
		return nil, storage.ErrProjectNotFound
	}
	s, err := root.ScopedExisting(ctx, project)
	if err != nil {
		return nil, err
	}
	page, err := s.ReadReportBranchJudgmentHistory(ctx, req.FlowID, req.ConditionKey, req.Cursor)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(page)
	if err != nil {
		return nil, fmt.Errorf("workbench judgment history response: %w", err)
	}
	return encoded, nil
}
