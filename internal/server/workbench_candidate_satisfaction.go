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

// ExecuteCandidateSatisfactionRecord keeps human, manager, and own judgments on their distinct authority paths.
func ExecuteCandidateSatisfactionRecord(ctx context.Context, root *postgres.Store, project, operation string, body io.Reader) (json.RawMessage, error) {
	var scope *storage.MailScope
	var request json.RawMessage
	switch operation {
	case "pm-candidate-satisfaction-record", "candidate-satisfaction-own":
		var envelope struct {
			Scope   storage.MailScope `json:"scope"`
			Request json.RawMessage   `json:"request"`
		}
		if decodeMailRequest(body, &envelope) != nil || len(envelope.Request) == 0 {
			return nil, storage.ErrInvalidCandidateSatisfaction
		}
		scope, request = &envelope.Scope, envelope.Request
		if project == "" {
			var err error
			project, err = root.ProjectForLocalMailScope(ctx, *scope)
			if err != nil {
				return nil, err
			}
		}
	case "candidate-satisfaction-record":
		var err error
		request, err = io.ReadAll(body)
		if err != nil {
			return nil, err
		}
	default:
		return nil, storage.ErrInvalidCandidateSatisfaction
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(request, &fields) != nil || len(fields["expectedJudgmentId"]) == 0 {
		return nil, storage.ErrInvalidCandidateSatisfaction
	}
	if project == "" || project == "_server" {
		return nil, storage.ErrProjectNotFound
	}
	s, err := root.ScopedExisting(ctx, project)
	if err != nil {
		return nil, err
	}
	var result *storage.CandidateSatisfactionJudgmentResult
	if operation == "candidate-satisfaction-own" {
		var req storage.RecordOwnCandidateSatisfactionJudgmentRequest
		if decodeMailRequest(bytes.NewReader(request), &req) != nil {
			return nil, storage.ErrInvalidCandidateSatisfaction
		}
		result, err = s.RecordOwnCandidateSatisfactionJudgment(ctx, *scope, req)
	} else {
		var req storage.RecordCandidateSatisfactionJudgmentRequest
		if decodeMailRequest(bytes.NewReader(request), &req) != nil {
			return nil, storage.ErrInvalidCandidateSatisfaction
		}
		result, err = s.RecordCandidateSatisfactionJudgment(ctx, req, scope)
	}
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("workbench candidate judgment response: %w", err)
	}
	return encoded, nil
}

// ExecuteCandidateInterventionResolve accepts only a named human project and exact fact IDs.
func ExecuteCandidateInterventionResolve(ctx context.Context, root *postgres.Store, project string, body io.Reader) (json.RawMessage, error) {
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" || identity.UserKind != storage.KindHuman {
		return nil, storage.ErrPlanningForbidden
	}
	var req storage.ResolveCandidateInterventionRequest
	if decodeMailRequest(body, &req) != nil {
		return nil, storage.ErrInvalidCandidateSatisfaction
	}
	if project == "" || project == "_server" {
		return nil, storage.ErrProjectNotFound
	}
	s, err := root.ScopedExisting(ctx, project)
	if err != nil {
		return nil, err
	}
	result, err := s.ResolveCandidateIntervention(ctx, req)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("workbench candidate intervention response: %w", err)
	}
	return encoded, nil
}

// ExecuteHumanCandidateSatisfactionHistory reads one exact attempt without a live host association.
func ExecuteHumanCandidateSatisfactionHistory(ctx context.Context, root *postgres.Store, project string, body io.Reader) (json.RawMessage, error) {
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" || identity.UserKind != storage.KindHuman {
		return nil, storage.ErrPlanningForbidden
	}
	var req struct {
		RunID              string `json:"runId"`
		AttemptID          string `json:"attemptId"`
		JudgmentCursor     string `json:"judgmentCursor"`
		InterventionCursor string `json:"interventionCursor"`
	}
	if decodeMailRequest(body, &req) != nil || strings.TrimSpace(req.RunID) == "" || strings.TrimSpace(req.AttemptID) == "" {
		return nil, storage.ErrInvalidCandidateSatisfaction
	}
	if project == "" || project == "_server" {
		return nil, storage.ErrProjectNotFound
	}
	s, err := root.ScopedExisting(ctx, project)
	if err != nil {
		return nil, err
	}
	result, err := s.ReadCandidateSatisfactionHistory(ctx, req.RunID, req.AttemptID, req.JudgmentCursor, req.InterventionCursor)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("workbench candidate history response: %w", err)
	}
	return encoded, nil
}
