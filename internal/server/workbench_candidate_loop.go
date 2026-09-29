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

// ExecuteHumanCandidateLoopRead reads an existing project by exact run without a live host association.
func ExecuteHumanCandidateLoopRead(ctx context.Context, root *postgres.Store, project string, body io.Reader) (json.RawMessage, error) {
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" || identity.UserKind != storage.KindHuman {
		return nil, storage.ErrPlanningForbidden
	}
	var req struct {
		RunID string `json:"runId"`
	}
	if decodeMailRequest(body, &req) != nil || strings.TrimSpace(req.RunID) == "" {
		return nil, storage.ErrInvalidCandidateLoop
	}
	if project == "" || project == "_server" {
		return nil, storage.ErrProjectNotFound
	}
	s, err := root.ScopedExisting(ctx, project)
	if err != nil {
		return nil, err
	}
	result, err := s.ReadCandidateLoop(ctx, req.RunID)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("workbench candidate read response: %w", err)
	}
	return encoded, nil
}

// ExecuteCandidateLoop keeps human and manager configuration on their original authority paths.
func ExecuteCandidateLoop(ctx context.Context, root *postgres.Store, project, operation string, body io.Reader) (json.RawMessage, error) {
	var scope *storage.MailScope
	var request json.RawMessage
	if strings.HasPrefix(operation, "pm-") {
		var envelope struct {
			Scope   storage.MailScope `json:"scope"`
			Request json.RawMessage   `json:"request"`
		}
		if decodeMailRequest(body, &envelope) != nil || len(envelope.Request) == 0 {
			return nil, storage.ErrInvalidCandidateLoop
		}
		scope, request = &envelope.Scope, envelope.Request
		if project == "" {
			var err error
			project, err = root.ProjectForLocalMailScope(ctx, *scope)
			if err != nil {
				return nil, fmt.Errorf("workbench candidate project: %w", err)
			}
		}
	} else {
		var err error
		request, err = io.ReadAll(body)
		if err != nil {
			return nil, err
		}
	}
	if project == "" || project == "_server" {
		return nil, storage.ErrProjectNotFound
	}
	s, err := root.ScopedExisting(ctx, project)
	if err != nil {
		return nil, err
	}
	var result *storage.CandidateLoop
	switch operation {
	case "candidate-loop-arm", "pm-candidate-loop-arm":
		var req storage.ArmCandidateLoopRequest
		if decodeMailRequest(bytes.NewReader(request), &req) != nil {
			return nil, storage.ErrInvalidCandidateLoop
		}
		result, err = s.ArmCandidateLoop(ctx, req, scope)
	case "candidate-loop-stop", "pm-candidate-loop-stop":
		var req struct {
			RunID  string `json:"runId"`
			Reason string `json:"reason"`
		}
		if decodeMailRequest(bytes.NewReader(request), &req) != nil {
			return nil, storage.ErrInvalidCandidateLoop
		}
		result, err = s.StopCandidateLoop(ctx, req.RunID, req.Reason, scope)
	case "candidate-loop-abandon":
		var req struct {
			RunID  string `json:"runId"`
			Reason string `json:"reason"`
		}
		if decodeMailRequest(bytes.NewReader(request), &req) != nil {
			return nil, storage.ErrInvalidCandidateLoop
		}
		result, err = s.AbandonCandidateLoop(ctx, req.RunID, req.Reason)
	default:
		return nil, storage.ErrInvalidCandidateLoop
	}
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("workbench candidate response: %w", err)
	}
	return encoded, nil
}

// ExecuteOwnCandidateNext consumes the bound run's named predecessor without starting a new session.
func ExecuteOwnCandidateNext(ctx context.Context, root *postgres.Store, project string, body io.Reader) (json.RawMessage, error) {
	var req struct {
		Scope                     storage.MailScope `json:"scope"`
		ExpectedPreviousAttemptID string            `json:"expectedPreviousAttemptId"`
	}
	if decodeMailRequest(body, &req) != nil || req.ExpectedPreviousAttemptID == "" {
		return nil, storage.ErrInvalidCandidateLoop
	}
	if project == "" {
		var err error
		project, err = root.ProjectForLocalMailScope(ctx, req.Scope)
		if err != nil {
			return nil, err
		}
	}
	if project == "_server" {
		return nil, storage.ErrProjectNotFound
	}
	s, err := root.ScopedExisting(ctx, project)
	if err != nil {
		return nil, err
	}
	result, err := s.NextOwnCandidateAttempt(ctx, req.Scope, req.ExpectedPreviousAttemptID)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("workbench candidate next response: %w", err)
	}
	return encoded, nil
}
