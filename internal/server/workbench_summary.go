// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
)

// ExecuteWorkbenchSummary keeps human authorization separate from the PM scope,
// whose current manager responsibility is checked in the storage transaction.
func ExecuteWorkbenchSummary(ctx context.Context, root *postgres.Store, project, operation string, body io.Reader) (json.RawMessage, error) {
	identity, ok := auth.IdentityFromContext(ctx)
	pm := strings.HasPrefix(operation, "pm-")
	if !ok || identity.UserID == "" || (!pm && identity.UserKind != storage.KindHuman) {
		return nil, storage.ErrSummaryForbidden
	}
	var scope *storage.MailScope
	if pm {
		var envelope struct {
			Scope   storage.MailScope `json:"scope"`
			Request json.RawMessage   `json:"request"`
		}
		if decodeMailRequest(body, &envelope) != nil || len(envelope.Request) == 0 ||
			strings.TrimSpace(envelope.Scope.EnvironmentID) == "" || strings.TrimSpace(envelope.Scope.ThreadID) == "" ||
			strings.TrimSpace(envelope.Scope.ProviderSessionID) == "" || strings.TrimSpace(envelope.Scope.ProviderInstanceID) == "" {
			return nil, storage.ErrInvalidSummary
		}
		scope = &envelope.Scope
		body = bytes.NewReader(envelope.Request)
	}
	var call func(*postgres.Store) (any, error)
	switch operation {
	case "record-summary-disposition", "pm-summary-disposition":
		var req storage.SummaryDispositionRequest
		if decodeMailRequest(body, &req) != nil {
			return nil, storage.ErrInvalidSummary
		}
		call = func(s *postgres.Store) (any, error) { return s.RecordSummaryDisposition(ctx, req, scope) }
	case "accept-summary", "pm-accept-summary":
		var req storage.SummaryAcceptRequest
		if decodeMailRequest(body, &req) != nil {
			return nil, storage.ErrInvalidSummary
		}
		call = func(s *postgres.Store) (any, error) { return s.AcceptSummary(ctx, req, scope) }
	case "revoke-summary-acceptance", "pm-revoke-summary-acceptance":
		var req struct {
			AcceptanceID string `json:"acceptanceId"`
			Reason       string `json:"reason"`
		}
		if decodeMailRequest(body, &req) != nil {
			return nil, storage.ErrInvalidSummary
		}
		call = func(s *postgres.Store) (any, error) {
			return s.RevokeSummaryAcceptance(ctx, req.AcceptanceID, req.Reason, scope)
		}
	default:
		return nil, storage.ErrInvalidSummary
	}
	if project == "" && scope != nil {
		var err error
		project, err = root.ProjectForLocalMailScope(ctx, *scope)
		if err != nil {
			return nil, fmt.Errorf("workbench summary: %w", err)
		}
	}
	if strings.TrimSpace(project) == "" || project == "_server" {
		return nil, storage.ErrProjectNotFound
	}
	s, err := root.ScopedExisting(ctx, project)
	if err != nil {
		return nil, fmt.Errorf("workbench summary: %w", err)
	}
	result, err := call(s)
	if err != nil {
		return nil, err
	}
	encoded, encodeErr := json.Marshal(result)
	if encodeErr != nil {
		return encoded, fmt.Errorf("workbench summary: encode response: %w", encodeErr)
	}
	return encoded, nil
}

// ReadLocalWorkbenchSummary follows the knowledge read boundary: the trusted
// host supplies native scope and the transport authorizes spec.read.
func ReadLocalWorkbenchSummary(ctx context.Context, root *postgres.Store, project, operation string, body io.Reader) (json.RawMessage, error) {
	var req struct {
		EnvironmentID   string `json:"environment_id"`
		NativeProjectID string `json:"native_project_id"`
		GoalSlug        string `json:"goalSlug"`
	}
	var kind, beforeID string
	switch operation {
	case "summary-status":
		if decodeMailRequest(body, &req) != nil {
			return nil, storage.ErrInvalidSummary
		}
	case "summary-history":
		var history struct {
			EnvironmentID   string `json:"environment_id"`
			NativeProjectID string `json:"native_project_id"`
			GoalSlug        string `json:"goalSlug"`
			Kind            string `json:"kind"`
			BeforeID        string `json:"beforeId"`
		}
		if decodeMailRequest(body, &history) != nil || (history.Kind != "dispositions" && history.Kind != "acceptances") {
			return nil, storage.ErrInvalidSummary
		}
		if history.BeforeID != "" {
			id, err := strconv.ParseInt(history.BeforeID, 10, 64)
			if err != nil || id <= 0 || strconv.FormatInt(id, 10) != history.BeforeID {
				return nil, storage.ErrInvalidSummary
			}
		}
		req.EnvironmentID, req.NativeProjectID, req.GoalSlug = history.EnvironmentID, history.NativeProjectID, history.GoalSlug
		kind, beforeID = history.Kind, history.BeforeID
	default:
		return nil, storage.ErrInvalidSummary
	}
	if strings.TrimSpace(req.EnvironmentID) == "" || strings.TrimSpace(req.NativeProjectID) == "" || validateSlug(req.GoalSlug) != nil {
		return nil, storage.ErrInvalidSummary
	}
	if project == "" {
		var err error
		project, err = root.ProjectForWorkbenchKnowledge(ctx, req.EnvironmentID, req.NativeProjectID)
		if err != nil {
			return nil, fmt.Errorf("workbench summary: %w", err)
		}
	}
	if project == "_server" {
		return nil, storage.ErrProjectNotFound
	}
	s, err := root.ScopedExisting(ctx, project)
	if err != nil {
		return nil, fmt.Errorf("workbench summary: %w", err)
	}
	var result any
	if operation == "summary-history" {
		result, err = s.ReadSummaryHistory(ctx, req.GoalSlug, kind, beforeID)
	} else {
		result, err = s.ReadSummary(ctx, req.GoalSlug)
	}
	if err != nil {
		return nil, fmt.Errorf("workbench summary: %w", err)
	}
	encoded, encodeErr := json.Marshal(result)
	if encodeErr != nil {
		return encoded, fmt.Errorf("workbench summary: encode response: %w", encodeErr)
	}
	return encoded, nil
}
