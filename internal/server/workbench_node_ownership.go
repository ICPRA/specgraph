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

// ExecuteHumanNodeOwnershipRead uses a named local project without inventing a live host association.
func ExecuteHumanNodeOwnershipRead(ctx context.Context, root *postgres.Store, project, operation string, body io.Reader) (json.RawMessage, error) {
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" || identity.UserKind != storage.KindHuman {
		return nil, storage.ErrPlanningForbidden
	}
	var req struct {
		TaskSlug string `json:"taskSlug"`
		Cursor   string `json:"cursor"`
	}
	if decodeMailRequest(body, &req) != nil || strings.TrimSpace(req.TaskSlug) == "" {
		return nil, storage.ErrInvalidNodeOwnership
	}
	if project == "" || project == "_server" {
		return nil, storage.ErrProjectNotFound
	}
	s, err := root.ScopedExisting(ctx, project)
	if err != nil {
		return nil, err
	}
	var result any
	switch operation {
	case "node-owner-read":
		if req.Cursor != "" {
			return nil, storage.ErrInvalidNodeOwnership
		}
		result, err = s.ReadNodeOwnership(ctx, req.TaskSlug)
	case "node-owner-history":
		result, err = s.ReadNodeOwnershipHistory(ctx, req.TaskSlug, req.Cursor)
	default:
		return nil, storage.ErrInvalidNodeOwnership
	}
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("workbench node owner read response: %w", err)
	}
	return encoded, nil
}

// ExecuteNodeOwnershipCommand reserves human-only begin, commit, cancel and return paths.
func ExecuteNodeOwnershipCommand(ctx context.Context, root *postgres.Store, project, operation string, body io.Reader) (json.RawMessage, error) {
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" || identity.UserKind != storage.KindHuman {
		return nil, storage.ErrPlanningForbidden
	}
	if project == "" || project == "_server" {
		return nil, storage.ErrProjectNotFound
	}
	input, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	if operation == "node-owner-take-begin" || operation == "node-owner-return" {
		var fields map[string]json.RawMessage
		if json.Unmarshal(input, &fields) != nil || len(fields["expectedOwnerUserId"]) == 0 {
			return nil, storage.ErrInvalidNodeOwnership
		}
	}
	s, err := root.ScopedExisting(ctx, project)
	if err != nil {
		return nil, err
	}
	var result *storage.NodeOwnershipOperation
	switch operation {
	case "node-owner-take-begin":
		var req storage.BeginNodeOwnershipTakeRequest
		if decodeMailRequest(bytes.NewReader(input), &req) != nil {
			return nil, storage.ErrInvalidNodeOwnership
		}
		result, err = s.BeginNodeOwnershipTake(ctx, req)
	case "node-owner-take-commit":
		var req storage.CommitNodeOwnershipTakeRequest
		if decodeMailRequest(bytes.NewReader(input), &req) != nil {
			return nil, storage.ErrInvalidNodeOwnership
		}
		result, err = s.CommitNodeOwnershipTake(ctx, req)
	case "node-owner-take-cancel":
		var req storage.CancelNodeOwnershipTakeRequest
		if decodeMailRequest(bytes.NewReader(input), &req) != nil {
			return nil, storage.ErrInvalidNodeOwnership
		}
		result, err = s.CancelNodeOwnershipTake(ctx, req)
	case "node-owner-return":
		var req storage.ReturnNodeOwnershipRequest
		if decodeMailRequest(bytes.NewReader(input), &req) != nil {
			return nil, storage.ErrInvalidNodeOwnership
		}
		result, err = s.ReturnNodeOwnership(ctx, req)
	default:
		return nil, storage.ErrInvalidNodeOwnership
	}
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("workbench node owner response: %w", err)
	}
	return encoded, nil
}

// ExecuteOwnNodeHandoffSummary only accepts a host-injected native project and scope.
func ExecuteOwnNodeHandoffSummary(ctx context.Context, root *postgres.Store, project string, body io.Reader) (json.RawMessage, error) {
	var envelope struct {
		Scope           storage.MailScope                          `json:"scope"`
		NativeProjectID string                                     `json:"nativeProjectId"`
		Request         storage.RecordOwnNodeHandoffSummaryRequest `json:"request"`
	}
	if decodeMailRequest(body, &envelope) != nil {
		return nil, storage.ErrInvalidNodeOwnership
	}
	resolved, err := root.ProjectForOwnHandoffSummary(ctx, envelope.Scope, envelope.NativeProjectID, envelope.Request)
	if err != nil {
		return nil, err
	}
	if project != "" && project != resolved {
		return nil, storage.ErrProjectNotFound
	}
	s, err := root.ScopedExisting(ctx, resolved)
	if err != nil {
		return nil, err
	}
	result, err := s.RecordOwnNodeHandoffSummary(ctx, envelope.Scope, envelope.NativeProjectID, envelope.Request)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("workbench own node summary response: %w", err)
	}
	return encoded, nil
}

// ExecuteHumanProgramStop records an actual observed stop, not a cancellation request or timeout.
func ExecuteHumanProgramStop(ctx context.Context, root *postgres.Store, project string, body io.Reader) (json.RawMessage, error) {
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" || identity.UserKind != storage.KindHuman {
		return nil, storage.ErrPlanningForbidden
	}
	var req storage.ConfirmProgramRunStoppedRequest
	if decodeMailRequest(body, &req) != nil {
		return nil, storage.ErrInvalidRunPreparation
	}
	if project == "" || project == "_server" {
		return nil, storage.ErrProjectNotFound
	}
	s, err := root.ScopedExisting(ctx, project)
	if err != nil {
		return nil, err
	}
	result, err := s.ConfirmProgramRunStopped(ctx, req)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("workbench program stop response: %w", err)
	}
	return encoded, nil
}
