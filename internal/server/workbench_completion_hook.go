// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
)

// ExecuteHumanCompletionHook keeps the human grant separate from the verified
// host consumer. Storage matches that consumer to the fixed program target.
func ExecuteHumanCompletionHook(ctx context.Context, root *postgres.Store, project, operation string, body io.Reader, consumer *auth.Identity) (json.RawMessage, error) {
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" || identity.UserKind != storage.KindHuman {
		return nil, storage.ErrProgramRunForbidden
	}
	var call func(*postgres.Store) (*storage.CompletionProgramHook, error)
	switch operation {
	case "arm-completion-hook":
		var req struct {
			storage.ArmCompletionHookRequest
			HostCredential string `json:"hostCredential"`
		}
		if decodeMailRequest(body, &req) != nil || strings.TrimSpace(req.HostCredential) == "" {
			return nil, storage.ErrInvalidCompletionHook
		}
		if consumer == nil || consumer.UserID == "" || consumer.UserKind != storage.KindServiceAccount || consumer.Source != "apikey" {
			return nil, storage.ErrProgramRunForbidden
		}
		call = func(s *postgres.Store) (*storage.CompletionProgramHook, error) {
			return s.ArmHumanCompletionProgramHook(ctx, req.ArmCompletionHookRequest, consumer)
		}
	case "cancel-completion-hook":
		var req struct {
			HookID string `json:"hookId"`
			Reason string `json:"reason"`
		}
		if decodeMailRequest(body, &req) != nil {
			return nil, storage.ErrInvalidCompletionHook
		}
		call = func(s *postgres.Store) (*storage.CompletionProgramHook, error) {
			return s.CancelHumanCompletionProgramHook(ctx, req.HookID, req.Reason)
		}
	case "retry-completion-hook":
		var req struct {
			HookID string `json:"hookId"`
		}
		if decodeMailRequest(body, &req) != nil {
			return nil, storage.ErrInvalidCompletionHook
		}
		call = func(s *postgres.Store) (*storage.CompletionProgramHook, error) {
			return s.RetryHumanCompletionProgramHook(ctx, req.HookID)
		}
	default:
		return nil, storage.ErrInvalidCompletionHook
	}
	if strings.TrimSpace(project) == "" || project == "_server" {
		return nil, storage.ErrProjectNotFound
	}
	s, err := root.ScopedExisting(ctx, project)
	if err != nil {
		return nil, fmt.Errorf("workbench completion hook: %w", err)
	}
	result, err := call(s)
	if err != nil {
		return nil, err
	}
	encoded, encodeErr := json.Marshal(result)
	if encodeErr != nil {
		return encoded, fmt.Errorf("workbench completion hook: encode response: %w", encodeErr)
	}
	return encoded, nil
}

// ExecuteHostCompletionHook uses the original host project association owner.
// Association is not authorization: storage rechecks the recorded consumer/scope.
func ExecuteHostCompletionHook(ctx context.Context, root *postgres.Store, project, operation string, body io.Reader) (json.RawMessage, error) {
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" || identity.UserKind != storage.KindServiceAccount || identity.Source != "apikey" {
		return nil, storage.ErrProgramRunForbidden
	}
	var scope storage.DeliveryHookHostScope
	var call func(*postgres.Store) (any, error)
	switch operation {
	case "host-list-hooks":
		var req struct {
			storage.DeliveryHookHostScope
			Limit  *int   `json:"limit"`
			Cursor string `json:"cursor"`
		}
		if decodeMailRequest(body, &req) != nil {
			return nil, storage.ErrInvalidCompletionHook
		}
		scope = req.DeliveryHookHostScope
		limit := 50
		if req.Limit != nil {
			limit = *req.Limit
		}
		call = func(s *postgres.Store) (any, error) { return s.ListHostHooks(ctx, scope, limit, req.Cursor) }
	case "host-read-completion-hook", "host-authorize-completion-hook":
		var req struct {
			storage.DeliveryHookHostScope
			HookID string `json:"hookId"`
		}
		if decodeMailRequest(body, &req) != nil {
			return nil, storage.ErrInvalidCompletionHook
		}
		scope = req.DeliveryHookHostScope
		call = func(s *postgres.Store) (any, error) {
			if operation == "host-read-completion-hook" {
				return s.ReadHostCompletionHook(ctx, scope, req.HookID)
			}
			return s.HostAuthorizeCompletionHook(ctx, scope, req.HookID)
		}
	case "host-result-completion-hook":
		var req struct {
			storage.DeliveryHookHostScope
			HookID string  `json:"hookId"`
			Status string  `json:"status"`
			Detail *string `json:"detail"`
		}
		if decodeMailRequest(body, &req) != nil || (req.Status != "blocked" && req.Status != "unconfirmed") {
			return nil, storage.ErrInvalidCompletionHook
		}
		scope = req.DeliveryHookHostScope
		call = func(s *postgres.Store) (any, error) {
			return s.RecordCompletionHookHostResult(ctx, scope, req.HookID, storage.DeliveryHookHostResult{Status: req.Status, Detail: req.Detail})
		}
	default:
		return nil, storage.ErrInvalidCompletionHook
	}
	if strings.TrimSpace(scope.EnvironmentID) == "" || strings.TrimSpace(scope.NativeProjectID) == "" {
		return nil, storage.ErrInvalidCompletionHook
	}
	if project == "_server" {
		return nil, storage.ErrProjectNotFound
	}
	resolved, err := root.ProjectForWorkbenchKnowledge(ctx, scope.EnvironmentID, scope.NativeProjectID)
	if err != nil {
		return nil, fmt.Errorf("workbench completion hook: %w", err)
	}
	if project != "" && project != resolved {
		return nil, storage.ErrProgramRunForbidden
	}
	if resolved == "_server" {
		return nil, storage.ErrProjectNotFound
	}
	s, err := root.ScopedExisting(ctx, resolved)
	if err != nil {
		return nil, fmt.Errorf("workbench completion hook: %w", err)
	}
	result, err := call(s)
	if err != nil {
		return nil, err
	}
	encoded, encodeErr := json.Marshal(result)
	if encodeErr != nil {
		return encoded, fmt.Errorf("workbench completion hook: encode response: %w", encodeErr)
	}
	return encoded, nil
}
