// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
)

// ExecuteHostDeliveryHook accepts only a local authenticated host; target, actor
// and native command ID are not inputs.
func ExecuteHostDeliveryHook(ctx context.Context, root *postgres.Store, project, operation string, body io.Reader) (json.RawMessage, error) {
	var scope storage.DeliveryHookHostScope
	var call func(*postgres.Store) (any, error)
	switch operation {
	case "host-list-delivery-hooks":
		var req struct {
			storage.DeliveryHookHostScope
			Limit  *int   `json:"limit"`
			Cursor string `json:"cursor"`
		}
		if decodeMailRequest(body, &req) != nil {
			return nil, storage.ErrInvalidDeliveryHook
		}
		scope = req.DeliveryHookHostScope
		limit := 50
		if req.Limit != nil {
			limit = *req.Limit
		}
		call = func(s *postgres.Store) (any, error) {
			return s.ListHostDeliveryTestHooks(ctx, scope, limit, req.Cursor)
		}
	case "host-read-delivery-hook", "host-bind-delivery-hook", "host-authorize-delivery-hook":
		var req struct {
			storage.DeliveryHookHostScope
			HookID string `json:"hookId"`
		}
		if decodeMailRequest(body, &req) != nil {
			return nil, storage.ErrInvalidDeliveryHook
		}
		scope = req.DeliveryHookHostScope
		action := strings.TrimSuffix(strings.TrimPrefix(operation, "host-"), "-delivery-hook")
		call = func(s *postgres.Store) (any, error) {
			return s.PrepareDeliveryHookDispatch(ctx, scope, req.HookID, action)
		}
	case "host-result-delivery-hook":
		var req struct {
			storage.DeliveryHookHostScope
			HookID string `json:"hookId"`
			storage.DeliveryHookHostResult
		}
		if decodeMailRequest(body, &req) != nil {
			return nil, storage.ErrInvalidDeliveryHook
		}
		scope = req.DeliveryHookHostScope
		call = func(s *postgres.Store) (any, error) {
			return s.RecordDeliveryHookHostResult(ctx, scope, req.HookID, req.DeliveryHookHostResult)
		}
	default:
		return nil, storage.ErrInvalidDeliveryHook
	}
	if strings.TrimSpace(scope.EnvironmentID) == "" || strings.TrimSpace(scope.NativeProjectID) == "" {
		return nil, storage.ErrInvalidDeliveryHook
	}
	resolved, err := root.ProjectForWorkbenchKnowledge(ctx, scope.EnvironmentID, scope.NativeProjectID)
	if err != nil {
		return nil, fmt.Errorf("workbench delivery hook host: %w", err)
	}
	if project != "" && project != resolved {
		return nil, storage.ErrPlanningForbidden
	}
	if resolved == "_server" {
		return nil, storage.ErrProjectNotFound
	}
	s, err := root.ScopedExisting(ctx, resolved)
	if err != nil {
		return nil, fmt.Errorf("workbench delivery hook host: %w", err)
	}
	result, err := call(s)
	if err != nil {
		return nil, err
	}
	encoded, encodeErr := json.Marshal(result)
	if encodeErr != nil {
		return encoded, fmt.Errorf("workbench delivery hook host: encode response: %w", encodeErr)
	}
	return encoded, nil
}
