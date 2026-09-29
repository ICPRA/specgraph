// SPDX-License-Identifier: Apache-2.0

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
)

// ExecuteAgentDeliveryHook only manages fixed configuration and delivery linkage.
// Every storage operation resolves the actual PM again in its transaction.
func ExecuteAgentDeliveryHook(ctx context.Context, root *postgres.Store, project, operation string, body io.Reader) (json.RawMessage, error) {
	var envelope struct {
		Scope   storage.MailScope `json:"scope"`
		Request json.RawMessage   `json:"request"`
	}
	if decodeMailRequest(body, &envelope) != nil || (len(envelope.Request) == 0 && operation != "delivery-hook-context") || (operation == "delivery-hook-context" && len(envelope.Request) != 0) {
		return nil, storage.ErrInvalidDeliveryHook
	}
	if project == "" {
		var err error
		project, err = root.ProjectForLocalMailScope(ctx, envelope.Scope)
		if err != nil {
			return nil, fmt.Errorf("workbench delivery hook: %w", err)
		}
	}
	if project == "_server" {
		return nil, storage.ErrProjectNotFound
	}
	s, err := root.ScopedExisting(ctx, project)
	if err != nil {
		return nil, fmt.Errorf("workbench delivery hook: %w", err)
	}
	var result any
	switch operation {
	case "delivery-hook-context":
		result, err = s.ReadOwnDeliveryHookContext(ctx, envelope.Scope)
	case "pm-retry-delivery-hook":
		var req struct {
			HookID string `json:"hookId"`
		}
		if decodeMailRequest(bytes.NewReader(envelope.Request), &req) != nil {
			return nil, storage.ErrInvalidDeliveryHook
		}
		result, err = s.RetryDeliveryTestHook(ctx, envelope.Scope, req.HookID)
	case "pm-arm-delivery-hook":
		var req storage.ArmDeliveryHookRequest
		if decodeMailRequest(bytes.NewReader(envelope.Request), &req) != nil {
			return nil, storage.ErrInvalidDeliveryHook
		}
		var fields map[string]json.RawMessage
		if json.Unmarshal(envelope.Request, &fields) != nil || (fields["deliveryId"] != nil && req.DeliveryID == nil) {
			return nil, storage.ErrInvalidDeliveryHook
		}
		result, err = s.ArmDeliveryTestHook(ctx, envelope.Scope, req)
	case "pm-read-delivery-hook":
		var req struct {
			HookID string `json:"hookId"`
		}
		if decodeMailRequest(bytes.NewReader(envelope.Request), &req) != nil {
			return nil, storage.ErrInvalidDeliveryHook
		}
		result, err = s.ReadDeliveryTestHook(ctx, envelope.Scope, req.HookID)
	case "pm-list-delivery-hooks":
		var req struct {
			Limit  *int   `json:"limit"`
			Cursor string `json:"cursor"`
		}
		if decodeMailRequest(bytes.NewReader(envelope.Request), &req) != nil {
			return nil, storage.ErrInvalidDeliveryHook
		}
		limit := 50
		if req.Limit != nil {
			limit = *req.Limit
		}
		result, err = s.ListDeliveryTestHooks(ctx, envelope.Scope, limit, req.Cursor)
	case "pm-cancel-delivery-hook":
		var req struct {
			HookID string `json:"hookId"`
			Reason string `json:"reason"`
		}
		if decodeMailRequest(bytes.NewReader(envelope.Request), &req) != nil {
			return nil, storage.ErrInvalidDeliveryHook
		}
		result, err = s.CancelDeliveryTestHook(ctx, envelope.Scope, req.HookID, req.Reason)
	default:
		return nil, storage.ErrInvalidDeliveryHook
	}
	if err != nil {
		return nil, fmt.Errorf("workbench delivery hook: %w", err)
	}
	encoded, encodeErr := json.Marshal(result)
	if encodeErr != nil {
		return encoded, fmt.Errorf("workbench delivery hook: encode response: %w", encodeErr)
	}
	return encoded, nil
}
