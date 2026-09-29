// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
)

// ExecuteHumanDeliveryHook authorizes workbench.manage and resolves the private host
// credential independently. Only its verified service identity reaches storage.
func ExecuteHumanDeliveryHook(ctx context.Context, root *postgres.Store, project, operation string, body io.Reader, consumer *auth.Identity) (json.RawMessage, error) {
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" || identity.UserKind != storage.KindHuman {
		return nil, storage.ErrPlanningForbidden
	}
	if project == "" || project == "_server" {
		return nil, storage.ErrProjectNotFound
	}
	s, err := root.ScopedExisting(ctx, project)
	if err != nil {
		return nil, fmt.Errorf("workbench delivery hook human: %w", err)
	}
	var result *storage.DeliveryTestHook
	switch operation {
	case "arm-delivery-hook":
		var req struct {
			storage.ArmDeliveryHookRequest
			HostCredential string `json:"hostCredential"`
		}
		if decodeMailRequest(body, &req) != nil || req.HostCredential == "" {
			return nil, storage.ErrInvalidDeliveryHook
		}
		result, err = s.ArmHumanDeliveryTestHook(ctx, req.ArmDeliveryHookRequest, consumer)
	case "cancel-delivery-hook":
		var req struct {
			HookID string `json:"hookId"`
			Reason string `json:"reason"`
		}
		if decodeMailRequest(body, &req) != nil {
			return nil, storage.ErrInvalidDeliveryHook
		}
		result, err = s.CancelHumanDeliveryTestHook(ctx, req.HookID, req.Reason)
	case "retry-delivery-hook":
		var req struct {
			HookID string `json:"hookId"`
		}
		if decodeMailRequest(body, &req) != nil {
			return nil, storage.ErrInvalidDeliveryHook
		}
		result, err = s.RetryHumanDeliveryTestHook(ctx, req.HookID)
	default:
		return nil, storage.ErrInvalidDeliveryHook
	}
	if err != nil {
		return nil, fmt.Errorf("workbench delivery hook human: %w", err)
	}
	encoded, encodeErr := json.Marshal(result)
	if encodeErr != nil {
		return encoded, fmt.Errorf("workbench delivery hook human: encode response: %w", encodeErr)
	}
	return encoded, nil
}
