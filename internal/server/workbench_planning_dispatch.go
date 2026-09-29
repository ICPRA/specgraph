// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"connectrpc.com/connect"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
)

// ExecuteAgentPlanningDispatch keeps dispatch ownership in the original commands.
// Host scope and native project containment are checked in their write transaction.
func ExecuteAgentPlanningDispatch(ctx context.Context, root *postgres.Store, project, operation string, body io.Reader) (json.RawMessage, error) {
	var req struct {
		Scope   storage.MailScope `json:"scope"`
		RunID   string            `json:"runId"`
		Request json.RawMessage   `json:"request"`
	}
	invalid := connect.NewError(connect.CodeInvalidArgument, errors.New("invalid manager dispatch request"))
	if decodeMailRequest(body, &req) != nil || len(req.Request) == 0 {
		return nil, invalid
	}
	var target json.RawMessage
	var key string
	switch operation {
	case "pm-prepare-run":
		var prepare struct {
			TaskSlug  string          `json:"task_slug"`
			Workspace string          `json:"workspace"`
			Key       string          `json:"idempotency_key"`
			Target    json.RawMessage `json:"dispatch_target"`
		}
		if req.RunID != "" || decodeMailRequest(bytes.NewReader(req.Request), &prepare) != nil || prepare.Key == "" || len(prepare.Target) == 0 {
			return nil, invalid
		}
		target = prepare.Target
	case "pm-bind-run", "pm-authorize-run":
		if req.RunID == "" {
			return nil, invalid
		}
	case "pm-read-preparation":
		var read struct {
			Key string `json:"idempotency_key"`
		}
		if decodeMailRequest(bytes.NewReader(req.Request), &read) != nil || (req.RunID == "") == (read.Key == "") {
			return nil, invalid
		}
		key = read.Key
	default:
		return nil, invalid
	}
	if project == "" {
		var err error
		project, err = root.ProjectForLocalMailScope(ctx, req.Scope)
		if err != nil {
			return nil, fmt.Errorf("workbench planning dispatch: %w", err)
		}
	}
	if project == "_server" {
		return nil, storage.ErrProjectNotFound
	}
	scoped, scopeErr := root.ScopedExisting(ctx, project)
	if scopeErr != nil {
		return nil, fmt.Errorf("workbench planning dispatch: %w", scopeErr)
	}
	var result json.RawMessage
	transactionErr := scoped.RunInTransaction(ctx, func(txCtx context.Context) error {
		actor, err := scoped.PlanningManagerRun(txCtx, req.Scope)
		if err != nil {
			return fmt.Errorf("workbench planning dispatch: %w", err)
		}
		manager, err := scoped.ReadRunContext(txCtx, actor)
		if err != nil {
			return fmt.Errorf("workbench planning dispatch: %w", err)
		}
		var managerBody struct {
			Target struct {
				EnvironmentID string `json:"environmentId"`
				ProjectID     string `json:"projectId"`
			} `json:"dispatch_target"`
		}
		if json.Unmarshal(manager.Body, &managerBody) != nil || managerBody.Target.ProjectID == "" || managerBody.Target.EnvironmentID != req.Scope.EnvironmentID {
			return storage.ErrPlanningForbidden
		}
		var prepared storage.RunContext
		var dispatch storage.RunDispatchStatus
		if operation != "pm-prepare-run" {
			prepared, dispatch, err = scoped.ReadRunPreparation(txCtx, req.RunID, key)
			if err != nil {
				return fmt.Errorf("workbench planning dispatch: %w", err)
			}
			var saved struct {
				Target json.RawMessage `json:"dispatch_target"`
			}
			if json.Unmarshal(prepared.Body, &saved) != nil {
				return storage.ErrInvalidRunPreparation
			}
			target = saved.Target
		}
		var child struct {
			EnvironmentID string `json:"environmentId"`
			ProjectID     string `json:"projectId"`
		}
		if json.Unmarshal(target, &child) != nil || child.EnvironmentID != managerBody.Target.EnvironmentID || child.ProjectID != managerBody.Target.ProjectID {
			return storage.ErrPlanningForbidden
		}
		if operation == "pm-read-preparation" {
			result, err = json.Marshal(struct {
				Project  string                    `json:"project"`
				Context  storage.RunContext        `json:"context"`
				Dispatch storage.RunDispatchStatus `json:"dispatch"`
			}{project, prepared, dispatch})
			if err != nil {
				err = fmt.Errorf("workbench planning dispatch: %w", err)
			}
			return err
		}
		result, err = executeDispatchCommand(txCtx, scoped, strings.TrimPrefix(operation, "pm-"), req.RunID, actor, req.Request)
		return err
	})
	if transactionErr != nil {
		transactionErr = fmt.Errorf("workbench planning dispatch: %w", transactionErr)
	}
	return result, transactionErr
}
