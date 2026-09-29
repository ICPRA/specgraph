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

// ExecuteAgentPlanning retains the original owners and derives the actor from
// the saved manager responsibility, never from model-provided role or actor fields.
func ExecuteAgentPlanning(ctx context.Context, root *postgres.Store, project, operation string, body io.Reader) (json.RawMessage, error) {
	var req struct {
		Scope    storage.MailScope `json:"scope"`
		TaskSlug string            `json:"taskSlug"`
		Request  json.RawMessage   `json:"request"`
	}
	invalid := connect.NewError(connect.CodeInvalidArgument, errors.New("invalid manager planning request"))
	if decodeMailRequest(body, &req) != nil || len(req.Request) == 0 {
		return nil, invalid
	}
	var decision struct {
		RunID              string          `json:"runId"`
		AttemptID          string          `json:"attemptId"`
		Value              string          `json:"value"`
		Basis              string          `json:"basis"`
		ExpectedJudgmentID json.RawMessage `json:"expectedJudgmentId"`
	}
	var expectedJudgmentID *string
	var stop struct {
		RunID  string `json:"runId"`
		Reason string `json:"reason"`
	}
	switch operation {
	case "pm-create-node":
		var create struct {
			Slug       string `json:"slug"`
			Intent     string `json:"intent"`
			Priority   string `json:"priority"`
			Complexity string `json:"complexity"`
		}
		if req.TaskSlug != "" || decodeMailRequest(bytes.NewReader(req.Request), &create) != nil {
			return nil, invalid
		}
	case "pm-merge-nodes":
		if req.TaskSlug != "" {
			return nil, invalid
		}
	case "pm-subdivide-node", "pm-add-dependency", "pm-remove-dependency", "pm-approve-node", "pm-set-node-mark":
		if validateSlug(req.TaskSlug) != nil {
			return nil, invalid
		}
	case "pm-record-program-loop-decision":
		if req.TaskSlug != "" || decodeMailRequest(bytes.NewReader(req.Request), &decision) != nil || len(decision.ExpectedJudgmentID) == 0 || json.Unmarshal(decision.ExpectedJudgmentID, &expectedJudgmentID) != nil || strings.TrimSpace(decision.RunID) == "" || strings.TrimSpace(decision.AttemptID) == "" || strings.TrimSpace(decision.Basis) == "" || (decision.Value != "true" && decision.Value != "false" && decision.Value != "unknown") {
			return nil, invalid
		}
	case "pm-stop-program-loop":
		if req.TaskSlug != "" || decodeMailRequest(bytes.NewReader(req.Request), &stop) != nil || strings.TrimSpace(stop.RunID) == "" || strings.TrimSpace(stop.Reason) == "" {
			return nil, invalid
		}
	default:
		return nil, invalid
	}
	if project == "" {
		var err error
		project, err = root.ProjectForLocalMailScope(ctx, req.Scope)
		if err != nil {
			return nil, fmt.Errorf("workbench planning: %w", err)
		}
	}
	if project == "_server" {
		return nil, storage.ErrProjectNotFound
	}
	scoped, scopeErr := root.ScopedExisting(ctx, project)
	if scopeErr != nil {
		return nil, fmt.Errorf("workbench planning: %w", scopeErr)
	}
	if operation == "pm-record-program-loop-decision" {
		result, err := scoped.PlanningRecordProgramLoopJudgment(ctx, req.Scope, decision.RunID, decision.AttemptID, decision.Value, decision.Basis, expectedJudgmentID)
		if err != nil {
			return nil, fmt.Errorf("workbench planning: %w", err)
		}
		encoded, encodeErr := json.Marshal(result)
		if encodeErr != nil {
			return encoded, fmt.Errorf("workbench planning: encode response: %w", encodeErr)
		}
		return encoded, nil
	}
	if operation == "pm-stop-program-loop" {
		result, err := scoped.PlanningStopProgramLoop(ctx, req.Scope, stop.RunID, stop.Reason)
		if err != nil {
			return nil, fmt.Errorf("workbench planning: %w", err)
		}
		encoded, encodeErr := json.Marshal(result)
		if encodeErr != nil {
			return encoded, fmt.Errorf("workbench planning: encode response: %w", encodeErr)
		}
		return encoded, nil
	}
	if operation == "pm-merge-nodes" {
		var merge storage.MergeRequest
		if decodeMailRequest(bytes.NewReader(req.Request), &merge) != nil {
			return nil, storage.ErrInvalidNodeMerge
		}
		result, err := scoped.MergeNodes(ctx, merge, &req.Scope)
		if err != nil {
			return nil, fmt.Errorf("workbench planning: %w", err)
		}
		return json.Marshal(result)
	}
	var result json.RawMessage
	transactionErr := scoped.RunInTransaction(ctx, func(txCtx context.Context) error {
		actor, err := scoped.PlanningManagerRun(txCtx, req.Scope)
		if err != nil {
			return fmt.Errorf("workbench planning: %w", err)
		}
		if operation == "pm-set-node-mark" {
			result, err = setWorkbenchNodeMark(txCtx, scoped, req.TaskSlug, actor, req.Request, true)
			return err
		}
		result, err = ExecuteWorkbenchCommand(txCtx, scoped, strings.TrimPrefix(operation, "pm-"), project, req.TaskSlug, actor, req.Request)
		return err
	})
	if transactionErr != nil {
		transactionErr = fmt.Errorf("workbench planning: %w", transactionErr)
	}
	return result, transactionErr
}
