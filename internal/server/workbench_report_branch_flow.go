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

	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
)

// ExecuteReportBranchFlow routes human and manager operations through their original authority.
func ExecuteReportBranchFlow(ctx context.Context, root *postgres.Store, project, operation string, body io.Reader) (json.RawMessage, error) {
	var scope *storage.MailScope
	var request json.RawMessage
	if strings.HasPrefix(operation, "pm-") {
		var envelope struct {
			Scope   storage.MailScope `json:"scope"`
			Request json.RawMessage   `json:"request"`
		}
		if decodeMailRequest(body, &envelope) != nil || len(envelope.Request) == 0 {
			return nil, storage.ErrInvalidReportBranchFlow
		}
		scope, request = &envelope.Scope, envelope.Request
		if project == "" {
			var err error
			project, err = root.ProjectForLocalMailScope(ctx, *scope)
			if err != nil {
				return nil, fmt.Errorf("workbench report branch project: %w", err)
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
	var result any
	switch operation {
	case "report-join-arm", "pm-report-join-arm":
		var req storage.ArmReportFlowJoinRequest
		if decodeMailRequest(bytes.NewReader(request), &req) != nil {
			return nil, storage.ErrInvalidReportFlowJoin
		}
		result, err = s.ArmReportFlowJoin(ctx, req, scope)
	case "report-flow-arm", "pm-report-flow-arm":
		var req storage.ArmReportBranchFlowRequest
		if decodeMailRequest(bytes.NewReader(request), &req) != nil {
			return nil, storage.ErrInvalidReportBranchFlow
		}
		result, err = s.ArmReportBranchFlow(ctx, req, scope)
	case "report-flow-cancel", "pm-report-flow-cancel":
		var req struct {
			FlowID string `json:"flowId"`
			Reason string `json:"reason"`
		}
		if decodeMailRequest(bytes.NewReader(request), &req) != nil {
			return nil, storage.ErrInvalidReportBranchFlow
		}
		result, err = s.CancelReportBranchFlow(ctx, req.FlowID, req.Reason, scope)
	default:
		return nil, storage.ErrInvalidReportBranchFlow
	}
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("workbench report branch response: %w", err)
	}
	return encoded, nil
}
