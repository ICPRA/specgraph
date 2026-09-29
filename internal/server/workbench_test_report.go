// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package server

import (
	"context"
	"fmt"
	"io"

	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
)

// ExecuteAgentTestReport receives native identity from the host, never from report arguments.
func ExecuteAgentTestReport(ctx context.Context, root *postgres.Store, project string, body io.Reader) (*storage.TestReport, error) {
	var req struct {
		Scope storage.MailScope `json:"scope"`
		storage.TestReportFields
	}
	if decodeMailRequest(body, &req) != nil {
		return nil, storage.ErrInvalidTestReport
	}
	if project == "" {
		var err error
		project, err = root.ProjectForLocalMailScope(ctx, req.Scope)
		if err != nil {
			return nil, fmt.Errorf("workbench test report: %w", err)
		}
	}
	if project == "_server" {
		return nil, storage.ErrProjectNotFound
	}
	scoped, err := root.ScopedExisting(ctx, project)
	if err != nil {
		return nil, fmt.Errorf("workbench test report: %w", err)
	}
	result, callErr := scoped.RecordTestReport(ctx, &storage.RecordTestReportRequest{TestReportFields: req.TestReportFields}, &req.Scope)
	if callErr != nil {
		return result, fmt.Errorf("workbench test report: %w", callErr)
	}
	return result, nil
}
