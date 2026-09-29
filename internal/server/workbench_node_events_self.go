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

// ExecuteOwnNodeEvent resolves the run and task from the authenticated native session.
func ExecuteOwnNodeEvent(ctx context.Context, root *postgres.Store, project string, body io.Reader) (*storage.NodeExecutionEvent, error) {
	var req struct {
		Scope   storage.MailScope                 `json:"scope"`
		Request storage.RecordOwnNodeEventRequest `json:"request"`
	}
	if decodeMailRequest(body, &req) != nil {
		return nil, storage.ErrInvalidNodeEvent
	}
	if project == "" {
		var err error
		project, err = root.ProjectForLocalMailScope(ctx, req.Scope)
		if err != nil {
			return nil, fmt.Errorf("workbench own node event project: %w", err)
		}
	}
	if project == "_server" {
		return nil, storage.ErrProjectNotFound
	}
	s, err := root.ScopedExisting(ctx, project)
	if err != nil {
		return nil, err
	}
	return s.RecordOwnNodeEvent(ctx, req.Scope, req.Request)
}
