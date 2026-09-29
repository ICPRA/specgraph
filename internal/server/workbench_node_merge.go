// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

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

func ReadLocalWorkbenchNodeMerge(ctx context.Context, root *postgres.Store, project, operation string, body io.Reader) (json.RawMessage, error) {
	var req struct {
		EnvironmentID   string   `json:"environment_id"`
		NativeProjectID string   `json:"native_project_id"`
		SourceSlugs     []string `json:"sourceSlugs"`
		ContextSlugs    []string `json:"contextSlugs"`
		TaskSlug        string   `json:"taskSlug"`
		BeforeID        string   `json:"beforeId"`
		ID              string   `json:"id"`
	}
	if decodeMailRequest(body, &req) != nil {
		return nil, storage.ErrInvalidNodeMerge
	}
	if project == "" {
		if strings.TrimSpace(req.EnvironmentID) == "" || strings.TrimSpace(req.NativeProjectID) == "" {
			return nil, storage.ErrProjectNotFound
		}
		var err error
		project, err = root.ProjectForWorkbenchKnowledge(ctx, req.EnvironmentID, req.NativeProjectID)
		if err != nil {
			return nil, fmt.Errorf("workbench node merge read: %w", err)
		}
	}
	if project == "_server" || !validProjectSlug.MatchString(project) {
		return nil, storage.ErrProjectNotFound
	}
	s, err := root.ScopedExisting(ctx, project)
	if err != nil {
		return nil, err
	}
	var result any
	switch operation {
	case "preview-node-merge":
		result, err = s.PreviewNodeMerge(ctx, req.SourceSlugs, req.ContextSlugs)
	case "node-merge-history":
		result, err = s.ReadNodeMergeHistory(ctx, req.TaskSlug, req.BeforeID)
	case "node-merge-receipt":
		result, err = s.ReadNodeMergeReceipt(ctx, req.ID)
	default:
		return nil, storage.ErrInvalidNodeMerge
	}
	if err != nil {
		return nil, err
	}
	return json.Marshal(result)
}
