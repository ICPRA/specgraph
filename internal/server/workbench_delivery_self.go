// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
)

// ExecuteAgentDelivery never takes the run, saved workspace, baseline or native HEAD
// from model arguments.
func ExecuteAgentDelivery(ctx context.Context, root *postgres.Store, project, operation string, body io.Reader) (any, error) {
	var scope storage.MailScope
	var perform func(*postgres.Store) (any, error)
	switch operation {
	case "delivery-self-context":
		var req struct {
			Scope storage.MailScope `json:"scope"`
		}
		if decodeMailRequest(body, &req) != nil {
			return nil, storage.ErrInvalidDeliverySubmission
		}
		scope = req.Scope
		perform = func(s *postgres.Store) (any, error) { return s.ReadOwnDeliveryContext(ctx, scope) }
	case "delivery-submit-self":
		var req struct {
			Scope             storage.MailScope `json:"scope"`
			ExpectedRunID     string            `json:"expectedRunId"`
			ExpectedAttemptID string            `json:"expectedAttemptId"`
			Summary           string            `json:"summary"`
			Head              *struct {
				IsRepo    *bool           `json:"isRepo"`
				CommitSHA json.RawMessage `json:"commitSha"`
			} `json:"head"`
		}
		if decodeMailRequest(body, &req) != nil || req.Head == nil || req.Head.IsRepo == nil || len(req.Head.CommitSHA) == 0 {
			return nil, storage.ErrInvalidDeliverySubmission
		}
		var commit *string
		if json.Unmarshal(req.Head.CommitSHA, &commit) != nil {
			return nil, storage.ErrInvalidDeliverySubmission
		}
		scope = req.Scope
		head := storage.DeliveryHead{IsRepo: *req.Head.IsRepo, CommitSHA: commit}
		perform = func(s *postgres.Store) (any, error) {
			return s.SubmitOwnDelivery(ctx, scope, req.ExpectedRunID, req.ExpectedAttemptID, req.Summary, head)
		}
	default:
		return nil, storage.ErrInvalidDeliverySubmission
	}
	if project == "" {
		var err error
		project, err = root.ProjectForLocalMailScope(ctx, scope)
		if err != nil {
			return nil, fmt.Errorf("workbench delivery self: %w", err)
		}
	}
	if project == "_server" {
		return nil, storage.ErrProjectNotFound
	}
	scoped, err := root.ScopedExisting(ctx, project)
	if err != nil {
		return nil, fmt.Errorf("workbench delivery self: %w", err)
	}
	return perform(scoped)
}
