// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package server

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
)

// ExecuteAgentReview receives a host-bound scope, never a model-selected actor.
func ExecuteAgentReview(ctx context.Context, root *postgres.Store, project, operation string, body io.Reader) (any, error) {
	var scope storage.MailScope
	var run func(*postgres.Store) (any, error)
	switch operation {
	case "review-assign":
		var req struct {
			Scope storage.MailScope `json:"scope"`
			storage.AssignReviewRequest
		}
		if decodeMailRequest(body, &req) != nil || validateSlug(req.TaskSlug) != nil {
			return nil, storage.ErrInvalidReview
		}
		scope = req.Scope
		run = func(s *postgres.Store) (any, error) {
			return s.AssignReview(ctx, &req.AssignReviewRequest, &scope)
		}
	case "review-submit":
		var req struct {
			Scope     storage.MailScope `json:"scope"`
			RequestID string            `json:"requestId"`
			Verdict   string            `json:"verdict"`
			Basis     string            `json:"basis"`
		}
		if decodeMailRequest(body, &req) != nil {
			return nil, storage.ErrInvalidReview
		}
		scope = req.Scope
		run = func(s *postgres.Store) (any, error) {
			return s.SubmitReview(ctx, scope, req.RequestID, req.Verdict, req.Basis)
		}
	default:
		return nil, storage.ErrInvalidReview
	}
	if project == "" {
		var err error
		project, err = root.ProjectForLocalMailScope(ctx, scope)
		if err != nil {
			return nil, fmt.Errorf("workbench review: %w", err)
		}
	}
	if project == "_server" {
		return nil, storage.ErrProjectNotFound
	}
	s, err := root.ScopedExisting(ctx, project)
	if err != nil {
		return nil, fmt.Errorf("workbench review: %w", err)
	}
	return run(s)
}

func (l *workbenchLoop) assignReview(w http.ResponseWriter, r *http.Request) {
	s, err := resolveWorkbenchStore(r, l.root)
	if err != nil {
		l.mapError(w, "scope store", err)
		return
	}
	var req storage.AssignReviewRequest
	if decodeMailRequest(http.MaxBytesReader(w, r.Body, 32<<10), &req) != nil || validateSlug(r.PathValue("slug")) != nil {
		l.mapError(w, "assign review", storage.ErrInvalidReview)
		return
	}
	if req.TaskSlug != "" && req.TaskSlug != r.PathValue("slug") {
		l.mapError(w, "assign review", storage.ErrInvalidReview)
		return
	}
	req.TaskSlug = r.PathValue("slug")
	result, err := s.AssignReview(r.Context(), &req, nil)
	if err != nil {
		l.mapError(w, "assign review", err)
		return
	}
	writeLoopJSON(w, http.StatusOK, result)
}

func (l *workbenchLoop) reviewSource(w http.ResponseWriter, r *http.Request) {
	s, err := resolveWorkbenchStore(r, l.root)
	if err != nil {
		l.mapError(w, "scope store", err)
		return
	}
	var req struct {
		RequestID string `json:"requestId"`
		Verdict   string `json:"verdict"`
		Basis     string `json:"basis"`
	}
	if decodeMailRequest(http.MaxBytesReader(w, r.Body, 32<<10), &req) != nil || validateSlug(r.PathValue("slug")) != nil {
		l.mapError(w, "review source", storage.ErrInvalidReview)
		return
	}
	result, err := s.ReviewSource(r.Context(), r.PathValue("slug"), req.RequestID, req.Verdict, req.Basis)
	if err != nil {
		l.mapError(w, "review source", err)
		return
	}
	writeLoopJSON(w, http.StatusOK, result)
}

func (l *workbenchLoop) readReviewStatus(w http.ResponseWriter, r *http.Request) {
	s, err := resolveWorkbenchStore(r, l.root)
	if err != nil {
		l.mapError(w, "scope store", err)
		return
	}
	result, err := s.ReadReviewStatus(r.Context(), r.PathValue("slug"))
	if err != nil {
		l.mapError(w, "review status", err)
		return
	}
	writeLoopJSON(w, http.StatusOK, result)
}
