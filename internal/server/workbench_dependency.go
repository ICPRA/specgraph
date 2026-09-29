// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
)

func decodeDependencyEdit(w http.ResponseWriter, r *http.Request) (storage.DependencyEditRequest, error) {
	r.Body = http.MaxBytesReader(w, r.Body, 32*1024)
	return decodeDependencyEditBody(r.Body)
}

func decodeDependencyEditBody(body io.Reader) (storage.DependencyEditRequest, error) {
	var req storage.DependencyEditRequest
	var revision string
	d := json.NewDecoder(body)
	opening, openingErr := d.Token()
	if openingErr != nil || opening != json.Delim('{') {
		return req, errors.New("body must be an object")
	}
	seen := make(map[string]bool, 6)
	for d.More() {
		token, err := d.Token()
		if err != nil {
			return req, fmt.Errorf("workbench dependency: %w", err)
		}
		name, ok := token.(string)
		if !ok || seen[name] {
			return req, errors.New("duplicate field")
		}
		seen[name] = true
		var target any
		switch name {
		case "prerequisite":
			target = &req.Prerequisite
		case "expected_version":
			target = &req.ExpectedVersion
		case "expected_prerequisite_version":
			target = &req.ExpectedPrerequisiteVersion
		case "expected_revision":
			target = &revision
		case "reason":
			target = &req.Reason
		case "idempotency_key":
			target = &req.IdempotencyKey
		default:
			return req, errors.New("unknown field")
		}
		if err := d.Decode(target); err != nil {
			return req, fmt.Errorf("workbench dependency: %w", err)
		}
	}
	if _, err := d.Token(); err != nil {
		return req, fmt.Errorf("workbench dependency: %w", err)
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return req, errors.New("one object required")
	}
	parsedRevision, parseErr := strconv.ParseInt(revision, 10, 64)
	req.ExpectedRevision = parsedRevision
	if parseErr != nil || req.ExpectedRevision < 0 || strconv.FormatInt(req.ExpectedRevision, 10) != revision || len(seen) != 6 || req.ExpectedVersion < 1 || req.ExpectedPrerequisiteVersion < 1 || validateSlug(req.Prerequisite) != nil || strings.TrimSpace(req.Reason) == "" || utf8.RuneCountInString(req.Reason) > 4000 || !manualCompletionKeyPattern.MatchString(req.IdempotencyKey) {
		return req, errors.New("invalid dependency edit fields")
	}
	return req, nil
}

func (l *workbenchLoop) readDependencyEditState(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	s, err := resolveWorkbenchStore(r.WithContext(ctx), l.root)
	if err != nil {
		l.mapError(w, "scope store", err)
		return
	}
	state, err := s.ReadDependencyEditState(ctx, r.PathValue("slug"))
	if err != nil {
		l.mapError(w, "read dependency state", err)
		return
	}
	writeLoopJSON(w, http.StatusOK, state)
}

func (l *workbenchLoop) addDependency(w http.ResponseWriter, r *http.Request) {
	l.editDependency(w, r, false)
}

func (l *workbenchLoop) removeDependency(w http.ResponseWriter, r *http.Request) {
	l.editDependency(w, r, true)
}

func (l *workbenchLoop) editDependency(w http.ResponseWriter, r *http.Request, remove bool) {
	req, err := decodeDependencyEdit(w, r)
	if err != nil || validateSlug(r.PathValue("slug")) != nil {
		writeLoopJSON(w, http.StatusBadRequest, map[string]string{"code": "invalid_argument", "error": "invalid dependency edit body"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	s, err := resolveWorkbenchStore(r.WithContext(ctx), l.root)
	if err != nil {
		l.mapError(w, "scope store", err)
		return
	}
	identity, _ := auth.IdentityFromContext(ctx)
	var result storage.DependencyEditResult
	if remove {
		result, err = s.RemoveDependency(ctx, r.PathValue("slug"), identity.UserID, req)
	} else {
		result, err = s.AddDependency(ctx, r.PathValue("slug"), identity.UserID, req)
	}
	if err != nil {
		l.mapError(w, "edit dependency", err)
		return
	}
	writeLoopJSON(w, http.StatusOK, result)
}
