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
	"regexp"
	"time"
	"unicode/utf8"

	"github.com/specgraph/specgraph/internal/auth"
)

var manualCompletionKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

type manualCompletionRequest struct {
	ExpectedVersion int32  `json:"expected_version"`
	IdempotencyKey  string `json:"idempotency_key"`
	Note            string `json:"note"`
}

func decodeManualCompletion(w http.ResponseWriter, r *http.Request) (manualCompletionRequest, error) {
	r.Body = http.MaxBytesReader(w, r.Body, 32*1024)
	return decodeManualCompletionBody(r.Body)
}

func decodeManualCompletionBody(body io.Reader) (manualCompletionRequest, error) {
	var req manualCompletionRequest
	decoder := json.NewDecoder(body)
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return req, errors.New("body must be an object")
	}
	seen := make(map[string]bool, 3)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return req, fmt.Errorf("workbench manual completion: %w", err)
		}
		name, ok := token.(string)
		if !ok || seen[name] {
			return req, errors.New("duplicate or invalid field")
		}
		seen[name] = true
		var target any
		switch name {
		case "expected_version":
			target = &req.ExpectedVersion
		case "idempotency_key":
			target = &req.IdempotencyKey
		case "note":
			target = &req.Note
		default:
			return req, errors.New("unknown field")
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return req, fmt.Errorf("workbench manual completion: %w", err)
		}
		if string(value) == "null" {
			return req, errors.New("null field")
		}
		if err := json.Unmarshal(value, target); err != nil {
			return req, fmt.Errorf("workbench manual completion: %w", err)
		}
	}
	if _, err := decoder.Token(); err != nil {
		return req, fmt.Errorf("workbench manual completion: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return req, errors.New("body must contain one JSON object")
	}
	if req.ExpectedVersion < 1 || !manualCompletionKeyPattern.MatchString(req.IdempotencyKey) || utf8.RuneCountInString(req.Note) > 4000 {
		return req, errors.New("expected_version must be positive; idempotency_key must be 1-128 ASCII letters, digits, underscores or hyphens; note maximum 4000 characters")
	}
	return req, nil
}

func (l *workbenchLoop) manualComplete(w http.ResponseWriter, r *http.Request) {
	req, err := decodeManualCompletion(w, r)
	if err != nil {
		writeLoopJSON(w, http.StatusBadRequest, map[string]string{"code": "invalid_argument", "error": "invalid manual completion body"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	store, err := resolveWorkbenchStore(r.WithContext(ctx), l.root)
	if err != nil {
		l.mapError(w, "scope store", err)
		return
	}
	identity, _ := auth.IdentityFromContext(ctx)
	slug := r.PathValue("slug")
	version, replayed, err := store.ManualComplete(ctx, slug, identity.UserID, req.ExpectedVersion, req.IdempotencyKey, req.Note)
	if err != nil {
		l.mapError(w, "manual completion", err)
		return
	}
	writeLoopJSON(w, http.StatusOK, map[string]any{"completed": slug, "version": version, "replayed": replayed})
}
