// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
)

func decodeDispatchBody(w http.ResponseWriter, r *http.Request, body any) bool {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32<<10))
	d.DisallowUnknownFields()
	err := d.Decode(body)
	if err == nil {
		err = d.Decode(new(any))
		if err == io.EOF {
			return true
		}
	}
	var oversized *http.MaxBytesError
	if errors.As(err, &oversized) {
		writeLoopJSON(w, 413, map[string]string{"code": "resource_exhausted", "error": "dispatch request exceeds 32 KiB"})
	} else {
		writeLoopJSON(w, 400, map[string]string{"code": "invalid_argument", "error": "invalid dispatch request"})
	}
	return false
}

func (l *workbenchLoop) readDispatch(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	s, err := resolveWorkbenchStore(r.WithContext(ctx), l.root)
	if err != nil {
		l.mapError(w, "scope store", err)
		return
	}
	result, err := s.ReadRunDispatch(ctx, r.PathValue("id"))
	if err != nil {
		l.mapError(w, "read dispatch", err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeLoopJSON(w, http.StatusOK, result)
}

func (l *workbenchLoop) authorizeDispatch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PackageID string          `json:"packageId"`
		Target    json.RawMessage `json:"target"`
	}
	if !decodeDispatchBody(w, r, &req) {
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
	result, err := s.AuthorizeRunDispatch(ctx, r.PathValue("id"), identity.UserID, req.PackageID, req.Target)
	if err != nil {
		l.mapError(w, "authorize dispatch", err)
		return
	}
	writeLoopJSON(w, http.StatusOK, result)
}

func (l *workbenchLoop) resolveDispatch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AdmissionID string `json:"admissionId"`
		Kind        string `json:"kind"`
		Note        string `json:"note"`
	}
	if !decodeDispatchBody(w, r, &req) {
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
	if err := s.ResolveRunDispatch(ctx, r.PathValue("id"), req.AdmissionID, identity.UserID, req.Kind, req.Note); err != nil {
		l.mapError(w, "resolve dispatch", err)
		return
	}
	writeLoopJSON(w, http.StatusOK, map[string]any{"recorded": true, "admissionId": req.AdmissionID})
}

func (l *workbenchLoop) cancelPreparation(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PackageID  string                         `json:"packageId"`
		Note       string                         `json:"note"`
		RawHandoff *storage.RawPreparationHandoff `json:"rawHandoff"`
	}
	if !decodeDispatchBody(w, r, &req) {
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
	if err := s.CancelRunPreparation(ctx, r.PathValue("id"), req.PackageID, identity.UserID, req.Note, req.RawHandoff); err != nil {
		l.mapError(w, "cancel preparation", err)
		return
	}
	writeLoopJSON(w, http.StatusOK, map[string]bool{"recorded": true})
}

func (l *workbenchLoop) abortPreparation(w http.ResponseWriter, r *http.Request) {
	var req storage.PreparationAbortRequest
	if !decodeDispatchBody(w, r, &req) {
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
	result, err := s.AbortPreparation(ctx, identity.UserID, &req)
	if errors.Is(err, storage.ErrDispatchResponsibilityHeld) && result.RunID != nil {
		writeLoopJSON(w, http.StatusPreconditionRequired, map[string]any{"code": "failed_precondition", "error": "dispatch already admitted; preparation was not cancelled", "runId": *result.RunID, "idempotencyKey": result.IdempotencyKey})
		return
	}
	if err != nil {
		l.mapError(w, "abort preparation", err)
		return
	}
	writeLoopJSON(w, http.StatusOK, result)
}
