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

func decodeSubdivisionBody(w http.ResponseWriter, r *http.Request, body any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	err := d.Decode(body)
	if err == nil {
		err = d.Decode(new(any))
		if err == io.EOF {
			return true
		}
	}
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeLoopJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"code": "resource_exhausted", "error": "subdivision request exceeds 2 MiB"})
	} else {
		writeLoopJSON(w, http.StatusBadRequest, map[string]string{"code": "invalid_argument", "error": "invalid subdivision request"})
	}
	return false
}

func (l *workbenchLoop) listSpecSubdivisions(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	s, err := resolveWorkbenchStore(r.WithContext(ctx), l.root)
	if err != nil {
		l.mapError(w, "scope store", err)
		return
	}
	state, err := s.ListSpecSubdivisions(ctx, r.PathValue("slug"))
	if err != nil {
		l.mapError(w, "list spec subdivisions", err)
		return
	}
	writeLoopJSON(w, http.StatusOK, state)
}

func (l *workbenchLoop) readSubdivision(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	s, err := resolveWorkbenchStore(r.WithContext(ctx), l.root)
	if err != nil {
		l.mapError(w, "scope store", err)
		return
	}
	state, err := s.ReadSubdivision(ctx, r.PathValue("id"))
	if err != nil {
		l.mapError(w, "read subdivision", err)
		return
	}
	writeLoopJSON(w, http.StatusOK, state)
}

func (l *workbenchLoop) subdivideSpec(w http.ResponseWriter, r *http.Request) {
	var req storage.SubdivideRequest
	if !decodeSubdivisionBody(w, r, &req) {
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
	result, err := s.SubdivideSpec(ctx, r.PathValue("slug"), identity.UserID, req)
	if err != nil {
		l.mapError(w, "subdivide spec", err)
		return
	}
	writeLoopJSON(w, http.StatusOK, result)
}
