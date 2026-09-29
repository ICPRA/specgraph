// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package server

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/specgraph/specgraph/internal/storage"
)

func (l *workbenchLoop) readRunContext(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	s, err := resolveWorkbenchStore(r.WithContext(ctx), l.root)
	if err != nil {
		l.mapError(w, "scope store", err)
		return
	}
	result, err := s.ReadRunContext(ctx, r.PathValue("id"))
	if errors.Is(err, storage.ErrRunContextNotFound) {
		writeLoopJSON(w, http.StatusNotFound, map[string]string{"code": "not_found", "error": err.Error()})
		return
	}
	if err != nil {
		l.mapError(w, "read run context", err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeLoopJSON(w, http.StatusOK, result)
}
