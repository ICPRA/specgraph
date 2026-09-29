// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package server

import (
	"context"
	"net/http"
	"time"
)

func (l *workbenchLoop) readRequirementChangePreview(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	s, err := resolveWorkbenchStore(r.WithContext(ctx), l.root)
	if err != nil {
		l.mapError(w, "scope store", err)
		return
	}
	preview, err := s.ReadRequirementChangePreview(ctx, r.PathValue("slug"))
	if err != nil {
		l.mapError(w, "requirement change preview", err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeLoopJSON(w, http.StatusOK, preview)
}
