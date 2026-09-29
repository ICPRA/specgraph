// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package server

import (
	"log/slog"
	"net/http"

	"github.com/specgraph/specgraph/internal/auth"
)

// workbenchWriteAuth uses the same identity and policy owners as ConnectRPC.
// Cross-origin checks protect browser cookies; they do not replace authentication.
func workbenchWriteAuth(resolver auth.Resolver, authorizer auth.Authorizer, procedure string, next http.Handler) http.Handler {
	authorized := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity, ok := auth.IdentityFromContext(r.Context())
		if !ok || identity.UserID == "" {
			writeLoopJSON(w, http.StatusUnauthorized, map[string]string{"code": "unauthenticated", "error": "authenticated user required"})
			return
		}
		decision, err := authorizer.Authorize(r.Context(), identity, procedure, nil)
		if err != nil {
			slog.ErrorContext(r.Context(), "workbench authorization failed", slog.String("procedure", procedure), slog.Any("error", err))
			writeLoopJSON(w, http.StatusInternalServerError, map[string]string{"code": "internal", "error": "authorization failed"})
			return
		}
		if !decision.Allowed {
			writeLoopJSON(w, http.StatusForbidden, map[string]string{"code": "permission_denied", "error": "permission denied"})
			return
		}
		next.ServeHTTP(w, r)
	})
	return http.NewCrossOriginProtection().Handler(auth.RequireAuth(resolver)(authorized))
}
