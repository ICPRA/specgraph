// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/specgraph/specgraph/internal/auth"
)

func TestMailInspectionAuthorizationAndInput(t *testing.T) {
	engine, err := auth.NewCedarEngine(context.Background(), []auth.PolicySource{auth.NewEmbeddedPolicySource()}, auth.ActionNames())
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/loop/mail/threads", "/loop/mail/threads/thread-1"} {
		for _, tc := range []struct {
			name    string
			role    auth.Role
			cookie  bool
			query   string
			project string
			want    int
		}{
			{name: "anonymous", want: 401},
			{name: "reader bridge cannot inspect", role: auth.RoleReader, project: "project", want: 403},
			{name: "writer cannot inspect", role: auth.RoleWriter, project: "project", want: 403},
			{name: "admin requires project", role: auth.RoleAdmin, want: 400},
			{name: "session accepted", role: auth.RoleAdmin, cookie: true, want: 400},
			{name: "zero limit", role: auth.RoleAdmin, project: "project", query: "?limit=0", want: 400},
			{name: "high limit", role: auth.RoleAdmin, project: "project", query: "?limit=101", want: 400},
			{name: "empty limit", role: auth.RoleAdmin, project: "project", query: "?limit=", want: 400},
			{name: "invalid limit", role: auth.RoleAdmin, project: "project", query: "?limit=x", want: 400},
			{name: "invalid state", role: auth.RoleAdmin, project: "project", query: "?state=unread", want: 400},
			{name: "empty state", role: auth.RoleAdmin, project: "project", query: "?state=", want: 400},
		} {
			t.Run(path+"/"+tc.name, func(t *testing.T) {
				resolver := &workbenchAuthResolver{identity: &auth.Identity{UserID: "user", Subject: "fixture", Source: "apikey", EffectiveRole: tc.role}}
				mux := http.NewServeMux()
				RegisterWorkbenchLoop(mux, nil, resolver, auth.NewCedarAuthorizer(engine))
				r := httptest.NewRequest(http.MethodGet, path+tc.query, nil)
				if tc.cookie {
					r.Header.Set("Cookie", "specgraph_session=session")
				} else if tc.role != "" {
					r.Header.Set("Authorization", "Bearer fixture")
				}
				r.Header.Set("X-Specgraph-Project", tc.project)
				w := httptest.NewRecorder()
				mux.ServeHTTP(w, r)
				if w.Code != tc.want {
					t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
				}
				if tc.cookie && resolver.token != "session" {
					t.Fatal("cookie was not resolved")
				}
			})
		}
	}
}
