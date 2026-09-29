// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/specgraph/specgraph/internal/storage/postgres/postgrestest"
	"github.com/stretchr/testify/require"
)

func TestWorkbenchDependencyEditHTTPPostgres(t *testing.T) {
	ctx := context.Background()
	url, err := postgrestest.ConnString(ctx)
	require.NoError(t, err)
	s, err := postgres.New(ctx, url, postgres.WithProject("dependency-http"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(ctx)) })
	_, err = s.EnsureProject(ctx, "dependency-http-other")
	require.NoError(t, err)
	for _, slug := range []string{"task", "upstream"} {
		_, err = s.CreateSpec(ctx, slug, slug, "p2", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
		require.NoError(t, err)
	}
	engine, err := auth.NewCedarEngine(ctx, []auth.PolicySource{auth.NewEmbeddedPolicySource()}, auth.ActionNames())
	require.NoError(t, err)
	resolver := &workbenchAuthResolver{}
	mux := http.NewServeMux()
	RegisterWorkbenchLoop(mux, s, resolver, auth.NewCedarAuthorizer(engine))
	request := func(method, project string, body any, role auth.Role, authenticated bool, status int, suffix ...string) map[string]any {
		t.Helper()
		resolver.identity = &auth.Identity{UserID: "real-operator", Subject: "operator", EffectiveRole: role}
		encoded, err := json.Marshal(body)
		require.NoError(t, err)
		path := "/loop/specs/task/dependencies"
		if len(suffix) > 0 {
			path += suffix[0]
		}
		r := httptest.NewRequest(method, path, bytes.NewReader(encoded))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-Specgraph-Project", project)
		if authenticated {
			r.AddCookie(&http.Cookie{Name: "specgraph_session", Value: "fixture"})
		}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		require.Equal(t, status, w.Code, w.Body.String())
		var result map[string]any
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
		return result
	}
	request(http.MethodGet, "dependency-http", nil, auth.RoleAdmin, false, 401)
	request(http.MethodGet, "dependency-http", nil, auth.RoleWriter, true, 403)
	state := request(http.MethodGet, "dependency-http", nil, auth.RoleAdmin, true, 200)
	require.Equal(t, "0", state["revision"])
	edit := map[string]any{"prerequisite": "upstream", "expected_version": state["specVersion"], "expected_prerequisite_version": 1, "expected_revision": state["revision"], "reason": "Input contract requires upstream", "idempotency_key": "http-edit"}
	request(http.MethodPost, "dependency-http", edit, auth.RoleWriter, true, 403)
	request(http.MethodPost, "dependency-http-other", edit, auth.RoleAdmin, true, 404)
	result := request(http.MethodPost, "dependency-http", edit, auth.RoleAdmin, true, 200)
	require.Equal(t, true, result["changed"])
	require.Equal(t, false, result["replayed"])
	firstRevision := result["revision"]
	result = request(http.MethodPost, "dependency-http", edit, auth.RoleAdmin, true, 200)
	require.Equal(t, true, result["replayed"])
	require.Equal(t, firstRevision, result["revision"])
	edit["idempotency_key"] = "stale-edit"
	request(http.MethodPost, "dependency-http", edit, auth.RoleAdmin, true, 409)
	state = request(http.MethodGet, "dependency-http", nil, auth.RoleAdmin, true, 200)
	ops := state["operations"].([]any)
	require.Len(t, ops, 1)
	operation := ops[0].(map[string]any)
	require.Equal(t, "real-operator", operation["actor"])
	require.Equal(t, "Input contract requires upstream", operation["reason"])
	require.Equal(t, "upstream", operation["prerequisite"])
	require.Equal(t, firstRevision, state["revision"])
	deps, err := s.GetDependencies(ctx, "task")
	require.NoError(t, err)
	require.Len(t, deps, 1)
	require.Equal(t, "upstream", deps[0].Slug)
	var changes int
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT count(*) FROM dependency_changes WHERE project_slug='dependency-http'`).Scan(&changes))
	require.Equal(t, 1, changes)
	edit["idempotency_key"] = "http-edit"
	request(http.MethodPost, "dependency-http", edit, auth.RoleAdmin, true, 409, "/remove")
	edit["idempotency_key"] = "http-remove"
	edit["expected_revision"] = state["revision"]
	edit["reason"] = "Explicitly release the prerequisite"
	request(http.MethodPost, "dependency-http", edit, auth.RoleAdmin, false, 401, "/remove")
	request(http.MethodPost, "dependency-http", edit, auth.RoleWriter, true, 403, "/remove")
	result = request(http.MethodPost, "dependency-http", edit, auth.RoleAdmin, true, 200, "/remove")
	require.Equal(t, "remove", result["operation"])
	require.Equal(t, true, result["changed"])
	result = request(http.MethodPost, "dependency-http", edit, auth.RoleAdmin, true, 200, "/remove")
	require.Equal(t, true, result["replayed"])
	deps, err = s.GetDependencies(ctx, "task")
	require.NoError(t, err)
	require.Empty(t, deps)
	for _, slug := range []string{"task", "upstream"} {
		_, err = s.GetSpec(ctx, slug)
		require.NoError(t, err)
	}
	state = request(http.MethodGet, "dependency-http", nil, auth.RoleAdmin, true, 200)
	ops = state["operations"].([]any)
	require.Len(t, ops, 2)
	require.Equal(t, "remove", ops[0].(map[string]any)["operation"])
	require.Equal(t, "add", ops[1].(map[string]any)["operation"])
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT count(*) FROM dependency_changes WHERE project_slug='dependency-http'`).Scan(&changes))
	require.Equal(t, 2, changes)
}
