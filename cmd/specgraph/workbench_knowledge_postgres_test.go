// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/server"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/specgraph/specgraph/internal/storage/postgres/postgrestest"
	"github.com/stretchr/testify/require"
)

func TestWorkbenchKnowledgeLocalPostgres(t *testing.T) {
	ctx := context.Background()
	url, err := postgrestest.ConnString(ctx)
	require.NoError(t, err)
	s, err := postgres.New(ctx, url, postgres.WithProject("knowledge-local"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(ctx)) })
	_, err = s.EnsureProject(ctx, "knowledge-other")
	require.NoError(t, err)
	spec, err := s.CreateSpec(ctx, "task", "Local source needle", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	_, err = s.Pool().Exec(ctx, `INSERT INTO specs(project_slug,slug,intent) VALUES('knowledge-other','foreign','Foreign needle');
	 INSERT INTO decisions(project_slug,slug,id,title) VALUES('knowledge-local','choice','decision-id','Local decision needle')`)
	require.NoError(t, err)
	authStore, err := postgres.NewAuth(ctx, s.Pool())
	require.NoError(t, err)
	user, err := authStore.CreateHuman(ctx, &storage.User{Kind: storage.KindHuman, DisplayName: "Source reader", Role: "reader"}, nil)
	require.NoError(t, err)
	secret, hash, err := auth.GenerateAPIKeySecret()
	require.NoError(t, err)
	key, err := authStore.CreateAPIKey(ctx, &storage.APIKey{UserID: user.ID, PHCHash: hash})
	require.NoError(t, err)
	credential := auth.FormatAPIKeyToken(key.Prefix, secret)
	search := json.RawMessage(`{"environment_id":"env","native_project_id":"native","query":"needle"}`)
	call := func(project, operation string, body json.RawMessage, dirs []string) (any, error) {
		t.Helper()
		procedures := workbenchKnowledgeProcedures(operation, body)
		identity, err := resolveWorkbenchOperator(ctx, s, credential, dirs, procedures[0], procedures[1:]...)
		if err != nil {
			return nil, err
		}
		return server.ExecuteLocalWorkbenchKnowledge(auth.WithIdentity(ctx, identity), s, project, operation, bytes.NewReader(body))
	}
	_, err = call("", "knowledge-search", search, nil)
	require.ErrorIs(t, err, postgres.ErrWorkbenchProjectAssociationMissing)
	// Explicit trusted host mapping needs neither run history nor mailbox enrollment.
	result, err := call("knowledge-local", "knowledge-search", search, nil)
	require.NoError(t, err)
	require.NotEmpty(t, result.(*postgres.WorkbenchKnowledgeSearch).Items)
	for _, item := range result.(*postgres.WorkbenchKnowledgeSearch).Items {
		require.NotEqual(t, "foreign", item.Slug)
	}
	_, err = s.Pool().Exec(ctx, `INSERT INTO context_packages(id,project_slug,task_spec_slug,body) VALUES
	 ('local-association','knowledge-local','task','{"dispatch_target":{"environmentId":"env","projectId":"native"}}'),
	 ('local-association-repeat','knowledge-local','task','{"dispatch_target":{"environmentId":"env","projectId":"native"}}')`)
	require.NoError(t, err)
	result, err = call("", "knowledge-search", search, nil)
	require.NoError(t, err)
	require.Equal(t, "specgraph-records", result.(*postgres.WorkbenchKnowledgeSearch).Scope)
	var changeID string
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT id FROM changelog_entries WHERE project_slug='knowledge-local' AND spec_slug='task' LIMIT 1`).Scan(&changeID))
	for _, source := range []struct{ kind, reference, id string }{{"spec", "task", spec.ID}, {"decision", "choice", "decision-id"}, {"change", changeID, changeID}} {
		body, err := json.Marshal(map[string]string{"environment_id": "env", "native_project_id": "native", "kind": source.kind, "reference": source.reference})
		require.NoError(t, err)
		result, err := call("", "knowledge-record", body, nil)
		require.NoError(t, err)
		require.Equal(t, source.id, result.(*postgres.WorkbenchKnowledgeRecord).ID)
	}
	_, err = call("", "knowledge-record", json.RawMessage(`{"environment_id":"env","native_project_id":"native","kind":"spec","reference":"foreign"}`), nil)
	require.ErrorIs(t, err, storage.ErrSpecNotFound)
	// A custom decision forbid must deny aggregate search, not silently omit decisions.
	policyDir, err := os.MkdirTemp(".", ".graph-current-policy-")
	require.NoError(t, err)
	policyDir, err = filepath.Abs(policyDir)
	require.NoError(t, err)
	workingDir, err := os.Getwd()
	require.NoError(t, err)
	require.Equal(t, workingDir, filepath.Dir(policyDir))
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(policyDir)) })
	require.NoError(t, os.WriteFile(filepath.Join(policyDir, "deny.cedar"), []byte(`forbid(principal, action == SpecGraph::Action::"decision.read", resource);`), 0600))
	_, err = call("", "knowledge-search", search, []string{policyDir})
	require.ErrorIs(t, err, storage.ErrMailForbidden)
	_, err = call("", "knowledge-record", json.RawMessage(`{"environment_id":"env","native_project_id":"native","kind":"decision","reference":"choice"}`), []string{policyDir})
	require.ErrorIs(t, err, storage.ErrMailForbidden)
	_, err = call("", "knowledge-record", json.RawMessage(`{"environment_id":"env","native_project_id":"native","kind":"spec","reference":"task"}`), []string{policyDir})
	require.NoError(t, err, "unrelated decision forbid must not deny a spec read")
	_, err = s.Pool().Exec(ctx, `INSERT INTO context_packages(id,project_slug,task_spec_slug,body) VALUES
	 ('other-association','knowledge-other','foreign','{"dispatch_target":{"environmentId":"env","projectId":"native"}}')`)
	require.NoError(t, err)
	_, err = call("", "knowledge-search", search, nil)
	require.ErrorIs(t, err, postgres.ErrWorkbenchProjectAssociationAmbiguous)
	_, err = call("knowledge-local", "knowledge-search", search, nil)
	require.NoError(t, err, "explicit host mapping resolves ambiguity")
	for _, body := range []string{
		`{"environment_id":"","native_project_id":"native","query":"needle"}`,
		`{"environment_id":"env","native_project_id":"","query":"needle"}`,
		`{"environment_id":"env","native_project_id":"native","query":" "}`,
		`{"environment_id":"env","native_project_id":"native","query":"needle","project":"forged"}`,
		`{"scope":{},"query":"needle"}`,
	} {
		_, err = call("knowledge-local", "knowledge-search", json.RawMessage(body), nil)
		require.Error(t, err)
		require.Equal(t, "invalid_argument", workbenchCommandError(err).Code)
	}
	var responsibilityRows int
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT (SELECT count(*) FROM run_bindings)+(SELECT count(*) FROM mail_grants)+(SELECT count(*) FROM mail_bindings)`).Scan(&responsibilityRows))
	require.Zero(t, responsibilityRows, "knowledge reading must not create run or mail records")
	// Delivery discovery needs no caller run or enrollment, and carries no snapshot bodies.
	listBody := func(slug, cursor string) json.RawMessage {
		t.Helper()
		body, err := json.Marshal(map[string]string{"environment_id": "env", "native_project_id": "native", "taskSlug": slug, "cursor": cursor})
		require.NoError(t, err)
		return body
	}
	empty, err := call("knowledge-local", "node-deliveries", listBody("task", ""), nil)
	require.NoError(t, err)
	emptyPage := empty.(*postgres.WorkbenchNodeDeliveries)
	require.Empty(t, emptyPage.Deliveries)
	require.Nil(t, emptyPage.NextCursor)
	emptyJSON, err := json.Marshal(emptyPage)
	require.NoError(t, err)
	require.Contains(t, string(emptyJSON), `"deliveries":[]`)
	_, err = call("knowledge-local", "node-deliveries", listBody("missing", ""), nil)
	require.ErrorIs(t, err, storage.ErrSpecNotFound)
	_, err = s.CreateSpec(ctx, "delivery-sibling", "Sibling delivery source", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	other, err := s.ScopedExisting(ctx, "knowledge-other")
	require.NoError(t, err)
	_, err = other.CreateSpec(ctx, "task", "Same slug in another project", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	seedRun := func(store *postgres.Store, slug string) string {
		t.Helper()
		approved := "approved"
		_, err := store.UpdateSpec(ctx, slug, nil, &approved, nil, nil, nil)
		require.NoError(t, err)
		run, err := store.PrepareRun(ctx, slug, "synthetic-workspace-"+slug)
		require.NoError(t, err)
		return run
	}
	run := seedRun(s, "task")
	siblingRun := seedRun(s, "delivery-sibling")
	foreignRun := seedRun(other, "task")
	// Equal timestamps across the page boundary require the original ID tie-breaker.
	_, err = s.Pool().Exec(ctx, `INSERT INTO deliveries(id,project_slug,run_binding_id,snapshot,submitted_by,submitted_at)
	 SELECT 'history-'||lpad(i::text,3,'0'),'knowledge-local',$1,jsonb_build_object('summary','Synthetic original delivery '||i),'fixture',
	 '2026-01-01 00:00:00+00'::timestamptz+(i/4)*interval '1 second' FROM generate_series(1,52) i`, run)
	require.NoError(t, err)
	siblingDelivery, err := s.CreateDelivery(ctx, siblingRun, json.RawMessage(`{"summary":"Sibling original"}`), "fixture")
	require.NoError(t, err)
	foreignDelivery, err := other.CreateDelivery(ctx, foreignRun, json.RawMessage(`{"summary":"Other project original"}`), "fixture")
	require.NoError(t, err)
	firstResult, err := call("knowledge-local", "node-deliveries", listBody("task", ""), nil)
	require.NoError(t, err)
	first := firstResult.(*postgres.WorkbenchNodeDeliveries)
	require.Len(t, first.Deliveries, 50)
	require.True(t, first.HasMore)
	require.NotNil(t, first.NextCursor)
	require.Equal(t, "history-052", first.Deliveries[0].ID)
	require.Equal(t, "history-003", *first.NextCursor)
	for _, delivery := range first.Deliveries {
		require.Equal(t, run, delivery.RunBindingID)
	}
	listJSON, err := json.Marshal(first)
	require.NoError(t, err)
	require.NotContains(t, string(listJSON), "snapshot")
	require.NotContains(t, string(listJSON), "Synthetic original delivery")
	_, err = s.CreateDelivery(ctx, run, json.RawMessage(`{"summary":"New delivery after first page"}`), "fixture")
	require.NoError(t, err)
	secondResult, err := call("knowledge-local", "node-deliveries", listBody("task", *first.NextCursor), nil)
	require.NoError(t, err)
	second := secondResult.(*postgres.WorkbenchNodeDeliveries)
	require.Len(t, second.Deliveries, 2)
	require.False(t, second.HasMore)
	require.Nil(t, second.NextCursor)
	require.Equal(t, "history-002", second.Deliveries[0].ID)
	require.Equal(t, "history-001", second.Deliveries[1].ID)
	for _, cursor := range []string{siblingDelivery, foreignDelivery, "missing-delivery"} {
		_, err := call("knowledge-local", "node-deliveries", listBody("task", cursor), nil)
		require.ErrorIs(t, err, storage.ErrInvalidDeliveryCursor)
		require.Equal(t, "invalid_argument", workbenchCommandError(err).Code)
	}
	siblingPage, err := call("knowledge-local", "node-deliveries", listBody("delivery-sibling", ""), nil)
	require.NoError(t, err)
	require.Len(t, siblingPage.(*postgres.WorkbenchNodeDeliveries).Deliveries, 1)
	foreignPage, err := call("knowledge-other", "node-deliveries", listBody("task", ""), nil)
	require.NoError(t, err)
	require.Len(t, foreignPage.(*postgres.WorkbenchNodeDeliveries).Deliveries, 1)
	followup, err := call("knowledge-local", "review-delivery-source", json.RawMessage(`{"environment_id":"env","native_project_id":"native","deliveryId":"history-052"}`), nil)
	require.NoError(t, err)
	require.JSONEq(t, `{"summary":"Synthetic original delivery 52"}`, string(followup.(*postgres.WorkbenchDeliveryDetail).Snapshot))
	var runRows, mailRows int
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT (SELECT count(*) FROM run_bindings),(SELECT count(*) FROM mail_grants)+(SELECT count(*) FROM mail_bindings)`).Scan(&runRows, &mailRows))
	require.Equal(t, 3, runRows, "only the three explicit fixture runs exist; reader creates none")
	require.Zero(t, mailRows, "source lookup never enrolls the caller")
}

func TestWorkbenchGraphCurrentStdioPostgres(t *testing.T) {
	ctx := context.Background()
	url, err := postgrestest.ConnString(ctx)
	require.NoError(t, err)
	s, err := postgres.New(ctx, url, postgres.WithProject("graph-current-stdio"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(ctx)) })
	_, err = s.EnsureProject(ctx, "graph-current-foreign")
	require.NoError(t, err)
	_, err = s.EnsureProject(ctx, "graph-current-empty")
	require.NoError(t, err)
	for _, slug := range []string{"parent", "child"} {
		_, err = s.CreateSpec(ctx, slug, "secret intent", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
		require.NoError(t, err)
	}
	_, err = s.Pool().Exec(ctx, `INSERT INTO decisions(project_slug,slug,id,title) VALUES('graph-current-stdio','choice','graph-choice','secret title');
		INSERT INTO specs(project_slug,slug,intent) VALUES('graph-current-foreign','foreign','foreign intent')`)
	require.NoError(t, err)
	require.NoError(t, s.CreateSlice(ctx, &storage.Slice{Slug: "parent/part", ParentSlug: "parent", SliceID: "part", Intent: "secret slice"}))
	for _, edge := range []struct {
		from, to string
		kind     storage.EdgeType
	}{
		{"parent", "child", storage.EdgeTypeComposes},
		{"child", "parent", storage.EdgeTypeDependsOn},
		{"parent", "child", storage.EdgeTypeBlocks},
	} {
		_, err = s.AddEdge(ctx, edge.from, edge.to, edge.kind)
		require.NoError(t, err)
	}
	authStore, err := postgres.NewAuth(ctx, s.Pool())
	require.NoError(t, err)
	user, err := authStore.CreateHuman(ctx, &storage.User{Kind: storage.KindHuman, DisplayName: "Graph reader", Role: "reader"}, nil)
	require.NoError(t, err)
	secret, hash, err := auth.GenerateAPIKeySecret()
	require.NoError(t, err)
	key, err := authStore.CreateAPIKey(ctx, &storage.APIKey{UserID: user.ID, PHCHash: hash})
	require.NoError(t, err)
	credential := auth.FormatAPIKeyToken(key.Prefix, secret)
	call := func(project, body string, dirs []string) workbenchCommandResponse {
		t.Helper()
		request, err := json.Marshal(workbenchCommandRequest{ID: "graph", Operation: "graph-current", Project: project, Credential: credential, Body: json.RawMessage(body)})
		require.NoError(t, err)
		var output bytes.Buffer
		require.NoError(t, runWorkbenchCommandStdio(ctx, strings.NewReader(string(request)+"\n"), &output,
			func(callCtx context.Context, req workbenchCommandRequest, procedure string) (json.RawMessage, error) {
				identity, err := resolveWorkbenchOperator(callCtx, s, req.Credential, dirs, procedure)
				if err != nil {
					return nil, err
				}
				result, err := server.ExecuteLocalWorkbenchKnowledge(auth.WithIdentity(callCtx, identity), s, req.Project, req.Operation, bytes.NewReader(req.Body))
				if err != nil {
					return nil, err
				}
				return json.Marshal(result)
			}))
		var response workbenchCommandResponse
		require.NoError(t, json.Unmarshal(output.Bytes(), &response))
		return response
	}
	body := `{"environment_id":"graph-env","native_project_id":"graph-native"}`
	require.Equal(t, "project_association_missing", call("", body, nil).Error.Code)
	blank := call("graph-current-empty", body, nil)
	require.Nil(t, blank.Error)
	require.JSONEq(t, `{"nodes":[],"edges":[],"totalNodes":0,"totalEdges":0,"hasMore":false,"nextOffset":null}`, string(blank.Data))
	empty := call("graph-current-foreign", body, nil)
	require.Nil(t, empty.Error)
	require.JSONEq(t, `{"nodes":[{"slug":"foreign","label":"Spec","stage":"spark","priority":""}],"edges":[],"totalNodes":1,"totalEdges":0,"hasMore":false,"nextOffset":null}`, string(empty.Data))
	_, err = s.Pool().Exec(ctx, `INSERT INTO context_packages(id,project_slug,task_spec_slug,body) VALUES('graph-current-association','graph-current-stdio','parent','{"dispatch_target":{"environmentId":"graph-env","projectId":"graph-native"}}')`)
	require.NoError(t, err)
	actual := call("", body, nil)
	require.Nil(t, actual.Error)
	require.JSONEq(t, `{"nodes":[{"slug":"child","label":"Spec","stage":"spark","priority":"p2"},{"slug":"choice","label":"Decision","stage":"proposed","priority":""},{"slug":"parent","label":"Spec","stage":"spark","priority":"p2"},{"slug":"parent/part","label":"Slice","stage":"open","priority":""}],"edges":[{"from":"child","to":"parent","type":"DEPENDS_ON"},{"from":"parent","to":"child","type":"BLOCKS"},{"from":"parent","to":"child","type":"COMPOSES"},{"from":"parent/part","to":"parent","type":"COMPOSES"}],"totalNodes":4,"totalEdges":4,"hasMore":false,"nextOffset":null}`, string(actual.Data))
	require.NotContains(t, string(actual.Data), "secret")
	for _, invalid := range []string{`{"environment_id":"graph-env","native_project_id":"graph-native","offset":-1}`, `{"environment_id":"graph-env","native_project_id":"graph-native","offset":2147483648}`, `{"environment_id":"graph-env","native_project_id":"graph-native","offset":null}`, `{"environment_id":"graph-env","native_project_id":"graph-native","offset":1.5}`} {
		require.Equal(t, "invalid_argument", call("", invalid, nil).Error.Code)
	}
	policyDir, err := os.MkdirTemp(".", ".graph-current-policy-")
	require.NoError(t, err)
	policyDir, err = filepath.Abs(policyDir)
	require.NoError(t, err)
	workingDir, err := os.Getwd()
	require.NoError(t, err)
	require.Equal(t, workingDir, filepath.Dir(policyDir))
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(policyDir)) })
	require.NoError(t, os.WriteFile(filepath.Join(policyDir, "deny.cedar"), []byte(`forbid(principal, action == SpecGraph::Action::"graph.read", resource);`), 0600))
	require.Equal(t, "forbidden", call("", body, []string{policyDir}).Error.Code)
	var responsibilityRows int
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT (SELECT count(*) FROM run_bindings)+(SELECT count(*) FROM mail_grants)+(SELECT count(*) FROM mail_bindings)`).Scan(&responsibilityRows))
	require.Zero(t, responsibilityRows)
}
