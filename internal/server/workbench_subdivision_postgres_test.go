// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/specgraph/specgraph/internal/storage/postgres/postgrestest"
	"github.com/stretchr/testify/require"
)

func TestWorkbenchSubdivisionCommandPostgres(t *testing.T) {
	ctx := context.Background()
	url, err := postgrestest.ConnString(ctx)
	require.NoError(t, err)
	s, err := postgres.New(ctx, url, postgres.WithProject("subdivision-command"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(ctx)) })
	parent, err := s.CreateSpec(ctx, "parent", "Parent goal", "p2", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	body, err := json.Marshal(storage.SubdivideRequest{ExpectedVersion: parent.Version, Reason: "Separate work", Children: []storage.ChildSpecDraft{{Slug: "child", Intent: "Independent task", Priority: "p2", Complexity: "low"}}})
	require.NoError(t, err)
	raw, err := ExecuteWorkbenchCommand(ctx, s, "subdivide-node", "subdivision-command", parent.Slug, "operator", body)
	require.NoError(t, err)
	var result storage.SubdivisionResult
	require.NoError(t, json.Unmarshal(raw, &result))
	require.Equal(t, []string{"child"}, result.ChildSlugs)
	state, err := s.ReadSubdivision(ctx, result.ID)
	require.NoError(t, err)
	require.Equal(t, storage.SpecRoleSummary, state.Parent.Role)
	require.Equal(t, storage.SpecStageSpark, state.Children[0].Spec.Stage)
	records, err := s.ListSpecSubdivisions(ctx, "child")
	require.NoError(t, err)
	require.Equal(t, "operator", records.Items[0].Actor)
}

func TestWorkbenchSubdivisionHTTPPostgres(t *testing.T) {
	ctx := context.Background()
	url, err := postgrestest.ConnString(ctx)
	require.NoError(t, err)
	s, err := postgres.New(ctx, url, postgres.WithProject("subdivision-http"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(ctx)) })
	_, err = s.EnsureProject(ctx, "subdivision-http-other")
	require.NoError(t, err)
	parent, err := s.CreateSpec(ctx, "parent", "Original work", "p2", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	approved := "approved"
	parent, err = s.UpdateSpec(ctx, parent.Slug, nil, &approved, nil, nil, nil)
	require.NoError(t, err)
	run, err := s.PrepareRun(ctx, parent.Slug, "original-workspace")
	require.NoError(t, err)
	engine, err := auth.NewCedarEngine(ctx, []auth.PolicySource{auth.NewEmbeddedPolicySource()}, auth.ActionNames())
	require.NoError(t, err)
	resolver := &workbenchAuthResolver{}
	mux := http.NewServeMux()
	RegisterWorkbenchLoop(mux, s, resolver, auth.NewCedarAuthorizer(engine))
	encode := func(body any) string {
		t.Helper()
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		return string(raw)
	}
	request := func(method, path, project, body string, role auth.Role, actor string, status int) *httptest.ResponseRecorder {
		t.Helper()
		resolver.identity = &auth.Identity{UserID: actor, Subject: "fixture", EffectiveRole: role}
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-Specgraph-Project", project)
		if actor != "" {
			r.AddCookie(&http.Cookie{Name: "specgraph_session", Value: "fixture"})
		}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		require.Equal(t, status, w.Code, w.Body.String())
		return w
	}
	const project = "subdivision-http"
	const actor = "real-operator"
	const createPath = "/loop/specs/parent/subdivide"
	create := map[string]any{
		"expectedVersion": parent.Version,
		"reason":          "Separate concrete work",
		"children":        []storage.ChildSpecDraft{{Slug: "child", Intent: "Child work", Priority: "p2", Complexity: "low"}},
	}
	createBody := encode(create)
	request(http.MethodPost, createPath, project, createBody, auth.RoleAdmin, "", 401)
	request(http.MethodPost, createPath, project, createBody, auth.RoleReader, actor, 403)
	request(http.MethodPost, createPath, project, createBody, auth.RoleWriter, actor, 403)
	request(http.MethodPost, createPath, "subdivision-http-other", createBody, auth.RoleAdmin, actor, 404)
	for _, field := range []string{"expectedDependencyRevision", "expectedChildren", "idempotencyKey"} {
		create[field] = "removed field"
		request(http.MethodPost, createPath, project, encode(create), auth.RoleAdmin, actor, 400)
		delete(create, field)
	}
	create["expectedVersion"] = parent.Version + 1
	request(http.MethodPost, createPath, project, encode(create), auth.RoleAdmin, actor, 409)
	create["expectedVersion"] = parent.Version
	create["reason"] = ""
	request(http.MethodPost, createPath, project, encode(create), auth.RoleAdmin, actor, 400)
	create["reason"] = "Separate concrete work"
	w := request(http.MethodPost, createPath, project, createBody, auth.RoleAdmin, actor, 200)
	var division storage.SubdivisionResult
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &division))
	require.NotEmpty(t, division.ID)
	require.Equal(t, parent.Version+1, division.ParentVersion)
	var receiptFields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &receiptFields))
	require.Len(t, receiptFields, 4)
	const previewPath = "/loop/specs/parent/change-preview"
	request(http.MethodGet, previewPath, project, "", auth.RoleAdmin, "", 401)
	request(http.MethodGet, previewPath, project, "", auth.RoleReader, actor, 403)
	request(http.MethodGet, previewPath, project, "", auth.RoleWriter, actor, 403)
	request(http.MethodGet, previewPath, "subdivision-http-other", "", auth.RoleAdmin, actor, 404)
	previewResponse := request(http.MethodGet, previewPath, project, "", auth.RoleAdmin, actor, 200)
	require.Equal(t, "no-store", previewResponse.Header().Get("Cache-Control"))
	var preview storage.RequirementChangePreview
	require.NoError(t, json.Unmarshal(previewResponse.Body.Bytes(), &preview))
	require.Equal(t, "parent", preview.SpecSlug)
	require.Len(t, preview.Nodes, 2)
	require.NotEmpty(t, preview.Scope.Sources)
	request(http.MethodPost, createPath, project, createBody, auth.RoleAdmin, actor, 409)
	request(http.MethodPost, createPath, project, createBody, auth.RoleAdmin, "different-operator", 409)
	var storedActor string
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT actor FROM subdivisions WHERE id=$1`, division.ID).Scan(&storedActor))
	require.Equal(t, actor, storedActor)

	path := "/loop/subdivisions/" + division.ID
	for _, route := range []struct{ method, path string }{
		{http.MethodGet, path},
		{http.MethodGet, "/loop/specs/parent/subdivisions"},
	} {
		request(route.method, route.path, project, `{}`, auth.RoleAdmin, "", 401)
		request(route.method, route.path, project, `{}`, auth.RoleReader, actor, 403)
		request(route.method, route.path, project, `{}`, auth.RoleWriter, actor, 403)
	}
	for _, slug := range []string{"parent", "child"} {
		lookupPath := "/loop/specs/" + slug + "/subdivisions"
		w = request(http.MethodGet, lookupPath, project, "", auth.RoleAdmin, actor, 200)
		var list storage.SubdivisionList
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &list))
		require.Len(t, list.Items, 1)
		require.Equal(t, division.ID, list.Items[0].ID)
		require.Equal(t, actor, list.Items[0].Actor)
		require.False(t, list.HasMore)
		request(http.MethodGet, lookupPath, "subdivision-http-other", "", auth.RoleAdmin, actor, 404)
	}
	request(http.MethodGet, path, "subdivision-http-other", "", auth.RoleAdmin, actor, 404)
	request(http.MethodGet, "/loop/subdivisions/missing", project, "", auth.RoleAdmin, actor, 404)
	w = request(http.MethodGet, path, project, "", auth.RoleAdmin, actor, 200)
	var state storage.SubdivisionState
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &state))
	require.Equal(t, division.ID, state.Receipt.ID)
	require.Equal(t, storage.SubdivisionSourceReference{ID: parent.ID, Slug: parent.Slug, Version: parent.Version}, state.SourceReference)
	parentClaim, err := s.GetActiveClaim(ctx, parent.Slug)
	require.NoError(t, err)
	require.Equal(t, run, parentClaim.Agent)
	require.Len(t, state.Children, 1)
	require.Equal(t, storage.SpecStageSpark, state.Children[0].Spec.Stage)
	request(http.MethodPost, path+"/review", project, "{}", auth.RoleAdmin, actor, 404)
	request(http.MethodPost, path+"/handoff", project, "{}", auth.RoleAdmin, actor, 404)
	_, err = s.UpdateSpec(ctx, "child", nil, &approved, nil, nil, nil)
	require.NoError(t, err)
	const targetBody = `{"version":1,"promptFormatVersion":"vacpms-run-v2","environmentId":"local-fixture","projectId":"native-project","threadId":"native-thread","workspace":"isolated-child","createCommandId":"create","startCommandId":"start","messageId":"message"}`
	const prepareBody = `{"task_slug":"child","workspace":"isolated-child","idempotency_key":"prepare-child","dispatch_target":` + targetBody + `}`
	w = request(http.MethodPost, "/loop/runs", project, prepareBody, auth.RoleWriter, actor, 200)
	var prepared struct {
		ID       string `json:"binding_id"`
		Replayed bool   `json:"replayed"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &prepared))
	require.False(t, prepared.Replayed)
	w = request(http.MethodPost, "/loop/runs", project, prepareBody, auth.RoleWriter, actor, 200)
	var prepareReplay struct {
		ID       string `json:"binding_id"`
		Replayed bool   `json:"replayed"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &prepareReplay))
	require.True(t, prepareReplay.Replayed)
	require.Equal(t, prepared.ID, prepareReplay.ID)
	bindPath := "/loop/runs/" + prepared.ID + "/bind"
	const bindBody = `{"thread_ref":"native-thread","environment_id":"local-fixture"}`
	request(http.MethodPost, bindPath, project, `{"thread_ref":"native-thread","environment_id":"other-fixture"}`, auth.RoleWriter, actor, 409)
	request(http.MethodPost, bindPath, project, bindBody, auth.RoleWriter, actor, 200)
	request(http.MethodPost, bindPath, project, bindBody, auth.RoleWriter, actor, 200)
	request(http.MethodPost, bindPath, project, `{"thread_ref":"native-thread","environment_id":"other-fixture"}`, auth.RoleWriter, actor, 409)
	request(http.MethodPost, bindPath, project, `{"thread_ref":"native-thread"}`, auth.RoleWriter, actor, 409)
	request(http.MethodPost, bindPath, project, `{"thread_ref":"native-thread","environment_id":null}`, auth.RoleWriter, actor, 400)
	metadata, err := s.ReadWorkbenchMetadata(ctx)
	require.NoError(t, err)
	var recordedEnvironment string
	for _, binding := range metadata.Runs {
		if binding.ID == prepared.ID {
			recordedEnvironment = binding.EnvironmentID
		}
	}
	require.Equal(t, "local-fixture", recordedEnvironment)
	request(http.MethodPost, "/loop/runs", project, prepareBody, auth.RoleWriter, "other-operator", 409)
	request(http.MethodPost, "/loop/runs", project, prepareBody+` {}`, auth.RoleWriter, actor, 400)
	request(http.MethodPost, "/loop/runs", project, `{"task_slug":"child","workspace":"isolated-child","dispatch_target":{}}`, auth.RoleWriter, actor, 400)
	request(http.MethodPost, "/loop/runs", project, strings.Replace(prepareBody, "local-fixture", "other-fixture", 1), auth.RoleWriter, actor, 409)
	contextPath := "/loop/runs/" + prepared.ID + "/context"
	request(http.MethodGet, contextPath, project, "", auth.RoleAdmin, "", 401)
	request(http.MethodGet, contextPath, project, "", auth.RoleReader, actor, 403)
	request(http.MethodGet, contextPath, "subdivision-http-other", "", auth.RoleWriter, actor, 404)
	request(http.MethodGet, "/loop/runs/missing/context", project, "", auth.RoleWriter, actor, 404)
	w = request(http.MethodGet, contextPath, project, "", auth.RoleWriter, actor, 200)
	require.Equal(t, "no-store", w.Header().Get("Cache-Control"))
	var runContext storage.RunContext
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &runContext))
	require.Equal(t, prepared.ID, runContext.RunID)
	require.Equal(t, "child", runContext.TaskSlug)
	require.NotEmpty(t, runContext.PackageID)
	var contents struct {
		Scope  storage.ScopeContext `json:"scope_context"`
		Target json.RawMessage      `json:"dispatch_target"`
	}
	require.NoError(t, json.Unmarshal(runContext.Body, &contents))
	require.NotContains(t, string(runContext.Body), `"scope_reviews"`)
	require.Len(t, contents.Scope.Sources, 2)
	require.JSONEq(t, targetBody, string(contents.Target))
	dispatchPath := "/loop/runs/" + prepared.ID + "/dispatch"
	authorization := encode(map[string]any{"packageId": runContext.PackageID, "target": json.RawMessage(targetBody)})
	for _, route := range []struct{ method, path, body string }{{http.MethodGet, dispatchPath, ""}, {http.MethodPost, dispatchPath + "/authorize", authorization}, {http.MethodPost, dispatchPath + "/resolve", `{}`}} {
		request(route.method, route.path, project, route.body, auth.RoleAdmin, "", 401)
		request(route.method, route.path, project, route.body, auth.RoleReader, actor, 403)
		request(route.method, route.path, project, route.body, auth.RoleWriter, actor, 403)
	}
	request(http.MethodGet, dispatchPath, "subdivision-http-other", "", auth.RoleAdmin, actor, 404)
	w = request(http.MethodGet, dispatchPath, project, "", auth.RoleAdmin, actor, 200)
	var dispatch storage.RunDispatchStatus
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &dispatch))
	require.Nil(t, dispatch.Admission)
	require.Nil(t, dispatch.Resolution)
	request(http.MethodPost, dispatchPath+"/authorize", project, authorization+` {}`, auth.RoleAdmin, actor, 400)
	request(http.MethodPost, dispatchPath+"/authorize", project, `{"packageId":"x","target":{},"actor":"forged"}`, auth.RoleAdmin, actor, 400)
	w = request(http.MethodPost, dispatchPath+"/authorize", project, authorization, auth.RoleAdmin, actor, 200)
	var admission storage.RunDispatch
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &admission))
	require.Equal(t, actor, admission.Actor)
	require.False(t, admission.Replayed)
	w = request(http.MethodPost, dispatchPath+"/authorize", project, authorization, auth.RoleAdmin, actor, 200)
	var replay storage.RunDispatch
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &replay))
	require.True(t, replay.Replayed)
	require.Equal(t, admission.ID, replay.ID)
	request(http.MethodPost, dispatchPath+"/authorize", project, authorization, auth.RoleAdmin, "other-operator", 409)
	resolution := map[string]string{"admissionId": admission.ID, "kind": "host_confirmed", "note": "Not host evidence"}
	request(http.MethodPost, dispatchPath+"/resolve", project, encode(resolution), auth.RoleAdmin, actor, 400)
	resolution["kind"] = "isolated_workspace"
	resolution["note"] = "Operator confirmed workspace isolation"
	request(http.MethodPost, dispatchPath+"/resolve", project, encode(resolution), auth.RoleAdmin, actor, 200)
	request(http.MethodPost, dispatchPath+"/resolve", project, encode(resolution), auth.RoleAdmin, actor, 200)
	w = request(http.MethodGet, dispatchPath, project, "", auth.RoleAdmin, actor, 200)
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &dispatch))
	require.Equal(t, admission.ID, dispatch.Admission.ID)
	require.Equal(t, actor, dispatch.Resolution.Actor)
	require.Equal(t, "isolated_workspace", dispatch.Resolution.Kind)
	request(http.MethodPost, dispatchPath+"/authorize", project, authorization, auth.RoleAdmin, actor, 428)
	request(http.MethodPost, dispatchPath+"/cancel-preparation", project, encode(map[string]string{"packageId": runContext.PackageID, "note": "cannot cancel admitted work"}), auth.RoleAdmin, actor, 428)
	_, err = s.CreateSpec(ctx, "cancel-task", "Cancel preparation", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	_, err = s.UpdateSpec(ctx, "cancel-task", nil, &approved, nil, nil, nil)
	require.NoError(t, err)
	w = request(http.MethodPost, "/loop/runs", project, `{"task_slug":"cancel-task","workspace":"isolated-child","idempotency_key":"cancel-request","dispatch_target":`+targetBody+`}`, auth.RoleWriter, actor, 200)
	var cancelRun struct {
		ID string `json:"binding_id"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &cancelRun))
	cancelContext, err := s.ReadRunContext(ctx, cancelRun.ID)
	require.NoError(t, err)
	cancelPath := "/loop/runs/" + cancelRun.ID + "/dispatch/cancel-preparation"
	cancelBody := encode(map[string]string{"packageId": cancelContext.PackageID, "note": "Operator cancelled unadmitted preparation"})
	request(http.MethodPost, cancelPath, project, cancelBody, auth.RoleAdmin, "", 401)
	request(http.MethodPost, cancelPath, project, cancelBody, auth.RoleWriter, actor, 403)
	request(http.MethodPost, cancelPath, "subdivision-http-other", cancelBody, auth.RoleAdmin, actor, 404)
	request(http.MethodPost, cancelPath, project, cancelBody, auth.RoleAdmin, actor, 200)
	request(http.MethodPost, cancelPath, project, cancelBody, auth.RoleAdmin, actor, 200)
	w = request(http.MethodGet, "/loop/runs/"+cancelRun.ID+"/dispatch", project, "", auth.RoleAdmin, actor, 200)
	var cancelled storage.RunDispatchStatus
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &cancelled))
	require.Nil(t, cancelled.Admission)
	require.Equal(t, actor, cancelled.Cancellation.Actor)
	abortBody := encode(storage.PreparationAbortRequest{TaskSlug: "cancel-task", Workspace: "isolated-child", IdempotencyKey: "unknown-prepare", Target: json.RawMessage(targetBody), Note: "Withdraw original request"})
	request(http.MethodPost, "/loop/preparations/abort", project, abortBody, auth.RoleAdmin, "", 401)
	request(http.MethodPost, "/loop/preparations/abort", project, abortBody, auth.RoleWriter, actor, 403)
	w = request(http.MethodPost, "/loop/preparations/abort", project, abortBody, auth.RoleAdmin, actor, 200)
	var aborted storage.PreparationAbortResult
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &aborted))
	require.Nil(t, aborted.RunID)
	request(http.MethodPost, "/loop/preparations/abort", project, abortBody, auth.RoleAdmin, actor, 200)
	request(http.MethodPost, "/loop/runs", project, `{"task_slug":"cancel-task","workspace":"isolated-child","idempotency_key":"unknown-prepare","dispatch_target":`+targetBody+`}`, auth.RoleWriter, actor, 428)
	parentChanged := "Parent changed after preparation"
	_, err = s.UpdateSpec(ctx, "parent", &parentChanged, nil, nil, nil, nil)
	require.NoError(t, err)
	w = request(http.MethodGet, contextPath, project, "", auth.RoleWriter, actor, 200)
	var historical storage.RunContext
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &historical))
	require.Equal(t, runContext, historical, "read the exact recorded package, not a regenerated current context")

	for _, route := range []struct{ path, body string }{
		{createPath, createBody},
	} {
		var forged map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(route.body), &forged))
		forged["actor"] = json.RawMessage(`"forged-operator"`)
		request(http.MethodPost, route.path, project, encode(forged), auth.RoleAdmin, actor, 400)
		request(http.MethodPost, route.path, project, route.body+` {}`, auth.RoleAdmin, actor, 400)
		for _, body := range []string{`null`, `{}`, `[]`} {
			request(http.MethodPost, route.path, project, body, auth.RoleAdmin, actor, 400)
		}
		request(http.MethodPost, route.path, project, strings.Repeat(" ", 2<<20)+`{}`, auth.RoleAdmin, actor, 413)
	}
}

func TestWorkbenchSubdivisionErrorMapping(t *testing.T) {
	for _, test := range []struct {
		err    error
		status int
	}{
		{storage.ErrInvalidSubdivisionRequest, 400},
		{storage.ErrDecisionNotFound, 404},
		{storage.ErrSpecAlreadyExists, 409},
	} {
		w := httptest.NewRecorder()
		(&workbenchLoop{}).mapError(w, "subdivision", test.err)
		require.Equal(t, test.status, w.Code, w.Body.String())
	}
}
