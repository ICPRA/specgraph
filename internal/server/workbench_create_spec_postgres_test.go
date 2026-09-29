// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/specgraph/specgraph/internal/storage/postgres/postgrestest"
	"github.com/stretchr/testify/require"
)

func TestWorkbenchCreateSpecHTTPPostgres(t *testing.T) {
	ctx := context.Background()
	url, err := postgrestest.ConnString(ctx)
	require.NoError(t, err)
	s, err := postgres.New(ctx, url, postgres.WithProject("node-create-http"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(ctx)) })
	_, err = s.EnsureProject(ctx, "node-create-http-other")
	require.NoError(t, err)
	engine, err := auth.NewCedarEngine(ctx, []auth.PolicySource{auth.NewEmbeddedPolicySource()}, auth.ActionNames())
	require.NoError(t, err)
	resolver := &workbenchAuthResolver{identity: &auth.Identity{UserID: "operator", Subject: "operator", EffectiveRole: auth.RoleAdmin}}
	authorizer := auth.NewCedarAuthorizer(engine)
	mux := NewMux(s, connect.WithInterceptors(auth.NewAuthInterceptor(resolver, authorizer)))
	RegisterWorkbenchLoop(mux, s, resolver, authorizer)
	handler := ProjectMiddleware(mux)
	post := func(body string, authenticated bool, status int) map[string]any {
		t.Helper()
		r := httptest.NewRequest(http.MethodPost, "/specgraph.v1.SpecService/CreateSpec", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Connect-Protocol-Version", "1")
		r.Header.Set("X-Specgraph-Project", "node-create-http")
		if authenticated {
			r.AddCookie(&http.Cookie{Name: "specgraph_session", Value: "fixture"})
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		require.Equal(t, status, w.Code, w.Body.String())
		var result map[string]any
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
		return result
	}
	const body = `{"slug":"draft","intent":"Original intent","priority":"p2","complexity":"medium"}`
	post(body, false, 401)
	resolver.identity.EffectiveRole = auth.RoleReader
	post(body, true, 403)
	resolver.identity.EffectiveRole = auth.RoleAdmin
	result := post(body, true, 200)
	require.Equal(t, "spark", result["spec"].(map[string]any)["stage"])
	post(`{"slug":"draft","intent":"Do not overwrite"}`, true, 409)
	spec, err := s.GetSpec(ctx, "draft")
	require.NoError(t, err)
	require.Equal(t, "Original intent", spec.Intent)
	other, err := s.ScopedExisting(ctx, "node-create-http-other")
	require.NoError(t, err)
	_, err = other.GetSpec(ctx, "draft")
	require.ErrorIs(t, err, storage.ErrSpecNotFound)
	r := httptest.NewRequest(http.MethodGet, "/wb/current-view", nil)
	r.RemoteAddr = "127.0.0.1:1234"
	r.Header.Set("X-Specgraph-Project", "node-create-http")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	require.Equal(t, 200, w.Code, w.Body.String())
	var view map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &view))
	require.Len(t, view["specs"], 1)
	graph := view["graph"].(map[string]any)
	require.Len(t, graph["nodes"], 1)
	require.Equal(t, "draft", graph["nodes"].([]any)[0].(map[string]any)["slug"])
	require.Empty(t, view["runs"])
	claim, err := s.GetActiveClaim(ctx, "draft")
	require.NoError(t, err)
	require.Nil(t, claim)
	// Exercise detail history queries on the same snapshot connection, not only mocks.
	note := "snapshot detail fixture"
	_, err = s.UpdateSpec(ctx, "draft", nil, nil, nil, nil, &note)
	require.NoError(t, err)
	r = httptest.NewRequest(http.MethodGet, "/wb/specs/draft", nil)
	r.Header.Set("X-Specgraph-Project", "node-create-http")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var detail map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &detail))
	require.Equal(t, note, detail["spec"].(map[string]any)["notes"])
	require.NotEmpty(t, detail["changes"])
}

func TestWorkbenchApproveNodePostgres(t *testing.T) {
	ctx := context.Background()
	url, err := postgrestest.ConnString(ctx)
	require.NoError(t, err)
	const project = "node-approval"
	s, err := postgres.New(ctx, url, postgres.WithProject(project))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(ctx)) })
	create := func(slug string) *storage.Spec {
		t.Helper()
		spec, err := s.CreateSpec(ctx, slug, "Existing work contract", "p2", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
		require.NoError(t, err)
		return spec
	}
	approve := func(slug string, version int32) (json.RawMessage, error) {
		return ExecuteWorkbenchCommand(ctx, s, "approve-node", project, slug, "real-operator", json.RawMessage(fmt.Sprintf(`{"expectedVersion":%d,"basis":"Reviewed existing work contract and acceptance criteria"}`, version)))
	}
	draft := create("draft")
	draft, err = s.GetSpec(ctx, draft.Slug)
	require.NoError(t, err)
	for _, body := range []string{
		`null`, `{}`, `{"expectedVersion":1,"basis":" "}`, `{"expectedVersion":"1","basis":"Checked"}`,
		`{"expectedVersion":1.5,"basis":"Checked"}`, `{"expectedVersion":1,"basis":null}`,
		`{"expectedVersion":1,"basis":"Checked","actor":"forged"}`, `{"expectedVersion":1,"basis":"Checked"} {}`,
		`{"expectedVersion":1,"basis":"` + strings.Repeat("a", 4001) + `"}`,
	} {
		_, err := ExecuteWorkbenchCommand(ctx, s, "approve-node", project, draft.Slug, "real-operator", json.RawMessage(body))
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), body)
	}
	_, err = approve(draft.Slug, draft.Version+1)
	require.ErrorIs(t, err, storage.ErrConcurrentModification)
	before, err := s.ListChanges(ctx, draft.Slug, storage.ChangeLogFilter{})
	require.NoError(t, err)
	// A later failure in the same transaction must roll back approval and its audit.
	err = s.RunInTransaction(ctx, func(txCtx context.Context) error {
		_, err := s.ApproveWorkbenchNode(txCtx, draft.Slug, "real-operator", draft.Version, "Rollback fixture")
		require.NoError(t, err)
		return storage.ErrConcurrentModification
	})
	require.ErrorIs(t, err, storage.ErrConcurrentModification)
	unchanged, err := s.GetSpec(ctx, draft.Slug)
	require.NoError(t, err)
	require.Equal(t, draft, unchanged)
	afterRollback, err := s.ListChanges(ctx, draft.Slug, storage.ChangeLogFilter{})
	require.NoError(t, err)
	require.Equal(t, before, afterRollback)
	result, err := approve(draft.Slug, draft.Version)
	require.NoError(t, err)
	require.JSONEq(t, fmt.Sprintf(`{"approved":"draft","version":%d}`, draft.Version+1), string(result))
	approved, err := s.GetSpec(ctx, draft.Slug)
	require.NoError(t, err)
	require.Equal(t, storage.SpecStageApproved, approved.Stage)
	require.Equal(t, draft.Version+1, approved.Version)
	require.Nil(t, approved.ShapeOutput)
	require.Nil(t, approved.SpecifyOutput)
	require.Nil(t, approved.DecomposeOutput)
	require.Empty(t, approved.ConversationLogs)
	require.Zero(t, approved.ConversationCount)
	events, err := s.GetExecutionEvents(ctx, draft.Slug, 0)
	require.NoError(t, err)
	require.Empty(t, events)
	changes, err := s.ListChanges(ctx, draft.Slug, storage.ChangeLogFilter{})
	require.NoError(t, err)
	require.Len(t, changes, len(before)+1)
	audit := changes[len(changes)-1]
	require.Equal(t, fmt.Sprintf("Workbench execution approval by real-operator (version %d -> %d)", draft.Version, approved.Version), audit.Summary)
	require.Equal(t, "Reviewed existing work contract and acceptance criteria", audit.Reason)
	require.True(t, audit.Checkpoint)
	require.Equal(t, []storage.FieldChange{{Field: "stage", OldValue: "spark", NewValue: "approved"}}, audit.Changes)
	_, err = approve(draft.Slug, draft.Version)
	require.ErrorIs(t, err, storage.ErrConcurrentModification)
	_, err = approve(draft.Slug, approved.Version)
	require.ErrorIs(t, err, storage.ErrSpecIneligibleStage)
	_, err = s.PrepareRun(ctx, draft.Slug, "workspace")
	require.NoError(t, err, "approval must allow preparing a real run without fabricated authoring outputs")

	for _, stage := range []storage.SpecStage{storage.SpecStageShape, storage.SpecStageSpecify, storage.SpecStageDecompose, storage.SpecStageReview, storage.SpecStageDone, storage.SpecStageInProgress, storage.SpecStageAbandoned, storage.SpecStageSuperseded} {
		t.Run(string(stage), func(t *testing.T) {
			spec := create("stage-" + string(stage))
			_, err := s.Pool().Exec(ctx, `UPDATE specs SET stage=$3 WHERE project_slug=$1 AND slug=$2`, project, spec.Slug, stage)
			require.NoError(t, err)
			_, err = approve(spec.Slug, spec.Version)
			switch {
			case stage.IsValidReEntryStage():
				require.NoError(t, err)
			case stage.IsFullyTerminal():
				require.ErrorIs(t, err, storage.ErrSpecTerminal)
			default:
				require.ErrorIs(t, err, storage.ErrSpecIneligibleStage)
			}
		})
	}
	claimed := create("claimed")
	_, err = s.ClaimSpec(ctx, claimed.Slug, "worker", 0)
	require.NoError(t, err)
	_, err = approve(claimed.Slug, claimed.Version)
	require.ErrorIs(t, err, storage.ErrSpecAlreadyClaimed)
	parent := create("parent")
	split, err := s.SubdivideSpec(ctx, parent.Slug, "real-operator", storage.SubdivideRequest{ExpectedVersion: parent.Version, Reason: "Separate work", Children: []storage.ChildSpecDraft{{Slug: "child-a", Intent: "First work", Priority: "p2", Complexity: "low"}, {Slug: "child-b", Intent: "Second work", Priority: "p2", Complexity: "low"}}})
	require.NoError(t, err)
	_, err = approve(parent.Slug, split.ParentVersion)
	require.ErrorIs(t, err, storage.ErrSummaryNotExecutable)
	child, err := s.GetSpec(ctx, "child-a")
	require.NoError(t, err)
	_, err = approve(child.Slug, child.Version)
	require.NoError(t, err, "creation lineage is not an approval gate")
	blocked := create("blocked")
	upstream := create("upstream")
	_, err = s.AddEdge(ctx, blocked.Slug, upstream.Slug, storage.EdgeTypeDependsOn)
	require.NoError(t, err)
	_, err = approve(blocked.Slug, blocked.Version)
	require.NoError(t, err)
	_, err = s.PrepareRun(ctx, blocked.Slug, "workspace")
	require.ErrorIs(t, err, storage.ErrDependenciesNotReady, "approval must not bypass execution prerequisites")
}
