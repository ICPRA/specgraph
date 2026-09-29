// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/specgraph/specgraph/gen/specgraph/v1/specgraphv1connect"
	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestWorkbenchProjectBindingCommandProtocol(t *testing.T) {
	ctx := context.Background()
	engine, err := auth.NewCedarEngine(ctx, []auth.PolicySource{auth.NewEmbeddedPolicySource()}, auth.ActionNames())
	require.NoError(t, err)
	for _, role := range []auth.Role{auth.RoleReader, auth.RoleAdmin} {
		for _, test := range []struct{ operation, body string }{
			{"bind-project", `{"environmentId":"env","nativeProjectId":"native","workspaceRoot":"/ws","reason":"Confirmed first dispatch"}`},
			{"unbind-project", `{}`},
		} {
			var output bytes.Buffer
			called := false
			err := runWorkbenchCommandStdio(ctx, strings.NewReader(`{"id":"binding","operation":"`+test.operation+`","project":"alpha","slug":"task","credential":"credential","body":`+test.body+`}`+"\n"), &output,
				func(ctx context.Context, req workbenchCommandRequest, procedure string) (json.RawMessage, error) {
					called = true
					require.Equal(t, auth.WorkbenchBindProjectProcedure, procedure)
					require.Equal(t, "alpha", req.Project)
					require.Equal(t, "task", req.Slug)
					action, ok := auth.ActionForProcedure(procedure)
					require.True(t, ok)
					require.Equal(t, "workbench.manage", action)
					decision, err := auth.NewCedarAuthorizer(engine).Authorize(ctx, &auth.Identity{UserID: "operator", EffectiveRole: role}, procedure, nil)
					require.NoError(t, err)
					require.Equal(t, role == auth.RoleAdmin, decision.Allowed, "reader must not gain binding-write authority")
					return json.RawMessage(`{"id":"1","projectSlug":"alpha"}`), nil
				})
			require.NoError(t, err)
			require.True(t, called, test.operation)
			var response workbenchCommandResponse
			require.NoError(t, json.Unmarshal(output.Bytes(), &response))
			require.Nil(t, response.Error)
		}
	}
	// Bindings carry the triggering node slug and an explicit project.
	for _, test := range []struct {
		name, line string
	}{
		{"missing slug", `{"id":"x","operation":"bind-project","project":"alpha","body":{"environmentId":"env","nativeProjectId":"native","reason":"Confirmed"}}`},
		{"missing slug on unbind", `{"id":"x","operation":"unbind-project","project":"alpha","body":{}}`},
		{"missing project", `{"id":"x","operation":"bind-project","slug":"task","body":{"environmentId":"env","nativeProjectId":"native","reason":"Confirmed"}}`},
		{"missing body", `{"id":"x","operation":"unbind-project","project":"alpha","slug":"task"}`},
	} {
		var output bytes.Buffer
		called := false
		require.NoError(t, runWorkbenchCommandStdio(ctx, strings.NewReader(test.line+"\n"), &output,
			func(context.Context, workbenchCommandRequest, string) (json.RawMessage, error) {
				called = true
				return nil, nil
			}))
		require.False(t, called, test.name)
		var response workbenchCommandResponse
		require.NoError(t, json.Unmarshal(output.Bytes(), &response))
		require.NotNil(t, response.Error, test.name)
		require.Equal(t, "invalid_request", response.Error.Code, test.name)
	}
	require.Equal(t, "project_binding_mismatch", workbenchCommandError(fmt.Errorf("prepare: %w", storage.ErrProjectBindingMismatch)).Code)
	require.Equal(t, "not_found", workbenchCommandError(storage.ErrProjectBindingNotFound).Code)
	require.Equal(t, "invalid_argument", workbenchCommandError(storage.ErrInvalidProjectBinding).Code)
}

func TestWorkbenchProjectBindingHistoryProtocol(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		project, slug string
		called        bool
	}{
		{"", "", true},
		{"alpha", "", true},
		{"", "task", false},
	} {
		request, err := json.Marshal(workbenchCommandRequest{ID: "history", Operation: "project-binding-history", Project: test.project, Slug: test.slug, Credential: "credential", Body: json.RawMessage(`{"environment_id":"env","native_project_id":"native","cursor":"51"}`)})
		require.NoError(t, err)
		var output bytes.Buffer
		called := false
		err = runWorkbenchCommandStdio(ctx, strings.NewReader(string(request)+"\n"), &output, func(_ context.Context, req workbenchCommandRequest, procedure string) (json.RawMessage, error) {
			called = true
			require.Equal(t, []string{specgraphv1connect.SpecServiceGetSpecProcedure}, workbenchKnowledgeProcedures(req.Operation, req.Body))
			require.Equal(t, specgraphv1connect.SpecServiceGetSpecProcedure, procedure)
			return json.RawMessage(`{"items":[],"hasMore":false,"nextCursor":""}`), nil
		})
		require.NoError(t, err)
		require.Equal(t, test.called, called, "project=%q slug=%q", test.project, test.slug)
	}
}
