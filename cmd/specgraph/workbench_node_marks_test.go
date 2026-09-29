// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/stretchr/testify/require"
)

func TestWorkbenchNodeMarkProtocols(t *testing.T) {
	ctx := context.Background()
	engine, err := auth.NewCedarEngine(ctx, []auth.PolicySource{auth.NewEmbeddedPolicySource()}, auth.ActionNames())
	require.NoError(t, err)
	for _, role := range []auth.Role{auth.RoleReader, auth.RoleAdmin} {
		var output bytes.Buffer
		called := false
		err := runWorkbenchCommandStdio(ctx, strings.NewReader(`{"id":"mark","operation":"set-node-mark","project":"alpha","slug":"task","body":{"kind":"risk","value":"watch","reason":"Operator review"}}`+"\n"), &output,
			func(ctx context.Context, _ workbenchCommandRequest, procedure string) (json.RawMessage, error) {
				called = true
				require.Equal(t, auth.WorkbenchSetNodeMarkProcedure, procedure)
				action, ok := auth.ActionForProcedure(procedure)
				require.True(t, ok)
				require.Equal(t, "workbench.manage", action)
				decision, err := auth.NewCedarAuthorizer(engine).Authorize(ctx, &auth.Identity{UserID: "operator", EffectiveRole: role}, procedure, nil)
				require.NoError(t, err)
				require.Equal(t, role == auth.RoleAdmin, decision.Allowed, "reader must not gain mark-write authority")
				return nil, nil
			})
		require.NoError(t, err)
		require.True(t, called)
	}
	var planning bytes.Buffer
	require.NoError(t, runWorkbenchCommandStdio(ctx, strings.NewReader(`{"id":"pm-mark","operation":"pm-set-node-mark","body":{"scope":{"environment_id":"local","thread_id":"manager","provider_session_id":"session","provider_instance_id":"codex"},"taskSlug":"task","request":{"kind":"risk","value":"watch","reason":"Clear evidence","expectedMarkId":""}}}`+"\n"), &planning,
		func(ctx context.Context, _ workbenchCommandRequest, procedure string) (json.RawMessage, error) {
			require.Equal(t, auth.WorkbenchAgentPlanningProcedure, procedure)
			action, ok := auth.ActionForProcedure(procedure)
			require.True(t, ok)
			require.Equal(t, "planning.write", action)
			decision, err := auth.NewCedarAuthorizer(engine).Authorize(ctx, &auth.Identity{UserID: "host", EffectiveRole: auth.RoleReader}, procedure, nil)
			require.NoError(t, err)
			require.True(t, decision.Allowed)
			return json.RawMessage(`{"id":"1"}`), nil
		}))
	var response workbenchCommandResponse
	require.NoError(t, json.Unmarshal(planning.Bytes(), &response))
	require.Nil(t, response.Error)
	require.JSONEq(t, `{"id":"1"}`, string(response.Data))
	for _, cursor := range []string{"", "1", "9223372036854775807", "0", "-1", "+1", "01", "1.2", "9223372036854775808"} {
		request, err := json.Marshal(workbenchReadRequest{ID: "history", Resource: "node-mark-history", Project: "alpha", Slug: "task", Cursor: cursor})
		require.NoError(t, err)
		var output bytes.Buffer
		called := false
		err = runWorkbenchStdio(ctx, strings.NewReader(string(request)+"\n"), &output, func(context.Context, workbenchReadRequest) (map[string]any, error) {
			called = true
			return map[string]any{"items": []any{}, "hasMore": false, "nextCursor": ""}, nil
		})
		require.NoError(t, err)
		require.Equal(t, cursor == "" || cursor == "1" || cursor == "9223372036854775807", called, cursor)
	}
	var output bytes.Buffer
	require.NoError(t, runWorkbenchStdio(ctx, strings.NewReader(`{"id":"view","resource":"current-view","project":"alpha"}`+"\n"), &output,
		func(context.Context, workbenchReadRequest) (map[string]any, error) {
			return map[string]any{"capabilities": map[string]bool{}, "nodeMarks": []postgres.NodeMark{}}, nil
		}))
	var view workbenchReadResponse
	require.NoError(t, json.Unmarshal(output.Bytes(), &view))
	require.Equal(t, true, view.Data["capabilities"].(map[string]any)["nodeMarks"])
}
