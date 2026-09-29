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

func TestConversationRunsStdioPostgres(t *testing.T) {
	ctx := context.Background()
	url, err := postgrestest.ConnString(ctx)
	require.NoError(t, err)
	s, err := postgres.New(ctx, url, postgres.WithProject("conversation-runs-stdio"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(ctx)) })
	_, err = s.EnsureProject(ctx, "conversation-runs-foreign")
	require.NoError(t, err)
	authStore, err := postgres.NewAuth(ctx, s.Pool())
	require.NoError(t, err)
	user, err := authStore.CreateHuman(ctx, &storage.User{Kind: storage.KindHuman, DisplayName: "Run reader", Role: "reader"}, nil)
	require.NoError(t, err)
	secret, hash, err := auth.GenerateAPIKeySecret()
	require.NoError(t, err)
	key, err := authStore.CreateAPIKey(ctx, &storage.APIKey{UserID: user.ID, PHCHash: hash})
	require.NoError(t, err)
	credential := auth.FormatAPIKeyToken(key.Prefix, secret)
	_, err = s.Pool().Exec(ctx, `INSERT INTO context_packages(id,project_slug,task_spec_slug,body)
	 SELECT 'package-'||lpad(i::text,3,'0'),'conversation-runs-stdio',
	 CASE WHEN i%2=0 THEN 'task-a' ELSE 'task-b' END,
	 jsonb_build_object('dispatch_target',jsonb_build_object('environmentId','env','projectId','native','messageId','message-'||i))
	 FROM generate_series(1,51) i;
	 INSERT INTO run_bindings(id,project_slug,task_spec_slug,package_id,environment_id,thread_ref,state,created_at)
	 SELECT 'run-'||lpad(i::text,3,'0'),'conversation-runs-stdio',
	 CASE WHEN i%2=0 THEN 'task-a' ELSE 'task-b' END,'package-'||lpad(i::text,3,'0'),
	 'env','thread-a',CASE WHEN i%2=0 THEN 'completed' ELSE 'cancelled' END,'2026-01-01 00:00:00+00'
	 FROM generate_series(1,51) i;
	 UPDATE context_packages SET body=jsonb_set(body,'{dispatch_target}', '{"environmentId":"env","projectId":"native"}'::jsonb)
	 WHERE project_slug='conversation-runs-stdio' AND id='package-050'`)
	require.NoError(t, err)
	call := func(project, body string, dirs []string) workbenchCommandResponse {
		t.Helper()
		request, marshalErr := json.Marshal(workbenchCommandRequest{ID: "runs", Operation: "conversation-runs", Project: project, Credential: credential, Body: json.RawMessage(body)})
		require.NoError(t, marshalErr)
		var output bytes.Buffer
		require.NoError(t, runWorkbenchCommandStdio(ctx, strings.NewReader(string(request)+"\n"), &output,
			func(callCtx context.Context, req workbenchCommandRequest, procedure string) (json.RawMessage, error) {
				identity, resolveErr := resolveWorkbenchOperator(callCtx, s, req.Credential, dirs, procedure)
				if resolveErr != nil {
					return nil, resolveErr
				}
				result, readErr := server.ExecuteLocalWorkbenchKnowledge(auth.WithIdentity(callCtx, identity), s, req.Project, req.Operation, bytes.NewReader(req.Body))
				if readErr != nil {
					return nil, readErr
				}
				return json.Marshal(result)
			}))
		var response workbenchCommandResponse
		require.NoError(t, json.Unmarshal(output.Bytes(), &response))
		return response
	}
	request := `{"environment_id":"env","native_project_id":"native","threadId":"thread-a"}`
	first := call("", request, nil)
	require.Nil(t, first.Error)
	var firstPage postgres.WorkbenchConversationRuns
	require.NoError(t, json.Unmarshal(first.Data, &firstPage))
	require.Equal(t, "thread-a", firstPage.ThreadID)
	require.Len(t, firstPage.Runs, 50)
	require.True(t, firstPage.HasMore)
	require.Equal(t, "run-051", firstPage.Runs[0].RunID)
	require.Equal(t, "task-b", firstPage.Runs[0].TaskSlug)
	require.Equal(t, "cancelled", firstPage.Runs[0].State)
	require.Equal(t, "run-050", firstPage.Runs[1].RunID)
	require.Nil(t, firstPage.Runs[1].DispatchMessageID)
	require.Equal(t, "completed", firstPage.Runs[1].State)
	require.Equal(t, "run-002", *firstPage.NextCursor)
	require.NotContains(t, string(first.Data), "package-")
	require.NotContains(t, string(first.Data), "workspace")
	_, err = s.Pool().Exec(ctx, `INSERT INTO context_packages(id,project_slug,task_spec_slug,body) VALUES
	 ('package-wrong-native','conversation-runs-stdio','task-a','{"dispatch_target":{"projectId":"other"}}'),
	 ('package-wrong-env','conversation-runs-stdio','task-a','{"dispatch_target":{"projectId":"native"}}'),
	 ('package-wrong-thread','conversation-runs-stdio','task-a','{"dispatch_target":{"projectId":"native"}}'),
	 ('package-legacy','conversation-runs-stdio','task-a','{}'),
	 ('package-program','conversation-runs-stdio','task-a','{"dispatch_target":{"projectId":"native"}}'),
	 ('package-foreign','conversation-runs-foreign','task-a','{"dispatch_target":{"projectId":"native"}}');
	 INSERT INTO run_bindings(id,project_slug,task_spec_slug,package_id,environment_id,thread_ref,executor_kind) VALUES
	 ('wrong-native','conversation-runs-stdio','task-a','package-wrong-native','env','thread-a','agent'),
	 ('wrong-env','conversation-runs-stdio','task-a','package-wrong-env','other','thread-a','agent'),
	 ('wrong-thread','conversation-runs-stdio','task-a','package-wrong-thread','env','thread-b','agent'),
	 ('legacy','conversation-runs-stdio','task-a','package-legacy','env','thread-a','agent'),
	 ('program','conversation-runs-stdio','task-a','package-program','env','','program'),
	 ('missing-package','conversation-runs-stdio','task-a','', 'env','thread-a','agent'),
	 ('foreign','conversation-runs-foreign','task-a','package-foreign','env','thread-a','agent')`)
	require.NoError(t, err)
	rescoped := call("conversation-runs-stdio", request, nil)
	require.Nil(t, rescoped.Error)
	require.JSONEq(t, string(first.Data), string(rescoped.Data), "unrelated newer runs must not enter the first page")
	second := call("conversation-runs-stdio", `{"environment_id":"env","native_project_id":"native","threadId":"thread-a","cursor":"run-002"}`, nil)
	require.Nil(t, second.Error)
	var secondPage postgres.WorkbenchConversationRuns
	require.NoError(t, json.Unmarshal(second.Data, &secondPage))
	require.Len(t, secondPage.Runs, 1)
	require.Equal(t, "run-001", secondPage.Runs[0].RunID)
	require.False(t, secondPage.HasMore)
	require.Nil(t, secondPage.NextCursor)
	empty := call("conversation-runs-stdio", `{"environment_id":"env","native_project_id":"native","threadId":"unknown"}`, nil)
	require.Nil(t, empty.Error)
	require.JSONEq(t, `{"threadId":"unknown","runs":[],"hasMore":false,"nextCursor":null}`, string(empty.Data))
	for _, cursor := range []string{"wrong-native", "wrong-env", "wrong-thread", "legacy", "program", "missing-package", "foreign", "absent"} {
		body, marshalErr := json.Marshal(map[string]string{"environment_id": "env", "native_project_id": "native", "threadId": "thread-a", "cursor": cursor})
		require.NoError(t, marshalErr)
		require.Equal(t, "invalid_argument", call("conversation-runs-stdio", string(body), nil).Error.Code)
	}
	for _, body := range []string{`{"environment_id":"env","native_project_id":"native"}`, `{"environment_id":"","native_project_id":"native","threadId":"thread-a"}`, `{"environment_id":"env","native_project_id":"","threadId":"thread-a"}`} {
		require.Equal(t, "invalid_argument", call("conversation-runs-stdio", body, nil).Error.Code)
	}
	policyDir, err := os.MkdirTemp(".", ".conversation-runs-policy-")
	require.NoError(t, err)
	policyDir, err = filepath.Abs(policyDir)
	require.NoError(t, err)
	workingDir, err := os.Getwd()
	require.NoError(t, err)
	require.Equal(t, workingDir, filepath.Dir(policyDir))
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(policyDir)) })
	require.NoError(t, os.WriteFile(filepath.Join(policyDir, "deny.cedar"), []byte(`forbid(principal, action == SpecGraph::Action::"execution.read", resource);`), 0600))
	require.Equal(t, "forbidden", call("conversation-runs-stdio", request, []string{policyDir}).Error.Code)
}
