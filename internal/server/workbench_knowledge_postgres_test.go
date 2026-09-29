// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package server

import (
	"context"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/specgraph/specgraph/internal/storage/postgres/postgrestest"
	"github.com/stretchr/testify/require"
)

func TestWorkbenchKnowledgeRecordExpectedProject(t *testing.T) {
	ctx := context.Background()
	url, err := postgrestest.ConnString(ctx)
	require.NoError(t, err)
	s, err := postgres.New(ctx, url, postgres.WithProject("knowledge-identity-a"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(ctx)) })
	_, err = s.EnsureProject(ctx, "knowledge-identity-b")
	require.NoError(t, err)
	_, err = s.Pool().Exec(ctx, `INSERT INTO specs(project_slug,id,slug,intent) VALUES
		('knowledge-identity-a','id-a','same','Original A'),
		('knowledge-identity-b','id-b','same','Original B')`)
	require.NoError(t, err)
	_, err = s.Pool().Exec(ctx, `INSERT INTO context_packages(id,project_slug,task_spec_slug,body) VALUES
		('knowledge-identity-package','knowledge-identity-a','same',
		'{"dispatch_target":{"environmentId":"host-env","projectId":"host-project"}}')`)
	require.NoError(t, err)
	read := func(project, expected string) (*postgres.WorkbenchKnowledgeRecord, error) {
		body := `{"environment_id":"host-env","native_project_id":"host-project","kind":"spec","reference":"same"` + expected + `}`
		result, readErr := ExecuteLocalWorkbenchKnowledge(ctx, s, project, "knowledge-record", strings.NewReader(body))
		if readErr != nil {
			return nil, readErr
		}
		return result.(*postgres.WorkbenchKnowledgeRecord), nil
	}
	first, err := read("", "")
	require.NoError(t, err)
	require.Equal(t, "id-a", first.ID)
	require.Equal(t, "knowledge-identity-a", first.SpecgraphProject)
	_, err = s.Pool().Exec(ctx, `UPDATE context_packages SET project_slug='knowledge-identity-b'
		WHERE id='knowledge-identity-package'`)
	require.NoError(t, err)
	_, err = read("", `,"expectedProject":"knowledge-identity-a"`)
	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	second, err := read("", `,"expectedProject":"knowledge-identity-b"`)
	require.NoError(t, err)
	require.Equal(t, "id-b", second.ID)
	current, err := read("", "")
	require.NoError(t, err)
	require.Equal(t, "id-b", current.ID)
	for _, invalid := range []string{`,"expectedProject":""`, `,"expectedProject":null`, `,"expectedProject":1`} {
		_, err = read("", invalid)
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	}
}
