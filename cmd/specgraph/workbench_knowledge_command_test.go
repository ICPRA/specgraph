// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/specgraph/specgraph/gen/specgraph/v1/specgraphv1connect"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/stretchr/testify/require"
)

func TestWorkbenchKnowledgeProtocol(t *testing.T) {
	for _, test := range []struct {
		operation, body string
		procedures      []string
	}{
		{"graph-current", `{"environment_id":"env","native_project_id":"native","offset":0}`, []string{specgraphv1connect.GraphServiceGetFullGraphProcedure}},
		{"node-deliveries", `{"environment_id":"env","native_project_id":"native","taskSlug":"task"}`, []string{specgraphv1connect.SpecServiceGetSpecProcedure}},
		{"node-mark-history", `{"environment_id":"env","native_project_id":"native","taskSlug":"task","cursor":"51"}`, []string{specgraphv1connect.SpecServiceGetSpecProcedure}},
		{"test-results", `{"environment_id":"env","native_project_id":"native","deliveryId":"delivery-1"}`, []string{specgraphv1connect.SpecServiceGetSpecProcedure}},
		{"review-request-source", `{"environment_id":"env","native_project_id":"native","requestId":"1"}`, []string{specgraphv1connect.SpecServiceGetSpecProcedure}},
		{"knowledge-search", `{"environment_id":"env","native_project_id":"native","query":"intent"}`, []string{specgraphv1connect.SpecServiceGetSpecProcedure, specgraphv1connect.DecisionServiceGetDecisionProcedure, specgraphv1connect.SpecServiceListChangesProcedure}},
		{"knowledge-record", `{"environment_id":"env","native_project_id":"native","kind":"spec","reference":"task"}`, []string{specgraphv1connect.SpecServiceGetSpecProcedure}},
		{"knowledge-record", `{"environment_id":"env","native_project_id":"native","kind":"decision","reference":"choice"}`, []string{specgraphv1connect.DecisionServiceGetDecisionProcedure}},
		{"knowledge-record", `{"environment_id":"env","native_project_id":"native","kind":"change","reference":"change-1"}`, []string{specgraphv1connect.SpecServiceListChangesProcedure}},
	} {
		for _, project := range []string{"", "host-configured"} {
			for _, slug := range []string{"", "forbidden-selector"} {
				request, err := json.Marshal(workbenchCommandRequest{ID: "knowledge", Operation: test.operation, Project: project, Slug: slug, Credential: "credential", Body: json.RawMessage(test.body)})
				require.NoError(t, err)
				var output bytes.Buffer
				called := false
				err = runWorkbenchCommandStdio(context.Background(), strings.NewReader(string(request)+"\n"), &output, func(_ context.Context, req workbenchCommandRequest, procedure string) (json.RawMessage, error) {
					called = true
					require.Equal(t, test.procedures, workbenchKnowledgeProcedures(req.Operation, req.Body))
					require.Equal(t, test.procedures[0], procedure)
					return json.RawMessage(`{"source":"original"}`), nil
				})
				require.NoError(t, err)
				require.Equal(t, slug == "", called)
			}
		}
	}
	require.Nil(t, workbenchKnowledgeProcedures("knowledge-record", json.RawMessage(`{"kind":"mail"}`)))
	require.Equal(t, "project_association_missing", workbenchCommandError(postgres.ErrWorkbenchProjectAssociationMissing).Code)
	require.Equal(t, "project_association_ambiguous", workbenchCommandError(postgres.ErrWorkbenchProjectAssociationAmbiguous).Code)
	require.Equal(t, "not_found", workbenchCommandError(postgres.ErrWorkbenchChangeNotFound).Code)
}
