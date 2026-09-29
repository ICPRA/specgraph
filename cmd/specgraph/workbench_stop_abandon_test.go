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
	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestWorkbenchStopAbandonProtocol(t *testing.T) {
	for _, test := range []struct{ operation, procedure string }{
		{"confirm-run-stopped", auth.WorkbenchDispatchProcedure},
		{"abandon-node", specgraphv1connect.LifecycleServiceTransitionAbandonProcedure},
	} {
		var output bytes.Buffer
		called := false
		err := runWorkbenchCommandStdio(context.Background(), strings.NewReader(`{"id":"operation","operation":"`+test.operation+`","project":"alpha","slug":"target","body":{}}`+"\n"), &output,
			func(_ context.Context, _ workbenchCommandRequest, procedure string) (json.RawMessage, error) {
				called = true
				require.Equal(t, test.procedure, procedure)
				return nil, storage.ErrAbandonExecutionPending
			})
		require.NoError(t, err)
		require.True(t, called)
		var result workbenchCommandResponse
		require.NoError(t, json.Unmarshal(output.Bytes(), &result))
		require.Equal(t, "failed_precondition", result.Error.Code)
	}
	status, err := json.Marshal(storage.RunDispatchStatus{})
	require.NoError(t, err)
	require.Contains(t, string(status), `"stopConfirmation":null`)
}
