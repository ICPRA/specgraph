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

	"github.com/specgraph/specgraph/internal/auth"
)

func TestReportBranchCommandPermissions(t *testing.T) {
	for operation, action := range map[string]string{
		"report-flow-arm": "workbench.write", "report-flow-cancel": "workbench.write",
		"report-join-arm":        "workbench.write",
		"report-judgment-record": "workbench.write",
		"pm-report-flow-arm":     "planning.write", "pm-report-flow-cancel": "planning.write",
		"pm-report-join-arm":        "planning.write",
		"pm-report-judgment-record": "planning.write",
		"report-flow-read":          "execution.read", "report-join-read": "execution.read",
		"report-judgment-history": "execution.read",
	} {
		t.Run(operation, func(t *testing.T) {
			var output bytes.Buffer
			called := false
			input := fmt.Sprintf(`{"id":"flow","operation":%q,"project":"fixture","credential":"fixture","body":{}}`, operation)
			err := runWorkbenchCommandStdio(context.Background(), strings.NewReader(input+"\n"), &output,
				func(_ context.Context, req workbenchCommandRequest, procedure string) (json.RawMessage, error) {
					called = true
					if req.Slug != "" {
						t.Fatal("report flow operations are not node-slug writes")
					}
					got, ok := auth.ActionForProcedure(procedure)
					if !ok || got != action {
						t.Fatalf("want %s permission, got %s", action, got)
					}
					return json.RawMessage(`{}`), nil
				})
			if err != nil || !called {
				t.Fatalf("operation not routed: called=%v err=%v output=%s", called, err, output.Bytes())
			}
		})
	}
}
