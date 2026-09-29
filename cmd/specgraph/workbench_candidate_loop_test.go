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

func TestCandidateLoopCommandPermissions(t *testing.T) {
	for operation, action := range map[string]string{
		"candidate-loop-arm": "workbench.write", "candidate-loop-stop": "workbench.write", "candidate-loop-abandon": "workbench.write",
		"pm-candidate-loop-arm": "planning.write", "pm-candidate-loop-stop": "planning.write",
		"candidate-loop-read": "execution.read", "candidate-next-own": "delivery-submission.write",
		"candidate-satisfaction-record": "workbench.write", "candidate-intervention-resolve": "workbench.write",
		"pm-candidate-satisfaction-record": "planning.write", "candidate-satisfaction-own": "candidate-judgment.write",
		"candidate-satisfaction-history": "execution.read",
	} {
		t.Run(operation, func(t *testing.T) {
			var output bytes.Buffer
			called := false
			input := fmt.Sprintf(`{"id":"candidate","operation":%q,"project":"fixture","credential":"fixture","body":{}}`, operation)
			err := runWorkbenchCommandStdio(context.Background(), strings.NewReader(input+"\n"), &output,
				func(_ context.Context, req workbenchCommandRequest, procedure string) (json.RawMessage, error) {
					called = true
					if req.Slug != "" {
						t.Fatal("candidate operations have no node-slug selector")
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
