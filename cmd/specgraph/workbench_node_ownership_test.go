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

func TestNodeOwnershipCommandPermissions(t *testing.T) {
	for operation, action := range map[string]string{
		"node-owner-read": "execution.read", "node-owner-history": "execution.read",
		"node-owner-take-begin": "workbench.manage", "node-owner-take-commit": "workbench.manage",
		"node-owner-take-cancel": "workbench.manage", "node-owner-return": "workbench.manage",
		"confirm-program-run-stopped": "workbench.manage", "node-handoff-summary-own": "node-handoff.write",
	} {
		t.Run(operation, func(t *testing.T) {
			var output bytes.Buffer
			called := false
			input := fmt.Sprintf(`{"id":"node-owner","operation":%q,"project":"fixture","credential":"fixture","body":{}}`, operation)
			err := runWorkbenchCommandStdio(context.Background(), strings.NewReader(input+"\n"), &output,
				func(_ context.Context, req workbenchCommandRequest, procedure string) (json.RawMessage, error) {
					called = true
					if req.Slug != "" {
						t.Fatal("node ownership operations use body selectors, not slug")
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
