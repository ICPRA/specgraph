// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/specgraph/specgraph/internal/auth"
)

func TestWorkbenchMailEnrollmentProtocol(t *testing.T) {
	for _, operation := range []string{"mail-grant", "mail-bind"} {
		var output bytes.Buffer
		called := false
		err := runWorkbenchCommandStdio(context.Background(), strings.NewReader(`{"id":"check","operation":"`+operation+`","project":"project","credential":"test-only","body":{}}`+"\n"), &output,
			func(_ context.Context, request workbenchCommandRequest, procedure string) (json.RawMessage, error) {
				called = true
				if procedure != auth.WorkbenchMailManageProcedure || request.Operation != operation {
					t.Fatal("enrollment must use mail.manage")
				}
				return nil, auth.ErrUnauthenticated
			})
		if err != nil || !called {
			t.Fatalf("command not routed: %v", err)
		}
		var result workbenchCommandResponse
		if err := json.Unmarshal(output.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if result.Error == nil || result.Error.Code != "unauthorized" {
			t.Fatal("authentication failure must not become success")
		}
	}
}

func TestWorkbenchMailDirectoryProtocol(t *testing.T) {
	var output bytes.Buffer
	called := false
	err := runWorkbenchCommandStdio(context.Background(), strings.NewReader(`{"id":"directory","operation":"mail-directory","credential":"test-only","body":{"scope":{"environment_id":"environment","thread_id":"thread","provider_session_id":"session","provider_instance_id":"codex"},"limit":1,"assignment_role":"knowledge"}}`+"\n"), &output,
		func(_ context.Context, request workbenchCommandRequest, procedure string) (json.RawMessage, error) {
			called = true
			if procedure != auth.WorkbenchMailReadProcedure || request.Project != "" || request.Slug != "" {
				t.Fatal("directory must use mail.read and allow host project resolution")
			}
			return json.RawMessage(`{"contacts":[{"run_id":"run","task_slug":"node","assignment_role":null}],"next_cursor":"run"}`), nil
		})
	if err != nil || !called {
		t.Fatalf("directory not routed: %v", err)
	}
	var result workbenchCommandResponse
	if err := json.Unmarshal(output.Bytes(), &result); err != nil || result.Error != nil || !strings.Contains(string(result.Data), `"assignment_role":null`) {
		t.Fatalf("directory response lost missing-role provenance: %s %v", output.Bytes(), err)
	}
}

func TestWorkbenchMailHandoffProtocol(t *testing.T) {
	var output bytes.Buffer
	called := false
	err := runWorkbenchCommandStdio(context.Background(), strings.NewReader(`{"id":"handoff","operation":"mail-handoff","credential":"test-only","body":{"scope":{"environment_id":"env","thread_id":"thread","provider_session_id":"session","provider_instance_id":"codex"},"mail_thread_id":"mail-thread","recipient_run_id":"successor","body":"reason","idempotency_key":"stable-key"}}`+"\n"), &output,
		func(_ context.Context, request workbenchCommandRequest, procedure string) (json.RawMessage, error) {
			called = true
			if procedure != auth.WorkbenchMailWriteProcedure || request.Project != "" || request.Slug != "" {
				t.Fatal("handoff must use mail.write and existing scope resolution")
			}
			return nil, auth.ErrUnauthenticated
		})
	if err != nil || !called {
		t.Fatalf("handoff not routed: %v", err)
	}
	var result workbenchCommandResponse
	if err := json.Unmarshal(output.Bytes(), &result); err != nil || result.Error == nil || result.Error.Code != "unauthorized" {
		t.Fatalf("handoff must preserve authentication failure: %s %v", output.Bytes(), err)
	}
}

func TestWorkbenchMailOwnedProtocol(t *testing.T) {
	var output bytes.Buffer
	called := false
	err := runWorkbenchCommandStdio(context.Background(), strings.NewReader(`{"id":"owned","operation":"mail-owned","credential":"test-only","body":{"scope":{"environment_id":"env","thread_id":"thread","provider_session_id":"session","provider_instance_id":"codex"},"limit":50}}`+"\n"), &output,
		func(_ context.Context, request workbenchCommandRequest, procedure string) (json.RawMessage, error) {
			called = true
			if procedure != auth.WorkbenchMailReadProcedure || request.Project != "" || request.Slug != "" {
				t.Fatal("owned discovery must use mail.read and existing scope resolution")
			}
			return json.RawMessage(`{"threads":[],"next_cursor":""}`), nil
		})
	if err != nil || !called {
		t.Fatalf("owned discovery not routed: %v", err)
	}
	var result workbenchCommandResponse
	if err := json.Unmarshal(output.Bytes(), &result); err != nil || result.Error != nil {
		t.Fatalf("owned discovery response: %s %v", output.Bytes(), err)
	}
}

func TestWorkbenchMailTakeoverProtocols(t *testing.T) {
	for operation, action := range map[string]string{"mail-takeover": "mail.write", "mail-retired": "mail.read", "mail-owner-history": "mail.read", "takeover-mail": "workbench.manage"} {
		t.Run(operation, func(t *testing.T) {
			var output bytes.Buffer
			called := false
			err := runWorkbenchCommandStdio(context.Background(), strings.NewReader(`{"id":"check","operation":"`+operation+`","project":"project","credential":"test-only","body":{}}`+"\n"), &output,
				func(_ context.Context, _ workbenchCommandRequest, procedure string) (json.RawMessage, error) {
					called = true
					got, ok := auth.ActionForProcedure(procedure)
					if !ok || got != action {
						t.Fatalf("action=%s want=%s", got, action)
					}
					return nil, auth.ErrUnauthenticated
				})
			if err != nil || !called {
				t.Fatalf("not routed: %v", err)
			}
		})
	}
	for _, resource := range []string{"mail-directory", "mail-owner-history"} {
		var output bytes.Buffer
		thread := ""
		if resource == "mail-owner-history" {
			thread = `,"threadId":"thread"`
		}
		called := false
		err := runWorkbenchStdio(context.Background(), strings.NewReader(`{"id":"read","resource":"`+resource+`","project":"project","credential":"test-only"`+thread+`}`+"\n"), &output,
			func(_ context.Context, req workbenchReadRequest) (map[string]any, error) {
				called = true
				if req.Limit != 50 {
					t.Fatalf("default limit=%d", req.Limit)
				}
				return nil, auth.ErrUnauthenticated
			})
		if err != nil || !called {
			t.Fatalf("read not routed: %v", err)
		}
	}
}

func TestWorkbenchMailDescendantScopeProtocol(t *testing.T) {
	for _, tc := range []struct {
		fields string
		valid  bool
	}{
		{`"resource":"mail-threads","taskSlug":"root","includeDescendants":true`, true},
		{`"resource":"mail-threads","includeDescendants":false`, true},
		{`"resource":"mail-threads","taskSlug":"root"`, true},
		{`"resource":"mail-threads","includeDescendants":true`, false},
		{`"resource":"mail-threads","taskSlug":"root","includeDescendants":"true"`, false},
		{`"resource":"mail-threads","taskSlug":"root","includeDescendants":null`, false},
		{`"resource":"mail-thread","threadId":"thread","includeDescendants":false`, false},
		{`"resource":"current-view","includeDescendants":false`, false},
	} {
		var output bytes.Buffer
		called := false
		err := runWorkbenchStdio(context.Background(), strings.NewReader(`{"id":"scope","project":"project",`+tc.fields+`}`+"\n"), &output,
			func(_ context.Context, req workbenchReadRequest) (map[string]any, error) {
				called = true
				return map[string]any{"includeDescendants": string(req.IncludeDescendants)}, nil
			})
		if err != nil || called != tc.valid {
			t.Fatalf("fields=%s called=%v err=%v", tc.fields, called, err)
		}
	}
}
