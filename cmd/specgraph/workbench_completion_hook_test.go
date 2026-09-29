// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/server"
	"github.com/specgraph/specgraph/internal/storage"
)

func TestWorkbenchCompletionHookProtocol(t *testing.T) {
	ctx := context.Background()
	engine, err := auth.NewCedarEngine(ctx, []auth.PolicySource{auth.NewEmbeddedPolicySource()}, auth.ActionNames())
	if err != nil {
		t.Fatal(err)
	}
	authorizer := auth.NewCedarAuthorizer(engine)
	for operation, action := range map[string]string{
		"arm-completion-hook":            "workbench.manage",
		"cancel-completion-hook":         "workbench.manage",
		"retry-completion-hook":          "workbench.manage",
		"host-list-hooks":                "execution.read",
		"host-read-completion-hook":      "execution.read",
		"host-authorize-completion-hook": "program-execution.write",
		"host-result-completion-hook":    "program-execution.write",
	} {
		t.Run(operation, func(t *testing.T) {
			for _, project := range []string{"fixture", ""} {
				var output bytes.Buffer
				called := false
				input := fmt.Sprintf(`{"id":"hook","operation":%q,"project":%q,"credential":"fixture","body":{}}`, operation, project)
				err := runWorkbenchCommandStdio(ctx, strings.NewReader(input+"\n"), &output, func(ctx context.Context, req workbenchCommandRequest, procedure string) (json.RawMessage, error) {
					called = true
					if actual, ok := auth.ActionForProcedure(procedure); !ok || actual != action || req.Slug != "" {
						t.Fatalf("wrong completion hook route: %s", actual)
					}
					for _, role := range []auth.Role{auth.RoleReader, auth.RoleWriter, auth.RoleAdmin, "unknown"} {
						decision, err := authorizer.Authorize(ctx, &auth.Identity{UserID: "fixture", Subject: "apikey:fixture", EffectiveRole: role}, procedure, nil)
						want := role != "unknown" && (action != "workbench.manage" || role == auth.RoleAdmin)
						if err != nil || decision.Allowed != want {
							t.Fatalf("wrong role gate %s: allowed=%v err=%v", role, decision.Allowed, err)
						}
					}
					return nil, storage.ErrProgramRunForbidden
				})
				if err != nil || called != (project != "" || action != "workbench.manage") {
					t.Fatalf("wrong project gate: called=%v err=%v", called, err)
				}
				var response workbenchCommandResponse
				if err := json.Unmarshal(output.Bytes(), &response); err != nil || response.Error == nil {
					t.Fatalf("missing failure: %s", output.Bytes())
				}
				want := "invalid_request"
				if called {
					want = "forbidden"
				}
				if response.Error.Code != want {
					t.Fatalf("want %s, got %s", want, response.Error.Code)
				}
			}
		})
	}
	for _, input := range []string{
		`{"id":"pm","operation":"pm-arm-completion-hook","project":"fixture","body":{}}`,
		`{"id":"unknown","operation":"host-start-completion-hook","project":"fixture","body":{}}`,
		`{"id":"large","operation":"arm-completion-hook","project":"fixture","body":{"x":"` + strings.Repeat("x", workbenchRequestLimit) + `"}}`,
	} {
		var output bytes.Buffer
		err := runWorkbenchCommandStdio(ctx, strings.NewReader(input+"\n"), &output, func(context.Context, workbenchCommandRequest, string) (json.RawMessage, error) {
			t.Fatal("unrecognized or oversize completion command must not execute")
			return nil, nil
		})
		if err != nil || !strings.Contains(output.String(), `"code":"invalid_request"`) {
			t.Fatalf("missing protocol rejection: err=%v output=%s", err, output.Bytes())
		}
	}
}

func TestWorkbenchCompletionHookBoundary(t *testing.T) {
	human := &auth.Identity{UserID: "human", UserKind: storage.KindHuman, Source: "session"}
	host := &auth.Identity{UserID: "host", UserKind: storage.KindServiceAccount, Source: "apikey"}
	arm := `{"sourceTaskSlug":"source","sourceSpecId":"spec","targetRunId":"run","targetPackageId":"package","idempotencyKey":"intent","confirmTrigger":true,"fact":{"kind":"completion","id":"fact"},"hostCredential":"private"}`
	for _, tc := range []struct {
		name, operation, body string
		identity, consumer    *auth.Identity
		want                  error
	}{
		{"arm schema", "arm-completion-hook", arm, human, host, storage.ErrProjectNotFound},
		{"cancel schema", "cancel-completion-hook", `{"hookId":"hook","reason":"cancel"}`, human, nil, storage.ErrProjectNotFound},
		{"retry schema", "retry-completion-hook", `{"hookId":"hook"}`, human, nil, storage.ErrProjectNotFound},
		{"agent cannot grant", "arm-completion-hook", arm, host, host, storage.ErrProgramRunForbidden},
		{"human cannot consume", "arm-completion-hook", arm, human, human, storage.ErrProgramRunForbidden},
		{"missing consumer", "arm-completion-hook", arm, human, nil, storage.ErrProgramRunForbidden},
		{"missing credential", "arm-completion-hook", `{}`, human, host, storage.ErrInvalidCompletionHook},
		{"actor injection", "arm-completion-hook", strings.TrimSuffix(arm, "}") + `,"configuredByUserId":"spoof"}`, human, host, storage.ErrInvalidCompletionHook},
		{"fact injection", "arm-completion-hook", strings.Replace(arm, `"id":"fact"`, `"id":"fact","sourceRole":"summary"`, 1), human, host, storage.ErrInvalidCompletionHook},
		{"trailing JSON", "arm-completion-hook", arm + ` {}`, human, host, storage.ErrInvalidCompletionHook},
		{"retry rejects target", "retry-completion-hook", `{"hookId":"hook","targetRunId":"other"}`, human, nil, storage.ErrInvalidCompletionHook},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := server.ExecuteHumanCompletionHook(auth.WithIdentity(context.Background(), tc.identity), nil, "_server", tc.operation, strings.NewReader(tc.body), tc.consumer)
			if !errors.Is(err, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
		})
	}
	scope := `"environmentId":"env","nativeProjectId":"native"`
	for _, tc := range []struct {
		name, operation, body string
		identity              *auth.Identity
		want                  error
	}{
		{"list schema", "host-list-hooks", "{" + scope + `,"limit":50,"cursor":""}`, host, storage.ErrProjectNotFound},
		{"read schema", "host-read-completion-hook", "{" + scope + `,"hookId":"hook"}`, host, storage.ErrProjectNotFound},
		{"authorize schema", "host-authorize-completion-hook", "{" + scope + `,"hookId":"hook"}`, host, storage.ErrProjectNotFound},
		{"blocked schema", "host-result-completion-hook", "{" + scope + `,"hookId":"hook","status":"blocked","detail":"inputs changed"}`, host, storage.ErrProjectNotFound},
		{"unconfirmed schema", "host-result-completion-hook", "{" + scope + `,"hookId":"hook","status":"unconfirmed"}`, host, storage.ErrProjectNotFound},
		{"human cannot dispatch", "host-authorize-completion-hook", "{" + scope + `,"hookId":"hook"}`, human, storage.ErrProgramRunForbidden},
		{"service session rejected", "host-list-hooks", "{" + scope + "}", &auth.Identity{UserID: "host", UserKind: storage.KindServiceAccount, Source: "session"}, storage.ErrProgramRunForbidden},
		{"missing scope", "host-read-completion-hook", `{"hookId":"hook"}`, host, storage.ErrInvalidCompletionHook},
		{"no arbitrary target", "host-authorize-completion-hook", "{" + scope + `,"hookId":"hook","targetRunId":"other"}`, host, storage.ErrInvalidCompletionHook},
		{"no accepted result", "host-result-completion-hook", "{" + scope + `,"hookId":"hook","status":"accepted"}`, host, storage.ErrInvalidCompletionHook},
		{"no running result", "host-result-completion-hook", "{" + scope + `,"hookId":"hook","status":"running"}`, host, storage.ErrInvalidCompletionHook},
		{"no phase even null", "host-result-completion-hook", "{" + scope + `,"hookId":"hook","status":"unconfirmed","phase":null}`, host, storage.ErrInvalidCompletionHook},
		{"no command injection", "host-list-hooks", "{" + scope + `,"command":{}}`, host, storage.ErrInvalidCompletionHook},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := server.ExecuteHostCompletionHook(auth.WithIdentity(context.Background(), tc.identity), nil, "_server", tc.operation, strings.NewReader(tc.body))
			if !errors.Is(err, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
		})
	}
}

func TestWorkbenchCompletionHookReadAndErrors(t *testing.T) {
	for _, slug := range []string{"hook", ""} {
		var output bytes.Buffer
		called := false
		input := fmt.Sprintf(`{"id":"hook","resource":"completion-hook","project":"fixture","slug":%q,"credential":"fixture"}`, slug)
		err := runWorkbenchStdio(context.Background(), strings.NewReader(input+"\n"), &output, func(_ context.Context, req workbenchReadRequest) (map[string]any, error) {
			called = true
			return map[string]any{"hook": storage.CompletionProgramHook{ID: req.Slug}}, nil
		})
		if err != nil || called != (slug != "") {
			t.Fatalf("completion hook read: called=%v err=%v", called, err)
		}
		if called && !strings.Contains(output.String(), `"hook":{"id":"hook"`) {
			t.Fatalf("hook response lost: %s", output.Bytes())
		}
	}
	for err, code := range map[error]string{
		storage.ErrInvalidCompletionHook:  "invalid_argument",
		storage.ErrCompletionHookConflict: "conflict",
		storage.ErrCompletionHookNotFound: "not_found",
		storage.ErrProgramHookRequired:    "failed_precondition",
	} {
		if got := workbenchCommandError(fmt.Errorf("wrapped: %w", err)); got.Code != code {
			t.Errorf("%v: want %s, got %s", err, code, got.Code)
		}
	}
}
