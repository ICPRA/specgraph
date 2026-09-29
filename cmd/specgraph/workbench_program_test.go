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

func TestWorkbenchProgramProtocol(t *testing.T) {
	ctx := context.Background()
	engine, err := auth.NewCedarEngine(ctx, []auth.PolicySource{auth.NewEmbeddedPolicySource()}, auth.ActionNames())
	if err != nil {
		t.Fatal(err)
	}
	authorizer := auth.NewCedarAuthorizer(engine)
	for operation, action := range map[string]string{
		"prepare-program-run":                 "workbench.manage",
		"host-read-program-run":               "execution.read",
		"host-authorize-program-run":          "program-execution.write",
		"host-result-program-run":             "program-execution.write",
		"host-complete-program-run":           "program-execution.write",
		"host-authorize-next-program-attempt": "program-execution.write",
		"host-result-program-attempt":         "program-execution.write",
		"host-stop-program-loop":              "program-execution.write",
		"host-read-program-loop-history":      "execution.read",
		"record-program-loop-decision":        "workbench.manage",
		"stop-program-loop":                   "workbench.manage",
		"pm-record-program-loop-decision":     "planning.write",
		"pm-stop-program-loop":                "planning.write",
		"program-loop-read":                   "execution.read",
		"program-loop-history":                "execution.read",
	} {
		t.Run(operation, func(t *testing.T) {
			for _, project := range []string{"fixture", ""} {
				var output bytes.Buffer
				called := false
				input := fmt.Sprintf(`{"id":"program","operation":%q,"project":%q,"credential":"fixture","body":{}}`, operation, project)
				err := runWorkbenchCommandStdio(ctx, strings.NewReader(input+"\n"), &output,
					func(ctx context.Context, req workbenchCommandRequest, procedure string) (json.RawMessage, error) {
						called = true
						actual, ok := auth.ActionForProcedure(procedure)
						if !ok || actual != action || req.Slug != "" {
							t.Fatalf("incorrect route: %s", actual)
						}
						for _, role := range []auth.Role{auth.RoleReader, auth.RoleWriter, auth.RoleAdmin} {
							decision, err := authorizer.Authorize(ctx, &auth.Identity{UserID: "fixture", Subject: "apikey:fixture", EffectiveRole: role}, procedure, nil)
							want := action != "workbench.manage" || role == auth.RoleAdmin
							if err != nil || decision.Allowed != want {
								t.Fatalf("wrong %s gate: allowed=%v error=%v", role, decision.Allowed, err)
							}
						}
						return nil, storage.ErrProgramRunForbidden
					})
				if err != nil || called != (project != "" || action == "planning.write" || operation == "program-loop-read" || operation == "program-loop-history") {
					t.Fatalf("routing error: called=%v err=%v", called, err)
				}
				var response workbenchCommandResponse
				if err := json.Unmarshal(output.Bytes(), &response); err != nil || response.Error == nil {
					t.Fatalf("missing error: %s", output.Bytes())
				}
				want := "forbidden"
				if !called {
					want = "invalid_request"
				}
				if response.Error.Code != want {
					t.Fatalf("want %s, got %s", want, response.Error.Code)
				}
			}
		})
	}
}

func TestWorkbenchProgramBoundary(t *testing.T) {
	human := &auth.Identity{UserID: "human", UserKind: storage.KindHuman, Source: "session"}
	host := &auth.Identity{UserID: "host", UserKind: storage.KindServiceAccount, Source: "apikey"}
	prepare := `{"taskSlug":"node","idempotencyKey":"intent","command":{"executable":"C:/tool.exe","args":[],"cwd":"C:/project","environmentId":"env","nativeProjectId":"native","timeoutMs":1000,"workPurpose":"investigation"},"hostCredential":"fixture"}`
	for _, tc := range []struct {
		name     string
		identity *auth.Identity
		consumer *auth.Identity
		body     string
		want     error
	}{
		{"human schema", human, host, prepare, storage.ErrProjectNotFound},
		{"service cannot prepare", host, host, prepare, storage.ErrProgramRunForbidden},
		{"human is not host", human, human, prepare, storage.ErrProgramRunForbidden},
		{"missing host", human, nil, prepare, storage.ErrProgramRunForbidden},
		{"unknown human field", human, host, strings.TrimSuffix(prepare, "}") + `,"actor":"spoof"}`, storage.ErrInvalidProgramRun},
		{"unknown command field", human, host, strings.Replace(prepare, `"timeoutMs":1000`, `"timeoutMs":1000,"shell":true`, 1), storage.ErrInvalidProgramRun},
		{"unknown loop field", human, host, strings.Replace(prepare, `"timeoutMs":1000`, `"timeoutMs":1000,"loop":{"kind":"mechanical","retryAll":true}`, 1), storage.ErrInvalidProgramRun},
		{"missing credential", human, host, `{}`, storage.ErrInvalidProgramRun},
		{"trailing body", human, host, prepare + ` {}`, storage.ErrInvalidProgramRun},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := server.ExecuteHumanProgramRun(auth.WithIdentity(context.Background(), tc.identity), nil, "_server", strings.NewReader(tc.body), tc.consumer)
			if !errors.Is(err, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
		})
	}
	scope := `"environmentId":"env","nativeProjectId":"native","runId":"run"`
	for _, tc := range []struct {
		name, operation, body string
		identity              *auth.Identity
		want                  error
	}{
		{"read schema", "host-read-program-run", "{" + scope + "}", host, storage.ErrProjectNotFound},
		{"authorize schema", "host-authorize-program-run", "{" + scope + "}", host, storage.ErrProjectNotFound},
		{"complete schema", "host-complete-program-run", "{" + scope + "}", host, storage.ErrProjectNotFound},
		{"result schema", "host-result-program-run", "{" + scope + `,"observation":{"outcome":"exited","exitCode":0,"observedAt":"2026-09-27T00:00:00Z","finishedAt":"2026-09-27T00:00:00Z"}}`, host, storage.ErrProjectNotFound},
		{"human cannot consume", "host-authorize-program-run", "{" + scope + "}", human, storage.ErrProgramRunForbidden},
		{"session cannot consume", "host-read-program-run", "{" + scope + "}", &auth.Identity{UserID: "host", UserKind: storage.KindServiceAccount, Source: "session"}, storage.ErrProgramRunForbidden},
		{"missing identity", "host-read-program-run", "{" + scope + "}", &auth.Identity{}, storage.ErrProgramRunForbidden},
		{"command injection", "host-authorize-program-run", "{" + scope + `,"command":{}}`, host, storage.ErrInvalidProgramRun},
		{"read rejects observation", "host-read-program-run", "{" + scope + `,"observation":{}}`, host, storage.ErrInvalidProgramRun},
		{"unknown observation", "host-result-program-run", "{" + scope + `,"observation":{"passed":true}}`, host, storage.ErrInvalidProgramRun},
		{"missing scope", "host-read-program-run", `{"runId":"run"}`, host, storage.ErrInvalidProgramRun},
		{"unknown operation", "host-execute-command", "{" + scope + "}", host, storage.ErrInvalidProgramRun},
		{"next schema", "host-authorize-next-program-attempt", "{" + scope + `,"expectedPreviousAttemptId":"attempt"}`, host, storage.ErrProjectNotFound},
		{"next requires predecessor", "host-authorize-next-program-attempt", "{" + scope + "}", host, storage.ErrInvalidProgramLoop},
		{"attempt result schema", "host-result-program-attempt", "{" + scope + `,"attemptId":"attempt","observation":{"outcome":"exited","exitCode":0,"observedAt":"2026-09-27T00:00:00Z","finishedAt":"2026-09-27T00:00:00Z"}}`, host, storage.ErrProjectNotFound},
		{"attempt required", "host-result-program-attempt", "{" + scope + `,"observation":{}}`, host, storage.ErrInvalidProgramLoop},
		{"next rejects actor", "host-authorize-next-program-attempt", "{" + scope + `,"expectedPreviousAttemptId":"attempt","actor":"human"}`, host, storage.ErrInvalidProgramLoop},
		{"host no judgment", "host-result-program-attempt", "{" + scope + `,"attemptId":"attempt","observation":{"value":"true","basis":"spoof"}}`, host, storage.ErrInvalidProgramLoop},
		{"stop schema", "host-stop-program-loop", "{" + scope + `,"reason":"Future attempts cancelled"}`, host, storage.ErrProjectNotFound},
		{"stop no reason", "host-stop-program-loop", "{" + scope + "}", host, storage.ErrInvalidProgramLoop},
		{"history schema", "host-read-program-loop-history", "{" + scope + `,"attemptCursor":"1","eventCursor":"event"}`, host, storage.ErrProjectNotFound},
		{"history rejects old cursor", "host-read-program-loop-history", "{" + scope + `,"cursor":"event"}`, host, storage.ErrInvalidProgramLoop},
		{"human no next", "host-authorize-next-program-attempt", "{" + scope + `,"expectedPreviousAttemptId":"attempt"}`, human, storage.ErrProgramRunForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := server.ExecuteHostProgramRun(auth.WithIdentity(context.Background(), tc.identity), nil, "_server", tc.operation, strings.NewReader(tc.body))
			if !errors.Is(err, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
		})
	}
}

func TestWorkbenchProgramLoopProtocolFields(t *testing.T) {
	for _, operation := range []string{"record-program-loop-decision", "stop-program-loop", "pm-record-program-loop-decision", "pm-stop-program-loop", "host-authorize-next-program-attempt", "host-result-program-attempt", "host-stop-program-loop", "program-loop-read", "program-loop-history"} {
		for _, input := range []string{
			fmt.Sprintf(`{"id":"large","operation":%q,"project":"fixture","body":{"basis":%q}}`, operation, strings.Repeat("x", workbenchRequestLimit)),
			fmt.Sprintf(`{"id":"selector","operation":%q,"project":"fixture","slug":"spoof","body":{}}`, operation),
			fmt.Sprintf(`{"id":"trailing","operation":%q,"project":"fixture","body":{}} {}`, operation),
		} {
			var output bytes.Buffer
			err := runWorkbenchCommandStdio(context.Background(), strings.NewReader(input+"\n"), &output, func(context.Context, workbenchCommandRequest, string) (json.RawMessage, error) {
				t.Fatalf("invalid %s fields reached executor", operation)
				return nil, nil
			})
			if err != nil || !strings.Contains(output.String(), `"code":"invalid_request"`) {
				t.Fatalf("%s: missing bounded strict rejection: %v %s", operation, err, output.String())
			}
		}
	}
	for _, operation := range []string{"program-loop-read", "program-loop-history"} {
		_, err := server.ExecuteLocalWorkbenchKnowledge(context.Background(), nil, "_server", operation, strings.NewReader(`{"environment_id":"env","native_project_id":"native","runId":"run"}`))
		if !errors.Is(err, storage.ErrProjectNotFound) {
			t.Fatalf("valid knowledge loop schema: %v", err)
		}
		for _, extra := range []string{`,"actor":"spoof"`, `,"project":"foreign"`, `,"scope":{}`} {
			_, err = server.ExecuteLocalWorkbenchKnowledge(context.Background(), nil, "_server", operation, strings.NewReader(`{"environment_id":"env","native_project_id":"native","runId":"run"`+extra+`}`))
			if workbenchCommandError(err).Code != "invalid_argument" {
				t.Fatalf("knowledge loop forged field accepted: %v", err)
			}
		}
	}
}

func TestWorkbenchProgramErrors(t *testing.T) {
	for err, code := range map[error]string{
		storage.ErrInvalidProgramRun:            "invalid_argument",
		storage.ErrProgramRunForbidden:          "forbidden",
		storage.ErrProgramResultConflict:        "conflict",
		storage.ErrProgramCompletionUnavailable: "failed_precondition",
		storage.ErrRunBindingNotFound:           "not_found",
		storage.ErrInvalidProgramLoop:           "invalid_argument",
		storage.ErrProgramLoopConflict:          "conflict",
		storage.ErrProgramLoopWaiting:           "failed_precondition",
		storage.ErrProgramLoopStopped:           "failed_precondition",
		storage.ErrProgramLoopNeedsHuman:        "failed_precondition",
	} {
		if got := workbenchCommandError(fmt.Errorf("wrapped: %w", err)); got.Code != code {
			t.Errorf("%v: want %s, got %s", err, code, got.Code)
		}
	}
}

func TestWorkbenchProgramLoopControlBoundary(t *testing.T) {
	human := &auth.Identity{UserID: "human", UserKind: storage.KindHuman}
	host := &auth.Identity{UserID: "host", UserKind: storage.KindServiceAccount, Source: "apikey"}
	for _, tc := range []struct {
		operation, body string
		identity        *auth.Identity
		want            error
	}{
		{"record-program-loop-decision", `{"runId":"run","attemptId":"attempt","value":"true","basis":"Original criterion evidence","expectedJudgmentId":null}`, human, storage.ErrProjectNotFound},
		{"stop-program-loop", `{"runId":"run","reason":"Stop future attempts"}`, human, storage.ErrProjectNotFound},
		{"record-program-loop-decision", `{"runId":"run","attemptId":"attempt","value":"false","basis":"Original evidence","expectedJudgmentId":null}`, host, storage.ErrProgramRunForbidden},
		{"record-program-loop-decision", `{"runId":"run","attemptId":"attempt","value":"true","basis":"Evidence"}`, human, storage.ErrInvalidProgramLoop},
		{"stop-program-loop", `{"runId":"run","reason":"Stop"}`, host, storage.ErrProgramRunForbidden},
		{"record-program-loop-decision", `{"runId":"run","attemptId":"attempt","value":true,"basis":"Evidence"}`, human, storage.ErrInvalidProgramLoop},
		{"record-program-loop-decision", `{"runId":"run","attemptId":"attempt","value":"passed","basis":"Evidence"}`, human, storage.ErrInvalidProgramLoop},
		{"record-program-loop-decision", `{"runId":"run","value":"true","basis":"Evidence"}`, human, storage.ErrInvalidProgramLoop},
		{"record-program-loop-decision", `{"runId":"run","attemptId":"attempt","value":"true","basis":"Evidence","actorUserId":"spoof"}`, human, storage.ErrInvalidProgramLoop},
		{"stop-program-loop", `{"runId":"run","reason":"Stop","attemptId":"spoof"}`, human, storage.ErrInvalidProgramLoop},
	} {
		_, err := server.ExecuteHumanProgramLoop(auth.WithIdentity(context.Background(), tc.identity), nil, "_server", tc.operation, strings.NewReader(tc.body))
		if !errors.Is(err, tc.want) {
			t.Errorf("%s %s: want %v, got %v", tc.operation, tc.body, tc.want, err)
		}
	}
	for _, tc := range []struct{ operation, request string }{
		{"pm-record-program-loop-decision", `{"runId":"run","attemptId":"attempt","value":"unknown","basis":"Original ambiguous evidence","expectedJudgmentId":null}`},
		{"pm-stop-program-loop", `{"runId":"run","reason":"Stop future attempts"}`},
	} {
		_, err := server.ExecuteAgentPlanning(context.Background(), nil, "_server", tc.operation, strings.NewReader(`{"scope":{},"request":`+tc.request+`}`))
		if !errors.Is(err, storage.ErrProjectNotFound) {
			t.Fatalf("valid PM schema: %v", err)
		}
		_, err = server.ExecuteAgentPlanning(context.Background(), nil, "_server", tc.operation, strings.NewReader(`{"scope":{},"taskSlug":"forged","request":`+tc.request+`}`))
		if workbenchCommandError(err).Code != "invalid_argument" {
			t.Fatalf("PM task selector must be rejected: %v", err)
		}
	}
	_, err := server.ExecuteAgentPlanning(context.Background(), nil, "_server", "pm-record-program-loop-decision",
		strings.NewReader(`{"scope":{},"request":{"runId":"run","attemptId":"attempt","value":"true","basis":"Evidence"}}`))
	if workbenchCommandError(err).Code != "invalid_argument" {
		t.Fatalf("PM judgment must name expectedJudgmentId: %v", err)
	}
}
