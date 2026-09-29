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

func TestWorkbenchSummaryProtocol(t *testing.T) {
	ctx := context.Background()
	engine, setupErr := auth.NewCedarEngine(ctx, []auth.PolicySource{auth.NewEmbeddedPolicySource()}, auth.ActionNames())
	if setupErr != nil {
		t.Fatal(setupErr)
	}
	authorizer := auth.NewCedarAuthorizer(engine)
	for operation, action := range map[string]string{
		"summary-status":               "spec.read",
		"summary-history":              "spec.read",
		"record-summary-disposition":   "workbench.manage",
		"accept-summary":               "workbench.manage",
		"revoke-summary-acceptance":    "workbench.manage",
		"pm-summary-disposition":       "summary.write",
		"pm-accept-summary":            "summary.write",
		"pm-revoke-summary-acceptance": "summary.write",
	} {
		t.Run(operation, func(t *testing.T) {
			for _, project := range []string{"fixture", ""} {
				var output bytes.Buffer
				called := false
				input := fmt.Sprintf(`{"id":"summary","operation":%q,"project":%q,"credential":"fixture","body":{}}`, operation, project)
				err := runWorkbenchCommandStdio(ctx, strings.NewReader(input+"\n"), &output,
					func(ctx context.Context, req workbenchCommandRequest, procedure string) (json.RawMessage, error) {
						called = true
						if actual, ok := auth.ActionForProcedure(procedure); !ok || actual != action || req.Slug != "" {
							t.Fatalf("incorrect summary route: %s", actual)
						}
						for _, role := range []auth.Role{auth.RoleReader, auth.RoleWriter, auth.RoleAdmin, "unknown"} {
							decision, err := authorizer.Authorize(ctx, &auth.Identity{UserID: "fixture", Subject: "apikey:fixture", EffectiveRole: role}, procedure, nil)
							want := role != "unknown" && (action != "workbench.manage" || role == auth.RoleAdmin)
							if err != nil || decision.Allowed != want {
								t.Fatalf("incorrect %s gate: allowed=%v err=%v", role, decision.Allowed, err)
							}
						}
						return nil, storage.ErrSummaryForbidden
					})
				if err != nil || called != (project != "" || action != "workbench.manage") {
					t.Fatalf("incorrect project requirement: called=%v err=%v", called, err)
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
	for _, operation := range []string{"accept-summary", "pm-accept-summary", "record-summary-disposition"} {
		var output bytes.Buffer
		called := false
		input := fmt.Sprintf(`{"id":"vector","operation":%q,"project":"fixture","body":{"basis":%q}}`, operation, strings.Repeat("x", workbenchRequestLimit))
		err := runWorkbenchCommandStdio(ctx, strings.NewReader(input+"\n"), &output, func(context.Context, workbenchCommandRequest, string) (json.RawMessage, error) {
			called = true
			return json.RawMessage(`{}`), nil
		})
		if err != nil || called != strings.Contains(operation, "accept-summary") {
			t.Fatalf("incorrect bounded vector route %s: called=%v err=%v", operation, called, err)
		}
	}
	var output bytes.Buffer
	input := `{"id":"oversize","operation":"accept-summary","project":"fixture","body":{"basis":"` + strings.Repeat("x", 2<<20) + `"}}`
	setupErr = runWorkbenchCommandStdio(ctx, strings.NewReader(input+"\n"), &output, func(context.Context, workbenchCommandRequest, string) (json.RawMessage, error) {
		t.Fatal("oversize summary must not be dispatched")
		return nil, nil
	})
	if setupErr == nil || !strings.Contains(output.String(), `"code":"invalid_request"`) {
		t.Fatalf("oversize vector failure not explicit: err=%v output=%s", setupErr, output.Bytes())
	}
}

func TestWorkbenchSummaryBoundary(t *testing.T) {
	human := &auth.Identity{UserID: "human", UserKind: storage.KindHuman}
	agent := &auth.Identity{UserID: "agent", UserKind: storage.KindServiceAccount}
	scope := `{"environment_id":"env","thread_id":"thread","provider_session_id":"session","provider_instance_id":"codex"}`
	accept := `{"goalSlug":"goal","basis":"named review","evidenceSources":[],"goalsSatisfied":true,"idempotencyKey":"intent","expectedReferences":{"nodes":[{"id":"node","slug":"goal","role":"summary","sourceRefs":{},"lifecycleChangeId":"change"}],"relations":[{"fromSlug":"goal","toSlug":"child","type":"COMPOSES","changeId":"edge-change","present":true}],"dispositionIds":[],"decisionSources":[]},"impactReview":[]}`
	for _, tc := range []struct {
		name, operation, body string
		identity              *auth.Identity
		want                  error
	}{
		{"human accept schema", "accept-summary", accept, human, storage.ErrProjectNotFound},
		{"pm accept schema", "pm-accept-summary", `{"scope":` + scope + `,"request":` + accept + `}`, agent, storage.ErrProjectNotFound},
		{"human disposition schema", "record-summary-disposition", `{"goalSlug":"goal","affectedSlug":"child","disposition":"retain","reviewDecisionId":"decision","beforeSources":[],"afterSources":[],"reason":"review","idempotencyKey":"intent"}`, human, storage.ErrProjectNotFound},
		{"human revoke schema", "revoke-summary-acceptance", `{"acceptanceId":"acceptance","reason":"changed"}`, human, storage.ErrProjectNotFound},
		{"service cannot use human route", "accept-summary", accept, agent, storage.ErrSummaryForbidden},
		{"missing identity", "accept-summary", accept, &auth.Identity{}, storage.ErrSummaryForbidden},
		{"unknown actor", "accept-summary", strings.TrimSuffix(accept, "}") + `,"actorUserId":"spoof"}`, human, storage.ErrInvalidSummary},
		{"unknown metadata", "accept-summary", strings.Replace(accept, `"sourceRefs":{}`, `"sourceRefs":{},"hash":"pretend"`, 1), human, storage.ErrInvalidSummary},
		{"relation field type", "accept-summary", strings.Replace(accept, `"present":true`, `"present":"true"`, 1), human, storage.ErrInvalidSummary},
		{"lifecycle field type", "accept-summary", strings.Replace(accept, `"lifecycleChangeId":"change"`, `"lifecycleChangeId":42`, 1), human, storage.ErrInvalidSummary},
		{"trailing request", "accept-summary", accept + ` {}`, human, storage.ErrInvalidSummary},
		{"pm missing scope", "pm-accept-summary", `{"request":` + accept + `}`, agent, storage.ErrInvalidSummary},
		{"pm unknown scope", "pm-accept-summary", `{"scope":` + strings.TrimSuffix(scope, "}") + `,"actor":"spoof"},"request":` + accept + `}`, agent, storage.ErrInvalidSummary},
		{"pm unknown request", "pm-accept-summary", `{"scope":` + scope + `,"request":{"actor":"spoof"}}`, agent, storage.ErrInvalidSummary},
		{"revoke rejects actor", "revoke-summary-acceptance", `{"acceptanceId":"a","reason":"r","actor":"spoof"}`, human, storage.ErrInvalidSummary},
		{"unknown operation", "pm-complete-summary", `{"scope":` + scope + `,"request":{}}`, agent, storage.ErrInvalidSummary},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := server.ExecuteWorkbenchSummary(auth.WithIdentity(context.Background(), tc.identity), nil, "_server", tc.operation, strings.NewReader(tc.body))
			if !errors.Is(err, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
		})
	}
	for _, body := range []string{`{}`, `{"environment_id":"env","native_project_id":"native","goalSlug":"goal","actor":"spoof"}`} {
		if _, err := server.ReadLocalWorkbenchSummary(context.Background(), nil, "_server", "summary-status", strings.NewReader(body)); !errors.Is(err, storage.ErrInvalidSummary) {
			t.Fatalf("invalid read boundary accepted: %v", err)
		}
	}
	if _, err := server.ReadLocalWorkbenchSummary(context.Background(), nil, "_server", "summary-status", strings.NewReader(`{"environment_id":"env","native_project_id":"native","goalSlug":"goal"}`)); !errors.Is(err, storage.ErrProjectNotFound) {
		t.Fatalf("valid read schema not routed: %v", err)
	}
	for _, tc := range []struct {
		operation, body string
		want            error
	}{
		{"summary-history", `{"environment_id":"env","native_project_id":"native","goalSlug":"goal","kind":"dispositions"}`, storage.ErrProjectNotFound},
		{"summary-history", `{"environment_id":"env","native_project_id":"native","goalSlug":"goal","kind":"acceptances","beforeId":"12"}`, storage.ErrProjectNotFound},
		{"summary-history", `{"environment_id":"env","native_project_id":"native","goalSlug":"goal","kind":"acceptances","beforeId":"012"}`, storage.ErrInvalidSummary},
		{"summary-history", `{"environment_id":"env","native_project_id":"native","goalSlug":"goal","kind":"all"}`, storage.ErrInvalidSummary},
		{"summary-history", `{"environment_id":"env","native_project_id":"native","goalSlug":"goal","kind":"acceptances","actor":"spoof"}`, storage.ErrInvalidSummary},
		{"summary-history", `{"goalSlug":"goal","kind":"acceptances"}`, storage.ErrInvalidSummary},
		{"summary-status", `{"environment_id":"env","native_project_id":"native","goalSlug":"goal","kind":"acceptances"}`, storage.ErrInvalidSummary},
	} {
		if _, err := server.ReadLocalWorkbenchSummary(context.Background(), nil, "_server", tc.operation, strings.NewReader(tc.body)); !errors.Is(err, tc.want) {
			t.Fatalf("history boundary %s: want %v, got %v", tc.body, tc.want, err)
		}
	}
}

func TestWorkbenchSummaryReadAndErrors(t *testing.T) {
	for _, slug := range []string{"goal", ""} {
		var output bytes.Buffer
		called := false
		input := fmt.Sprintf(`{"id":"summary","resource":"summary","project":"fixture","slug":%q,"credential":"fixture"}`, slug)
		err := runWorkbenchStdio(context.Background(), strings.NewReader(input+"\n"), &output, func(_ context.Context, req workbenchReadRequest) (map[string]any, error) {
			called = true
			return map[string]any{"summary": storage.SummaryState{GoalSlug: req.Slug}}, nil
		})
		if err != nil || called != (slug != "") {
			t.Fatalf("summary read route: called=%v err=%v", called, err)
		}
		if called && !strings.Contains(output.String(), `"summary":{"goalSlug":"goal"`) {
			t.Fatalf("summary response lost: %s", output.Bytes())
		}
	}
	for _, tc := range []struct {
		kind, cursor string
		valid        bool
	}{
		{"dispositions", "", true}, {"acceptances", "15", true},
		{"all", "", false}, {"acceptances", "-1", false}, {"acceptances", "01", false},
	} {
		var output bytes.Buffer
		called := false
		input := fmt.Sprintf(`{"id":"history","resource":"summary-history","project":"fixture","slug":"goal","query":%q,"cursor":%q,"credential":"fixture"}`, tc.kind, tc.cursor)
		err := runWorkbenchStdio(context.Background(), strings.NewReader(input+"\n"), &output, func(_ context.Context, req workbenchReadRequest) (map[string]any, error) {
			called = true
			if req.Query != tc.kind || req.Cursor != tc.cursor || req.Slug != "goal" {
				t.Fatalf("history selectors changed: %+v", req)
			}
			return map[string]any{"history": storage.SummaryHistory{GoalSlug: req.Slug, Kind: req.Query}}, nil
		})
		if err != nil || called != tc.valid {
			t.Fatalf("history read: kind=%s cursor=%s called=%v err=%v", tc.kind, tc.cursor, called, err)
		}
		if tc.valid && !strings.Contains(output.String(), `"history":{"goalSlug":"goal"`) {
			t.Fatalf("history response lost: %s", output.Bytes())
		}
	}
	for err, code := range map[error]string{
		storage.ErrInvalidSummary:            "invalid_argument",
		storage.ErrSummaryForbidden:          "forbidden",
		storage.ErrSummaryConflict:           "conflict",
		storage.ErrSummaryNotAcceptable:      "failed_precondition",
		storage.ErrSummaryAcceptanceNotFound: "not_found",
	} {
		if got := workbenchCommandError(fmt.Errorf("wrapped: %w", err)); got.Code != code {
			t.Errorf("%v: want %s, got %s", err, code, got.Code)
		}
	}
}
