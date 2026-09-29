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

func TestWorkbenchDeliveryHookProtocol(t *testing.T) {
	engine, err := auth.NewCedarEngine(context.Background(), []auth.PolicySource{auth.NewEmbeddedPolicySource()}, auth.ActionNames())
	if err != nil {
		t.Fatal(err)
	}
	authorizer := auth.NewCedarAuthorizer(engine)
	for operation, action := range map[string]string{"pm-arm-delivery-hook": "hook-config.write", "pm-cancel-delivery-hook": "hook-config.write", "pm-retry-delivery-hook": "hook-config.write", "pm-read-delivery-hook": "execution.read", "pm-list-delivery-hooks": "execution.read", "host-list-delivery-hooks": "execution.read", "host-read-delivery-hook": "execution.read", "host-bind-delivery-hook": "hook-dispatch.write", "host-authorize-delivery-hook": "hook-dispatch.write", "host-result-delivery-hook": "hook-dispatch.write", "delivery-hook-context": "execution.read"} {
		var output bytes.Buffer
		called := false
		err := runWorkbenchCommandStdio(context.Background(), strings.NewReader(`{"id":"hook","operation":"`+operation+`","credential":"test-only","body":{"scope":{"environment_id":"env","thread_id":"manager-thread","provider_session_id":"session","provider_instance_id":"codex"},"request":{}}}`+"\n"), &output,
			func(ctx context.Context, req workbenchCommandRequest, procedure string) (json.RawMessage, error) {
				called = true
				actual, ok := auth.ActionForProcedure(procedure)
				if !ok || actual != action || req.Project != "" || req.Slug != "" {
					t.Fatalf("wrong scoped hook route: %s", actual)
				}
				decision, err := authorizer.Authorize(ctx, &auth.Identity{UserID: "fixture", Subject: "apikey:fixture", EffectiveRole: auth.RoleReader}, procedure, nil)
				if err != nil || !decision.Allowed {
					t.Fatalf("transport action unavailable: %v", err)
				}
				// The authenticated transport permit is not a PM business authorization.
				return nil, auth.ErrUnauthenticated
			})
		if err != nil || !called {
			t.Fatalf("hook not routed: %v", err)
		}
		var response workbenchCommandResponse
		if err := json.Unmarshal(output.Bytes(), &response); err != nil || response.Error == nil || response.Error.Code != "unauthorized" {
			t.Fatalf("authentication failure lost: %s", output.Bytes())
		}
	}
}

func TestWorkbenchHumanDeliveryHookProtocol(t *testing.T) {
	engine, err := auth.NewCedarEngine(context.Background(), []auth.PolicySource{auth.NewEmbeddedPolicySource()}, auth.ActionNames())
	if err != nil {
		t.Fatal(err)
	}
	authorizer := auth.NewCedarAuthorizer(engine)
	for _, operation := range []string{"arm-delivery-hook", "cancel-delivery-hook", "retry-delivery-hook"} {
		var output bytes.Buffer
		called := false
		err := runWorkbenchCommandStdio(context.Background(), strings.NewReader(`{"id":"human","operation":"`+operation+`","project":"project","credential":"human-credential","body":{}}`+"\n"), &output,
			func(ctx context.Context, req workbenchCommandRequest, procedure string) (json.RawMessage, error) {
				called = true
				action, ok := auth.ActionForProcedure(procedure)
				if !ok || action != "workbench.manage" || req.Slug != "" {
					t.Fatal("human hook must use management authorization")
				}
				for _, role := range []auth.Role{auth.RoleReader, auth.RoleAdmin} {
					decision, err := authorizer.Authorize(ctx, &auth.Identity{UserID: "fixture", Subject: "apikey:fixture", EffectiveRole: role}, procedure, nil)
					if err != nil || decision.Allowed != (role == auth.RoleAdmin) {
						t.Fatalf("incorrect human action gate for %s", role)
					}
				}
				return nil, auth.ErrUnauthenticated
			})
		if err != nil || !called {
			t.Fatalf("human route missing: %v", err)
		}
	}
}
