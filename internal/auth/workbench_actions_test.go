// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package auth

import (
	"context"
	"testing"

	"github.com/specgraph/specgraph/gen/specgraph/v1/specgraphv1connect"
)

func TestWorkbenchCedarActions(t *testing.T) {
	ctx := context.Background()
	engine, err := NewCedarEngine(ctx, []PolicySource{NewEmbeddedPolicySource()}, ActionNames())
	if err != nil {
		t.Fatal(err)
	}
	authorizer := NewCedarAuthorizer(engine)
	for _, procedure := range []string{WorkbenchInspectMailProcedure, WorkbenchPrepareRunProcedure, WorkbenchBindRunThreadProcedure, WorkbenchSubmitDeliveryProcedure, WorkbenchAcceptDeliveryProcedure, WorkbenchCompleteRunProcedure, WorkbenchManualCompleteProcedure, WorkbenchSubdivisionProcedure, WorkbenchManageProgramLoopProcedure} {
		wantAction := "workbench.write"
		if procedure == WorkbenchInspectMailProcedure || procedure == WorkbenchAcceptDeliveryProcedure || procedure == WorkbenchManualCompleteProcedure || procedure == WorkbenchSubdivisionProcedure || procedure == WorkbenchManageProgramLoopProcedure {
			wantAction = "workbench.manage"
		}
		if action, ok := ActionForProcedure(procedure); !ok || action != wantAction {
			t.Fatalf("%s: action %s", procedure, action)
		}
		for _, role := range []Role{RoleReader, RoleWriter, RoleAdmin, "unknown"} {
			decision, err := authorizer.Authorize(ctx, &Identity{UserID: "user-1", Subject: "apikey:key-1", EffectiveRole: role}, procedure, nil)
			if err != nil {
				t.Fatal(err)
			}
			wantAllowed := role == RoleAdmin || (role == RoleWriter && wantAction == "workbench.write")
			if decision.Allowed != wantAllowed {
				t.Fatalf("%s %s: allowed=%v, want %v", role, procedure, decision.Allowed, wantAllowed)
			}
		}
	}
}

func TestOwnNodeEventPermissionDoesNotOpenGeneralExecutionWrites(t *testing.T) {
	ctx := context.Background()
	engine, err := NewCedarEngine(ctx, []PolicySource{NewEmbeddedPolicySource()}, ActionNames())
	if err != nil {
		t.Fatal(err)
	}
	authorizer := NewCedarAuthorizer(engine)
	if action, ok := ActionForProcedure(WorkbenchRecordOwnNodeEventProcedure); !ok || action != "node-event.write" {
		t.Fatalf("own event action = %q", action)
	}
	for _, tc := range []struct {
		role         Role
		own, general bool
	}{
		{RoleReader, true, false}, {RoleWriter, true, true}, {RoleAdmin, true, true}, {"unknown", false, false},
	} {
		identity := &Identity{UserID: "user-1", Subject: "apikey:key-1", EffectiveRole: tc.role}
		for _, check := range []struct {
			procedure string
			allowed   bool
		}{
			{WorkbenchRecordOwnNodeEventProcedure, tc.own},
			{specgraphv1connect.ExecutionServiceReportProgressProcedure, tc.general},
		} {
			decision, err := authorizer.Authorize(ctx, identity, check.procedure, nil)
			if err != nil {
				t.Fatal(err)
			}
			if decision.Allowed != check.allowed {
				t.Fatalf("role %s procedure %s allowed=%v want=%v", tc.role, check.procedure, decision.Allowed, check.allowed)
			}
		}
	}
}

func TestOwnCandidateJudgmentPermissionIsNarrow(t *testing.T) {
	ctx := context.Background()
	engine, err := NewCedarEngine(ctx, []PolicySource{NewEmbeddedPolicySource()}, ActionNames())
	if err != nil {
		t.Fatal(err)
	}
	authorizer := NewCedarAuthorizer(engine)
	if action, ok := ActionForProcedure(WorkbenchRecordOwnCandidateJudgmentProcedure); !ok || action != "candidate-judgment.write" {
		t.Fatalf("own candidate action = %q", action)
	}
	reader := &Identity{UserID: "user-1", Subject: "apikey:key-1", EffectiveRole: RoleReader}
	for _, tc := range []struct {
		procedure string
		allowed   bool
	}{
		{WorkbenchRecordOwnCandidateJudgmentProcedure, true},
		{specgraphv1connect.ExecutionServiceReportProgressProcedure, false},
		{WorkbenchPrepareRunProcedure, false},
	} {
		decision, err := authorizer.Authorize(ctx, reader, tc.procedure, nil)
		if err != nil {
			t.Fatal(err)
		}
		if decision.Allowed != tc.allowed {
			t.Fatalf("%s allowed=%v want=%v", tc.procedure, decision.Allowed, tc.allowed)
		}
	}
}

func TestOwnNodeHandoffPermissionIsNarrow(t *testing.T) {
	ctx := context.Background()
	engine, err := NewCedarEngine(ctx, []PolicySource{NewEmbeddedPolicySource()}, ActionNames())
	if err != nil {
		t.Fatal(err)
	}
	authorizer := NewCedarAuthorizer(engine)
	if action, ok := ActionForProcedure(WorkbenchRecordOwnNodeHandoffSummaryProcedure); !ok || action != "node-handoff.write" {
		t.Fatalf("own handoff action = %q", action)
	}
	reader := &Identity{UserID: "user-1", Subject: "apikey:key-1", EffectiveRole: RoleReader}
	for _, tc := range []struct {
		procedure string
		allowed   bool
	}{
		{WorkbenchRecordOwnNodeHandoffSummaryProcedure, true},
		{specgraphv1connect.ExecutionServiceReportProgressProcedure, false},
		{WorkbenchDispatchProcedure, false},
	} {
		decision, err := authorizer.Authorize(ctx, reader, tc.procedure, nil)
		if err != nil {
			t.Fatal(err)
		}
		if decision.Allowed != tc.allowed {
			t.Fatalf("%s allowed=%v want=%v", tc.procedure, decision.Allowed, tc.allowed)
		}
	}
}
