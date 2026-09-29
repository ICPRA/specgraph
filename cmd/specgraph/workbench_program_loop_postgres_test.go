// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/server"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/specgraph/specgraph/internal/storage/postgres/postgrestest"
	"github.com/stretchr/testify/require"
)

func TestWorkbenchProgramLoopStdioPostgres(t *testing.T) {
	ctx := context.Background()
	url, err := postgrestest.ConnString(ctx)
	require.NoError(t, err)
	s, err := postgres.New(ctx, url, postgres.WithProject("program-loop-stdio"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(ctx)) })
	_, err = s.EnsureProject(ctx, "program-loop-foreign")
	require.NoError(t, err)
	users, err := postgres.NewAuth(ctx, s.Pool())
	require.NoError(t, err)
	admin, err := users.CreateHuman(ctx, &storage.User{Kind: storage.KindHuman, DisplayName: "Loop authorizer", Role: "admin"}, nil)
	require.NoError(t, err)
	reader, err := users.CreateHuman(ctx, &storage.User{Kind: storage.KindHuman, DisplayName: "Loop observer", Role: "reader"}, nil)
	require.NoError(t, err)
	host, err := users.CreateServiceAccount(ctx, &storage.User{Kind: storage.KindServiceAccount, DisplayName: "Inert loop host", Role: "reader", OwnerUserID: admin.ID})
	require.NoError(t, err)
	otherHost, err := users.CreateServiceAccount(ctx, &storage.User{Kind: storage.KindServiceAccount, DisplayName: "Foreign inert host", Role: "reader", OwnerUserID: admin.ID})
	require.NoError(t, err)
	credentials := map[string]string{}
	for name, user := range map[string]*storage.User{"admin": admin, "reader": reader, "host": host, "otherHost": otherHost} {
		secret, hash, err := auth.GenerateAPIKeySecret()
		require.NoError(t, err)
		key, err := users.CreateAPIKey(ctx, &storage.APIKey{UserID: user.ID, PHCHash: hash})
		require.NoError(t, err)
		credentials[name] = auth.FormatAPIKeyToken(key.Prefix, secret)
	}
	for _, slug := range []string{"target", "manager", "developer"} {
		_, err := s.CreateSpec(ctx, slug, "Synthetic loop protocol fixture", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
		require.NoError(t, err)
		stage := "approved"
		_, err = s.UpdateSpec(ctx, slug, nil, &stage, nil, nil, nil)
		require.NoError(t, err)
	}
	// The original bound manager is synthetic metadata, not a launched model.
	for _, role := range []string{"manager", "developer"} {
		runID, err := s.PrepareRun(ctx, role, "fixture-"+role)
		require.NoError(t, err)
		require.NoError(t, s.BindRunThreadInEnvironment(ctx, runID, "env", role+"-thread"))
		_, err = s.Pool().Exec(ctx, `UPDATE context_packages SET body=body || jsonb_build_object('dispatch_target',jsonb_build_object('assignmentRole',$2::text,'environmentId','env','projectId','native')) WHERE project_slug='program-loop-stdio' AND id=(SELECT package_id FROM run_bindings WHERE project_slug='program-loop-stdio' AND id=$1)`, runID, role)
		require.NoError(t, err)
	}
	var policyDirs []string
	call := func(credential, project, operation string, body any) workbenchCommandResponse {
		t.Helper()
		encoded, err := json.Marshal(body)
		require.NoError(t, err)
		request, err := json.Marshal(workbenchCommandRequest{ID: "same-request", Credential: credentials[credential], Project: project, Operation: operation, Body: encoded})
		require.NoError(t, err)
		var output bytes.Buffer
		require.NoError(t, runWorkbenchCommandStdio(ctx, strings.NewReader(string(request)+"\n"), &output,
			func(callCtx context.Context, req workbenchCommandRequest, procedure string) (json.RawMessage, error) {
				identity, err := resolveWorkbenchOperator(callCtx, s, req.Credential, policyDirs, procedure)
				if err != nil {
					return nil, err
				}
				callCtx = auth.WithIdentity(callCtx, identity)
				switch procedure {
				case auth.WorkbenchPrepareProgramRunProcedure:
					var private struct {
						HostCredential string `json:"hostCredential"`
					}
					if err := json.Unmarshal(req.Body, &private); err != nil {
						return nil, err
					}
					consumer, err := resolveWorkbenchOperator(callCtx, s, private.HostCredential, nil, auth.WorkbenchExecuteProgramRunProcedure)
					if err != nil {
						return nil, err
					}
					return server.ExecuteHumanProgramRun(callCtx, s, req.Project, bytes.NewReader(req.Body), consumer)
				case auth.WorkbenchManageProgramLoopProcedure:
					return server.ExecuteHumanProgramLoop(callCtx, s, req.Project, req.Operation, bytes.NewReader(req.Body))
				case auth.WorkbenchReadProgramRunProcedure, auth.WorkbenchExecuteProgramRunProcedure:
					if strings.HasPrefix(req.Operation, "host-") {
						return server.ExecuteHostProgramRun(callCtx, s, req.Project, req.Operation, bytes.NewReader(req.Body))
					}
					result, err := server.ExecuteLocalWorkbenchKnowledge(callCtx, s, req.Project, req.Operation, bytes.NewReader(req.Body))
					if err != nil {
						return nil, err
					}
					return json.Marshal(result)
				case auth.WorkbenchAgentPlanningProcedure:
					return server.ExecuteAgentPlanning(callCtx, s, req.Project, req.Operation, bytes.NewReader(req.Body))
				default:
					result, err := server.ExecuteLocalWorkbenchKnowledge(callCtx, s, req.Project, req.Operation, bytes.NewReader(req.Body))
					if err != nil {
						return nil, err
					}
					return json.Marshal(result)
				}
			}))
		var response workbenchCommandResponse
		require.NoError(t, json.Unmarshal(output.Bytes(), &response))
		return response
	}
	project := "program-loop-stdio"
	prepare := map[string]any{"taskSlug": "target", "idempotencyKey": "frozen-loop", "hostCredential": credentials["host"], "command": map[string]any{
		"executable": `C:\fixture\never-executed.exe`, "args": []string{}, "cwd": `C:\fixture`, "environmentId": "env", "nativeProjectId": "native", "timeoutMs": 10000, "workPurpose": "coordination",
		"loop": map[string]any{"kind": "innovative", "maxAttempts": 3, "termination": map[string]any{"kind": "judgment", "judgment": map[string]string{"criterion": "Original synthetic criterion"}}, "synchronousCompletionBasis": "Fixture command returns only after its synchronous work"},
	}}
	prepared := call("admin", project, "prepare-program-run", prepare)
	require.Nil(t, prepared.Error)
	var run storage.ProgramRun
	require.NoError(t, json.Unmarshal(prepared.Data, &run))
	require.Nil(t, run.CurrentAttempt, "prepare must not grant a physical attempt")
	scope := map[string]any{"environmentId": "env", "nativeProjectId": "native", "runId": run.Context.RunID}
	admit := call("host", project, "host-authorize-program-run", scope)
	require.Nil(t, admit.Error)
	var admission storage.ProgramAdmission
	require.NoError(t, json.Unmarshal(admit.Data, &admission))
	require.True(t, admission.MayStart)
	first := admission.Run.CurrentAttempt.ID
	replay := call("host", project, "host-authorize-program-run", scope)
	require.Nil(t, replay.Error)
	require.NoError(t, json.Unmarshal(replay.Data, &admission))
	require.False(t, admission.MayStart)
	observation := map[string]any{"outcome": "exited", "exitCode": 0, "observedAt": "2026-09-27T00:00:00Z", "finishedAt": "2026-09-27T00:00:00Z"}
	result := map[string]any{"environmentId": "env", "nativeProjectId": "native", "runId": run.Context.RunID, "attemptId": first, "observation": observation}
	saved := call("host", project, "host-result-program-attempt", result)
	require.Nil(t, saved.Error)
	var receipt struct {
		RunID, AttemptID string
		Result           *storage.ProgramRunResult
	}
	require.NoError(t, json.Unmarshal(saved.Data, &receipt))
	require.Equal(t, run.Context.RunID, receipt.RunID)
	require.Equal(t, first, receipt.AttemptID)
	require.NotNil(t, receipt.Result)
	legacy := map[string]any{"environmentId": "env", "nativeProjectId": "native", "runId": run.Context.RunID, "observation": observation}
	require.Equal(t, "conflict", call("host", project, "host-result-program-run", legacy).Error.Code)
	require.Equal(t, "forbidden", call("otherHost", project, "host-result-program-attempt", result).Error.Code)
	forgedScope := map[string]any{"environmentId": "foreign", "nativeProjectId": "native", "runId": run.Context.RunID}
	require.Equal(t, "forbidden", call("host", project, "host-read-program-run", forgedScope).Error.Code)
	require.NotNil(t, call("host", "program-loop-foreign", "host-read-program-run", scope).Error)
	decision := map[string]any{"runId": run.Context.RunID, "attemptId": first, "value": "false", "basis": "Original evidence: criterion not satisfied", "expectedJudgmentId": nil}
	require.Equal(t, "forbidden", call("reader", project, "record-program-loop-decision", decision).Error.Code)
	require.Equal(t, "forbidden", call("host", project, "record-program-loop-decision", decision).Error.Code)
	pmScope := map[string]string{"environment_id": "env", "thread_id": "manager-thread", "provider_session_id": "session", "provider_instance_id": "codex"}
	pm := map[string]any{"scope": pmScope, "request": decision}
	decided := call("host", project, "pm-record-program-loop-decision", pm)
	require.Nil(t, decided.Error)
	var event storage.ProgramLoopEvent
	require.NoError(t, json.Unmarshal(decided.Data, &event))
	require.NotEmpty(t, event.ActorRunID, "the original manager run must own the judgment")
	require.Equal(t, decision["basis"], event.Reason)
	pmScope["thread_id"] = "developer-thread"
	require.NotNil(t, call("host", project, "pm-record-program-loop-decision", pm).Error)
	pmScope["thread_id"] = "manager-thread"
	_, err = s.Pool().Exec(ctx, `UPDATE claims SET lease_expires=now()-interval '1 minute' WHERE project_slug=$1 AND spec_slug='target'`, project)
	require.NoError(t, err)
	nextBody := map[string]any{"environmentId": "env", "nativeProjectId": "native", "runId": run.Context.RunID, "expectedPreviousAttemptId": first}
	next := call("host", project, "host-authorize-next-program-attempt", nextBody)
	require.Nil(t, next.Error, "continuation must not require a new lease or prepare")
	require.NoError(t, json.Unmarshal(next.Data, &admission))
	require.True(t, admission.MayStart)
	second := admission.Run.CurrentAttempt.ID
	require.NotEqual(t, first, second)
	next = call("host", project, "host-authorize-next-program-attempt", nextBody)
	require.Nil(t, next.Error)
	require.NoError(t, json.Unmarshal(next.Data, &admission))
	require.False(t, admission.MayStart, "lost next admission receipt must not launch twice")
	decision["value"] = "true"
	require.Equal(t, "conflict", call("admin", project, "record-program-loop-decision", decision).Error.Code, "the same predecessor cannot have a different successor")
	decision["expectedJudgmentId"] = event.ID
	decision["basis"] = "Later correction after the false judgment was consumed"
	corrected := call("admin", project, "record-program-loop-decision", decision)
	require.Nil(t, corrected.Error)
	var correctedEvent storage.ProgramLoopEvent
	require.NoError(t, json.Unmarshal(corrected.Data, &correctedEvent))
	require.Equal(t, event.ID, *correctedEvent.PredecessorID)
	require.Equal(t, event.ID, *admission.Run.CurrentAttempt.ConsumedJudgmentID)
	late := call("host", project, "host-result-program-attempt", result)
	require.Nil(t, late.Error)
	require.NoError(t, json.Unmarshal(late.Data, &receipt))
	require.Equal(t, first, receipt.AttemptID, "late replay remains attributed to its original attempt")
	knowledge := map[string]any{"environment_id": "env", "native_project_id": "native", "runId": run.Context.RunID}
	current := call("reader", project, "program-loop-read", knowledge)
	require.Nil(t, current.Error)
	require.NoError(t, json.Unmarshal(current.Data, &run))
	require.Equal(t, second, run.CurrentAttempt.ID)
	require.Nil(t, run.Result, "old attempt result must not leak into the current projection")
	history := call("reader", project, "program-loop-history", knowledge)
	require.Nil(t, history.Error)
	var page storage.ProgramLoopHistoryPage
	require.NoError(t, json.Unmarshal(history.Data, &page))
	require.Len(t, page.Attempts, 2)
	require.Len(t, page.Events, 2)
	require.Equal(t, event.ID, *page.Events[1].PredecessorID)
	require.NotNil(t, page.Attempts[0].Result)
	require.Nil(t, page.Attempts[1].Result)
	policyDir, err := os.MkdirTemp(".", ".program-loop-policy-")
	require.NoError(t, err)
	policyDir, err = filepath.Abs(policyDir)
	require.NoError(t, err)
	workingDir, err := os.Getwd()
	require.NoError(t, err)
	require.Equal(t, workingDir, filepath.Dir(policyDir))
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(policyDir)) })
	require.NoError(t, os.WriteFile(filepath.Join(policyDir, "deny.cedar"), []byte(`forbid(principal, action == SpecGraph::Action::"execution.read", resource);`), 0600))
	readCmd, _, err := rootCmd.Find([]string{"workbench-stdio"})
	require.NoError(t, err)
	oldConfig, oldInput, oldOutput := cfgFile, readCmd.InOrStdin(), readCmd.OutOrStdout()
	oldContext := readCmd.Context()
	readCmd.SetContext(ctx)
	t.Cleanup(func() {
		cfgFile = oldConfig
		readCmd.SetIn(oldInput)
		readCmd.SetOut(oldOutput)
		readCmd.SetContext(oldContext)
	})
	cfgFile = filepath.Join(policyDir, "read-config.yaml")
	readLoop := func(readProject, resource, attemptCursor, eventCursor string, dirs []string) workbenchReadResponse {
		t.Helper()
		cfg, err := json.Marshal(map[string]any{"server": map[string]any{"postgres": map[string]string{"url": url}}, "auth": map[string]any{"policies": map[string]any{"extra_dirs": dirs}}})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(cfgFile, cfg, 0600))
		request, err := json.Marshal(workbenchReadRequest{ID: "human-loop-read", Resource: resource, Project: readProject, Slug: run.Context.RunID, Credential: credentials["reader"], AttemptCursor: attemptCursor, EventCursor: eventCursor})
		require.NoError(t, err)
		var output bytes.Buffer
		readCmd.SetIn(strings.NewReader(string(request) + "\n"))
		readCmd.SetOut(&output)
		require.NoError(t, readCmd.RunE(readCmd, nil))
		var response workbenchReadResponse
		require.NoError(t, json.Unmarshal(output.Bytes(), &response))
		return response
	}
	for _, resource := range []string{"program-loop", "program-loop-history"} {
		allowed := readLoop(project, resource, "", "", nil)
		require.Nil(t, allowed.Error, "execution reader must inspect without host or native scope")
		require.Len(t, allowed.Data, 1)
		require.Equal(t, "forbidden", readLoop(project, resource, "", "", []string{policyDir}).Error.Code, "the actual read route must enforce execution.read")
		require.Equal(t, "invalid_request", readLoop("", resource, "", "", nil).Error.Code)
		require.Equal(t, "not_found", readLoop("program-loop-foreign", resource, "", "", nil).Error.Code)
	}
	readPage := readLoop(project, "program-loop-history", "1", event.ID, nil)
	require.Nil(t, readPage.Error)
	encodedPage, err := json.Marshal(readPage.Data["history"])
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(encodedPage, &page))
	require.Len(t, page.Attempts, 1)
	require.Equal(t, second, page.Attempts[0].ID)
	require.Len(t, page.Events, 1, "the later correction is a separate immutable event")
	require.Equal(t, correctedEvent.ID, page.Events[0].ID)
	policyDirs = []string{policyDir}
	for _, operation := range []string{"program-loop-read", "program-loop-history"} {
		require.Equal(t, "forbidden", call("reader", project, operation, knowledge).Error.Code, "spec.read alone cannot disclose execution records")
	}
	specRead := map[string]any{"environment_id": "env", "native_project_id": "native", "kind": "spec", "reference": "target"}
	require.Nil(t, call("reader", project, "knowledge-record", specRead).Error, "execution forbid must preserve unrelated spec.read")
	policyDirs = nil
	require.NoError(t, os.RemoveAll(policyDir))
	for _, operation := range []string{"program-loop-read", "program-loop-history"} {
		require.Nil(t, call("reader", project, operation, knowledge).Error, "normal execution reader remains authorized")
	}
	stop := call("admin", project, "stop-program-loop", map[string]any{"runId": run.Context.RunID, "reason": "Correction prevents future attempts only"})
	require.Nil(t, stop.Error)
	nextBody["expectedPreviousAttemptId"] = second
	require.Equal(t, "failed_precondition", call("host", project, "host-authorize-next-program-attempt", nextBody).Error.Code)
}
