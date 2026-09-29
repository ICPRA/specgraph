// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/specgraph/specgraph/gen/specgraph/v1/specgraphv1connect"
	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/server"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/specgraph/specgraph/internal/storage/postgres/postgrestest"
	"github.com/stretchr/testify/require"
)

func TestWorkbenchNodeEventsLocalPostgres(t *testing.T) {
	ctx := context.Background()
	url, err := postgrestest.ConnString(ctx)
	require.NoError(t, err)
	s, err := postgres.New(ctx, url, postgres.WithProject("node-events"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(ctx)) })
	users, err := postgres.NewAuth(ctx, s.Pool())
	require.NoError(t, err)
	credentials := map[string]string{}
	userIDs := map[string]string{}
	for _, role := range []string{"admin", "reader"} {
		user, err := users.CreateHuman(ctx, &storage.User{Kind: storage.KindHuman, DisplayName: "Synthetic node event " + role, Role: role}, nil)
		require.NoError(t, err)
		userIDs[role] = user.ID
		secret, hash, err := auth.GenerateAPIKeySecret()
		require.NoError(t, err)
		key, err := users.CreateAPIKey(ctx, &storage.APIKey{UserID: user.ID, PHCHash: hash})
		require.NoError(t, err)
		credentials[role] = auth.FormatAPIKeyToken(key.Prefix, secret)
	}
	create := func(store *postgres.Store, slug string) (string, string) {
		t.Helper()
		_, err := store.CreateSpec(ctx, slug, "Synthetic occurrence fixture", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
		require.NoError(t, err)
		approved := "approved"
		_, err = store.UpdateSpec(ctx, slug, nil, &approved, nil, nil, nil)
		require.NoError(t, err)
		run, err := store.PrepareRun(ctx, slug, "workspace-"+slug)
		require.NoError(t, err)
		delivery, err := store.CreateDelivery(ctx, run, json.RawMessage(`{"summary":"Synthetic original delivery"}`), "fixture")
		require.NoError(t, err)
		return run, delivery
	}
	run, delivery := create(s, "task")
	otherRun, otherDelivery := create(s, "other-task")
	_, err = s.EnsureProject(ctx, "node-events-other")
	require.NoError(t, err)
	other, err := s.ScopedExisting(ctx, "node-events-other")
	require.NoError(t, err)
	foreignRun, foreignDelivery := create(other, "task")
	before, err := s.GetSpec(ctx, "task")
	require.NoError(t, err)
	beforeRun, err := s.ReadRunContext(ctx, run)
	require.NoError(t, err)
	beforeClaim, err := s.GetActiveClaim(ctx, "task")
	require.NoError(t, err)
	beforeMetadata, err := s.ReadWorkbenchMetadata(ctx)
	require.NoError(t, err)
	command := func(role, project, slug string, body any) workbenchCommandResponse {
		t.Helper()
		encoded, err := json.Marshal(body)
		require.NoError(t, err)
		request, err := json.Marshal(workbenchCommandRequest{ID: "event", Operation: "record-node-event", Project: project, Slug: slug, Credential: credentials[role], Body: encoded})
		require.NoError(t, err)
		var output bytes.Buffer
		err = runWorkbenchCommandStdio(ctx, bytes.NewReader(append(request, '\n')), &output, func(callCtx context.Context, req workbenchCommandRequest, procedure string) (json.RawMessage, error) {
			require.Equal(t, auth.WorkbenchRecordNodeEventProcedure, procedure)
			action, ok := auth.ActionForProcedure(procedure)
			require.True(t, ok)
			require.Equal(t, "workbench.manage", action)
			identity, err := resolveWorkbenchOperator(callCtx, s, req.Credential, nil, procedure)
			if err != nil {
				return nil, err
			}
			return server.ExecuteWorkbenchCommand(auth.WithIdentity(callCtx, identity), s, req.Operation, req.Project, req.Slug, "not-the-authenticated-actor", req.Body)
		})
		require.NoError(t, err)
		var response workbenchCommandResponse
		require.NoError(t, json.Unmarshal(output.Bytes(), &response))
		return response
	}
	read := func(resource, project, slug, cursor string) workbenchReadResponse {
		t.Helper()
		req := workbenchReadRequest{ID: "history", Resource: resource, Project: project, Slug: slug, Cursor: cursor}
		if resource != "current-view" {
			req.Credential = credentials["reader"]
		}
		input, err := json.Marshal(req)
		require.NoError(t, err)
		var output bytes.Buffer
		err = runWorkbenchStdio(ctx, bytes.NewReader(append(input, '\n')), &output, func(callCtx context.Context, req workbenchReadRequest) (map[string]any, error) {
			scoped, err := s.ScopedExisting(callCtx, req.Project)
			if err != nil {
				return nil, err
			}
			if req.Resource == "current-view" {
				view, _, err := server.ReadWorkbenchCurrentView(callCtx, scoped, req.Project)
				return view, err
			}
			if _, err := resolveWorkbenchOperator(callCtx, s, req.Credential, nil, specgraphv1connect.SpecServiceGetSpecProcedure); err != nil {
				return nil, err
			}
			page, err := scoped.ReadNodeEvents(callCtx, req.Slug, req.Cursor)
			if err != nil {
				return nil, err
			}
			return map[string]any{"taskSlug": page.TaskSlug, "events": page.Events, "hasMore": page.HasMore, "nextCursor": page.NextCursor}, nil
		})
		require.NoError(t, err)
		var response workbenchReadResponse
		require.NoError(t, json.Unmarshal(output.Bytes(), &response))
		return response
	}
	view := read("current-view", "node-events", "", "")
	require.Nil(t, view.Error)
	counts, err := json.Marshal(view.Data["nodeEventCounts"])
	require.NoError(t, err)
	require.Equal(t, "[]", string(counts))
	require.Equal(t, true, view.Data["capabilities"].(map[string]any)["nodeEvents"])
	retry := storage.RecordNodeEventRequest{EventID: "retry-same", Kind: "retry", Reason: "Operator declares a fresh attempt after a synthetic failure", PreviousRunID: &run, RunID: &run}
	denied := command("reader", "node-events", "task", retry)
	require.NotNil(t, denied.Error)
	require.Equal(t, "forbidden", denied.Error.Code)
	saved := command("admin", "node-events", "task", retry)
	require.Nil(t, saved.Error)
	var first storage.NodeExecutionEvent
	require.NoError(t, json.Unmarshal(saved.Data, &first))
	require.Equal(t, retry.EventID, first.ID)
	require.Equal(t, userIDs["admin"], first.Actor)
	require.Equal(t, *first.PreviousRunID, *first.RunID)
	require.Nil(t, first.DeliveryID)
	require.NotZero(t, first.RecordedAt)
	duplicate := command("admin", "node-events", "task", retry)
	require.NotNil(t, duplicate.Error)
	require.Equal(t, "conflict", duplicate.Error.Code)
	retry.EventID = "retry-prev"
	retry.RunID = nil
	previousOnly := command("admin", "node-events", "task", retry)
	require.Nil(t, previousOnly.Error)
	require.Contains(t, string(previousOnly.Data), `"runId":null`)
	rework := storage.RecordNodeEventRequest{EventID: "rework-one", Kind: "rework", Reason: "Operator declares actual rework of the named delivery", DeliveryID: &delivery}
	require.Nil(t, command("admin", "node-events", "task", rework).Error)
	for i, invalid := range []storage.RecordNodeEventRequest{
		{EventID: "bad-foreign-run", Kind: "retry", Reason: "Wrong project", PreviousRunID: &foreignRun},
		{EventID: "bad-node-run", Kind: "retry", Reason: "Wrong node", PreviousRunID: &otherRun},
		{EventID: "bad-current-run", Kind: "retry", Reason: "Wrong current node", PreviousRunID: &run, RunID: &otherRun},
		{EventID: "bad-foreign-delivery", Kind: "rework", Reason: "Wrong project", DeliveryID: &foreignDelivery},
		{EventID: "bad-node-delivery", Kind: "rework", Reason: "Wrong node", DeliveryID: &otherDelivery},
		{EventID: "bad-retry-shape", Kind: "retry", Reason: "Unexpected delivery", PreviousRunID: &run, DeliveryID: &delivery},
		{EventID: "bad-rework-shape", Kind: "rework", Reason: "Unexpected run", PreviousRunID: &run, DeliveryID: &delivery},
		{EventID: "bad id", Kind: "retry", Reason: "Invalid stable ID", PreviousRunID: &run},
		{EventID: "no-git-operation", Kind: "revert", Reason: "Not part of this record kind", DeliveryID: &delivery},
	} {
		response := command("admin", "node-events", "task", invalid)
		require.NotNil(t, response.Error, i)
		require.Equal(t, "invalid_argument", response.Error.Code)
	}
	forged := command("admin", "node-events", "task", map[string]any{"eventId": "forged", "kind": "retry", "reason": "Must not select actor", "previousRunId": run, "actor": "forged"})
	require.NotNil(t, forged.Error)
	require.Nil(t, command("admin", "node-events-other", "task", storage.RecordNodeEventRequest{EventID: "retry-same", Kind: "retry", Reason: "Different project occurrence", PreviousRunID: &foreignRun}).Error)
	view = read("current-view", "node-events", "", "")
	require.Nil(t, view.Error)
	counts, _ = json.Marshal(view.Data["nodeEventCounts"])
	require.JSONEq(t, `[{"taskSlug":"task","retries":2,"reworks":1,"gitUndos":0}]`, string(counts))
	for i := 0; i < 49; i++ {
		retry.EventID = fmt.Sprintf("pagination-%d", i)
		require.Nil(t, command("admin", "node-events", "task", retry).Error)
	}
	history := read("node-events", "node-events", "task", "")
	require.Nil(t, history.Error)
	body, _ := json.Marshal(history.Data)
	var page storage.NodeEventPage
	require.NoError(t, json.Unmarshal(body, &page))
	require.Len(t, page.Events, 50)
	require.True(t, page.HasMore)
	require.NotNil(t, page.NextCursor)
	require.Equal(t, "pagination-48", page.Events[0].ID)
	history = read("node-events", "node-events", "task", *page.NextCursor)
	require.Nil(t, history.Error)
	body, _ = json.Marshal(history.Data)
	require.NoError(t, json.Unmarshal(body, &page))
	require.Len(t, page.Events, 2)
	require.False(t, page.HasMore)
	require.Nil(t, page.NextCursor)
	require.Equal(t, first.ID, page.Events[1].ID)
	foreignHistory := read("node-events", "node-events-other", "task", "")
	require.Nil(t, foreignHistory.Error)
	body, _ = json.Marshal(foreignHistory.Data)
	require.NoError(t, json.Unmarshal(body, &page))
	require.Len(t, page.Events, 1)
	after, err := s.GetSpec(ctx, "task")
	require.NoError(t, err)
	require.Equal(t, before, after)
	afterRun, err := s.ReadRunContext(ctx, run)
	require.NoError(t, err)
	require.Equal(t, beforeRun, afterRun)
	afterClaim, err := s.GetActiveClaim(ctx, "task")
	require.NoError(t, err)
	require.Equal(t, beforeClaim, afterClaim)
	afterMetadata, err := s.ReadWorkbenchMetadata(ctx)
	require.NoError(t, err)
	require.Equal(t, beforeMetadata.Runs, afterMetadata.Runs)
	require.Equal(t, beforeMetadata.Deliveries, afterMetadata.Deliveries)
	originalEvents, err := s.GetExecutionEvents(ctx, "task", 0)
	require.NoError(t, err)
	require.Empty(t, originalEvents, "occurrences do not fabricate execution progress/completion")
	_, err = s.SetNodeMark(ctx, "task", "risk", "cleared", "Synthetic judgment resolution", userIDs["admin"])
	require.NoError(t, err)
	done := "done"
	_, err = s.UpdateSpec(ctx, "task", nil, &done, nil, nil, nil)
	require.NoError(t, err)
	view = read("current-view", "node-events", "", "")
	require.Nil(t, view.Error)
	counts, _ = json.Marshal(view.Data["nodeEventCounts"])
	require.JSONEq(t, `[{"taskSlug":"task","retries":51,"reworks":1,"gitUndos":0}]`, string(counts), "completion and cleared judgment cannot erase occurrences")

	agent, err := users.CreateServiceAccount(ctx, &storage.User{Kind: storage.KindServiceAccount, DisplayName: "Own event Agent", Role: "reader", OwnerUserID: userIDs["admin"]})
	require.NoError(t, err)
	agentSecret, agentHash, err := auth.GenerateAPIKeySecret()
	require.NoError(t, err)
	agentKey, err := users.CreateAPIKey(ctx, &storage.APIKey{UserID: agent.ID, PHCHash: agentHash})
	require.NoError(t, err)
	credentials["agent"] = auth.FormatAPIKeyToken(agentKey.Prefix, agentSecret)
	_, err = s.CreateSpec(ctx, "own-task", "Synthetic own event task", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	approved := "approved"
	_, err = s.UpdateSpec(ctx, "own-task", nil, &approved, nil, nil, nil)
	require.NoError(t, err)
	ownTarget := json.RawMessage(`{"workPurpose":"coordination","purposeGuidance":"Synthetic own event","environmentId":"local","projectId":"native","threadId":"own-thread","workspace":"C:/own-task","createCommandId":"create-own","startCommandId":"start-own","messageId":"message-own"}`)
	ownRun, _, err := s.PrepareRunForOperator(ctx, "own-task", "C:/own-task", userIDs["admin"], "prepare-own", ownTarget)
	require.NoError(t, err)
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, ownRun, "local", "own-thread"))
	ownDelivery, err := s.CreateDelivery(ctx, ownRun, json.RawMessage(`{"summary":"Synthetic own delivery"}`), ownRun)
	require.NoError(t, err)
	ownScope := storage.MailScope{EnvironmentID: "local", ThreadID: "own-thread", ProviderSessionID: "session", ProviderInstanceID: "codex"}
	ownCommand := func(project string, request any) workbenchCommandResponse {
		t.Helper()
		body, err := json.Marshal(map[string]any{"scope": ownScope, "request": request})
		require.NoError(t, err)
		input, err := json.Marshal(workbenchCommandRequest{ID: "own", Operation: "record-own-node-event", Project: project, Credential: credentials["agent"], Body: body})
		require.NoError(t, err)
		var output bytes.Buffer
		err = runWorkbenchCommandStdio(ctx, bytes.NewReader(append(input, '\n')), &output, func(callCtx context.Context, req workbenchCommandRequest, procedure string) (json.RawMessage, error) {
			require.Equal(t, auth.WorkbenchRecordOwnNodeEventProcedure, procedure)
			identity, err := resolveWorkbenchOperator(callCtx, s, req.Credential, nil, procedure)
			if err != nil {
				return nil, err
			}
			event, err := server.ExecuteOwnNodeEvent(auth.WithIdentity(callCtx, identity), s, req.Project, bytes.NewReader(req.Body))
			if err != nil {
				return nil, err
			}
			return json.Marshal(event)
		})
		require.NoError(t, err)
		var response workbenchCommandResponse
		require.NoError(t, json.Unmarshal(output.Bytes(), &response))
		return response
	}
	ownRetry := storage.RecordOwnNodeEventRequest{EventID: "own-retry", Kind: "retry", Reason: "Agent declares its own formal retry"}
	ownSaved := ownCommand("", ownRetry)
	require.Nil(t, ownSaved.Error)
	var ownEvent storage.NodeExecutionEvent
	require.NoError(t, json.Unmarshal(ownSaved.Data, &ownEvent))
	require.Equal(t, ownRun, ownEvent.Actor)
	require.Equal(t, ownRun, *ownEvent.RunID)
	require.Equal(t, ownRun, *ownEvent.PreviousRunID)
	require.Nil(t, ownEvent.DeliveryID)
	require.Equal(t, "conflict", ownCommand("", ownRetry).Error.Code)
	ownRework := storage.RecordOwnNodeEventRequest{EventID: "own-rework", Kind: "rework", Reason: "Agent names its own prior delivery", DeliveryID: &ownDelivery}
	ownSaved = ownCommand("node-events", ownRework)
	require.Nil(t, ownSaved.Error)
	require.NoError(t, json.Unmarshal(ownSaved.Data, &ownEvent))
	require.Equal(t, ownRun, ownEvent.Actor)
	require.Nil(t, ownEvent.RunID)
	require.Equal(t, ownDelivery, *ownEvent.DeliveryID)
	commitA, commitB := strings.Repeat("a", 40), strings.Repeat("b", 40)
	reset := storage.RecordOwnNodeEventRequest{EventID: "own-reset", Kind: "git_undo", Reason: "Host observed unchanged HEAD reference", ExpectedRunID: ownRun,
		GitUndo: &storage.NodeGitUndo{Operation: "reset", SourceCommit: commitA, ResultCommit: commitA}}
	ownSaved = ownCommand("node-events", reset)
	require.Nil(t, ownSaved.Error)
	require.NoError(t, json.Unmarshal(ownSaved.Data, &ownEvent))
	require.Equal(t, ownRun, ownEvent.Actor)
	require.Equal(t, ownRun, *ownEvent.RunID)
	require.Nil(t, ownEvent.PreviousRunID)
	require.Nil(t, ownEvent.DeliveryID)
	require.Equal(t, *reset.GitUndo, *ownEvent.GitUndo)
	require.Equal(t, "conflict", ownCommand("node-events", reset).Error.Code)
	revert := storage.RecordOwnNodeEventRequest{EventID: "own-revert", Kind: "git_undo", Reason: "Host observed two fixed commit references", ExpectedRunID: ownRun,
		GitUndo: &storage.NodeGitUndo{Operation: "revert", SourceCommit: commitA, ResultCommit: commitB}}
	require.Nil(t, ownCommand("node-events", revert).Error)
	staleUndo := revert
	staleUndo.EventID = "own-stale-run"
	staleUndo.ExpectedRunID = otherRun
	require.Equal(t, "conflict", ownCommand("node-events", staleUndo).Error.Code)
	missingRun := revert
	missingRun.EventID = "own-missing-run"
	missingRun.ExpectedRunID = ""
	require.Equal(t, "conflict", ownCommand("node-events", missingRun).Error.Code)
	badSHA := revert
	badSHA.EventID = "own-bad-sha"
	badSHA.GitUndo = &storage.NodeGitUndo{Operation: "revert", SourceCommit: "not-a-sha", ResultCommit: commitB}
	require.Equal(t, "invalid_argument", ownCommand("node-events", badSHA).Error.Code)
	sameRevert := revert
	sameRevert.EventID = "own-same-revert"
	sameRevert.GitUndo = &storage.NodeGitUndo{Operation: "revert", SourceCommit: commitA, ResultCommit: commitA}
	require.Equal(t, "invalid_argument", ownCommand("node-events", sameRevert).Error.Code)
	caseOnlyRevert := revert
	caseOnlyRevert.EventID = "own-case-only-revert"
	caseOnlyRevert.GitUndo = &storage.NodeGitUndo{Operation: "revert", SourceCommit: commitA, ResultCommit: strings.ToUpper(commitA)}
	require.Equal(t, "invalid_argument", ownCommand("node-events", caseOnlyRevert).Error.Code, "hex letter case cannot turn one commit into two objects")
	badRetryUndo := ownRetry
	badRetryUndo.EventID = "own-retry-undo"
	badRetryUndo.GitUndo = reset.GitUndo
	require.Equal(t, "invalid_argument", ownCommand("node-events", badRetryUndo).Error.Code)
	humanUndo := storage.RecordNodeEventRequest{EventID: "human-revert", Kind: "git_undo", Reason: "Human records fixed Git refs, not execution", RunID: &run,
		GitUndo: &storage.NodeGitUndo{Operation: "revert", SourceCommit: commitA, ResultCommit: commitB}}
	require.Nil(t, command("admin", "node-events", "task", humanUndo).Error)
	humanUndo.EventID = "human-wrong-run"
	humanUndo.RunID = &otherRun
	require.Equal(t, "invalid_argument", command("admin", "node-events", "task", humanUndo).Error.Code)
	for _, invalid := range []storage.RecordOwnNodeEventRequest{
		{EventID: "own-other-task-run", Kind: "retry", Reason: "Wrong task", PreviousRunID: &otherRun},
		{EventID: "own-foreign-run", Kind: "retry", Reason: "Wrong project", PreviousRunID: &foreignRun},
		{EventID: "own-other-task-delivery", Kind: "rework", Reason: "Wrong task", DeliveryID: &otherDelivery},
		{EventID: "own-foreign-delivery", Kind: "rework", Reason: "Wrong project", DeliveryID: &foreignDelivery},
	} {
		require.Equal(t, "invalid_argument", ownCommand("node-events", invalid).Error.Code)
	}
	forgedOwn := ownCommand("node-events", map[string]any{"eventId": "own-forged", "kind": "retry", "reason": "Model selects actor", "actor": "fake", "taskSlug": "task", "runId": otherRun})
	require.Equal(t, "invalid_argument", forgedOwn.Error.Code)
	knowledgeBody, err := json.Marshal(map[string]string{"environment_id": "local", "native_project_id": "native", "taskSlug": "own-task"})
	require.NoError(t, err)
	knowledgeInput, err := json.Marshal(workbenchCommandRequest{ID: "own-read", Operation: "node-events", Credential: credentials["agent"], Body: knowledgeBody})
	require.NoError(t, err)
	var knowledgeOutput bytes.Buffer
	err = runWorkbenchCommandStdio(ctx, bytes.NewReader(append(knowledgeInput, '\n')), &knowledgeOutput, func(callCtx context.Context, req workbenchCommandRequest, procedure string) (json.RawMessage, error) {
		require.Equal(t, auth.WorkbenchReadProgramRunProcedure, procedure)
		identity, err := resolveWorkbenchOperator(callCtx, s, req.Credential, nil, procedure)
		if err != nil {
			return nil, err
		}
		page, err := server.ExecuteLocalWorkbenchKnowledge(auth.WithIdentity(callCtx, identity), s, req.Project, req.Operation, bytes.NewReader(req.Body))
		if err != nil {
			return nil, err
		}
		return json.Marshal(page)
	})
	require.NoError(t, err)
	var knowledgeResponse workbenchCommandResponse
	require.NoError(t, json.Unmarshal(knowledgeOutput.Bytes(), &knowledgeResponse))
	require.Nil(t, knowledgeResponse.Error)
	var ownPage storage.NodeEventPage
	require.NoError(t, json.Unmarshal(knowledgeResponse.Data, &ownPage))
	require.Len(t, ownPage.Events, 4)
	require.Equal(t, "own-task", ownPage.TaskSlug)
	require.Equal(t, "revert", ownPage.Events[0].GitUndo.Operation)
	wrongScope := ownScope
	wrongScope.ThreadID = "missing-thread"
	_, err = s.RecordOwnNodeEvent(auth.WithIdentity(ctx, &auth.Identity{UserID: agent.ID, UserKind: storage.KindServiceAccount}), wrongScope, storage.RecordOwnNodeEventRequest{EventID: "no-binding", Kind: "retry", Reason: "No current run"})
	require.ErrorIs(t, err, storage.ErrReviewForbidden)
	_, err = s.CreateSpec(ctx, "ambiguous-task", "Second bound run with the same native thread", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	_, err = s.UpdateSpec(ctx, "ambiguous-task", nil, &approved, nil, nil, nil)
	require.NoError(t, err)
	ambiguousTarget := json.RawMessage(`{"workPurpose":"coordination","purposeGuidance":"Synthetic duplicate native thread","environmentId":"local","projectId":"native","threadId":"own-thread","workspace":"C:/ambiguous","createCommandId":"create-ambiguous","startCommandId":"start-ambiguous","messageId":"message-ambiguous"}`)
	ambiguousRun, _, err := s.PrepareRunForOperator(ctx, "ambiguous-task", "C:/ambiguous", userIDs["admin"], "prepare-ambiguous", ambiguousTarget)
	require.NoError(t, err)
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, ambiguousRun, "local", "own-thread"))
	_, err = s.RecordOwnNodeEvent(auth.WithIdentity(ctx, &auth.Identity{UserID: agent.ID, UserKind: storage.KindServiceAccount}), ownScope, storage.RecordOwnNodeEventRequest{EventID: "ambiguous-binding", Kind: "retry", Reason: "Ambiguous native session"})
	require.ErrorIs(t, err, storage.ErrReviewForbidden)
	metadata, err := s.ReadWorkbenchMetadata(ctx)
	require.NoError(t, err)
	for _, count := range metadata.NodeEventCounts {
		if count.TaskSlug == "own-task" {
			require.EqualValues(t, 1, count.Retries)
			require.EqualValues(t, 1, count.Reworks)
			require.EqualValues(t, 2, count.GitUndos)
		} else if count.TaskSlug == "task" {
			require.EqualValues(t, 51, count.Retries)
			require.EqualValues(t, 1, count.Reworks)
			require.EqualValues(t, 1, count.GitUndos)
		}
	}
}
