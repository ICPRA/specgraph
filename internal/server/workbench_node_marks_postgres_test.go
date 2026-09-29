// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package server

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/specgraph/specgraph/internal/storage/postgres/postgrestest"
	"github.com/stretchr/testify/require"
)

func TestWorkbenchNodeMarksPostgres(t *testing.T) {
	ctx := context.Background()
	url, err := postgrestest.ConnString(ctx)
	require.NoError(t, err)
	s, err := postgres.New(ctx, url, postgres.WithProject("node-marks"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(ctx)) })
	_, err = s.CreateSpec(ctx, "task", "Preserve ordinary spec state", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	before, err := s.GetSpec(ctx, "task")
	require.NoError(t, err)
	_, err = s.EnsureProject(ctx, "node-marks-other")
	require.NoError(t, err)
	other, err := s.ScopedExisting(ctx, "node-marks-other")
	require.NoError(t, err)
	_, err = other.CreateSpec(ctx, "task", "Separate project task", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	_, err = other.SetNodeMark(ctx, "task", "risk", "high", "Other project", "other-actor")
	require.NoError(t, err)
	view, _, err := ReadWorkbenchCurrentView(ctx, s, "node-marks")
	require.NoError(t, err)
	require.Empty(t, view["nodeMarks"])
	require.False(t, view["capabilities"].(map[string]bool)["nodeMarks"], "HTTP must not advertise an unwired write")
	mark := func(kind, value, reason string) postgres.NodeMark {
		t.Helper()
		body, err := json.Marshal(map[string]string{"kind": kind, "value": value, "reason": reason})
		require.NoError(t, err)
		receipt, err := ExecuteWorkbenchCommand(ctx, s, "set-node-mark", "node-marks", "task", "authenticated-operator", body)
		require.NoError(t, err)
		var result postgres.NodeMark
		require.NoError(t, json.Unmarshal(receipt, &result))
		require.Equal(t, "authenticated-operator", result.Actor)
		require.Equal(t, "task", result.TaskSlug)
		require.Equal(t, reason, result.Reason)
		require.NotZero(t, result.CreatedAt)
		id, err := strconv.ParseInt(result.ID, 10, 64)
		require.NoError(t, err)
		require.Positive(t, id)
		return result
	}
	watch := mark("risk", "watch", "Needs review")
	high := mark("risk", "high", "New evidence")
	critical := mark("critical", "marked", "Business-critical path")
	view, _, err = ReadWorkbenchCurrentView(ctx, s, "node-marks")
	require.NoError(t, err)
	require.ElementsMatch(t, []postgres.NodeMark{high, critical}, view["nodeMarks"])
	cleared := mark("risk", "cleared", "Risk reviewed and resolved")
	criticalClear := mark("critical", "cleared", "No longer critical")
	view, _, err = ReadWorkbenchCurrentView(ctx, s, "node-marks")
	require.NoError(t, err)
	require.ElementsMatch(t, []postgres.NodeMark{cleared, criticalClear}, view["nodeMarks"])
	for range 50 {
		mark("risk", "watch", "Synthetic pagination revision")
	}
	last := mark("risk", "cleared", "Final review")
	first, err := s.ReadNodeMarkHistory(ctx, "task", 0)
	require.NoError(t, err)
	require.Len(t, first.Items, 50)
	require.True(t, first.HasMore)
	require.Equal(t, last.ID, first.Items[0].ID, "numeric ordering must not sort decimal IDs as text")
	cursor, err := strconv.ParseInt(first.NextCursor, 10, 64)
	require.NoError(t, err)
	second, err := s.ReadNodeMarkHistory(ctx, "task", cursor)
	require.NoError(t, err)
	require.Len(t, second.Items, 6)
	require.False(t, second.HasMore)
	require.Empty(t, second.NextCursor)
	require.Equal(t, watch.ID, second.Items[5].ID)
	for _, item := range append(first.Items, second.Items...) {
		require.Equal(t, "authenticated-operator", item.Actor)
	}
	view, _, err = ReadWorkbenchCurrentView(ctx, s, "node-marks")
	require.NoError(t, err)
	require.ElementsMatch(t, []postgres.NodeMark{last, criticalClear}, view["nodeMarks"])
	otherHistory, err := other.ReadNodeMarkHistory(ctx, "task", 0)
	require.NoError(t, err)
	require.Len(t, otherHistory.Items, 1)
	require.Equal(t, "other-actor", otherHistory.Items[0].Actor)
	_, err = s.ReadNodeMarkHistory(ctx, "missing", 0)
	require.ErrorIs(t, err, storage.ErrSpecNotFound)
	_, err = s.SetNodeMark(ctx, "missing", "risk", "watch", "Unknown task", "operator")
	require.ErrorIs(t, err, storage.ErrSpecNotFound)
	for _, body := range []string{
		`{"kind":"risk","value":"marked","reason":"wrong value"}`,
		`{"kind":"critical","value":"high","reason":"wrong value"}`,
		`{"kind":"risk","value":"watch","reason":" "}`,
		`{"kind":"risk","value":"watch","reason":"ok","actor":"forged"}`,
		`{"kind":"risk","value":"watch","reason":false}`,
		`{"kind":"risk","value":"watch","reason":"` + strings.Repeat("x", 4001) + `"}`,
	} {
		_, err := ExecuteWorkbenchCommand(ctx, s, "set-node-mark", "node-marks", "task", "authenticated-operator", json.RawMessage(body))
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	}
	after, err := s.GetSpec(ctx, "task")
	require.NoError(t, err)
	require.Equal(t, before, after, "marks must not change stage, version, content or spec timestamps")
	var marks, changes, runs int
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT (SELECT count(*) FROM node_marks WHERE project_slug='node-marks'),(SELECT count(*) FROM changelog_entries WHERE project_slug='node-marks'),(SELECT count(*) FROM run_bindings WHERE project_slug='node-marks')`).Scan(&marks, &changes, &runs))
	require.Equal(t, 56, marks)
	require.Equal(t, 1, changes, "only original spec creation belongs in spec changelog")
	require.Zero(t, runs)
}

func TestWorkbenchPMNodeMarksPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	url, err := postgrestest.ConnString(ctx)
	require.NoError(t, err)
	s, err := postgres.New(ctx, url, postgres.WithProject("pm-node-marks"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	runs := map[string]string{}
	for _, slug := range []string{"task", "manager", "worker"} {
		_, err = s.CreateSpec(ctx, slug, "Synthetic PM mark fixture", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
		require.NoError(t, err)
		if slug == "task" {
			continue
		}
		approved := "approved"
		_, err = s.UpdateSpec(ctx, slug, nil, &approved, nil, nil, nil)
		require.NoError(t, err)
		runs[slug], err = s.PrepareRun(ctx, slug, "workspace-"+slug)
		require.NoError(t, err)
		require.NoError(t, s.BindRunThreadInEnvironment(ctx, runs[slug], "local", "thread-"+slug))
	}
	_, err = s.Pool().Exec(ctx, `UPDATE context_packages SET body=jsonb_set(body,'{dispatch_target}',
		'{"assignmentRole":"manager","environmentId":"local","projectId":"native"}'::jsonb)
		WHERE id=(SELECT package_id FROM run_bindings WHERE id=$1)`, runs["manager"])
	require.NoError(t, err)
	_, err = s.EnsureProject(ctx, "pm-marks-other")
	require.NoError(t, err)
	other, err := s.ScopedExisting(ctx, "pm-marks-other")
	require.NoError(t, err)
	_, err = other.CreateSpec(ctx, "task", "Other project fixture", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	foreign, err := other.SetNodeMark(ctx, "task", "risk", "high", "Other project basis", "other-human")
	require.NoError(t, err)
	before, err := s.GetSpec(ctx, "task")
	require.NoError(t, err)
	counts := func() [4]int {
		t.Helper()
		var result [4]int
		require.NoError(t, s.Pool().QueryRow(ctx, `SELECT
			(SELECT count(*) FROM changelog_entries WHERE project_slug='pm-node-marks'),
			(SELECT count(*) FROM run_bindings WHERE project_slug='pm-node-marks'),
			(SELECT count(*) FROM execution_events WHERE project_slug='pm-node-marks'),
			(SELECT count(*) FROM node_execution_events WHERE project_slug='pm-node-marks')`).Scan(&result[0], &result[1], &result[2], &result[3]))
		return result
	}
	beforeCounts := counts()
	scope := storage.MailScope{EnvironmentID: "local", ThreadID: "thread-manager", ProviderSessionID: "session", ProviderInstanceID: "codex"}
	body := map[string]any{"scope": scope, "taskSlug": "task"}
	call := func(project, kind, value, expected string) (json.RawMessage, error) {
		t.Helper()
		body["request"] = map[string]string{"kind": kind, "value": value, "reason": "Synthetic PM evidence", "expectedMarkId": expected}
		encoded, err := json.Marshal(body)
		require.NoError(t, err)
		return ExecuteAgentPlanning(ctx, s, project, "pm-set-node-mark", strings.NewReader(string(encoded)))
	}
	result, err := call("", "risk", "watch", "")
	require.NoError(t, err)
	var risk postgres.NodeMark
	require.NoError(t, json.Unmarshal(result, &risk))
	require.Equal(t, runs["manager"], risk.Actor)
	_, err = call("pm-node-marks", "risk", "cleared", "")
	require.ErrorIs(t, err, storage.ErrConcurrentModification, "unknown-response resubmission must not append twice")
	_, err = call("pm-node-marks", "risk", "cleared", foreign.ID)
	require.ErrorIs(t, err, storage.ErrConcurrentModification, "a foreign project's ID is not this project's baseline")
	_, err = call("pm-node-marks", "critical", "marked", risk.ID)
	require.ErrorIs(t, err, storage.ErrConcurrentModification, "baseline must match the requested kind")
	_, err = call("pm-node-marks", "critical", "marked", "")
	require.NoError(t, err, "a different kind's write does not stale the risk baseline")
	_, err = call("pm-node-marks", "risk", "high", risk.ID)
	require.NoError(t, err)
	for _, thread := range []string{"thread-worker", "thread-missing"} {
		otherScope := scope
		otherScope.ThreadID = thread
		body["scope"] = otherScope
		_, err = call("pm-node-marks", "risk", "watch", "")
		require.True(t, errors.Is(err, storage.ErrReviewForbidden) || errors.Is(err, storage.ErrPlanningForbidden))
	}
	body["scope"] = scope
	_, err = call("pm-marks-other", "risk", "watch", "")
	require.ErrorIs(t, err, storage.ErrReviewForbidden)
	for _, state := range []string{"completed", "handed_off", "cancelled"} {
		_, err = s.Pool().Exec(ctx, `UPDATE run_bindings SET state=$1 WHERE id=$2`, state, runs["manager"])
		require.NoError(t, err)
		_, err = call("pm-node-marks", "risk", "watch", "")
		require.ErrorIs(t, err, storage.ErrReviewForbidden)
	}
	_, err = s.Pool().Exec(ctx, `UPDATE run_bindings SET state='bound' WHERE id=$1`, runs["manager"])
	require.NoError(t, err)
	for _, invalidRequest := range []string{
		`{"kind":"risk","value":"watch","reason":"basis"}`,
		`{"kind":"risk","value":"watch","reason":"basis","expectedMarkId":null}`,
		`{"kind":"risk","value":"watch","reason":"basis","expectedMarkId":1}`,
		`{"kind":"risk","value":"watch","reason":"basis","expectedMarkId":false}`,
		`{"kind":"risk","value":"watch","reason":"basis","expectedMarkId":"01"}`,
		`{"kind":"risk","value":"watch","reason":"basis","expectedMarkId":"0"}`,
		`{"kind":"risk","value":"watch","reason":"basis","expectedMarkId":"-1"}`,
		`{"kind":"risk","value":"watch","reason":"basis","expectedMarkId":"9223372036854775808"}`,
		`{"kind":"risk","value":"watch","reason":"basis","expectedMarkId":"","actor":"forged"}`,
		`{"kind":"critical","value":"high","reason":"basis","expectedMarkId":""}`,
		`{"kind":"risk","value":"watch","reason":" ","expectedMarkId":""}`,
	} {
		body["request"] = json.RawMessage(invalidRequest)
		encoded, err := json.Marshal(body)
		require.NoError(t, err)
		_, err = ExecuteAgentPlanning(ctx, s, "pm-node-marks", "pm-set-node-mark", strings.NewReader(string(encoded)))
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), invalidRequest)
	}
	body["actor"] = "forged"
	_, err = call("pm-node-marks", "risk", "watch", "")
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	delete(body, "actor")

	history, err := s.ReadNodeMarkHistory(ctx, "task", 0)
	require.NoError(t, err)
	staleID := history.Items[0].ID
	pmBody, err := json.Marshal(map[string]any{"scope": scope, "taskSlug": "task", "request": map[string]string{"kind": "risk", "value": "cleared", "reason": "Stale synthetic PM evidence", "expectedMarkId": staleID}})
	require.NoError(t, err)
	pmResult := make(chan error, 1)
	err = s.RunInTransaction(ctx, func(txCtx context.Context) error {
		_, err := ExecuteWorkbenchCommand(txCtx, s, "set-node-mark", "pm-node-marks", "task", "human-operator", json.RawMessage(`{"kind":"risk","value":"watch","reason":"New human basis"}`))
		if err != nil {
			return err
		}
		go func() {
			_, err := ExecuteAgentPlanning(ctx, s, "pm-node-marks", "pm-set-node-mark", strings.NewReader(string(pmBody)))
			pmResult <- err
		}()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			var waiting bool
			if err := s.Pool().QueryRow(txCtx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity
				WHERE datname=current_database() AND wait_event_type='Lock'
				AND query LIKE '%SELECT slug FROM projects%FOR NO KEY UPDATE%')`).Scan(&waiting); err != nil {
				return err
			}
			if waiting {
				return nil
			}
			time.Sleep(10 * time.Millisecond)
		}
		return errors.New("PM did not wait for the original human mark transaction")
	})
	require.NoError(t, err)
	require.ErrorIs(t, <-pmResult, storage.ErrConcurrentModification)
	history, err = s.ReadNodeMarkHistory(ctx, "task", 0)
	require.NoError(t, err)
	require.Len(t, history.Items, 4)
	require.Equal(t, "human-operator", history.Items[0].Actor)
	require.Equal(t, "New human basis", history.Items[0].Reason)

	for range 48 {
		_, err := s.SetNodeMark(ctx, "task", "risk", "watch", "Synthetic pagination record", "human-operator")
		require.NoError(t, err)
	}
	readBody := map[string]string{"environment_id": "local", "native_project_id": "native", "taskSlug": "task"}
	encoded, err := json.Marshal(readBody)
	require.NoError(t, err)
	page, err := ExecuteLocalWorkbenchKnowledge(ctx, s, "", "node-mark-history", strings.NewReader(string(encoded)))
	require.NoError(t, err)
	first := page.(*postgres.NodeMarkHistory)
	require.Len(t, first.Items, 50)
	require.True(t, first.HasMore)
	readBody["cursor"] = first.NextCursor
	encoded, err = json.Marshal(readBody)
	require.NoError(t, err)
	page, err = ExecuteLocalWorkbenchKnowledge(ctx, s, "", "node-mark-history", strings.NewReader(string(encoded)))
	require.NoError(t, err)
	second := page.(*postgres.NodeMarkHistory)
	require.Len(t, second.Items, 2)
	require.False(t, second.HasMore)
	require.Empty(t, second.NextCursor)
	require.Equal(t, risk, second.Items[1])
	for _, cursor := range []string{"0", "01", "+1", "-1", "1.2", "9223372036854775808"} {
		readBody["cursor"] = cursor
		encoded, err = json.Marshal(readBody)
		require.NoError(t, err)
		_, err = ExecuteLocalWorkbenchKnowledge(ctx, s, "", "node-mark-history", strings.NewReader(string(encoded)))
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	}
	after, err := s.GetSpec(ctx, "task")
	require.NoError(t, err)
	require.Equal(t, before, after, "marks must not modify Spec state")
	require.Equal(t, beforeCounts, counts(), "marks and history reads must not change spec logs, runs or execution/risk event counters")
}
