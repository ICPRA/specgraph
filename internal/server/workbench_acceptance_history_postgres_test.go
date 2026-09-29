// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/specgraph/specgraph/internal/storage/postgres/postgrestest"
	"github.com/stretchr/testify/require"
)

func TestWorkbenchAcceptanceHistoryHTTPPostgres(t *testing.T) {
	ctx := context.Background()
	url, err := postgrestest.ConnString(ctx)
	require.NoError(t, err)
	s, err := postgres.New(ctx, url, postgres.WithProject("history-http"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(ctx)) })
	_, err = s.EnsureProject(ctx, "history-other")
	require.NoError(t, err)
	_, err = s.CreateSpec(ctx, "task", "History fixture", "p2", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	approved := "approved"
	_, err = s.UpdateSpec(ctx, "task", nil, &approved, nil, nil, nil)
	require.NoError(t, err)
	run, err := s.PrepareRun(ctx, "task", "fixture")
	require.NoError(t, err)
	beforeSubmit, err := s.GetSpec(ctx, "task")
	require.NoError(t, err)
	for _, invalidBody := range []string{
		`{"run_binding_id":"rb-fixture"}`, `{"run_binding_id":"","snapshot":{}}`,
		`{"run_binding_id":"rb-fixture","snapshot":`, `{"run_binding_id":"rb-fixture","snapshot":{},"submitted_by":"forged"}`,
		`{"run_binding_id":"rb-fixture","snapshot":"` + strings.Repeat("x", 32*1024) + `"}`,
	} {
		_, err := ExecuteWorkbenchCommand(ctx, s, "submit-delivery", "history-http", "", "writer", json.RawMessage(invalidBody))
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	}
	body, err := json.Marshal(map[string]any{"run_binding_id": run, "snapshot": json.RawMessage(`{"summary":"Submitted work","files":["main.go"]}`)})
	require.NoError(t, err)
	_, err = ExecuteWorkbenchCommand(ctx, s, "submit-delivery", "history-other", "", "writer", body)
	require.ErrorIs(t, err, storage.ErrRunBindingNotFound)
	submission, err := ExecuteWorkbenchCommand(ctx, s, "submit-delivery", "history-http", "", "writer", body)
	require.NoError(t, err)
	var receipt struct {
		DeliveryID string `json:"delivery_id"`
	}
	require.NoError(t, json.Unmarshal(submission, &receipt))
	require.NotEmpty(t, receipt.DeliveryID)
	delivery := receipt.DeliveryID
	afterSubmit, err := s.GetSpec(ctx, "task")
	require.NoError(t, err)
	require.Equal(t, beforeSubmit, afterSubmit, "submitting a delivery must not modify or complete the node")
	detail, err := s.ReadWorkbenchDelivery(ctx, delivery)
	require.NoError(t, err)
	require.Equal(t, delivery, detail.ID)
	require.Equal(t, run, detail.RunBindingID)
	require.Equal(t, "writer", detail.SubmittedBy)
	require.False(t, detail.SubmittedAt.IsZero())
	require.JSONEq(t, `{"summary":"Submitted work","files":["main.go"]}`, string(detail.Snapshot))
	other, err := s.ScopedExisting(ctx, "history-other")
	require.NoError(t, err)
	_, err = other.ReadWorkbenchDelivery(ctx, delivery)
	require.ErrorIs(t, err, storage.ErrDeliveryNotFound)
	_, err = s.ReviewDelivery(auth.WithIdentity(ctx, &auth.Identity{UserID: "first-reviewer", UserKind: storage.KindHuman}), delivery, "original-basis", "accepted", "accessibility")
	require.NoError(t, err)
	_, err = s.ReviewDelivery(auth.WithIdentity(ctx, &auth.Identity{UserID: "second-reviewer", UserKind: storage.KindHuman}), delivery, "later-basis", "rejected", "Later review")
	require.NoError(t, err)
	reviewCtx := auth.WithIdentity(ctx, &auth.Identity{UserID: "command-reviewer", UserKind: storage.KindHuman})
	beforeReview, err := s.GetSpec(ctx, "task")
	require.NoError(t, err)
	for _, body := range []string{
		`null`, `{}`, `{"verdict":"accepted","basis":" "}`, `{"verdict":"pending","basis":"Reviewed"}`,
		`{"verdict":true,"basis":"Reviewed"}`, `{"verdict":"accepted","basis":null}`,
		`{"verdict":"accepted","basis":"Reviewed","actor":"forged"}`, `{"verdict":"accepted","basis":"Reviewed"} {}`,
		`{"verdict":"accepted","basis":"` + strings.Repeat("a", 4001) + `"}`,
	} {
		_, err := ExecuteWorkbenchCommand(ctx, s, "review-delivery", "history-http", delivery, "command-reviewer", json.RawMessage(body))
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), body)
	}
	for _, verdict := range []string{"accepted", "rejected"} {
		result, err := ExecuteWorkbenchCommand(reviewCtx, s, "review-delivery", "history-http", delivery, "command-reviewer", json.RawMessage(`{"verdict":"`+verdict+`","basis":"Written `+verdict+` review basis"}`))
		require.NoError(t, err)
		var recorded storage.ReviewResult
		require.NoError(t, json.Unmarshal(result, &recorded))
		require.True(t, recorded.Recorded)
		require.Equal(t, delivery, recorded.DeliveryID)
		require.Equal(t, verdict, recorded.Verdict)
		require.Equal(t, "not_requested", recorded.Completion.Status)
	}
	_, err = ExecuteWorkbenchCommand(reviewCtx, s, "review-delivery", "history-other", delivery, "command-reviewer", json.RawMessage(`{"verdict":"accepted","basis":"Wrong project"}`))
	require.ErrorIs(t, err, storage.ErrDeliveryNotFound)
	afterReview, err := s.GetSpec(ctx, "task")
	require.NoError(t, err)
	require.Equal(t, beforeReview, afterReview, "recording a verdict must not complete or modify the node")
	events, err := s.GetExecutionEvents(ctx, "task", 0)
	require.NoError(t, err)
	require.Empty(t, events, "review must not fabricate technical verification or completion events")
	mux := http.NewServeMux()
	registerWorkbenchReads(mux, s)
	for _, project := range []string{"history-http", "history-other"} {
		r := httptest.NewRequest(http.MethodGet, "/wb/current-view", nil)
		r.Header.Set("X-Specgraph-Project", project)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		var view struct {
			Acceptances []postgres.WorkbenchAcceptance `json:"acceptances"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &view))
		if project == "history-other" {
			require.Empty(t, view.Acceptances)
			continue
		}
		require.Len(t, view.Acceptances, 4)
		require.Equal(t, "accepted", view.Acceptances[0].Verdict)
		require.Equal(t, "rejected", view.Acceptances[1].Verdict)
		require.Equal(t, "original-basis", view.Acceptances[0].RequirementsFingerprint)
		require.Equal(t, "first-reviewer", view.Acceptances[0].Approver)
		require.JSONEq(t, `{"review":"accessibility"}`, string(view.Acceptances[0].Conditions))
		require.JSONEq(t, `{}`, string(view.Acceptances[1].Conditions))
		for i, verdict := range []string{"accepted", "rejected"} {
			review := view.Acceptances[i+2]
			require.Equal(t, verdict, review.Verdict)
			require.Equal(t, "command-reviewer", review.Approver)
			require.Empty(t, review.RequirementsFingerprint)
			require.JSONEq(t, `{"review":"Written `+verdict+` review basis"}`, string(review.Conditions))
		}
	}
}
