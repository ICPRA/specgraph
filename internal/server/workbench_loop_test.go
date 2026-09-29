// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/specgraph/specgraph/internal/storage"
)

type failingWorkbenchResponse struct {
	header   http.Header
	statuses []int
	writes   int
}

func (w *failingWorkbenchResponse) Header() http.Header { return w.header }

func (w *failingWorkbenchResponse) WriteHeader(status int) {
	w.statuses = append(w.statuses, status)
}

func (w *failingWorkbenchResponse) Write(_ []byte) (int, error) {
	w.writes++
	return 0, io.ErrClosedPipe
}

func TestWriteLoopJSONReportsWriteFailureWithoutSecondResponse(t *testing.T) {
	logs, restore := captureLogs(t)
	defer restore()
	w := &failingWorkbenchResponse{header: make(http.Header)}
	writeLoopJSON(w, http.StatusAccepted, map[string]string{"detail": "private response content"})
	if len(w.statuses) != 1 || w.statuses[0] != http.StatusAccepted || w.writes != 1 {
		t.Fatalf("response was rewritten after failure: statuses=%v writes=%d", w.statuses, w.writes)
	}
	if strings.Contains(logs.String(), "private response content") {
		t.Fatal("response body must not be logged")
	}
	entry := decodeLine(t, logs)
	if entry["msg"] != "write workbench response failed" || entry["error"] != io.ErrClosedPipe.Error() {
		t.Fatalf("missing transport failure: %v", entry)
	}
	if _, exists := entry["body"]; exists {
		t.Fatal("response body must not be logged")
	}
}

func TestWorkbenchLoopBindingErrors(t *testing.T) {
	for _, test := range []struct {
		err    error
		status int
		code   string
	}{
		{storage.ErrSpecNotApproved, http.StatusPreconditionRequired, "failed_precondition"},
		{storage.ErrSpecTerminal, http.StatusPreconditionRequired, "failed_precondition"},
		{storage.ErrCompletionRequiresRequirementReview, http.StatusPreconditionRequired, "failed_precondition"},
		{storage.ErrDependenciesNotReady, http.StatusPreconditionRequired, "failed_precondition"},
		{storage.ErrSummaryNotExecutable, http.StatusPreconditionRequired, "failed_precondition"},
		{storage.ErrExecutionDependenciesChanged, http.StatusPreconditionRequired, "failed_precondition"},
		{storage.ErrRunBindingConflict, http.StatusConflict, "conflict"},
		{storage.ErrAgentNotClaimOwner, http.StatusConflict, "conflict"},
		{storage.ErrRunBindingNotFound, http.StatusNotFound, "not_found"},
		{storage.ErrProjectNotFound, http.StatusNotFound, "not_found"},
		{storage.ErrDeliveryNotFound, http.StatusNotFound, "not_found"},
		{storage.ErrSubdivisionNotFound, http.StatusNotFound, "not_found"},
		{errors.New("private database failure"), http.StatusInternalServerError, "internal"},
	} {
		w := httptest.NewRecorder()
		(&workbenchLoop{}).mapError(w, "bind run thread", fmt.Errorf("wrapped: %w", test.err))
		var body map[string]string
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if w.Code != test.status || body["code"] != test.code {
			t.Fatalf("%v: %d %v", test.err, w.Code, body)
		}
		if test.code == "internal" && body["error"] != "bind run thread failed" {
			t.Fatalf("internal detail leaked: %v", body)
		}
	}
}
