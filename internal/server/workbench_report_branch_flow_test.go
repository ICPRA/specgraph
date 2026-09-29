// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package server

import (
	"context"
	"strings"
	"testing"
)

func TestReportBranchReadRequiresOneIdentifier(t *testing.T) {
	for _, body := range []string{
		`{"environment_id":"local","native_project_id":"native"}`,
		`{"environment_id":"local","native_project_id":"native","flowId":"flow","runId":"run"}`,
		`{"environment_id":"local","native_project_id":"native","flowId":"flow","admission":"fake"}`,
	} {
		if _, err := ExecuteLocalWorkbenchKnowledge(context.Background(), nil, "", "report-flow-read", strings.NewReader(body)); err == nil {
			t.Fatalf("ambiguous or caller-authored report flow read accepted: %s", body)
		}
	}
}

func TestReportJoinReadRequiresOnlyRun(t *testing.T) {
	for _, body := range []string{
		`{"environment_id":"local","native_project_id":"native"}`,
		`{"environment_id":"local","native_project_id":"native","runId":"run","flowId":"flow"}`,
	} {
		if _, err := ExecuteLocalWorkbenchKnowledge(context.Background(), nil, "", "report-join-read", strings.NewReader(body)); err == nil {
			t.Fatalf("invalid join read accepted: %s", body)
		}
	}
}

func TestReportJudgmentRecordRequiresExplicitPredecessor(t *testing.T) {
	_, err := ExecuteReportBranchJudgmentRecord(context.Background(), nil, "fixture", "report-judgment-record",
		strings.NewReader(`{"flowId":"flow","conditionKey":"J","value":"true","inputRefs":[],"reason":"Explicit choice"}`))
	if err == nil {
		t.Fatal("omitted expectedJudgmentId accepted")
	}
	_, err = ExecuteLocalWorkbenchKnowledge(context.Background(), nil, "", "report-judgment-history",
		strings.NewReader(`{"environment_id":"local","native_project_id":"native","flowId":"flow"}`))
	if err == nil {
		t.Fatal("history without conditionKey accepted")
	}
}

func TestCandidateJudgmentRecordRequiresExplicitPredecessor(t *testing.T) {
	_, err := ExecuteCandidateSatisfactionRecord(context.Background(), nil, "fixture", "candidate-satisfaction-record",
		strings.NewReader(`{"runId":"run","attemptId":"attempt","value":"true","reason":"Explicit choice"}`))
	if err == nil {
		t.Fatal("omitted expectedJudgmentId accepted")
	}
	_, err = ExecuteCandidateSatisfactionRecord(context.Background(), nil, "fixture", "candidate-satisfaction-own",
		strings.NewReader(`{"scope":{},"request":{"attemptId":"attempt","value":"true","reason":"Own choice"}}`))
	if err == nil {
		t.Fatal("own request omitted expectedJudgmentId accepted")
	}
	_, err = ExecuteLocalWorkbenchKnowledge(context.Background(), nil, "", "candidate-satisfaction-history",
		strings.NewReader(`{"environment_id":"local","native_project_id":"native","runId":"run"}`))
	if err == nil {
		t.Fatal("history without attemptId accepted")
	}
}
