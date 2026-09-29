// SPDX-License-Identifier: Apache-2.0
package render

import (
	"fmt"
	"strings"
	"testing"

	specv1 "github.com/specgraph/specgraph/gen/specgraph/v1"
)

func TestPrimePreservesDecisionAuthorityMetadata(t *testing.T) {
	statuses := []specv1.DecisionStatus{
		specv1.DecisionStatus_DECISION_STATUS_UNSPECIFIED,
		specv1.DecisionStatus_DECISION_STATUS_PROPOSED,
		specv1.DecisionStatus_DECISION_STATUS_ACCEPTED,
		specv1.DecisionStatus_DECISION_STATUS_DEPRECATED,
		specv1.DecisionStatus_DECISION_STATUS_SUPERSEDED,
	}
	view := &specv1.SpecView{}
	for i, status := range statuses {
		view.Decisions = append(view.Decisions, &specv1.Decision{
			Slug: fmt.Sprintf("decision-%d", i), Title: "Same title", Status: status, Version: int32(i + 1),
		})
	}
	view.Decisions[4].SupersededBy = "replacement"
	got := RenderSpecMarkdown(view, RenderOpts{})
	for i, status := range []string{"unspecified", "proposed", "accepted", "deprecated", "superseded"} {
		want := fmt.Sprintf("[decision-%d] Same title (status: %s, version: %d)", i, status, i+1)
		if !strings.Contains(got, want) {
			t.Errorf("missing decision metadata %q in %s", want, got)
		}
	}
	if !strings.Contains(got, "; superseded by: [replacement]") || !strings.Contains(got, "not blanket execution approval") {
		t.Fatalf("missing authority boundary or replacement: %s", got)
	}
}
