// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/specgraph/specgraph/internal/storage"
)

func TestDispatchPurposeBoundary(t *testing.T) {
	for _, purpose := range []string{"requirements", "requirements_review", "design", "design_review", "test_design", "implementation", "test_execution", "coordination", "knowledge", "investigation"} {
		target, err := json.Marshal(map[string]string{"workPurpose": purpose, "purposeGuidance": "Recorded instructions"})
		if err != nil {
			t.Fatal(err)
		}
		if err := checkDispatchPurpose(target); err != nil {
			t.Fatalf("valid purpose %s: %v", purpose, err)
		}
	}
	if err := checkDispatchPurpose(json.RawMessage(`{"assignmentRole":"executor"}`)); !errors.Is(err, storage.ErrInvalidRunPreparation) {
		t.Fatalf("unknown purpose cannot authorize native work: %v", err)
	}
	for _, target := range []string{
		`null`, `[]`, `{}`, `{"assignmentRole":"executor"}`,
		`{"workPurpose":"implementation"}`,
		`{"purposeGuidance":"Recorded instructions"}`,
		`{"workPurpose":"unknown","purposeGuidance":"Recorded instructions"}`,
		`{"workPurpose":"test_design","purposeGuidance":"  "}`,
		`{"workPurpose":null,"purposeGuidance":null}`,
		`{"workPurpose":42,"purposeGuidance":"Recorded instructions"}`,
	} {
		t.Run(target, func(t *testing.T) {
			// Invalid external input must fail before either operation touches storage.
			s := &Store{}
			_, _, err := s.PrepareRunForOperator(context.Background(), "task", "workspace", "operator", "purpose-check", json.RawMessage(target))
			if !errors.Is(err, storage.ErrInvalidRunPreparation) {
				t.Fatalf("prepare: %v", err)
			}
			_, err = s.AuthorizeRunDispatch(context.Background(), "run", "operator", "package", json.RawMessage(target))
			if !errors.Is(err, storage.ErrInvalidRunPreparation) {
				t.Fatalf("authorize: %v", err)
			}
		})
	}
}
