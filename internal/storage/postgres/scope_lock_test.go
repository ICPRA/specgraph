// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/storage"
)

// A failed project lock must stop each source mutation or eligibility read
// before any spec/decision row is read or locked, including nested transactions.
func TestScopeOperationsLockProjectFirst(t *testing.T) {
	value := "changed"
	for _, test := range []struct {
		name string
		run  func(context.Context, *Store) error
	}{
		{"create spec", func(ctx context.Context, s *Store) error {
			_, err := s.CreateSpec(ctx, "task", "intent", "p1", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
			return err
		}},
		{"update spec", func(ctx context.Context, s *Store) error {
			_, err := s.UpdateSpec(ctx, "task", &value, nil, nil, nil, nil)
			return err
		}},
		{"create decision", func(ctx context.Context, s *Store) error {
			_, err := s.CreateDecision(ctx, "decision", "title", "body", "rationale", "question", nil, "", nil, "", "task", "shape")
			return err
		}},
		{"update decision", func(ctx context.Context, s *Store) error {
			_, err := s.UpdateDecision(ctx, "decision", 0, &value, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
			return err
		}},
		{"transition before shape", func(ctx context.Context, s *Store) error {
			return s.TransitionStage(ctx, "task", storage.SpecStageSpark, storage.SpecStageShape)
		}},
		{"transition before approval", func(ctx context.Context, s *Store) error {
			return s.TransitionStage(ctx, "task", storage.SpecStageDecompose, storage.SpecStageApproved)
		}},
		{"spark output", func(ctx context.Context, s *Store) error {
			return s.StoreSparkOutput(ctx, "task", &storage.SparkOutput{})
		}},
		{"shape output", func(ctx context.Context, s *Store) error {
			return s.StoreShapeOutput(ctx, "task", &storage.ShapeOutput{})
		}},
		{"specify output", func(ctx context.Context, s *Store) error {
			return s.StoreSpecifyOutput(ctx, "task", &storage.SpecifyOutput{})
		}},
		{"decompose output", func(ctx context.Context, s *Store) error {
			_, err := s.StoreDecomposeOutput(ctx, "task", &storage.DecomposeOutput{Strategy: storage.StrategyVerticalSlice})
			return err
		}},
		{"safety flags", func(ctx context.Context, s *Store) error {
			return s.StoreSafetyFlags(ctx, "task", []storage.SafetyFlag{})
		}},
		{"claim eligibility", func(ctx context.Context, s *Store) error {
			_, err := s.ClaimSpec(ctx, "task", "agent", 0)
			return err
		}},
		{"manual completion eligibility", func(ctx context.Context, s *Store) error {
			_, _, err := s.ManualComplete(ctx, "task", "operator", 1, "key", "note")
			return err
		}},
		{"add decided in", func(ctx context.Context, s *Store) error {
			_, err := s.AddEdge(ctx, "task", "decision", storage.EdgeTypeDecidedIn)
			return err
		}},
		{"remove decided in", func(ctx context.Context, s *Store) error {
			return s.RemoveEdge(ctx, "task", "decision", storage.EdgeTypeDecidedIn)
		}},
		{"add composes", func(ctx context.Context, s *Store) error {
			_, err := s.AddEdge(ctx, "parent", "task", storage.EdgeTypeComposes)
			return err
		}},
		{"remove composes", func(ctx context.Context, s *Store) error {
			return s.RemoveEdge(ctx, "parent", "task", storage.EdgeTypeComposes)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			lockFailure := errors.New("project lock unavailable")
			reads := 0
			tx := workbenchReadTx{row: func(sql string, args ...any) pgx.Row {
				reads++
				if sql != `SELECT slug FROM projects WHERE slug = $1 FOR NO KEY UPDATE` || len(args) != 1 || args[0] != "scope-project" {
					t.Fatalf("operation before project lock: %s %v", sql, args)
				}
				return bindingRow(func(...any) error { return lockFailure })
			}}
			err := test.run(txToContext(context.Background(), tx), &Store{project: "scope-project", nowFunc: time.Now})
			if !errors.Is(err, lockFailure) || reads != 1 {
				t.Fatalf("must return the lock failure before accessing source rows: reads=%d err=%v", reads, err)
			}
		})
	}
}
