// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/stretchr/testify/require"
)

func TestPrepareRunRequiresCompletedSpecPrerequisites(t *testing.T) {
	for _, relation := range []storage.EdgeType{storage.EdgeTypeDependsOn, storage.EdgeTypeBlocks} {
		t.Run(string(relation), func(t *testing.T) {
			project := "prepare-dependency-" + string(relation)
			store := newStore(t, postgres.WithProject(project))
			ctx := context.Background()
			require.NoError(t, store.SetProjectManaged(ctx, project, true))
			approved := "approved"
			for _, slug := range []string{"upstream", "downstream"} {
				_, err := store.CreateSpec(ctx, slug, slug, "p2", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
				require.NoError(t, err)
				_, err = store.UpdateSpec(ctx, slug, nil, &approved, nil, nil, nil)
				require.NoError(t, err)
			}
			from, to := "downstream", "upstream"
			if relation == storage.EdgeTypeBlocks {
				from, to = to, from
			}
			_, err := store.AddEdge(ctx, from, to, relation)
			require.NoError(t, err)
			run, err := store.PrepareRun(ctx, "downstream", "fixture")
			require.ErrorIs(t, err, storage.ErrDependenciesNotReady, "direct preparation must not bypass an unfinished prerequisite")
			require.Empty(t, run)
			claim, err := store.GetActiveClaim(ctx, "downstream")
			require.NoError(t, err)
			require.Nil(t, claim)
			var artifacts int
			require.NoError(t, store.Pool().QueryRow(ctx, `SELECT
				(SELECT count(*) FROM context_packages WHERE project_slug=$1) +
				(SELECT count(*) FROM run_bindings WHERE project_slug=$1)`, project).Scan(&artifacts))
			require.Zero(t, artifacts)
			done := "done"
			_, err = store.UpdateSpec(ctx, "upstream", nil, &done, nil, nil, nil)
			require.NoError(t, err)
			run, err = store.PrepareRun(ctx, "downstream", "fixture")
			require.NoError(t, err)
			require.NotEmpty(t, run)
		})
	}
}

func TestDependencyChangeInvalidatesOnlyActualChanges(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		name := "change"
		if duplicate {
			name = "duplicate"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			project := "dependency-baseline-" + name
			s := newStore(t, postgres.WithProject(project))
			require.NoError(t, s.SetProjectManaged(ctx, project, true))
			for slug, stage := range map[string]string{"task": "approved", "upstream": "done"} {
				_, err := s.CreateSpec(ctx, slug, slug, "p2", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
				require.NoError(t, err)
				_, err = s.UpdateSpec(ctx, slug, nil, &stage, nil, nil, nil)
				require.NoError(t, err)
			}
			if duplicate {
				_, err := s.AddEdge(ctx, "task", "upstream", storage.EdgeTypeDependsOn)
				require.NoError(t, err)
			}
			run, err := s.PrepareRun(ctx, "task", "fixture")
			require.NoError(t, err)
			delivery, err := s.CreateDelivery(ctx, run, json.RawMessage(`{}`), "writer")
			require.NoError(t, err)
			require.NoError(t, s.InsertAcceptance(ctx, delivery, "fixture", "accepted", "operator", nil))
			_, err = s.AddEdge(ctx, "task", "upstream", storage.EdgeTypeDependsOn)
			require.NoError(t, err)
			var changes int
			require.NoError(t, s.Pool().QueryRow(ctx, `SELECT count(*) FROM dependency_changes WHERE project_slug=$1`, project).Scan(&changes))
			require.Equal(t, 1, changes)
			if duplicate {
				require.NoError(t, s.RecordCompletion(ctx, "task", run))
				return
			}
			require.ErrorIs(t, s.RecordCompletion(ctx, "task", run), storage.ErrExecutionDependenciesChanged)
			require.NoError(t, s.RemoveEdge(ctx, "task", "upstream", storage.EdgeTypeDependsOn))
			require.ErrorIs(t, s.RecordCompletion(ctx, "task", run), storage.ErrExecutionDependenciesChanged, "restoring the graph must not erase intervening changes")
			require.NoError(t, s.Pool().QueryRow(ctx, `SELECT count(*) FROM dependency_changes WHERE project_slug=$1`, project).Scan(&changes))
			require.Equal(t, 2, changes)
		})
	}
}

func TestDependencyPublicationOrdersExecution(t *testing.T) {
	for _, operation := range []string{"prepare", "complete"} {
		t.Run(operation, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			project := "dependency-order-" + operation
			s := newStore(t, postgres.WithProject(project))
			require.NoError(t, s.SetProjectManaged(ctx, project, true))
			approved := "approved"
			for _, slug := range []string{"task", "upstream"} {
				_, err := s.CreateSpec(ctx, slug, slug, "p2", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
				require.NoError(t, err)
				_, err = s.UpdateSpec(ctx, slug, nil, &approved, nil, nil, nil)
				require.NoError(t, err)
			}
			var run string
			if operation == "complete" {
				var err error
				run, err = s.PrepareRun(ctx, "task", "fixture")
				require.NoError(t, err)
				delivery, err := s.CreateDelivery(ctx, run, json.RawMessage(`{}`), "writer")
				require.NoError(t, err)
				require.NoError(t, s.InsertAcceptance(ctx, delivery, "fixture", "accepted", "operator", nil))
			}
			publisher, err := pgx.Connect(ctx, connString)
			require.NoError(t, err)
			defer publisher.Close(ctx)
			tx, err := publisher.Begin(ctx)
			require.NoError(t, err)
			defer tx.Rollback(ctx)
			_, err = tx.Exec(ctx, `SELECT slug FROM projects WHERE slug=$1 FOR NO KEY UPDATE`, project)
			require.NoError(t, err)
			_, err = tx.Exec(ctx, `INSERT INTO edges(project_slug,from_slug,to_slug,edge_type) VALUES($1,'task','upstream','DEPENDS_ON')`, project)
			require.NoError(t, err)
			result := make(chan error, 1)
			go func() {
				if operation == "complete" {
					result <- s.RecordCompletion(ctx, "task", run)
				} else {
					_, err := s.PrepareRun(ctx, "task", "fixture")
					result <- err
				}
			}()
			require.Eventually(t, func() bool {
				var waiting bool
				err := s.Pool().QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1::integer = ANY(pg_blocking_pids(pid)))`, int32(publisher.PgConn().PID())).Scan(&waiting)
				return err == nil && waiting
			}, 5*time.Second, 10*time.Millisecond, "execution must wait for the actual graph publisher lock")
			require.NoError(t, tx.Commit(ctx))
			if operation == "complete" {
				require.ErrorIs(t, <-result, storage.ErrExecutionDependenciesChanged)
			} else {
				require.ErrorIs(t, <-result, storage.ErrDependenciesNotReady)
			}
		})
	}
}

func TestDeletingSliceCannotEraseAnExecutionPrerequisite(t *testing.T) {
	for _, relation := range []storage.EdgeType{storage.EdgeTypeDependsOn, storage.EdgeTypeBlocks} {
		t.Run(string(relation), func(t *testing.T) {
			ctx := context.Background()
			s := newStore(t, postgres.WithProject("slice-prerequisite-"+string(relation)))
			for _, slug := range []string{"task", "parent"} {
				_, err := s.CreateSpec(ctx, slug, slug, "p2", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
				require.NoError(t, err)
			}
			approved := "approved"
			_, err := s.UpdateSpec(ctx, "task", nil, &approved, nil, nil, nil)
			require.NoError(t, err)
			children, err := s.StoreDecomposeOutput(ctx, "parent", &storage.DecomposeOutput{Strategy: storage.StrategySingleUnit, Slices: []storage.DecomposeSlice{{ID: "blocker", Intent: "Unresolved work"}}})
			require.NoError(t, err)
			from, to := "task", children[0]
			if relation == storage.EdgeTypeBlocks {
				from, to = to, from
			}
			_, err = s.AddEdge(ctx, from, to, relation)
			require.NoError(t, err)
			_, err = s.PrepareRun(ctx, "task", "fixture")
			require.ErrorIs(t, err, storage.ErrDependenciesNotReady)
			_, err = s.StoreDecomposeOutput(ctx, "parent", &storage.DecomposeOutput{Strategy: storage.StrategySingleUnit})
			require.ErrorIs(t, err, storage.ErrDependencyInUse)
			_, err = s.GetSlice(ctx, children[0])
			require.NoError(t, err)
			_, err = s.PrepareRun(ctx, "task", "fixture")
			require.ErrorIs(t, err, storage.ErrDependenciesNotReady)
			require.NoError(t, s.RemoveEdge(ctx, from, to, relation))
			_, err = s.StoreDecomposeOutput(ctx, "parent", &storage.DecomposeOutput{Strategy: storage.StrategySingleUnit})
			require.NoError(t, err)
			_, err = s.PrepareRun(ctx, "task", "fixture")
			require.NoError(t, err)
		})
	}
}
