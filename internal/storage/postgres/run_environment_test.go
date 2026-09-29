// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/specgraph/specgraph/internal/storage"
)

func TestRunEnvironmentBindingValidation(t *testing.T) {
	store := &Store{}
	for _, invalid := range []string{"", " \n", "nul\x00", string([]byte{0xff}), strings.Repeat("x", 257)} {
		for _, identity := range [][2]string{{invalid, "thread"}, {"environment", invalid}} {
			if err := store.BindRunThreadInEnvironment(context.Background(), "run", identity[0], identity[1]); !errors.Is(err, storage.ErrInvalidRunBinding) {
				t.Fatalf("invalid identity %q: %v", identity, err)
			}
		}
	}
}

func TestRunEnvironmentBindingSharedSQL(t *testing.T) {
	var legacySQL string
	now := time.Date(2026, 9, 21, 1, 0, 0, 0, time.UTC)
	store := &Store{project: "alpha", nowFunc: func() time.Time { return now }}
	for _, environment := range []string{"", "env-a", "env-b"} {
		tx := bindingTx{exec: func(sql string, args ...any) (pgconn.CommandTag, error) {
			if environment == "" {
				legacySQL = sql
			}
			if sql != legacySQL || len(args) != 5 || args[0] != "alpha" || args[1] != "run" || args[2] != "same-thread" || args[3] != environment || args[4] != now {
				t.Fatalf("environment identity must share the serialized first-bind path: %s %v", sql, args)
			}
			return pgconn.NewCommandTag("UPDATE 1"), nil
		}, workbenchReadTx: workbenchReadTx{row: func(sql string, _ ...any) pgx.Row {
			return bindingRow(func(dest ...any) error {
				switch {
				case strings.Contains(sql, "FROM projects WHERE slug"):
					*dest[0].(*string) = "alpha"
				case strings.Contains(sql, "FROM run_bindings rb"):
					*dest[0].(*string), *dest[1].(*string), *dest[2].(*string) = "task", "prepared", ""
					*dest[3].(*string), *dest[4].(*string), *dest[5].(*bool) = "", "agent", true
				case strings.Contains(sql, "FROM specs WHERE"):
					*dest[0].(**string), *dest[1].(*bool) = nil, false
				default:
					t.Fatalf("unexpected bind read: %s", sql)
				}
				return nil
			})
		}}}
		ctx := txToContext(context.Background(), tx)
		var err error
		if environment == "" {
			err = store.BindRunThread(ctx, "run", "same-thread")
		} else {
			err = store.BindRunThreadInEnvironment(ctx, "run", environment, "same-thread")
		}
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestRunEnvironmentMailApproval(t *testing.T) {
	scope := storage.MailScope{EnvironmentID: "env-a", ThreadID: "thread", ProviderSessionID: "session", ProviderInstanceID: "instance"}
	insertReached := errors.New("approval insert reached")
	for _, environment := range []string{"", "env-a", "env-b"} {
		t.Run("stored="+environment, func(t *testing.T) {
			reads := 0
			tx := bindingTx{
				workbenchReadTx: workbenchReadTx{row: func(sql string, args ...any) pgx.Row {
					reads++
					return bindingRow(func(dest ...any) error {
						if reads == 1 {
							*dest[0].(*string) = "grant"
							return nil
						}
						if sql != "SELECT thread_ref,state,environment_id FROM run_bindings WHERE project_slug=$1 AND id=$2 FOR NO KEY UPDATE" || len(args) != 2 || args[0] != "alpha" || args[1] != "run" {
							t.Fatalf("run identity lookup: %s %v", sql, args)
						}
						*dest[0].(*string), *dest[1].(*string), *dest[2].(*string) = "thread", "bound", environment
						return nil
					})
				}},
				exec: func(string, ...any) (pgconn.CommandTag, error) { return pgconn.CommandTag{}, insertReached },
			}
			store := &Store{project: "alpha", nowFunc: time.Now}
			_, err := store.ApproveMailBinding(txToContext(context.Background(), tx), "grant", "run", scope, "operator")
			want := insertReached
			if environment == "env-b" {
				want = storage.ErrMailConflict
			}
			if !errors.Is(err, want) || reads != 2 {
				t.Fatalf("environment approval: reads=%d err=%v want=%v", reads, err, want)
			}
		})
	}
}

func TestRunEnvironmentMailResolutionSQL(t *testing.T) {
	scope := storage.MailScope{EnvironmentID: "env-a", ThreadID: "thread", ProviderSessionID: "session", ProviderInstanceID: "instance"}
	for _, found := range []bool{false, true} {
		reads, called := 0, false
		tx := workbenchReadTx{row: func(sql string, args ...any) pgx.Row {
			reads++
			return bindingRow(func(dest ...any) error {
				switch reads {
				case 1:
					*dest[0].(*string) = "grant"
				case 2:
					*dest[0].(*string) = "run"
				case 3:
					if sql != "SELECT task_spec_slug FROM run_bindings WHERE project_slug=$1 AND id=$2 AND thread_ref=$3 AND (environment_id='' OR environment_id=$4) AND state='bound' FOR NO KEY UPDATE" || len(args) != 4 || args[0] != "alpha" || args[1] != "run" || args[2] != scope.ThreadID || args[3] != scope.EnvironmentID {
						t.Fatalf("mail actor must enforce stored environment: %s %v", sql, args)
					}
					if !found {
						return pgx.ErrNoRows
					}
					*dest[0].(*string) = "task"
				default:
					t.Fatal("unexpected query")
				}
				return nil
			})
		}}
		err := (&Store{project: "alpha"}).WithMailActor(txToContext(context.Background(), tx), "apikey:bridge", scope, func(_ context.Context, actor storage.MailActor) error {
			called = true
			if actor.RunID != "run" || actor.TaskSlug != "task" {
				t.Fatalf("actor: %+v", actor)
			}
			return nil
		})
		if called != found || reads != 3 || (found && err != nil) || (!found && !errors.Is(err, storage.ErrMailForbidden)) {
			t.Fatalf("resolution: found=%v called=%v reads=%d err=%v", found, called, reads, err)
		}
	}
}
