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

type bindingTx struct {
	workbenchReadTx
	exec func(string, ...any) (pgconn.CommandTag, error)
}

func (tx bindingTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return tx.exec(sql, args...)
}

type bindingRow func(...any) error

func (row bindingRow) Scan(dest ...any) error { return row(dest...) }

// These checks pin the serializing bind owner; the integration fixture covers PostgreSQL behavior.
func TestBindRunThreadConditionalUpdate(t *testing.T) {
	now := time.Date(2026, 9, 20, 1, 2, 3, 0, time.UTC)
	for _, project := range []string{"alpha", "beta"} {
		t.Run(project, func(t *testing.T) {
			execs, reads := 0, 0
			state, thread := "prepared", ""
			tx := bindingTx{exec: func(sql string, args ...any) (pgconn.CommandTag, error) {
				execs++
				if !strings.Contains(sql, "UPDATE run_bindings SET thread_ref=$3") || len(args) != 5 || args[0] != project || args[1] != "run-1" || args[2] != "thread-1" || args[3] != "" || args[4] != now {
					t.Fatalf("unexpected first bind write: %s %v", sql, args)
				}
				state, thread = "bound", "thread-1"
				return pgconn.NewCommandTag("UPDATE 1"), nil
			}, workbenchReadTx: workbenchReadTx{row: func(sql string, args ...any) pgx.Row {
				reads++
				return bindingRow(func(dest ...any) error {
					switch {
					case strings.Contains(sql, "FROM projects WHERE slug"):
						*dest[0].(*string) = project
					case strings.Contains(sql, "FROM run_bindings rb"):
						*dest[0].(*string), *dest[1].(*string), *dest[2].(*string) = "task", state, thread
						*dest[3].(*string), *dest[4].(*string), *dest[5].(*bool) = "", "agent", true
					case strings.Contains(sql, "FROM specs WHERE"):
						*dest[0].(**string), *dest[1].(*bool) = nil, false
					default:
						t.Fatalf("unexpected bind read: %s %v", sql, args)
					}
					return nil
				})
			}}}
			store := &Store{project: project, nowFunc: func() time.Time { return now }}
			if err := store.BindRunThread(txToContext(context.Background(), tx), "run-1", "thread-1"); err != nil {
				t.Fatal(err)
			}
			if err := store.BindRunThread(txToContext(context.Background(), tx), "run-1", "thread-1"); err != nil {
				t.Fatal(err)
			}
			if execs != 1 || reads != 5 {
				t.Fatalf("first bind should write once and exact replay should not: %d writes, %d reads", execs, reads)
			}
		})
	}
}

func TestBindRunThreadFailures(t *testing.T) {
	failure := errors.New("database failure")
	for _, tc := range []struct {
		name                       string
		state, thread              string
		bindingErr, writeErr, want error
		pending                    bool
	}{
		{name: "identity conflict", state: "bound", thread: "other", want: storage.ErrRunBindingConflict},
		{name: "missing binding", bindingErr: pgx.ErrNoRows, want: storage.ErrRunBindingNotFound},
		{name: "lookup error", bindingErr: failure, want: failure},
		{name: "pending takeover", state: "prepared", pending: true, want: storage.ErrNodeOwnershipPending},
		{name: "update error", state: "prepared", writeErr: failure, want: failure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := bindingTx{exec: func(string, ...any) (pgconn.CommandTag, error) { return pgconn.NewCommandTag("UPDATE 0"), tc.writeErr },
				workbenchReadTx: workbenchReadTx{row: func(sql string, _ ...any) pgx.Row {
					return bindingRow(func(dest ...any) error {
						switch {
						case strings.Contains(sql, "FROM projects WHERE slug"):
							*dest[0].(*string) = "alpha"
						case strings.Contains(sql, "FROM run_bindings rb"):
							if tc.bindingErr != nil {
								return tc.bindingErr
							}
							*dest[0].(*string), *dest[1].(*string), *dest[2].(*string) = "task", tc.state, tc.thread
							*dest[3].(*string), *dest[4].(*string), *dest[5].(*bool) = "", "agent", true
						case strings.Contains(sql, "FROM specs WHERE"):
							*dest[0].(**string), *dest[1].(*bool) = nil, tc.pending
						default:
							t.Fatalf("unexpected bind read: %s", sql)
						}
						return nil
					})
				}}}
			store := &Store{project: "alpha", nowFunc: time.Now}
			err := store.BindRunThread(txToContext(context.Background(), tx), "run-1", "thread-1")
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}
