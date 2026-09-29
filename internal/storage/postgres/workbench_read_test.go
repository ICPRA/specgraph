// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/specgraph/specgraph/internal/storage"
)

// Embedding leaves every write method unimplemented: any accidental write panics.
type workbenchReadTx struct {
	pgx.Tx
	query func(string, ...any) (pgx.Rows, error)
	row   func(string, ...any) pgx.Row
}

func (tx workbenchReadTx) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	return tx.query(sql, args...)
}
func (tx workbenchReadTx) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	return tx.row(sql, args...)
}

type workbenchReadRow struct {
	project string
	err     error
}

func (row workbenchReadRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}
	*dest[0].(*string) = row.project
	*dest[1].(*[]string) = []string{}
	*dest[2].(*string) = ""
	*dest[3].(*bool) = true
	*dest[4].(*time.Time) = time.Time{}
	*dest[5].(*time.Time) = time.Time{}
	return nil
}

func TestWorkbenchScopedExisting(t *testing.T) {
	for _, project := range []string{"alpha", "beta", "missing"} {
		t.Run(project, func(t *testing.T) {
			queries := 0
			tx := workbenchReadTx{row: func(sql string, args ...any) pgx.Row {
				queries++
				if !strings.HasPrefix(sql, "SELECT ") || !strings.Contains(sql, "FROM projects WHERE slug = $1") || len(args) != 1 || args[0] != project {
					t.Fatalf("non-scoped project read: %s %v", sql, args)
				}
				if project == "missing" {
					return workbenchReadRow{err: pgx.ErrNoRows}
				}
				return workbenchReadRow{project: project}
			}}
			root := &Store{project: "_server", shared: &sharedState{}, nowFunc: time.Now}
			scoped, err := root.ScopedExisting(txToContext(context.Background(), tx), project)
			if queries != 1 {
				t.Fatalf("queries: %d", queries)
			}
			if project == "missing" {
				if !errors.Is(err, storage.ErrProjectNotFound) || scoped != nil {
					t.Fatalf("missing project: %v %v", scoped, err)
				}
				return
			}
			if err != nil || scoped.project != project || scoped.ownsPool || root.project != "_server" || scoped.shared != root.shared {
				t.Fatalf("scoped store: %+v %v", scoped, err)
			}
		})
	}
}

type workbenchReadRows struct {
	pgx.Rows
	fields     []pgconn.FieldDescription
	values     [][]byte
	moreValues [][][]byte
	read       bool
	closed     bool
	err        error
}

func (r *workbenchReadRows) Close()     { r.closed = true }
func (r *workbenchReadRows) Err() error { return r.err }
func (r *workbenchReadRows) Next() bool {
	if r.read && len(r.moreValues) > 0 {
		r.values, r.moreValues = r.moreValues[0], r.moreValues[1:]
		return true
	}
	if r.read || r.values == nil {
		return false
	}
	r.read = true
	return true
}
func (r *workbenchReadRows) FieldDescriptions() []pgconn.FieldDescription { return r.fields }
func (r *workbenchReadRows) Scan(dest ...any) error {
	return pgx.ScanRow(pgtype.NewMap(), r.fields, r.values, dest...)
}

func TestWorkbenchMetadataRead(t *testing.T) {
	columns := []string{
		"id task_spec_slug package_id generation thread_ref environment_id executor_kind native_project_id workspace state created_at updated_at spec_version dispatch_message_id assignment_role work_purpose git_baseline",
		"id run_binding_id submitted_by submitted_at",
		"id delivery_id kind command exit_code verifier created_at",
		"id delivery_id verdict approver created_at requirements_fingerprint conditions actor_kind reviewer_run_id review_request_id",
		"id task_slug kind value reason actor created_at",
		"review_status",
		"task_slug retries reworks",
		"delivery_hook",
		"run_id loop_policy authorized_by_user_id attempt_id attempt_ordinal result stop_id judgment_id judgment_value",
	}
	for _, project := range []string{"alpha", "beta"} {
		for _, empty := range []bool{false, true} {
			for _, loopStatus := range []string{"none", "prepared", "admitted", "ready", "waiting", "needs_human", "stopped", "cancelled"} {
				calls := 0
				var allRows []*workbenchReadRows
				tx := workbenchReadTx{query: func(sql string, args ...any) (pgx.Rows, error) {
					wantArgs := 1
					if calls == 5 {
						wantArgs = 3
					}
					if !strings.HasPrefix(sql, "SELECT ") || len(args) != wantArgs || args[0] != project || !strings.Contains(sql, "project_slug = $1") {
						t.Fatalf("non-scoped metadata read: %s %v", sql, args)
					}
					if calls > 0 && calls < 4 && !strings.Contains(sql, "rb.project_slug = d.project_slug") {
						t.Fatalf("cross-project run relationship: %s", sql)
					}
					if calls == 0 && (!strings.Contains(sql, "LEFT JOIN context_packages cp ON cp.id = rb.package_id AND cp.project_slug = rb.project_slug") || !strings.Contains(sql, "(cp.body->>'spec_version')::integer AS spec_version") || !strings.Contains(sql, "rb.environment_id")) {
						t.Fatalf("run version must come only from its project-scoped package: %s", sql)
					}
					if calls == 2 && !strings.Contains(sql, "d.project_slug = e.project_slug") {
						t.Fatalf("cross-project evidence relationship: %s", sql)
					}
					if calls == 3 && !strings.Contains(sql, "d.project_slug = a.project_slug") {
						t.Fatalf("cross-project acceptance relationship: %s", sql)
					}
					if calls == 3 && (!strings.Contains(sql, "a.requirements_fingerprint, a.conditions") || !strings.Contains(sql, "ORDER BY a.created_at, a.id")) {
						t.Fatalf("acceptance provenance or history order missing: %s", sql)
					}
					if calls == 4 && (!strings.Contains(sql, "DISTINCT ON (task_slug,kind)") || !strings.Contains(sql, "node_marks.id DESC")) {
						t.Fatalf("latest node marks must use numeric revision identity: %s", sql)
					}
					if calls == 7 && (!strings.Contains(sql, "source.project_slug=h.project_slug") || !strings.Contains(sql, "target.project_slug=h.project_slug") || strings.Contains(sql, "context_packages")) {
						t.Fatalf("hook references must use scoped runs without copied context: %s", sql)
					}
					if calls == 8 && (!strings.Contains(sql, "cp.project_slug=rb.project_slug") || !strings.Contains(sql, "project_slug=rb.project_slug AND run_id=rb.id") || !strings.Contains(sql, "attempt_id=a.id") || !strings.Contains(sql, "ORDER BY ordinal DESC LIMIT 1") || !strings.Contains(sql, "ORDER BY sequence DESC LIMIT 1") || strings.Contains(sql, "criterion") || strings.Contains(sql, "synchronousCompletionBasis") || strings.Contains(sql, "summary") || strings.Contains(sql, "reason") || strings.Contains(sql, "SELECT *")) {
						t.Fatalf("loop query must batch scoped facts without bodies or derived status: %s", sql)
					}
					rows := &workbenchReadRows{}
					for _, name := range strings.Fields(columns[calls]) {
						oid, value := uint32(pgtype.TextOID), []byte(name)
						switch name {
						case "run_id":
							value = []byte("id")
						case "loop_policy":
							oid, value = pgtype.JSONBOID, []byte(`{"kind":"innovative","maxAttempts":2,"termination":{"kind":"judgment"}}`)
						case "authorized_by_user_id":
							value = []byte(project + "-human")
						case "attempt_id":
							value = []byte("attempt")
							if loopStatus == "prepared" {
								value = nil
							}
						case "attempt_ordinal":
							oid, value = pgtype.Int4OID, []byte("1")
							if loopStatus == "prepared" {
								value = nil
							}
							if loopStatus == "needs_human" || loopStatus == "stopped" || loopStatus == "cancelled" {
								value = []byte("2")
							}
						case "result":
							oid, value = pgtype.JSONBOID, []byte(`{"outcome":"exited","exitCode":0,"finishedAt":"2026-09-27T00:00:00Z"}`)
							if loopStatus == "prepared" || loopStatus == "admitted" {
								value = nil
							}
						case "stop_id":
							value = nil
							if loopStatus == "cancelled" {
								value = []byte("stop")
							}
						case "judgment_id", "judgment_value":
							value = []byte("false")
							if name == "judgment_id" {
								value = []byte("judgment")
							}
							if loopStatus == "prepared" || loopStatus == "admitted" {
								value = nil
							}
							if name == "judgment_value" && loopStatus == "waiting" {
								value = []byte("unknown")
							}
							if name == "judgment_value" && loopStatus == "stopped" {
								value = []byte("true")
							}
						case "executor_kind":
							value = []byte("program")
						case "native_project_id":
							value = nil
						case "delivery_hook":
							oid, value = pgtype.JSONBOID, []byte(`{"id":"hook","project":"`+project+`","sourceRunId":"source-run","targetRunId":"target-run","sourceTaskSlug":"`+project+`-source","targetTaskSlug":"`+project+`-target","state":"armed","deliveryId":null,"dispatchStatus":null,"dispatchPhase":null}`)
						case "reviewer_run_id", "review_request_id":
							value = nil
						case "actor_kind":
							value = []byte("human")
						case "review_status":
							oid, value = pgtype.JSONBOID, []byte(`{"taskSlug":"`+project+`-task","reviews":[{"kind":"requirements","request":null,"agentRejections":0,"maxReviewRounds":3,"humanHold":false,"responsibleUserId":null},{"kind":"design","request":null,"agentRejections":0,"maxReviewRounds":3,"humanHold":false,"responsibleUserId":null}]}`)
						case "assignment_role":
							value = nil
							if project == "alpha" {
								value = []byte("knowledge")
							}
						case "work_purpose":
							value = nil
							if project == "alpha" {
								value = []byte("knowledge")
							}
						case "dispatch_message_id":
							value = nil
							if project == "alpha" {
								value = []byte("dispatch-message-alpha")
							}
						case "environment_id":
							value = []byte("")
							if project == "alpha" {
								value = []byte("environment-alpha")
							}
						case "conditions":
							oid, value = pgtype.JSONBOID, []byte(`{"review":"operator follow-up"}`)
						case "git_baseline":
							oid, value = pgtype.JSONBOID, nil
							if project == "alpha" {
								value = []byte(`{"isRepo":true,"commitSha":"observed-commit"}`)
							}
						case "generation":
							oid, value = pgtype.Int8OID, []byte("2")
						case "retries":
							oid, value = pgtype.Int8OID, []byte("2")
						case "reworks":
							oid, value = pgtype.Int8OID, []byte("1")
						case "exit_code":
							oid, value = pgtype.Int4OID, nil
						case "spec_version":
							oid, value = pgtype.Int4OID, nil
							if project == "alpha" {
								value = []byte("7")
							}
						case "created_at", "updated_at", "submitted_at":
							oid, value = pgtype.TimestamptzOID, []byte("2026-09-20 00:00:00+00")
						case "task_spec_slug", "task_slug":
							value = []byte(project + "-task")
						case "state":
							value = []byte("prepared")
						}
						rows.fields = append(rows.fields, pgconn.FieldDescription{Name: name, DataTypeOID: oid, Format: pgx.TextFormatCode})
						if !empty && (calls != 8 || loopStatus != "none") {
							rows.values = append(rows.values, value)
						}
					}
					calls++
					if !empty && calls == 1 {
						for _, id := range []string{"single-run", "second-loop"} {
							values := append([][]byte(nil), rows.values...)
							values[0] = []byte(id)
							rows.moreValues = append(rows.moreValues, values)
						}
					}
					if !empty && calls == 9 && loopStatus != "none" {
						values := append([][]byte(nil), rows.values...)
						values[0] = []byte("second-loop")
						rows.moreValues = append(rows.moreValues, values)
					}
					allRows = append(allRows, rows)
					return rows, nil
				}}
				got, err := (&Store{project: project}).ReadWorkbenchMetadata(txToContext(context.Background(), tx))
				if err != nil {
					t.Fatal(err)
				}
				if calls != 9 {
					t.Fatalf("query count: %d", calls)
				}
				if !empty && ((project == "alpha" && (got.Runs[0].AssignmentRole == nil || *got.Runs[0].AssignmentRole != "knowledge")) || (project == "beta" && got.Runs[0].AssignmentRole != nil)) {
					t.Fatal("assignment role must reflect the recorded target without inventing a role")
				}
				if !empty && ((project == "alpha" && (got.Runs[0].WorkPurpose == nil || *got.Runs[0].WorkPurpose != "knowledge")) || (project == "beta" && got.Runs[0].WorkPurpose != nil)) {
					t.Fatal("work purpose must be projected, not inferred")
				}
				if !empty && ((project == "alpha" && (got.Runs[0].DispatchMessageID == nil || *got.Runs[0].DispatchMessageID != "dispatch-message-alpha")) || (project == "beta" && got.Runs[0].DispatchMessageID != nil)) {
					t.Fatal("dispatch message identity or legacy absence not preserved")
				}
				for _, rows := range allRows {
					if !rows.closed {
						t.Fatal("rows not closed")
					}
				}
				if !empty && ((project == "alpha" && !strings.Contains(string(got.Runs[0].GitBaseline), "observed-commit")) || (project == "beta" && len(got.Runs[0].GitBaseline) != 0)) {
					t.Fatal("recorded Git baseline or absence was not preserved")
				}
				encoded, err := json.Marshal(got)
				if err != nil {
					t.Fatal(err)
				}
				if !empty {
					if len(got.Runs) != 3 || got.Runs[1].ProgramLoop != nil {
						t.Fatal("single invocation leaked loop state or batch lost runs")
					}
					loop := got.Runs[0].ProgramLoop
					if loopStatus == "none" {
						if loop != nil || got.Runs[2].ProgramLoop != nil || strings.Contains(string(encoded), `"programLoop"`) {
							t.Fatal("single invocation must not acquire loop metadata")
						}
					} else {
						if got.Runs[2].ProgramLoop == nil || got.Runs[2].ProgramLoop.Status != loopStatus {
							t.Fatal("batch projection did not attach second loop by run id")
						}
						if loop == nil || loop.Status != loopStatus || loop.Kind != "innovative" || loop.MaxAttempts == nil || *loop.MaxAttempts != 2 || loop.AuthorizedByUserID != project+"-human" {
							t.Fatalf("loop projection mismatch: %s", encoded)
						}
						if (loopStatus == "prepared") != (loop.AttemptID == nil && loop.AttemptOrdinal == nil) {
							t.Fatal("attempt absence was not preserved")
						}
						loopJSON, err := json.Marshal(loop)
						if err != nil {
							t.Fatal(err)
						}
						var fields map[string]any
						if err := json.Unmarshal(loopJSON, &fields); err != nil {
							t.Fatal(err)
						}
						if len(fields) != 6 {
							t.Fatalf("loop overview disclosed extra fields: %s", loopJSON)
						}
					}
				}
				if empty {
					if string(encoded) != `{"runs":[],"deliveries":[],"evidence":[],"acceptances":[],"nodeMarks":[],"reviewStates":[],"nodeEventCounts":[],"deliveryHooks":[]}` {
						t.Fatalf("empty metadata: %s", encoded)
					}
				} else if got.Runs[0].TaskSlug != project+"-task" || got.Runs[0].Generation != 2 || got.Runs[0].State != "prepared" || got.Evidence[0].ExitCode != nil || !strings.Contains(string(encoded), `"exitCode":null`) {
					t.Fatalf("metadata mismatch: %s", encoded)
				}
				if !empty && ((project == "alpha" && (got.Runs[0].SpecVersion == nil || *got.Runs[0].SpecVersion != 7)) || (project == "beta" && (got.Runs[0].SpecVersion != nil || !strings.Contains(string(encoded), `"specVersion":null`)))) {
					t.Fatalf("package version projection: %s", encoded)
				}
				if !empty && ((project == "alpha" && (got.Runs[0].EnvironmentID != "environment-alpha" || !strings.Contains(string(encoded), `"environmentId":"environment-alpha"`))) || (project == "beta" && strings.Contains(string(encoded), `"environmentId"`))) {
					t.Fatalf("run environment projection: %s", encoded)
				}
				if !empty && (got.Acceptances[0].RequirementsFingerprint != "requirements_fingerprint" || string(got.Acceptances[0].Conditions) != `{"review":"operator follow-up"}`) {
					t.Fatalf("acceptance basis lost: %s", encoded)
				}
				if !empty && (len(got.NodeEventCounts) != 1 || got.NodeEventCounts[0].TaskSlug != project+"-task" || got.NodeEventCounts[0].Retries != 2 || got.NodeEventCounts[0].Reworks != 1) {
					t.Fatal("event counts must remain distinct and scoped")
				}
				if !empty && (len(got.DeliveryHooks) != 1 || got.DeliveryHooks[0].SourceTaskSlug != project+"-source" || got.DeliveryHooks[0].TargetTaskSlug != project+"-target" || got.DeliveryHooks[0].State != "armed" || got.DeliveryHooks[0].DeliveryID != nil) {
					t.Fatal("hook reference metadata or unknown delivery provenance lost")
				}
			}
		}
	}
}

func TestWorkbenchMetadataErrors(t *testing.T) {
	failure := errors.New("query failure")
	for failAt := range 9 {
		for _, rowError := range []bool{false, true} {
			calls := 0
			tx := workbenchReadTx{query: func(string, ...any) (pgx.Rows, error) {
				current := calls
				calls++
				if current == failAt {
					if rowError {
						return &workbenchReadRows{err: failure}, nil
					}
					return nil, failure
				}
				return &workbenchReadRows{}, nil
			}}
			got, err := (&Store{project: "alpha"}).ReadWorkbenchMetadata(txToContext(context.Background(), tx))
			if got != nil || !errors.Is(err, failure) || calls != failAt+1 {
				t.Fatalf("failure swallowed: %+v, %v, calls=%d", got, err, calls)
			}
		}
	}
}

func TestProgramLoopStateProjection(t *testing.T) {
	maxAttempts := 2
	zero, stopCode := int64(0), int64(7)
	finished := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	normal := &storage.ProgramRunResult{ProgramObservation: storage.ProgramObservation{Outcome: "exited", ExitCode: &zero, FinishedAt: &finished}}
	mechanical := &storage.ProgramLoopConfig{Kind: "mechanical", MaxAttempts: &maxAttempts, Termination: storage.ProgramLoopTermination{Kind: "exit_code", ExitCode: &storage.ProgramLoopExitCode{StopCode: stopCode, ContinueCodes: []int64{zero}}}}
	judgment := &storage.ProgramLoopConfig{Kind: "innovative", MaxAttempts: &maxAttempts, Termination: storage.ProgramLoopTermination{Kind: "judgment"}}
	for _, test := range []struct {
		name    string
		loop    *storage.ProgramLoopConfig
		ordinal int
		result  *storage.ProgramRunResult
		stop    bool
		value   string
		want    string
	}{
		{"single", nil, 0, nil, false, "", ""},
		{"prepared", judgment, 0, nil, false, "", "prepared"},
		{"prepared-stop", judgment, 0, nil, true, "", "cancelled"},
		{"admitted-at-max", judgment, 2, nil, false, "", "admitted"},
		{"false-before-max", judgment, 1, normal, false, "false", "ready"},
		{"unknown-before-max", judgment, 1, normal, false, "unknown", "waiting"},
		{"false-at-max", judgment, 2, normal, false, "false", "needs_human"},
		{"unknown-at-max", judgment, 2, normal, false, "unknown", "needs_human"},
		{"true-at-max", judgment, 2, normal, false, "true", "stopped"},
		{"human-stop-at-max", judgment, 2, normal, true, "unknown", "cancelled"},
		{"mechanical-false", mechanical, 1, normal, false, "", "ready"},
		{"mechanical-true-at-max", mechanical, 2, &storage.ProgramRunResult{ProgramObservation: storage.ProgramObservation{Outcome: "exited", ExitCode: &stopCode, FinishedAt: &finished}}, false, "", "stopped"},
		{"non-normal-exit", mechanical, 1, &storage.ProgramRunResult{ProgramObservation: storage.ProgramObservation{Outcome: "exited", ExitCode: &zero, FinishedAt: &finished, TimedOutAt: &finished}}, false, "", "waiting"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var attempt *storage.ProgramAttemptMetadata
			var stop, condition *storage.ProgramLoopEvent
			if test.ordinal > 0 {
				attempt = &storage.ProgramAttemptMetadata{ID: "attempt", Ordinal: test.ordinal}
			}
			if test.stop {
				stop = &storage.ProgramLoopEvent{ID: "stop"}
			}
			if test.value != "" {
				condition = &storage.ProgramLoopEvent{ID: "judgment", Value: test.value}
			}
			got := deriveProgramLoopState(test.loop, attempt, test.result, stop, condition, nil)
			if test.loop == nil {
				if got != nil {
					t.Fatal("single invocation acquired loop state")
				}
				return
			}
			if got.Status != test.want || got.Stop != stop {
				t.Fatalf("projection: %+v want=%s", got, test.want)
			}
			if condition != nil && (got.Condition.JudgmentID != condition.ID || got.Condition.Value != condition.Value) {
				t.Fatal("judgment identity lost")
			}
		})
	}
}
