// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/server"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
)

func TestWorkbenchNodeMarkCursorParsedAtBoundary(t *testing.T) {
	for _, test := range []struct {
		cursor string
		want   int64
		valid  bool
	}{{"", 0, true}, {"37", 37, true}, {"037", 0, false}, {"9223372036854775808", 0, false}} {
		t.Run("cursor="+test.cursor, func(t *testing.T) {
			input := fmt.Sprintf(`{"id":"cursor","resource":"node-mark-history","project":"alpha","slug":"task","cursor":%q}`, test.cursor)
			var output bytes.Buffer
			called := false
			readErr := runWorkbenchStdio(context.Background(), strings.NewReader(input+"\n"), &output,
				func(_ context.Context, request workbenchReadRequest) (map[string]any, error) {
					called = true
					if request.cursorNumber != test.want || request.Cursor != test.cursor {
						t.Fatalf("cursor changed: raw=%q parsed=%d", request.Cursor, request.cursorNumber)
					}
					return map[string]any{"items": []any{}}, nil
				})
			if readErr != nil || called != test.valid {
				t.Fatalf("cursor validation: called=%v error=%v response=%s", called, readErr, output.String())
			}
			var response workbenchReadResponse
			if decodeErr := json.Unmarshal(output.Bytes(), &response); decodeErr != nil {
				t.Fatal(decodeErr)
			}
			if test.valid && response.Error != nil {
				t.Fatalf("valid cursor rejected: %+v", response.Error)
			}
			if !test.valid && (response.Error == nil || response.Error.Code != "invalid_request") {
				t.Fatalf("invalid cursor was not reported: %s", output.String())
			}
		})
	}
}

func TestWorkbenchDatabaseUnavailableProtocol(t *testing.T) {
	for _, test := range []struct {
		err         error
		unavailable bool
	}{
		{fmt.Errorf("connect: %w", syscall.ECONNREFUSED), true},
		{&pgconn.PgError{Code: "57P03", Message: "private startup detail"}, true},
		{&pgconn.PgError{Code: "28P01"}, false},
		{&pgconn.PgError{Code: "57P01"}, false},
		{postgres.ErrSchemaVersionMismatch, false},
		{context.DeadlineExceeded, false},
		{errors.New("invalid configuration"), false},
		{nil, false},
	} {
		if workbenchDatabaseUnavailable(test.err) != test.unavailable {
			t.Fatalf("misclassified error: %v", test.err)
		}
		var output bytes.Buffer
		err := runWorkbenchStdio(context.Background(), strings.NewReader(`{"id":"startup","resource":"projects"}`+"\n"), &output,
			func(context.Context, workbenchReadRequest) (map[string]any, error) {
				return map[string]any{"projects": []any{}}, test.err
			})
		if err != nil {
			t.Fatal(err)
		}
		var result workbenchReadResponse
		if err := json.Unmarshal(output.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		classified := result.Error != nil && result.Error.Code == "database_unavailable"
		if classified != test.unavailable || strings.Contains(output.String(), "private startup detail") {
			t.Fatalf("unexpected protocol error: %s", output.Bytes())
		}
	}
}

func TestWorkbenchSchemaMismatchProtocol(t *testing.T) {
	var output bytes.Buffer
	err := runWorkbenchStdio(context.Background(), strings.NewReader(`{"id":"schema","resource":"projects"}`+"\n"), &output,
		func(context.Context, workbenchReadRequest) (map[string]any, error) {
			return nil, postgres.ErrSchemaVersionMismatch
		})
	if err != nil {
		t.Fatal(err)
	}
	var result workbenchReadResponse
	if err := json.Unmarshal(output.Bytes(), &result); err != nil || result.Error == nil || result.Error.Code != "schema_mismatch" || result.Data != nil {
		t.Fatalf("schema mismatch must be a structured error, not empty data: %s %v", output.Bytes(), err)
	}
	if workbenchCommandError(postgres.ErrSchemaVersionMismatch).Code != "schema_mismatch" {
		t.Fatal("command schema mismatch lost")
	}
}

func TestWorkbenchProgramLoopReadProtocol(t *testing.T) {
	for _, resource := range []string{"program-loop", "program-loop-history"} {
		for _, field := range []string{"", "project", "slug", "cursor", "query", "taskSlug", "state", "attemptCursor", "eventCursor", "invalidAttemptCursor"} {
			req := workbenchReadRequest{ID: "loop", Resource: resource, Project: "alpha", Slug: "run", Credential: "spgr_sk_reader"} //nolint:gosec // Synthetic token passed to a stub resolver, never a real credential.
			switch field {
			case "project":
				req.Project = ""
			case "slug":
				req.Slug = ""
			case "cursor":
				req.Cursor = "old-cursor"
			case "query":
				req.Query = "unexpected"
			case "taskSlug":
				req.TaskSlug = "unexpected"
			case "state":
				req.State = "open"
			case "attemptCursor":
				req.AttemptCursor = "50"
			case "eventCursor":
				req.EventCursor = "ple-previous"
			case "invalidAttemptCursor":
				req.AttemptCursor = "0"
			}
			line, err := json.Marshal(req)
			if err != nil {
				t.Fatal(err)
			}
			called := false
			var output bytes.Buffer
			err = runWorkbenchStdio(context.Background(), strings.NewReader(string(line)+"\n"), &output, func(_ context.Context, got workbenchReadRequest) (map[string]any, error) {
				called = true
				if got.Project != req.Project || got.Slug != req.Slug || got.Resource != req.Resource || got.AttemptCursor != req.AttemptCursor || got.EventCursor != req.EventCursor {
					t.Fatal("loop read identity or cursors changed")
				}
				return map[string]any{"history": storage.ProgramLoopHistoryPage{Attempts: []storage.ProgramAttempt{}, Events: []storage.ProgramLoopEvent{}, NextAttemptCursor: "100", NextEventCursor: "ple-next"}}, nil
			})
			want := field == "" || (resource == "program-loop-history" && (field == "attemptCursor" || field == "eventCursor"))
			if err != nil || called != want {
				t.Fatalf("resource=%s field=%s called=%v err=%v", resource, field, called, err)
			}
		}
	}
	for _, resource := range []string{"projects", "current-view", "spec", "summary-history"} {
		for _, cursor := range []string{"attemptCursor", "eventCursor"} {
			req := map[string]any{"id": "other", "resource": resource, cursor: "1"}
			if resource != "projects" {
				req["project"] = "alpha"
			}
			if resource == "spec" || resource == "summary-history" {
				req["slug"] = "task"
			}
			if resource == "summary-history" {
				req["query"] = "dispositions"
			}
			line, err := json.Marshal(req)
			if err != nil {
				t.Fatal(err)
			}
			var output bytes.Buffer
			if err := runWorkbenchStdio(context.Background(), strings.NewReader(string(line)+"\n"), &output, func(context.Context, workbenchReadRequest) (map[string]any, error) {
				t.Fatal("non-loop resource ignored loop cursor")
				return nil, nil
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestWorkbenchChangePreviewProtocol(t *testing.T) {
	for _, field := range []string{"", "project", "slug", "cursor", "query", "state", "threadId", "taskSlug"} {
		request := workbenchReadRequest{ID: "preview", Resource: "change-preview", Project: "alpha", Slug: "task", Credential: "spgr_sk_test-only"} //nolint:gosec // Synthetic token passed to a stub resolver, never a real credential.
		switch field {
		case "project":
			request.Project = ""
		case "slug":
			request.Slug = ""
		case "cursor":
			request.Cursor = "1"
		case "query":
			request.Query = "query"
		case "state":
			request.State = "open"
		case "threadId":
			request.ThreadID = "thread"
		case "taskSlug":
			request.TaskSlug = "other"
		}
		line, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		var output bytes.Buffer
		called := false
		err = runWorkbenchStdio(context.Background(), strings.NewReader(string(line)+"\n"), &output, func(_ context.Context, req workbenchReadRequest) (map[string]any, error) {
			called = true
			if req.Resource != "change-preview" || req.Project != "alpha" || req.Slug != "task" {
				t.Fatal("preview source identity changed")
			}
			return map[string]any{"specSlug": "task", "scope": storage.ScopeContext{}, "nodes": []storage.ChangeImpactNode{}, "relations": []storage.ScopeRelation{}}, nil
		})
		if err != nil || called != (field == "") {
			t.Fatalf("field=%s called=%v err=%v", field, called, err)
		}
		if called {
			var response workbenchReadResponse
			if err := json.Unmarshal(output.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if response.Error != nil || response.Data["specSlug"] != "task" || len(response.Data) != 4 {
				t.Fatalf("preview shape changed: %s", output.Bytes())
			}
		}
	}
	var output bytes.Buffer
	err := runWorkbenchStdio(context.Background(), strings.NewReader(`{"id":"view","resource":"current-view","project":"alpha"}`+"\n"), &output,
		func(context.Context, workbenchReadRequest) (map[string]any, error) {
			return map[string]any{"capabilities": map[string]bool{"changePreview": false}}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	var response workbenchReadResponse
	if err := json.Unmarshal(output.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Data["capabilities"].(map[string]any)["changePreview"] != true {
		t.Fatal("local preview capability not advertised")
	}
}

func TestWorkbenchSubdivisionCommandProtocol(t *testing.T) {
	children := make([]map[string]string, 0, 8)
	for _, slug := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		children = append(children, map[string]string{"slug": slug, "intent": strings.Repeat("x", 10000), "priority": "p2", "complexity": "low"})
	}
	body, err := json.Marshal(map[string]any{"id": "split", "operation": "subdivide-node", "project": "project", "slug": "parent", "body": map[string]any{"expectedVersion": 1, "reason": "Independent work", "children": children}})
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	called := false
	err = runWorkbenchCommandStdio(context.Background(), strings.NewReader(string(body)+"\n"), &output,
		func(_ context.Context, request workbenchCommandRequest, procedure string) (json.RawMessage, error) {
			called = true
			if procedure != auth.WorkbenchSubdivisionProcedure || request.Slug != "parent" || request.Project != "project" {
				t.Fatal("subdivision must retain its project, node and manage authorization")
			}
			return nil, auth.ErrUnauthenticated
		})
	if err != nil || !called {
		t.Fatalf("bounded subdivision request did not reach authorization: %v", err)
	}
	var result workbenchCommandResponse
	if err := json.Unmarshal(output.Bytes(), &result); err != nil || result.Error == nil || result.Error.Code != "unauthorized" {
		t.Fatalf("authorization failure was lost: %s %v", output.Bytes(), err)
	}
	if workbenchCommandError(storage.ErrInvalidSubdivisionRequest).Code != "invalid_argument" || workbenchCommandError(storage.ErrSpecAlreadyExists).Code != "already_exists" {
		t.Fatal("known subdivision rejections must not appear as uncertain internal failures")
	}
}

func TestWorkbenchStdioReadProtocol(t *testing.T) {
	input := strings.Join([]string{
		`{"id":"1","resource":"spec","project":"existing","slug":"node"}`,
		`{"id":"2","resource":"spec","project":"existing","slug":"missing"}`,
		`{"id":"3","resource":"spec","project":"existing","slug":"slow"}`,
		`{"id":"4","resource":"spec","project":"existing","slug":"broken"}`,
		`{"id":"5","resource":"spec","project":"existing","slug":"node","method":"write"}`,
		`{"id":"6","resource":"spec","project":" ","slug":"node"}`,
		`{"id":"7","resource":"spec","project":"existing","slug":"node"} {}`,
		`null`,
		`not json`,
		`{"id":"10","resource":"projects"}`,
		`{"id":"11","resource":"current-view","project":"existing"}`,
		`{"id":"12","resource":"write","project":"existing"}`,
		`{"id":"13","resource":"spec","project":"existing"}`,
		`{"id":"14","resource":"spec","project":"existing","slug":"old-schema"}`,
		`{"id":"15","resource":"mail-identity"}`,
		`{"id":"16","resource":"mail-identity","credential":"spgr_sk_test-reader"}`,
		`{"id":"17","resource":"mail-threads","project":"existing","credential":"spgr_sk_test-only","taskSlug":"node","cursor":"previous"}`,
		`{"id":"18","resource":"mail-thread","project":"existing","credential":"spgr_ws_test-only","threadId":"thread","cursor":"message"}`,
		`{"id":"19","resource":"mail-thread","project":"existing","credential":"spgr_ws_test-only","threadId":"thread","markRead":true}`,
		`{"id":"20","resource":"mail-identity","credential":"unsupported-test-token"}`,
		`{"id":"21","resource":"mail-identity","credential":"spgr_sk_test-only"}`,
	}, "\n")
	calls := 0
	read := func(ctx context.Context, req workbenchReadRequest) (map[string]any, error) {
		calls++
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > workbenchReadTimeout || ctx.Err() != nil || (req.Resource != "projects" && req.Resource != "mail-identity" && req.Project != "existing") {
			t.Fatalf("read must be project-scoped with a live bounded context")
		}
		switch req.Slug {
		case "missing":
			return nil, storage.ErrSpecNotFound
		case "slow":
			return nil, context.DeadlineExceeded
		case "broken":
			return nil, errors.New("sensitive driver details")
		case "old-schema":
			return nil, &pgconn.PgError{Code: "42703", Message: "sensitive driver details"}
		}
		switch req.Resource {
		case "mail-identity":
			switch req.Credential {
			case "", "unsupported-test-token":
				return nil, auth.ErrUnauthenticated
			case "spgr_sk_test-reader":
				return nil, storage.ErrMailForbidden
			}
			return map[string]any{"identity": map[string]any{"subject": "apikey:test", "display_name": "Test operator", "role": "admin"}}, nil
		case "mail-threads":
			if req.State != "open" || req.TaskSlug != "node" || req.Cursor != "previous" {
				t.Fatal("mail list filters or default state changed")
			}
			return server.MailInspectionThreadsJSON(storage.MailInspectionThreads{Threads: []storage.MailThreadSummary{{
				MailThread: storage.MailThread{ID: "thread"},
				NodeLinks:  []storage.MailNodeLink{{SenderTaskSlug: "node", RecipientTaskSlug: "recipient", MessageCount: 2, PendingAckCount: 1}},
			}}, NextCursor: "next-thread"}), nil
		case "mail-thread":
			if req.ThreadID != "thread" || req.Cursor != "message" {
				t.Fatal("mail detail identity or cursor changed")
			}
			return server.MailInspectionThreadJSON(&storage.MailInspectionPage{
				Thread: storage.MailThread{ID: req.ThreadID}, NextCursor: "next-message",
				Items: []storage.MailInspectionItem{{
					Message:  storage.MailMessage{ID: "message", Body: "test body", References: []storage.MailReference{{MessageID: "source", Body: "test quote"}}},
					Receipts: []storage.MailInspectionReceipt{{RecipientRunID: "run"}},
				}},
			}), nil
		case "projects":
			return map[string]any{"projects": []any{map[string]any{"slug": "existing", "managed": true}}}, nil
		case "current-view":
			return map[string]any{"project": req.Project, "specs": []any{}, "deliveryHooks": []any{}, "capabilities": map[string]bool{"manualCompletion": true, "mailInspection": true, "deliveryHooks": true}}, nil
		default:
			return map[string]any{"project": req.Project, "slug": req.Slug, "spec": map[string]any{"slug": req.Slug, "intent": "stored intent", "stage": "spark", "role": "work", "version": 7, "shape": nil, "specify": nil}, "dependencies": []any{}}, nil
		}
	}
	var output bytes.Buffer
	if err := runWorkbenchStdio(context.Background(), strings.NewReader(input), &output, read); err != nil {
		t.Fatal(err)
	}
	if calls != 13 || strings.Contains(output.String(), "sensitive") || strings.Contains(output.String(), "test-only") || strings.Contains(output.String(), "test-reader") {
		t.Fatalf("unexpected reads or leaked driver error: calls=%d output=%s", calls, &output)
	}
	decoder := json.NewDecoder(&output)
	codes := []string{"", "not_found", "timeout", "read_failed", "invalid_request", "invalid_request", "invalid_request", "invalid_request", "invalid_request", "", "", "invalid_request", "invalid_request", "schema_mismatch", "unauthorized", "forbidden", "", "", "invalid_request", "unauthorized", ""}
	for i, code := range codes {
		var response workbenchReadResponse
		if err := decoder.Decode(&response); err != nil {
			t.Fatal(err)
		}
		if code == "" {
			if response.Error != nil || response.Data == nil {
				t.Fatalf("response %d lost data: %+v", i, response)
			}
		} else if response.Data != nil || response.Error == nil || response.Error.Code != code {
			t.Fatalf("response %d must fail with %s: %+v", i, code, response)
		}
		if i == 0 {
			spec := response.Data["spec"].(map[string]any)
			if response.Data["project"] != "existing" || spec["intent"] != "stored intent" || spec["version"] != float64(7) || spec["shape"] != nil || len(response.Data["dependencies"].([]any)) != 0 {
				t.Fatalf("spec projection changed: %+v", response.Data)
			}
		}
		if i == 9 && response.Data["projects"].([]any)[0].(map[string]any)["slug"] != "existing" {
			t.Fatal("project projection changed")
		}
		if i == 10 {
			for capability, value := range response.Data["capabilities"].(map[string]any) {
				if value != (capability == "mailInspection" || capability == "manualCompletion" || capability == "dependencyEditing" || capability == "dependencyRemoval" || capability == "createNode" || capability == "nodeApproval" || capability == "runDispatch" || capability == "deliveryReview" || capability == "subdivision" || capability == "nodeMarks" || capability == "changePreview" || capability == "abandonNode" || capability == "independentReview" || capability == "deliveryHooks" || capability == "nodeEvents" || capability == "testReports") {
					t.Fatal("stdio must not advertise unimplemented actions")
				}
			}
			if response.Data["readOnlyTransport"] != true || !strings.Contains(response.Data["basis"].(map[string]any)["source"].(string), "stdio") {
				t.Fatal("read-only transport and provenance must be explicit")
			}
		}
		if i == 16 {
			threads := response.Data["threads"].([]any)
			if response.Data["next_cursor"] != "next-thread" || len(threads) != 1 {
				t.Fatal("mail thread-list projection changed")
			}
			links := threads[0].(map[string]any)["node_links"].([]any)
			if len(links) != 1 || links[0].(map[string]any)["sender_task_slug"] != "node" || links[0].(map[string]any)["recipient_task_slug"] != "recipient" || links[0].(map[string]any)["message_count"] != float64(2) || links[0].(map[string]any)["pending_ack_count"] != float64(1) {
				t.Fatal("mail directed node-link projection changed")
			}
		}
		if i == 17 {
			item := response.Data["items"].([]any)[0].(map[string]any)
			message := item["message"].(map[string]any)
			receipt := item["receipts"].([]any)[0].(map[string]any)
			if response.Data["next_cursor"] != "next-message" || message["body"] != "test body" || message["references"].([]any)[0].(map[string]any)["body"] != "test quote" || receipt["read_at"] != nil || receipt["acknowledged_at"] != nil {
				t.Fatal("mail body, quote, receipt or pagination projection changed")
			}
		}
		if i == 20 && response.Data["identity"].(map[string]any)["role"] != "admin" {
			t.Fatal("mail identity projection changed")
		}
		if i == 0 && response.ID != "1" || i == 9 && response.ID != "10" || i == 10 && response.ID != "11" {
			t.Fatalf("request correlation lost: %+v", response)
		}
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		t.Fatalf("unexpected extra stdout: %v", err)
	}
	output.Reset()
	if err := runWorkbenchStdio(context.Background(), strings.NewReader(strings.Repeat("x", workbenchRequestLimit+3)), &output, read); err == nil {
		t.Fatal("oversized input must stop the stream")
	}
	if calls != 13 || !strings.Contains(output.String(), `"code":"invalid_request"`) {
		t.Fatal("oversized input must fail explicitly without reading the database")
	}
}
