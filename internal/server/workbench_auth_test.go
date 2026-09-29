// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/specgraph/specgraph/internal/auth"
)

type workbenchAuthResolver struct {
	auth.Resolver
	identity *auth.Identity
	err      error
	calls    int
	token    string
}

func (r *workbenchAuthResolver) Resolve(_ context.Context, token string) (*auth.Identity, error) {
	r.calls++
	r.token = token
	return r.identity, r.err
}

type workbenchAuthorizer struct {
	allowed   bool
	err       error
	calls     int
	procedure string
	identity  *auth.Identity
}

func (a *workbenchAuthorizer) Authorize(_ context.Context, identity *auth.Identity, procedure string, _ any) (auth.Decision, error) {
	a.calls++
	a.procedure = procedure
	a.identity = identity
	return auth.Decision{Allowed: a.allowed}, a.err
}

func TestWorkbenchWriteAuth(t *testing.T) {
	for _, test := range []struct {
		name                                 string
		token, cookie, origin, fetchSite     string
		userID                               string
		allowed                              bool
		resolveErr, authorizeErr             error
		status, resolveCalls, authorizeCalls int
	}{
		{name: "anonymous", status: 401},
		{name: "invalid bearer", token: "invalid", resolveErr: auth.ErrUnauthenticated, status: 401, resolveCalls: 1},
		{name: "reader denied", token: "reader", userID: "user-1", status: 403, resolveCalls: 1, authorizeCalls: 1},
		{name: "missing user ID", token: "key", allowed: true, status: 401, resolveCalls: 1},
		{name: "authorizer failure", token: "key", userID: "user-1", authorizeErr: errors.New("policy engine failed"), status: 500, resolveCalls: 1, authorizeCalls: 1},
		{name: "CLI bearer without origin", token: "key", userID: "user-1", allowed: true, status: 204, resolveCalls: 1, authorizeCalls: 1},
		{name: "same-origin session", cookie: "session", origin: "http://workbench.local", fetchSite: "same-origin", userID: "user-1", allowed: true, status: 204, resolveCalls: 1, authorizeCalls: 1},
		{name: "cross-site session", cookie: "session", origin: "https://evil.example", fetchSite: "cross-site", userID: "user-1", allowed: true, status: 403},
		{name: "cross-site origin only", cookie: "session", origin: "https://evil.example", userID: "user-1", allowed: true, status: 403},
	} {
		t.Run(test.name, func(t *testing.T) {
			identity := &auth.Identity{UserID: test.userID, Subject: "apikey:key-1", EffectiveRole: auth.RoleWriter}
			resolver := &workbenchAuthResolver{identity: identity, err: test.resolveErr}
			authorizer := &workbenchAuthorizer{allowed: test.allowed, err: test.authorizeErr}
			handled := false
			handler := workbenchWriteAuth(resolver, authorizer, auth.WorkbenchSubmitDeliveryProcedure, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				handled = true
				got, ok := auth.IdentityFromContext(r.Context())
				if !ok || got != identity || got.UserID != "user-1" {
					t.Fatalf("lost server identity: %+v", got)
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			r := httptest.NewRequest(http.MethodPost, "http://workbench.local/loop/deliveries", strings.NewReader(`{"submitted_by":"forged-user","approver":"forged-user"}`))
			if test.token != "" {
				r.Header.Set("Authorization", "Bearer "+test.token)
			}
			if test.cookie != "" {
				r.Header.Set("Cookie", "specgraph_session="+test.cookie)
			}
			r.Header.Set("Origin", test.origin)
			r.Header.Set("Sec-Fetch-Site", test.fetchSite)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != test.status || handled != (test.status == 204) || resolver.calls != test.resolveCalls || authorizer.calls != test.authorizeCalls {
				t.Fatalf("status=%d handled=%v resolver=%d authorizer=%d: %s", w.Code, handled, resolver.calls, authorizer.calls, w.Body.String())
			}
			if authorizer.calls > 0 && (authorizer.identity != identity || authorizer.procedure != auth.WorkbenchSubmitDeliveryProcedure) {
				t.Fatal("wrong policy input")
			}
			if test.cookie != "" && resolver.calls > 0 && resolver.token != test.cookie {
				t.Fatal("session cookie did not reach resolver")
			}
		})
	}
}

func TestWorkbenchRegisteredWritesRejectBeforeStorage(t *testing.T) {
	for _, route := range []struct{ path, procedure string }{
		{"/loop/runs", auth.WorkbenchPrepareRunProcedure},
		{"/loop/runs/run-1/bind", auth.WorkbenchBindRunThreadProcedure},
		{"/loop/deliveries", auth.WorkbenchSubmitDeliveryProcedure},
		{"/loop/deliveries/delivery-1/accept", auth.WorkbenchAcceptDeliveryProcedure},
		{"/loop/runs/run-1/complete", auth.WorkbenchCompleteRunProcedure},
		{"/loop/specs/task/manual-complete", auth.WorkbenchManualCompleteProcedure},
		{"/loop/specs/task/dependencies", auth.WorkbenchEditDependencyProcedure},
		{"/loop/specs/task/dependencies/remove", auth.WorkbenchEditDependencyProcedure},
		{"/loop/specs/task/subdivide", auth.WorkbenchSubdivisionProcedure},
	} {
		for _, failure := range []string{"anonymous", "reader", "authorizer", "cross-site"} {
			t.Run(route.path+"/"+failure, func(t *testing.T) {
				resolver := &workbenchAuthResolver{identity: &auth.Identity{UserID: "reader-1", EffectiveRole: auth.RoleReader}}
				authorizer := &workbenchAuthorizer{}
				mux := http.NewServeMux()
				RegisterWorkbenchLoop(mux, nil, resolver, authorizer)
				r := httptest.NewRequest(http.MethodPost, route.path, strings.NewReader(`{}`))
				want := http.StatusForbidden
				switch failure {
				case "anonymous":
					want = http.StatusUnauthorized
				case "reader":
					r.Header.Set("Authorization", "Bearer reader")
				case "authorizer":
					r.Header.Set("Authorization", "Bearer key")
					authorizer.err = errors.New("policy unavailable")
					want = http.StatusInternalServerError
				case "cross-site":
					r.Header.Set("Cookie", "specgraph_session=session")
					r.Header.Set("Sec-Fetch-Site", "cross-site")
					authorizer.allowed = true
				}
				w := httptest.NewRecorder()
				mux.ServeHTTP(w, r)
				if w.Code != want {
					t.Fatalf("got %d: %s", w.Code, w.Body.String())
				}
				if authorizer.calls > 0 && authorizer.procedure != route.procedure {
					t.Fatalf("wrong procedure: %s", authorizer.procedure)
				}
			})
		}
	}
}

func TestWorkbenchAcceptanceRequiresExplicitVerdict(t *testing.T) {
	mux := http.NewServeMux()
	RegisterWorkbenchLoop(mux, nil, &workbenchAuthResolver{identity: &auth.Identity{UserID: "admin-1", EffectiveRole: auth.RoleAdmin}}, &workbenchAuthorizer{allowed: true})
	r := httptest.NewRequest(http.MethodPost, "/loop/deliveries/delivery-1/accept", strings.NewReader(`{}`))
	r.Header.Set("Authorization", "Bearer admin")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "verdict required") {
		t.Fatalf("implicit acceptance: %d %s", w.Code, w.Body.String())
	}
}
