// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/stretchr/testify/require"
)

type summaryFixture struct {
	t                       *testing.T
	s                       *postgres.Store
	ctx                     context.Context
	project, user, reviewer string
}

func newSummaryFixture(t *testing.T, project string) *summaryFixture {
	t.Helper()
	s := newStore(t, postgres.WithProject(project))
	users, err := postgres.NewAuth(context.Background(), s.Pool())
	require.NoError(t, err)
	u, err := users.CreateHuman(context.Background(), &storage.User{Kind: storage.KindHuman, DisplayName: project, Role: "admin"}, nil)
	require.NoError(t, err)
	f := &summaryFixture{t: t, s: s, ctx: auth.WithIdentity(context.Background(), &auth.Identity{UserID: u.ID, UserKind: storage.KindHuman}), project: project, user: u.ID}
	f.create("reviewer", false, "approved")
	f.reviewer, err = s.PrepareRun(f.ctx, "reviewer", "summary-review-workspace")
	require.NoError(t, err)
	require.NoError(t, s.BindRunThreadInEnvironment(f.ctx, f.reviewer, "local", "summary-review-thread"))
	f.create("change-basis", false, "approved")
	return f
}

func (f *summaryFixture) create(slug string, summary bool, stage string) {
	f.t.Helper()
	_, err := f.s.CreateSpec(f.ctx, slug, "Declared requirement and integration standard for "+slug, "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(f.t, err)
	f.stage(slug, stage)
	if summary {
		// Seed the explicit role, not an acceptance or a source-review decision.
		_, err = f.s.Pool().Exec(f.ctx, `UPDATE specs SET role='summary' WHERE project_slug=$1 AND slug=$2`, f.project, slug)
		require.NoError(f.t, err)
	}
}

func (f *summaryFixture) stage(slug, stage string) {
	f.t.Helper()
	_, err := f.s.UpdateSpec(f.ctx, slug, nil, &stage, nil, nil, nil)
	require.NoError(f.t, err)
}

func (f *summaryFixture) edge(from, to string, kind storage.EdgeType) {
	f.t.Helper()
	_, err := f.s.AddEdge(f.ctx, from, to, kind)
	require.NoError(f.t, err)
}

func (f *summaryFixture) source(slug string) storage.ReviewSource {
	f.t.Helper()
	refs, err := f.s.ReadSpecSourceRefs(f.ctx, slug)
	require.NoError(f.t, err)
	return storage.ReviewSource{Kind: "specgraph", SpecSlug: slug, Field: "intent", ChangeID: refs["intent"]}
}

func (f *summaryFixture) approve(sources ...storage.ReviewSource) *storage.SourceReviewResult {
	f.t.Helper()
	state, err := f.s.AssignReview(f.ctx, &storage.AssignReviewRequest{TaskSlug: "change-basis", Kind: "requirements", Sources: sources, AuthorResponsibility: storage.ReviewAuthor{Kind: "human", UserID: f.user}, ReviewerRunID: f.reviewer}, nil)
	require.NoError(f.t, err)
	result, err := f.s.ReviewSource(f.ctx, "change-basis", state.Reviews[0].Request.ID, "accepted", "Explicit approval of the named goal-specific obligation change")
	require.NoError(f.t, err)
	return result
}

func (f *summaryFixture) withdrawal(goal, affected, key string, approval *storage.SourceReviewResult, source storage.ReviewSource) storage.SummaryDispositionRequest {
	return storage.SummaryDispositionRequest{GoalSlug: goal, AffectedSlug: affected, Disposition: "withdraw", ReviewDecisionID: approval.Decision.ID, BeforeSources: []storage.ReviewSource{source}, AfterSources: []storage.ReviewSource{source}, Reason: "The approved requirement no longer requires this work for this goal only", IdempotencyKey: key}
}

func (f *summaryFixture) acceptRequest(goal, key string) storage.SummaryAcceptRequest {
	f.t.Helper()
	state, err := f.s.ReadSummary(f.ctx, goal)
	require.NoError(f.t, err)
	return storage.SummaryAcceptRequest{GoalSlug: goal, Basis: "The current goal and intermediate integration standards are satisfied by the declared existing results", EvidenceSources: []storage.ReviewSource{f.source(goal)}, GoalsSatisfied: true, IdempotencyKey: key, ExpectedReferences: state.References}
}

func (f *summaryFixture) state(goal string) *storage.SummaryState {
	f.t.Helper()
	state, err := f.s.ReadSummary(f.ctx, goal)
	require.NoError(f.t, err)
	return state
}

func TestSummaryGoalScopedAcceptanceAndRealReadiness(t *testing.T) {
	f := newSummaryFixture(t, "summary-shared")
	for _, slug := range []string{"a", "b", "s"} {
		f.create(slug, true, "done")
	}
	f.create("c", false, "approved")
	f.create("d", false, "done")
	for _, pair := range [][2]string{{"a", "s"}, {"b", "s"}, {"s", "c"}, {"s", "d"}} {
		f.edge(pair[0], pair[1], storage.EdgeTypeComposes)
	}
	for _, root := range []string{"a", "b", "s"} {
		f.create("after-"+root, false, "approved")
		f.edge("after-"+root, root, storage.EdgeTypeDependsOn)
		require.False(t, f.state(root).Acceptable)
	}
	source := f.source("change-basis")
	approval := f.approve(source)
	req := f.withdrawal("a", "c", "withdraw-a-c", approval, source)
	disposition, err := f.s.RecordSummaryDisposition(f.ctx, req, nil)
	require.NoError(t, err)
	require.Equal(t, f.user, disposition.ActorUserID)
	replayed, err := f.s.RecordSummaryDisposition(f.ctx, req, nil)
	require.NoError(t, err)
	require.Equal(t, disposition.ID, replayed.ID)
	conflict := req
	conflict.Reason = "A different judgment must not reuse an operation identity"
	_, err = f.s.RecordSummaryDisposition(f.ctx, conflict, nil)
	require.ErrorIs(t, err, storage.ErrSummaryConflict)
	a := f.state("a")
	require.True(t, a.Acceptable)
	require.False(t, a.Accepted, "all effective work done is not root integration acceptance")
	for _, obligation := range a.Obligations {
		if obligation.Slug == "c" {
			require.False(t, obligation.Effective)
			require.Equal(t, "approved", obligation.Stage)
		}
	}
	accept := f.acceptRequest("a", "accept-a")
	accepted, err := f.s.AcceptSummary(f.ctx, accept, nil)
	require.NoError(t, err)
	require.NotNil(t, accepted.ImpactReview, "an empty review list is returned as an array, not null")
	require.Equal(t, "human", accepted.ActorKind)
	require.Equal(t, f.user, accepted.ActorUserID)
	require.Nil(t, accepted.ActorRunID, "root acceptance does not fabricate an execution")
	again, err := f.s.AcceptSummary(f.ctx, accept, nil)
	require.NoError(t, err)
	require.Equal(t, accepted.ID, again.ID)
	conflictingAccept := accept
	conflictingAccept.Basis = "Another acceptance basis"
	_, err = f.s.AcceptSummary(f.ctx, conflictingAccept, nil)
	require.ErrorIs(t, err, storage.ErrSummaryConflict)
	require.True(t, f.state("a").Accepted)
	for _, goal := range []string{"b", "s"} {
		require.False(t, f.state(goal).Accepted)
		_, err = f.s.AcceptSummary(f.ctx, f.acceptRequest(goal, "accept-"+goal), nil)
		require.ErrorIs(t, err, storage.ErrSummaryNotAcceptable)
		_, err = f.s.PrepareRun(f.ctx, "after-"+goal, "fixture")
		require.ErrorIs(t, err, storage.ErrDependenciesNotReady)
	}
	ready, err := f.s.GetReady(f.ctx)
	require.NoError(t, err)
	readySlugs := []string{}
	for _, node := range ready {
		readySlugs = append(readySlugs, node.Slug)
	}
	require.Contains(t, readySlugs, "after-a")
	require.NotContains(t, readySlugs, "after-b")
	require.NotContains(t, readySlugs, "after-s")
	_, err = f.s.PrepareRun(f.ctx, "after-a", "fixture")
	require.NoError(t, err, "the actual preparation gate must agree with GetReady")
	var count int
	require.NoError(t, f.s.Pool().QueryRow(f.ctx, `SELECT count(*) FROM deliveries WHERE project_slug=$1`, f.project).Scan(&count))
	require.Zero(t, count, "summary acceptance is not a delivery")
	for _, stage := range []string{"done", "approved", "done"} {
		f.stage("c", stage)
		require.True(t, f.state("a").Accepted, "B's work on an excluded obligation cannot invalidate A")
	}
	bIntent := "B's independent goal changed without changing A's applicable standards"
	_, err = f.s.UpdateSpec(f.ctx, "b", &bIntent, nil, nil, nil, nil)
	require.NoError(t, err)
	require.True(t, f.state("a").Accepted, "the shared obligation does not import B-only source requirements into A")
	_, err = f.s.ReviewSource(f.ctx, "change-basis", approval.Decision.RequestID, "rejected", "The obligation withdrawal is explicitly rejected")
	require.NoError(t, err)
	require.False(t, f.state("a").Accepted)
	_, err = f.s.ReviewSource(f.ctx, "change-basis", approval.Decision.RequestID, "accepted", "A new approval does not revive the revoked old decision ID")
	require.NoError(t, err)
	require.False(t, f.state("a").Accepted)
	require.False(t, f.state("a").Acceptable)
}

func TestSummaryChangesRequireCurrentReferencesAndExplicitImpactReview(t *testing.T) {
	for _, change := range []string{"root-standard", "intermediate-standard", "ancestor-standard", "new-child", "reopened-work", "revoke"} {
		t.Run(change, func(t *testing.T) {
			f := newSummaryFixture(t, "summary-change-"+change)
			for _, slug := range []string{"ancestor", "goal", "middle"} {
				f.create(slug, true, "approved")
			}
			f.create("work", false, "done")
			for _, pair := range [][2]string{{"ancestor", "goal"}, {"goal", "middle"}, {"middle", "work"}} {
				f.edge(pair[0], pair[1], storage.EdgeTypeComposes)
			}
			original := f.acceptRequest("goal", "initial")
			accepted, err := f.s.AcceptSummary(f.ctx, original, nil)
			require.NoError(t, err)
			switch change {
			case "root-standard", "intermediate-standard", "ancestor-standard":
				slug := map[string]string{"root-standard": "goal", "intermediate-standard": "middle", "ancestor-standard": "ancestor"}[change]
				intent := "Revised acceptance standard requiring an explicit assessment of existing results"
				_, err = f.s.UpdateSpec(f.ctx, slug, &intent, nil, nil, nil, nil)
				require.NoError(t, err)
			case "new-child":
				f.create("new-work", false, "done")
				f.edge("middle", "new-work", storage.EdgeTypeComposes)
			case "reopened-work":
				f.stage("work", "approved")
				require.False(t, f.state("goal").Accepted)
				f.stage("work", "done")
			case "revoke":
				revoked, err := f.s.RevokeSummaryAcceptance(f.ctx, accepted.ID, "Integration evidence needs another assessment", nil)
				require.NoError(t, err)
				require.NotNil(t, revoked.RevokedAt)
				again, err := f.s.RevokeSummaryAcceptance(f.ctx, accepted.ID, "Integration evidence needs another assessment", nil)
				require.NoError(t, err)
				require.Equal(t, revoked.RevokedAt, again.RevokedAt)
			}
			state := f.state("goal")
			require.False(t, state.Accepted, "old acceptance must not revive when work returns to done")
			require.Equal(t, accepted.ID, state.LatestAcceptance.ID)
			if change == "revoke" {
				return
			}
			stale := original
			stale.IdempotencyKey = "stale-input"
			_, err = f.s.AcceptSummary(f.ctx, stale, nil)
			require.Error(t, err, "the read/write boundary must reject the prior references")
			current := f.acceptRequest("goal", "review-current")
			_, err = f.s.AcceptSummary(f.ctx, current, nil)
			require.Error(t, err, "old done plus empty needs_review is not an impact assessment")
			if change == "new-child" {
				require.Contains(t, state.ReviewCandidates, "new-work")
			} else {
				require.Contains(t, state.ReviewCandidates, "work")
			}
			for _, slug := range state.ReviewCandidates {
				current.ImpactReview = append(current.ImpactReview, storage.SummaryImpactReview{Slug: slug, Basis: "Compared the current standard to this existing result; it remains applicable without re-execution"})
			}
			_, err = f.s.AcceptSummary(f.ctx, current, nil)
			require.NoError(t, err, "explicit review can preserve existing work; not every source change needs a rerun")
			require.True(t, f.state("goal").Accepted)
		})
	}
}

func TestSummaryCompositionRemovalRequiresEveryAffectedGoal(t *testing.T) {
	f := newSummaryFixture(t, "summary-removal")
	for _, slug := range []string{"a", "b", "s", "alternate"} {
		f.create(slug, true, "approved")
	}
	f.create("c", false, "done")
	for _, pair := range [][2]string{{"a", "s"}, {"b", "s"}, {"s", "c"}, {"a", "alternate"}, {"alternate", "c"}} {
		f.edge(pair[0], pair[1], storage.EdgeTypeComposes)
	}
	source := f.source("change-basis")
	approval := f.approve(source)
	_, err := f.s.RecordSummaryDisposition(f.ctx, f.withdrawal("b", "c", "withdraw-b", approval, source), nil)
	require.NoError(t, err)
	err = f.s.RemoveEdge(f.ctx, "s", "c", storage.EdgeTypeComposes)
	require.Error(t, err, "A's redundant path does not excuse S's own lost obligation")
	_, err = f.s.RecordSummaryDisposition(f.ctx, f.withdrawal("s", "c", "withdraw-s", approval, source), nil)
	require.NoError(t, err)
	require.NoError(t, f.s.RemoveEdge(f.ctx, "s", "c", storage.EdgeTypeComposes), "A still reaches C and needs no withdrawal")
	for _, o := range f.state("a").Obligations {
		if o.Slug == "c" {
			require.True(t, o.Effective)
		}
	}
	before := f.state("a").References
	retained := f.withdrawal("b", "c", "retain-after-reattach", approval, source)
	retained.Disposition = "retain"
	dispositionsBefore := len(f.state("b").Dispositions)
	_, err = f.s.RecordSummaryDisposition(f.ctx, retained, nil)
	require.ErrorIs(t, err, storage.ErrSummaryNotAcceptable, "a historical disposition cannot invent a current containment path")
	require.Len(t, f.state("b").Dispositions, dispositionsBefore, "rejected retention must not publish a disposition")
	const readEvents = `SELECT jsonb_agg(to_jsonb(c) ORDER BY id)::text FROM composition_changes c WHERE project_slug=$1`
	var eventsBefore, eventsAfter string
	require.NoError(t, f.s.Pool().QueryRow(f.ctx, readEvents, f.project).Scan(&eventsBefore))
	other, err := f.s.ExistingAuth().CreateHuman(f.ctx, &storage.User{Kind: storage.KindHuman, DisplayName: "Second graph operator", Role: "admin"}, nil)
	require.NoError(t, err)
	otherCtx := auth.WithIdentity(f.ctx, &auth.Identity{UserID: other.ID, UserKind: storage.KindHuman})
	require.NoError(t, f.s.RemoveEdge(otherCtx, "s", "c", storage.EdgeTypeComposes))
	require.Equal(t, before, f.state("a").References, "a repeated remove is not new lineage")
	require.NoError(t, f.s.Pool().QueryRow(f.ctx, readEvents, f.project).Scan(&eventsAfter))
	require.JSONEq(t, eventsBefore, eventsAfter, "a no-op removal cannot erase consumed dispositions or replace the original actor")
	_, err = f.s.AcceptSummary(f.ctx, f.acceptRequest("a", "accept-before-aba"), nil)
	require.NoError(t, err)
	f.edge("s", "c", storage.EdgeTypeComposes)
	require.False(t, f.state("a").Accepted)
	_, err = f.s.RecordSummaryDisposition(f.ctx, retained, nil)
	require.NoError(t, err, "the original graph owner must restore reachability before an obligation can be retained")
	for _, obligation := range f.state("b").Obligations {
		if obligation.Slug == "c" {
			require.True(t, obligation.Effective)
		}
	}
	_, err = f.s.RecordSummaryDisposition(f.ctx, f.withdrawal("b", "c", "withdraw-b-again", approval, source), nil)
	require.NoError(t, err)
	require.NoError(t, f.s.RemoveEdge(f.ctx, "s", "c", storage.EdgeTypeComposes))
	require.False(t, f.state("a").Accepted, "restoring exactly the old graph must not restore its old acceptance")
	_, err = f.s.ReviewSource(f.ctx, "change-basis", approval.Decision.RequestID, "rejected", "The consumed withdrawal approval is no longer valid")
	require.NoError(t, err)
	require.False(t, f.state("b").Acceptable, "a detached obligation cannot hide its invalidated change authority")
	require.False(t, f.state("s").Acceptable)
}

func TestSummaryRepeatedEdgesAndSliceMembership(t *testing.T) {
	f := newSummaryFixture(t, "summary-edge-noops")
	f.create("goal", true, "approved")
	f.create("work", false, "done")
	f.edge("goal", "work", storage.EdgeTypeComposes)
	_, err := f.s.AcceptSummary(f.ctx, f.acceptRequest("goal", "accepted"), nil)
	require.NoError(t, err)
	before := f.state("goal").References
	var transactional *storage.SummaryState
	err = f.s.RunInTransaction(f.ctx, func(ctx context.Context) error {
		var readErr error
		transactional, readErr = f.s.ReadSummary(ctx, "goal")
		return readErr
	})
	require.ErrorContains(t, err, "read snapshot cannot nest", "public summary reads must not silently inherit weaker write isolation")
	require.Nil(t, transactional)
	const readEvents = `SELECT jsonb_agg(to_jsonb(c) ORDER BY id)::text FROM composition_changes c WHERE project_slug=$1`
	var eventsBefore, eventsAfter string
	require.NoError(t, f.s.Pool().QueryRow(f.ctx, readEvents, f.project).Scan(&eventsBefore))
	other, err := f.s.ExistingAuth().CreateHuman(f.ctx, &storage.User{Kind: storage.KindHuman, DisplayName: "Second graph operator", Role: "admin"}, nil)
	require.NoError(t, err)
	otherCtx := auth.WithIdentity(f.ctx, &auth.Identity{UserID: other.ID, UserKind: storage.KindHuman})
	_, err = f.s.AddEdge(otherCtx, "goal", "work", storage.EdgeTypeComposes)
	require.NoError(t, err)
	require.NoError(t, f.s.Pool().QueryRow(f.ctx, readEvents, f.project).Scan(&eventsAfter))
	require.JSONEq(t, eventsBefore, eventsAfter, "a no-op addition cannot rewrite any original event, including its actor")
	require.Equal(t, before, f.state("goal").References)
	require.True(t, f.state("goal").Accepted)
	sl := &storage.Slice{Slug: "goal/native", ParentSlug: "goal", SliceID: "native", Intent: "Original authoring slice", Status: storage.SliceStatusOpen}
	require.NoError(t, f.s.CreateSlice(f.ctx, sl))
	require.True(t, f.state("goal").Accepted, "native Slice membership is not a new Spec work obligation")
	require.NoError(t, f.s.DeleteSlice(f.ctx, sl.Slug))
	require.True(t, f.state("goal").Accepted)
	// Historical malformed cycles are reported, not traversed forever or treated as done.
	f.edge("work", "goal", storage.EdgeTypeComposes)
	state := f.state("goal")
	require.False(t, state.Acceptable)
	require.Contains(t, state.Blockers, storage.SummaryBlocker{Slug: "goal", Code: "composition_cycle"})
}

func TestSummaryAuthorityAndSourceBoundaries(t *testing.T) {
	f := newSummaryFixture(t, "summary-authority")
	f.create("goal", true, "approved")
	f.create("work", false, "done")
	f.edge("goal", "work", storage.EdgeTypeComposes)
	f.create("manager", false, "approved")
	manager, err := f.s.PrepareRun(f.ctx, "manager", "manager-workspace")
	require.NoError(t, err)
	require.NoError(t, f.s.BindRunThreadInEnvironment(f.ctx, manager, "local", "manager-thread"))
	_, err = f.s.Pool().Exec(f.ctx, `UPDATE context_packages SET body=jsonb_set(body,'{dispatch_target}','{"assignmentRole":"manager"}'::jsonb) WHERE id=(SELECT package_id FROM run_bindings WHERE id=$1)`, manager)
	require.NoError(t, err)
	managerScope := &storage.MailScope{EnvironmentID: "local", ThreadID: "manager-thread", ProviderSessionID: "session", ProviderInstanceID: "codex"}
	executorScope := &storage.MailScope{EnvironmentID: "local", ThreadID: "summary-review-thread", ProviderSessionID: "session", ProviderInstanceID: "codex"}
	req := f.acceptRequest("goal", "authority")
	_, err = f.s.AcceptSummary(f.ctx, req, executorScope)
	require.ErrorIs(t, err, storage.ErrSummaryForbidden)
	_, err = f.s.AcceptSummary(context.Background(), req, nil)
	require.ErrorIs(t, err, storage.ErrSummaryForbidden)
	forged := auth.WithIdentity(context.Background(), &auth.Identity{UserID: "not-a-real-human", UserKind: storage.KindHuman})
	_, err = f.s.AcceptSummary(forged, req, nil)
	require.Error(t, err)
	// Human role permissions are tested by the transport Cedar authorizer, not duplicated in Store.
	accepted, err := f.s.AcceptSummary(f.ctx, req, managerScope)
	require.NoError(t, err)
	require.Equal(t, "agent", accepted.ActorKind)
	require.Equal(t, manager, *accepted.ActorRunID)
	_, err = f.s.Pool().Exec(f.ctx, `UPDATE run_bindings SET state='stopped' WHERE id=$1`, manager)
	require.NoError(t, err)
	_, err = f.s.AcceptSummary(f.ctx, req, managerScope)
	require.ErrorIs(t, err, storage.ErrSummaryForbidden, "even idempotent replay must check the current manager")
	_, err = f.s.RevokeSummaryAcceptance(f.ctx, accepted.ID, "An executor cannot revoke root acceptance", executorScope)
	require.ErrorIs(t, err, storage.ErrSummaryForbidden)
	otherProject := "summary-authority-other"
	_, err = f.s.EnsureProject(f.ctx, otherProject)
	require.NoError(t, err)
	other, err := f.s.ScopedExisting(f.ctx, otherProject)
	require.NoError(t, err)
	_, err = other.RevokeSummaryAcceptance(f.ctx, accepted.ID, "Not this project", nil)
	require.ErrorIs(t, err, storage.ErrSummaryAcceptanceNotFound)
	_, err = other.ReadSummary(f.ctx, "goal")
	require.ErrorIs(t, err, storage.ErrSpecNotFound)
	source := f.source("change-basis")
	approval := f.approve(source)
	disposition := f.withdrawal("goal", "work", "source-boundary", approval, source)
	for _, bad := range []string{"unknown-decision", "unapproved-source", "unknown-source"} {
		invalid := disposition
		invalid.IdempotencyKey = bad
		switch bad {
		case "unknown-decision":
			invalid.ReviewDecisionID = "99999999"
		case "unapproved-source":
			invalid.AfterSources = []storage.ReviewSource{f.source("work")}
		case "unknown-source":
			invalid.BeforeSources = []storage.ReviewSource{{Kind: "specgraph", SpecSlug: "work", Field: "intent", ChangeID: "missing"}}
		}
		_, err = f.s.RecordSummaryDisposition(f.ctx, invalid, nil)
		require.Error(t, err, bad)
	}
	beforeCount := len(f.state("goal").Dispositions)
	_, err = f.s.RecordSummaryDisposition(f.ctx, disposition, executorScope)
	require.ErrorIs(t, err, storage.ErrSummaryForbidden)
	require.Len(t, f.state("goal").Dispositions, beforeCount)
	foreignSource := source
	foreignSource.SpecSlug = "only-foreign"
	_, err = other.CreateSpec(f.ctx, foreignSource.SpecSlug, "Foreign requirement", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	foreignRefs, err := other.ReadSpecSourceRefs(f.ctx, foreignSource.SpecSlug)
	require.NoError(t, err)
	foreignSource.ChangeID = foreignRefs["intent"]
	invalid := f.acceptRequest("goal", "foreign-evidence")
	invalid.EvidenceSources = []storage.ReviewSource{foreignSource}
	_, err = f.s.AcceptSummary(f.ctx, invalid, nil)
	require.Error(t, err, "a real reference in another project is not current project evidence")
	var count int
	require.NoError(t, f.s.Pool().QueryRow(f.ctx, `SELECT count(*) FROM summary_acceptances WHERE project_slug=$1`, f.project).Scan(&count))
	require.Equal(t, 1, count, "failed writes cannot publish partial acceptance")
}

func TestSummaryImpactFollowsMixedPathsWithoutBlanketReview(t *testing.T) {
	for _, change := range []string{"mixed-path", "redundant-composition", "linked-decision"} {
		t.Run(change, func(t *testing.T) {
			f := newSummaryFixture(t, "summary-impact-"+change)
			f.create("goal", true, "approved")
			f.create("x", true, "approved")
			for _, slug := range []string{"a", "y", "sibling"} {
				f.create(slug, false, "done")
			}
			for _, pair := range [][2]string{{"goal", "a"}, {"goal", "x"}, {"x", "y"}, {"goal", "sibling"}} {
				f.edge(pair[0], pair[1], storage.EdgeTypeComposes)
			}
			f.edge("x", "a", storage.EdgeTypeDependsOn)
			var beforeDecision storage.ReviewSource
			if change == "linked-decision" {
				_, err := f.s.CreateDecision(f.ctx, "local-decision", "X integration decision", "Original design", "Applies to X and its contained work", "", nil, "", nil, "", "", "")
				require.NoError(t, err)
				f.edge("x", "local-decision", storage.EdgeTypeDecidedIn)
				beforeDecision = storage.ReviewSource{Kind: "specgraph", SpecSlug: "local-decision", Field: "title", ChangeID: f.state("goal").References.DecisionSources[0].SourceRefs["title"]}
			}
			_, err := f.s.AcceptSummary(f.ctx, f.acceptRequest("goal", "initial"), nil)
			require.NoError(t, err)
			switch change {
			case "mixed-path":
				intent := "Revised upstream result affecting the dependent integration and its contained work"
				_, err = f.s.UpdateSpec(f.ctx, "a", &intent, nil, nil, nil, nil)
				require.NoError(t, err)
			case "redundant-composition":
				// Goal is already an ancestor of Y: this shortcut adds neither work nor inherited standards.
				f.edge("goal", "y", storage.EdgeTypeComposes)
			case "linked-decision":
				title := "Revised X integration decision"
				_, err = f.s.UpdateDecision(f.ctx, "local-decision", 0, &title, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
				require.NoError(t, err)
			}
			state := f.state("goal")
			require.False(t, state.Accepted, "changed current references require a new named root acceptance")
			switch change {
			case "mixed-path":
				require.False(t, state.ScopeChanged)
				require.ElementsMatch(t, []string{"a", "y"}, state.ReviewCandidates, "A's effect must traverse dependency to X then containment to Y, but not siblings")
			case "redundant-composition":
				require.True(t, state.ScopeChanged)
				require.Empty(t, state.ReviewCandidates, "a graph-only shortcut does not force every existing result through impact review")
			case "linked-decision":
				require.ElementsMatch(t, []string{"y"}, state.ReviewCandidates, "a decision associated only with X does not change A or an independent sibling")
			}
			var decisionEvidence storage.ReviewSource
			if change == "linked-decision" {
				decisionEvidence = storage.ReviewSource{Kind: "specgraph", SpecSlug: "local-decision", Field: "title", ChangeID: state.References.DecisionSources[0].SourceRefs["title"]}
				record, err := f.s.ReadWorkbenchKnowledgeRecord(f.ctx, "change", decisionEvidence.ChangeID)
				require.NoError(t, err)
				require.Equal(t, "local-decision", record.Slug)
				require.Contains(t, record.Record.(*storage.ChangeLogEntry).Changes, storage.FieldChange{Field: "title", OldValue: "X integration decision", NewValue: "Revised X integration decision"})
				approval := f.approve(decisionEvidence)
				retained := storage.SummaryDispositionRequest{GoalSlug: "goal", AffectedSlug: "y", Disposition: "retain", ReviewDecisionID: approval.Decision.ID, BeforeSources: []storage.ReviewSource{beforeDecision}, AfterSources: []storage.ReviewSource{decisionEvidence}, Reason: "The existing Y result remains valid against this exact revised decision", IdempotencyKey: "retain-y-for-decision"}
				_, err = f.s.RecordSummaryDisposition(f.ctx, retained, nil)
				require.NoError(t, err, "the original ReviewSource contract supports real Decision field changes too")
				state = f.state("goal")
				require.Empty(t, state.ReviewCandidates, "a valid formal retention already reviews the same changed decision")
			}
			current := f.acceptRequest("goal", "current")
			if change == "linked-decision" {
				current.EvidenceSources = []storage.ReviewSource{decisionEvidence}
			}
			for _, slug := range state.ReviewCandidates {
				current.ImpactReview = append(current.ImpactReview, storage.SummaryImpactReview{Slug: slug, Basis: "The existing result remains applicable after comparing the named changed input"})
			}
			_, err = f.s.AcceptSummary(f.ctx, current, nil)
			require.NoError(t, err)
			require.True(t, f.state("goal").Accepted)
		})
	}
}

func TestSummaryDecisionMembershipChangesReviewOnlyItsBranch(t *testing.T) {
	f := newSummaryFixture(t, "summary-decision-membership")
	for _, slug := range []string{"goal", "x", "z"} {
		f.create(slug, true, "approved")
	}
	for _, slug := range []string{"x-work", "z-work"} {
		f.create(slug, false, "done")
	}
	for _, pair := range [][2]string{{"goal", "x"}, {"goal", "z"}, {"x", "x-work"}, {"z", "z-work"}} {
		f.edge(pair[0], pair[1], storage.EdgeTypeComposes)
	}
	_, err := f.s.CreateDecision(f.ctx, "shared-decision", "Unchanged integration decision", "The same original decision text", "Applicability belongs to the explicit linked branch", "", nil, "", nil, "", "", "")
	require.NoError(t, err)
	f.edge("x", "shared-decision", storage.EdgeTypeDecidedIn)
	_, err = f.s.AcceptSummary(f.ctx, f.acceptRequest("goal", "initial"), nil)
	require.NoError(t, err)
	original := f.state("goal").References.DecisionSources
	f.edge("z", "shared-decision", storage.EdgeTypeDecidedIn)
	state := f.state("goal")
	require.Equal(t, original, state.References.DecisionSources, "decision content has not changed")
	require.False(t, state.Accepted)
	require.ElementsMatch(t, []string{"z-work"}, state.ReviewCandidates, "the new association changes Z's inherited decision, not X's existing association")
	req := f.acceptRequest("goal", "new-z-association")
	req.ImpactReview = []storage.SummaryImpactReview{{Slug: "z-work", Basis: "Compared the existing Z result to the newly applicable decision"}}
	_, err = f.s.AcceptSummary(f.ctx, req, nil)
	require.NoError(t, err)
	require.NoError(t, f.s.RemoveEdge(f.ctx, "x", "shared-decision", storage.EdgeTypeDecidedIn))
	state = f.state("goal")
	require.Equal(t, original, state.References.DecisionSources, "the decision remains present through Z")
	require.False(t, state.Accepted)
	require.ElementsMatch(t, []string{"x-work"}, state.ReviewCandidates, "removing X's association changes only X's inherited decision")
	req = f.acceptRequest("goal", "removed-x-association")
	req.ImpactReview = []storage.SummaryImpactReview{{Slug: "x-work", Basis: "Compared the existing X result after its decision association was removed"}}
	_, err = f.s.AcceptSummary(f.ctx, req, nil)
	require.NoError(t, err)
	require.True(t, f.state("goal").Accepted)
}

func TestSummaryWithdrawalSourceChangesRemainReviewable(t *testing.T) {
	for _, changed := range []string{"change-basis", "work", "middle", "goal"} {
		t.Run(changed, func(t *testing.T) {
			f := newSummaryFixture(t, "summary-withdraw-source-"+changed)
			f.create("goal", true, "approved")
			f.create("middle", true, "approved")
			f.create("work", false, "approved")
			f.edge("goal", "middle", storage.EdgeTypeComposes)
			f.edge("middle", "work", storage.EdgeTypeComposes)
			source := f.source("change-basis")
			approval := f.approve(source)
			_, err := f.s.RecordSummaryDisposition(f.ctx, f.withdrawal("goal", "work", "withdraw-work", approval, source), nil)
			require.NoError(t, err)
			_, err = f.s.AcceptSummary(f.ctx, f.acceptRequest("goal", "initial"), nil)
			require.NoError(t, err)
			intent := "A revised requirement whose applicability must be reviewed, not inferred from the old withdrawal"
			_, err = f.s.UpdateSpec(f.ctx, changed, &intent, nil, nil, nil, nil)
			require.NoError(t, err)
			state := f.state("goal")
			require.False(t, state.Accepted)
			require.False(t, state.Acceptable)
			require.Len(t, state.Dispositions, 1, "invalidated authority stays visible as history")
			_, err = f.s.AcceptSummary(f.ctx, f.acceptRequest("goal", "unreviewed"), nil)
			require.Error(t, err, "a fresh acceptance cannot silently exclude a changed withdrawn obligation")
		})
	}
}

func TestSummaryHumanHoldAndReplacementObligations(t *testing.T) {
	f := newSummaryFixture(t, "summary-hold-replacement")
	f.create("goal", true, "approved")
	f.create("old-work", false, "done")
	f.create("replacement", false, "done")
	f.create("unrelated", false, "done")
	f.edge("goal", "old-work", storage.EdgeTypeComposes)
	f.edge("goal", "replacement", storage.EdgeTypeComposes)
	pending, err := f.s.RecordSummaryDisposition(f.ctx, storage.SummaryDispositionRequest{GoalSlug: "goal", AffectedSlug: "old-work", Disposition: "needs_review", Reason: "An unresolved impact needs assessment before it can be formally disposed", IdempotencyKey: "pending-impact"}, nil)
	require.NoError(t, err, "raising an unresolved issue does not require inventing its final approval first")
	state := f.state("goal")
	for _, obligation := range state.Obligations {
		if obligation.Slug == "old-work" {
			require.Equal(t, pending.ID, obligation.DispositionID)
			require.True(t, obligation.PendingReview)
			require.True(t, obligation.Effective, "pending review does not cancel unfinished obligations")
		}
	}
	_, err = f.s.AcceptSummary(f.ctx, f.acceptRequest("goal", "unresolved-impact"), nil)
	require.ErrorIs(t, err, storage.ErrSummaryNotAcceptable, "even completed work cannot bypass an unresolved impact")
	source := f.source("change-basis")
	approval := f.approve(source)
	retained := f.withdrawal("goal", "old-work", "retain-after-review", approval, source)
	retained.Disposition = "retain"
	_, err = f.s.RecordSummaryDisposition(f.ctx, retained, nil)
	require.NoError(t, err)
	require.True(t, f.state("goal").Acceptable, "a real approved disposition resolves the pending issue")
	f.stage("old-work", "approved")
	f.stage("replacement", "approved")
	req := f.withdrawal("goal", "old-work", "replace-work", approval, source)
	req.Disposition = "replace"
	req.ReplacementSlug = "unrelated"
	_, err = f.s.RecordSummaryDisposition(f.ctx, req, nil)
	require.Error(t, err, "an unrelated completed node cannot satisfy this goal's replacement obligation")
	req.ReplacementSlug = "old-work"
	_, err = f.s.RecordSummaryDisposition(f.ctx, req, nil)
	require.Error(t, err)
	req.ReplacementSlug = "replacement"
	_, err = f.s.RecordSummaryDisposition(f.ctx, req, nil)
	require.NoError(t, err)
	_, err = f.s.AcceptSummary(f.ctx, f.acceptRequest("goal", "unfinished-replacement"), nil)
	require.ErrorIs(t, err, storage.ErrSummaryNotAcceptable)
	f.stage("replacement", "done")
	maximum := int32(1)
	assigned, err := f.s.AssignReview(f.ctx, &storage.AssignReviewRequest{TaskSlug: "replacement", Kind: "requirements", Sources: []storage.ReviewSource{f.source("replacement")}, AuthorResponsibility: storage.ReviewAuthor{Kind: "human", UserID: f.user}, ReviewerRunID: f.reviewer, MaxReviewRounds: &maximum}, nil)
	require.NoError(t, err)
	requestID := assigned.Reviews[0].Request.ID
	_, err = f.s.SubmitReview(f.ctx, storage.MailScope{EnvironmentID: "local", ThreadID: "summary-review-thread", ProviderSessionID: "session", ProviderInstanceID: "codex"}, requestID, "rejected", "An unresolved issue requires human attention")
	require.NoError(t, err)
	_, err = f.s.AcceptSummary(f.ctx, f.acceptRequest("goal", "held-replacement"), nil)
	require.ErrorIs(t, err, storage.ErrSummaryNotAcceptable)
	_, err = f.s.ReviewSource(f.ctx, "replacement", requestID, "accepted", "Human resolves the issue against the actual source")
	require.NoError(t, err)
	_, err = f.s.AcceptSummary(f.ctx, f.acceptRequest("goal", "resolved-replacement"), nil)
	require.NoError(t, err)
	require.True(t, f.state("goal").Accepted)
}
