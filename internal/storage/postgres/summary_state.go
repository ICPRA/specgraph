// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/storage"
)

type summaryNode struct {
	storage.SummaryNodeReference
	Stage      string
	Generation int64
}

type summaryChange struct {
	ID             string
	From, To, Type string
	Lost           []summaryLoss
	Dispositions   []string
}

// One transaction-local graph is shared by all readiness candidates. No persistent cache.
type summaryGraph struct {
	nodes                           map[string]summaryNode
	children, parents, dependencies map[string][]string
	relations                       []storage.SummaryRelationReference
	changes                         []summaryChange
	decisions                       map[string]storage.SummaryDecisionReference
	holds                           map[string]bool
	memo                            map[string]*storage.SummaryState
}

func (s *Store) loadSummaryGraph(ctx context.Context) (*summaryGraph, error) {
	g := &summaryGraph{nodes: map[string]summaryNode{}, children: map[string][]string{}, parents: map[string][]string{}, dependencies: map[string][]string{}, decisions: map[string]storage.SummaryDecisionReference{}, holds: map[string]bool{}, memo: map[string]*storage.SummaryState{}}
	rows, err := s.query(ctx, `SELECT s.id,s.slug,s.role,s.stage,s.field_source_refs,s.completion_generation,
 COALESCE((SELECT c.id FROM changelog_entries c WHERE c.project_slug=s.project_slug AND c.spec_slug=s.slug
 AND EXISTS(SELECT 1 FROM jsonb_array_elements(c.changes) delta WHERE delta->>'old_value' IS DISTINCT FROM delta->>'new_value'
 AND (delta->>'field'='role' OR (delta->>'field'='stage' AND delta->>'new_value' IN('abandoned','superseded'))))
 ORDER BY c.version DESC,c.id DESC LIMIT 1),'') FROM specs s WHERE s.project_slug=$1 ORDER BY s.slug COLLATE "C"`, s.project)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var n summaryNode
		if scanErr := rows.Scan(&n.ID, &n.Slug, &n.Role, &n.Stage, &n.SourceRefs, &n.Generation, &n.LifecycleChangeID); scanErr != nil {
			rows.Close()
			return nil, fmt.Errorf("postgres: scan summary node: %w", scanErr)
		}
		g.nodes[n.Slug] = n
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		rows.Close()
		return nil, fmt.Errorf("postgres: read summary node rows: %w", rowsErr)
	}
	rows.Close()
	// Only known Slice membership is excluded. A missing Spec endpoint remains visible.
	rows, err = s.query(ctx, `SELECT e.from_slug,e.to_slug,e.edge_type,COALESCE((SELECT max(c.id)::text FROM composition_changes c WHERE c.project_slug=e.project_slug AND c.from_slug=e.from_slug AND c.to_slug=e.to_slug AND c.edge_type=e.edge_type),'0')
 FROM edges e WHERE e.project_slug=$1 AND e.edge_type IN ('COMPOSES','DECIDED_IN','DEPENDS_ON','BLOCKS')
 AND NOT EXISTS(SELECT 1 FROM slices x WHERE x.project_slug=e.project_slug AND x.slug IN(e.from_slug,e.to_slug))
 ORDER BY e.edge_type COLLATE "C",e.from_slug COLLATE "C",e.to_slug COLLATE "C"`, s.project)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var r storage.SummaryRelationReference
		if scanErr := rows.Scan(&r.FromSlug, &r.ToSlug, &r.Type, &r.ChangeID); scanErr != nil {
			rows.Close()
			return nil, fmt.Errorf("postgres: scan summary relation: %w", scanErr)
		}
		r.Present = true
		switch r.Type {
		case "COMPOSES":
			g.children[r.FromSlug] = append(g.children[r.FromSlug], r.ToSlug)
			g.parents[r.ToSlug] = append(g.parents[r.ToSlug], r.FromSlug)
		case "DEPENDS_ON":
			g.dependencies[r.FromSlug] = append(g.dependencies[r.FromSlug], r.ToSlug)
		case "BLOCKS":
			g.dependencies[r.ToSlug] = append(g.dependencies[r.ToSlug], r.FromSlug)
		}
		g.relations = append(g.relations, r)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		rows.Close()
		return nil, fmt.Errorf("postgres: read summary relation rows: %w", rowsErr)
	}
	rows.Close()
	rows, err = s.query(ctx, `WITH latest AS (
 SELECT DISTINCT ON(from_slug,to_slug,edge_type) id,from_slug,to_slug,edge_type,'[]'::jsonb AS lost_obligations,'[]'::jsonb AS dispositions
 FROM composition_changes WHERE project_slug=$1 ORDER BY from_slug,to_slug,edge_type,id DESC
 ), losses AS (
 SELECT DISTINCT ON(loss->>'goal',loss->>'slug') c.id,c.from_slug,c.to_slug,c.edge_type,
 jsonb_build_array(loss || jsonb_build_object('authorized',EXISTS(SELECT 1 FROM summary_obligations d
 WHERE d.project_slug=c.project_slug AND d.id::text=loss->>'dispositionId' AND d.goal_slug=loss->>'goal' AND d.affected_slug=loss->>'slug'
 AND c.dispositions ? d.id::text AND d.record->>'disposition' IN('withdraw','replace')))) AS lost_obligations,c.dispositions
 FROM composition_changes c CROSS JOIN LATERAL jsonb_array_elements(c.lost_obligations) loss
 WHERE c.project_slug=$1 ORDER BY loss->>'goal',loss->>'slug',c.id DESC
 ) SELECT id::text,from_slug,to_slug,edge_type,lost_obligations,dispositions FROM (SELECT * FROM latest UNION ALL SELECT * FROM losses) c ORDER BY c.id`, s.project)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var c summaryChange
		if scanErr := rows.Scan(&c.ID, &c.From, &c.To, &c.Type, &c.Lost, &c.Dispositions); scanErr != nil {
			rows.Close()
			return nil, fmt.Errorf("postgres: scan summary change: %w", scanErr)
		}
		g.changes = append(g.changes, c)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		rows.Close()
		return nil, fmt.Errorf("postgres: read summary change rows: %w", rowsErr)
	}
	rows.Close()
	decisions, err := s.readDecisionSourceRefs(ctx, nil)
	if err != nil {
		return nil, err
	}
	for _, d := range decisions {
		g.decisions[d.Slug] = d
	}
	rows, err = s.query(ctx, `SELECT DISTINCT task_slug FROM (SELECT DISTINCT ON(task_slug,kind) task_slug,human_hold FROM review_requests WHERE project_slug=$1 ORDER BY task_slug,kind,id DESC) r WHERE human_hold`, s.project)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var slug string
		if scanErr := rows.Scan(&slug); scanErr != nil {
			rows.Close()
			return nil, fmt.Errorf("postgres: scan summary hold: %w", scanErr)
		}
		g.holds[slug] = true
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		rows.Close()
		return nil, fmt.Errorf("postgres: read summary hold rows: %w", rowsErr)
	}
	rows.Close()
	return g, nil
}

// Three colours detect a real cycle while each shared descendant is visited once.
func summaryReach(root string, edges map[string][]string) (map[string]bool, bool) {
	seen := map[string]bool{}
	active := map[string]bool{}
	cycle := false
	var visit func(string)
	visit = func(slug string) {
		if active[slug] {
			cycle = true
			return
		}
		if seen[slug] {
			return
		}
		seen[slug] = true
		active[slug] = true
		for _, child := range edges[slug] {
			visit(child)
		}
		delete(active, slug)
	}
	visit(root)
	return seen, cycle
}

// ReadSummary reads goal obligations and acceptance validity in one snapshot.
func (s *Store) ReadSummary(ctx context.Context, goal string) (*storage.SummaryState, error) {
	var state *storage.SummaryState
	err := s.RunReadSnapshot(ctx, func(ctx context.Context) error {
		var err error
		state, err = s.readSummaryInSnapshot(ctx, goal)
		return err
	})
	return state, err
}

func (s *Store) readSummaryInSnapshot(ctx context.Context, goal string) (*storage.SummaryState, error) {
	g, err := s.loadSummaryGraph(ctx)
	if err != nil {
		return nil, err
	}
	return s.summaryState(ctx, g, goal)
}

func (s *Store) summaryState(ctx context.Context, g *summaryGraph, goal string) (*storage.SummaryState, error) {
	if state, ok := g.memo[goal]; ok {
		return state, nil
	}
	root, ok := g.nodes[goal]
	if !ok {
		return nil, storage.ErrSpecNotFound
	}
	if root.Role != storage.SpecRoleSummary {
		return nil, storage.ErrInvalidSummary
	}
	state := &storage.SummaryState{GoalSlug: goal, Acceptable: true, Blockers: []storage.SummaryBlocker{}, Obligations: []storage.SummaryObligation{}, Dispositions: []storage.SummaryDisposition{}, ChangedSources: []storage.SummarySourceChange{}, ReviewCandidates: []string{}, References: storage.SummaryReferences{Nodes: []storage.SummaryNodeReference{}, Relations: []storage.SummaryRelationReference{}, DispositionIDs: []string{}, DecisionSources: []storage.SummaryDecisionReference{}}}
	block := func(slug, code string) {
		state.Blockers = append(state.Blockers, storage.SummaryBlocker{Slug: slug, Code: code})
		state.Acceptable = false
	}
	reach, cycle := summaryReach(goal, g.children)
	ancestors, ancestorCycle := summaryReach(goal, g.parents)
	if cycle || ancestorCycle {
		block(goal, "composition_cycle")
	}
	if root.Stage == "abandoned" || root.Stage == "superseded" {
		block(goal, "goal_inactive")
	}
	rows, err := s.query(ctx, `SELECT id::text,record,applicability FROM (SELECT DISTINCT ON(affected_slug) id,record,applicability FROM summary_obligations WHERE project_slug=$1 AND goal_slug=$2 ORDER BY affected_slug,id DESC) d ORDER BY id`, s.project, goal)
	if err != nil {
		return nil, err
	}
	latest := map[string]storage.SummaryDisposition{}
	applicability := map[string]map[string]map[string]string{}
	for rows.Next() {
		var d storage.SummaryDisposition
		var id string
		var body []byte
		var refs map[string]map[string]string
		if scanErr := rows.Scan(&id, &body, &refs); scanErr != nil {
			rows.Close()
			return nil, fmt.Errorf("postgres: scan summary disposition: %w", scanErr)
		}
		if decodeErr := json.Unmarshal(body, &d); decodeErr != nil {
			rows.Close()
			return nil, fmt.Errorf("postgres: decode summary disposition: %w", decodeErr)
		}
		d.ID = id
		state.Dispositions = append(state.Dispositions, d)
		latest[d.AffectedSlug] = d
		applicability[id] = refs
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		rows.Close()
		return nil, fmt.Errorf("postgres: read summary disposition rows: %w", rowsErr)
	}
	rows.Close()
	// Previously consumed detached obligations remain reviewable without reconstructing a historical graph.
	for _, c := range g.changes {
		for _, loss := range c.Lost {
			if loss.Goal != goal {
				continue
			}
			if !loss.Authorized {
				block(loss.Slug, "unreviewed_removal")
				continue
			}
			reach[loss.Slug] = true
		}
	}
	keys := make([]string, 0, len(reach))
	for slug := range reach {
		keys = append(keys, slug)
	}
	slices.Sort(keys)
	effective := map[string]bool{}
	for _, slug := range keys {
		n, exists := g.nodes[slug]
		if !exists {
			block(slug, "missing_spec")
			continue
		}
		o := storage.SummaryObligation{Slug: slug, Role: n.Role, Stage: n.Stage, Disposition: "retain", Effective: true}
		if d, ok := latest[slug]; ok {
			o.Disposition = d.Disposition
			o.DispositionID = d.ID
			state.References.DispositionIDs = append(state.References.DispositionIDs, d.ID)
			valid, dispositionErr := s.summaryDispositionValid(ctx, g, &d, applicability[d.ID])
			if dispositionErr != nil {
				return nil, dispositionErr
			}
			o.PendingReview = !valid || d.Disposition == "needs_review"
			if o.PendingReview {
				block(slug, "disposition_needs_review")
			} else if d.Disposition == "withdraw" || d.Disposition == "replace" {
				o.Effective = false
			}
		}
		if slug == goal {
			o.Effective = true
		}
		if o.Effective {
			effective[slug] = true
			if n.Role == storage.SpecRoleSummary {
				owner, reason, sourceErr := s.mergedSourceResponsibility(ctx, slug)
				if sourceErr != nil {
					return nil, sourceErr
				}
				if reason != "" {
					block(owner, "merge_source_"+reason)
				}
			}
			if n.Role == storage.SpecRoleWork && n.Stage != "done" {
				block(slug, "work_not_done")
			}
			if n.Role == storage.SpecRoleSummary && (n.Stage == "abandoned" || n.Stage == "superseded") {
				block(slug, "goal_inactive")
			}
			if g.holds[slug] {
				block(slug, "human_hold")
			}
		}
		state.Obligations = append(state.Obligations, o)
	}
	for slug := range ancestors {
		if _, ok := g.nodes[slug]; !ok {
			block(slug, "missing_ancestor")
		}
		effective[slug] = true
	}
	keys = keys[:0]
	for slug := range effective {
		keys = append(keys, slug)
	}
	slices.Sort(keys)
	for _, slug := range keys {
		n, ok := g.nodes[slug]
		if !ok {
			continue
		}
		ref := n.SummaryNodeReference
		if reach[slug] && n.Role == storage.SpecRoleWork {
			generation := n.Generation
			ref.CompletionGeneration = &generation
		}
		state.References.Nodes = append(state.References.Nodes, ref)
	}
	linked := map[string]bool{}
	for _, r := range g.relations {
		if (r.Type == "COMPOSES" && ((reach[r.FromSlug] && reach[r.ToSlug]) || (ancestors[r.FromSlug] && ancestors[r.ToSlug]))) || (r.Type == "DECIDED_IN" && effective[r.FromSlug]) {
			state.References.Relations = append(state.References.Relations, r)
			if r.Type == "DECIDED_IN" {
				linked[r.ToSlug] = true
			}
		}
	}
	// Last event for each relevant pair prevents remove/re-add from reviving acceptance.
	for _, c := range g.changes {
		if (c.Type == "COMPOSES" && ((reach[c.From] && reach[c.To]) || (ancestors[c.From] && ancestors[c.To]))) || (c.Type == "DECIDED_IN" && effective[c.From]) {
			found := false
			for i, r := range state.References.Relations {
				if r.FromSlug == c.From && r.ToSlug == c.To && r.Type == c.Type {
					state.References.Relations[i].ChangeID = c.ID
					found = true
					break
				}
			}
			if !found {
				state.References.Relations = append(state.References.Relations, storage.SummaryRelationReference{FromSlug: c.From, ToSlug: c.To, Type: c.Type, ChangeID: c.ID})
			}
		}
	}
	keys = keys[:0]
	for slug := range linked {
		keys = append(keys, slug)
	}
	slices.Sort(keys)
	for _, slug := range keys {
		d, ok := g.decisions[slug]
		if !ok {
			block(slug, "missing_decision")
			continue
		}
		state.References.DecisionSources = append(state.References.DecisionSources, d)
	}
	slices.Sort(state.References.DispositionIDs)
	slices.SortFunc(state.References.Relations, func(a, b storage.SummaryRelationReference) int {
		for _, v := range [][2]string{{a.Type, b.Type}, {a.FromSlug, b.FromSlug}, {a.ToSlug, b.ToSlug}} {
			if v[0] < v[1] {
				return -1
			}
			if v[0] > v[1] {
				return 1
			}
		}
		return 0
	})
	var previous storage.SummaryReferences
	var encoded []byte
	err = s.queryRow(ctx, `SELECT record || jsonb_build_object('id',id::text),context FROM summary_acceptances WHERE project_slug=$1 AND goal_slug=$2 ORDER BY id DESC LIMIT 1`, s.project, goal).Scan(&encoded, &previous)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("postgres: read latest summary acceptance: %w", err)
	}
	if err == nil {
		var a storage.SummaryAcceptance
		if decodeErr := json.Unmarshal(encoded, &a); decodeErr != nil {
			return nil, fmt.Errorf("postgres: decode latest summary acceptance: %w", decodeErr)
		}
		state.LatestAcceptance = &a
		state.Accepted = state.Acceptable && a.RevokedAt == nil && reflect.DeepEqual(previous, state.References)
		if state.Accepted {
			for i := range a.EvidenceSources {
				source := &a.EvidenceSources[i]
				if !summaryCurrentSource(g, source) {
					state.Accepted = false
					break
				}
			}
		}
		a.Current = state.Accepted
		a.ExpectedReferences = previous
		if contextErr := s.summaryChangedContext(ctx, g, state, &previous); contextErr != nil {
			return nil, contextErr
		}
	}
	g.memo[goal] = state
	return state, nil
}

func (s *Store) summaryChangedContext(ctx context.Context, g *summaryGraph, state *storage.SummaryState, old *storage.SummaryReferences) error {
	before := map[string]map[string]string{}
	after := map[string]map[string]string{}
	for _, n := range old.Nodes {
		before[n.Slug] = n.SourceRefs
	}
	for _, n := range old.DecisionSources {
		before[n.Slug] = n.SourceRefs
	}
	for _, n := range state.References.Nodes {
		after[n.Slug] = n.SourceRefs
	}
	for _, n := range state.References.DecisionSources {
		after[n.Slug] = n.SourceRefs
	}
	for slug, refs := range before {
		for field, id := range refs {
			if after[slug][field] != id {
				state.ChangedSources = append(state.ChangedSources, storage.SummarySourceChange{Slug: slug, Field: field, Before: id, After: after[slug][field]})
			}
		}
	}
	for slug, refs := range after {
		for field, id := range refs {
			if before[slug][field] == "" {
				state.ChangedSources = append(state.ChangedSources, storage.SummarySourceChange{Slug: slug, Field: field, After: id})
			}
		}
	}
	state.ScopeChanged = !reflect.DeepEqual(old.Relations, state.References.Relations)
	seeds := map[string]bool{}
	structural := map[string]bool{}
	sources := map[string][]storage.ReviewSource{}
	oldLinks, newLinks := map[[2]string]bool{}, map[[2]string]bool{}
	for _, relation := range old.Relations {
		if relation.Present && relation.Type == "DECIDED_IN" {
			oldLinks[[2]string{relation.FromSlug, relation.ToSlug}] = true
		}
	}
	for _, relation := range state.References.Relations {
		if relation.Present && relation.Type == "DECIDED_IN" {
			newLinks[[2]string{relation.FromSlug, relation.ToSlug}] = true
		}
	}
	for link := range oldLinks {
		if !newLinks[link] {
			seeds[link[0]] = true
			structural[link[0]] = true
		}
	}
	for link := range newLinks {
		if !oldLinks[link] {
			seeds[link[0]] = true
			structural[link[0]] = true
		}
	}
	for _, change := range state.ChangedSources {
		if _, isSpec := g.nodes[change.Slug]; isSpec {
			seeds[change.Slug] = true
			if change.After == "" {
				structural[change.Slug] = true
			} else {
				sources[change.Slug] = append(sources[change.Slug], storage.ReviewSource{Kind: "specgraph", SpecSlug: change.Slug, Field: change.Field, ChangeID: change.After})
			}
			continue
		}
		// A decision affects its actual linked Specs, not every member of a shared goal.
		for _, relations := range [][]storage.SummaryRelationReference{old.Relations, state.References.Relations} {
			for _, relation := range relations {
				if relation.Present && relation.Type == "DECIDED_IN" && relation.ToSlug == change.Slug {
					seeds[relation.FromSlug] = true
					if change.After == "" {
						structural[relation.FromSlug] = true
					} else {
						sources[relation.FromSlug] = append(sources[relation.FromSlug], storage.ReviewSource{Kind: "specgraph", SpecSlug: change.Slug, Field: change.Field, ChangeID: change.After})
					}
				}
			}
		}
	}
	oldNodes := map[string]storage.SummaryNodeReference{}
	for _, n := range old.Nodes {
		oldNodes[n.Slug] = n
	}
	for _, n := range state.References.Nodes {
		previous, ok := oldNodes[n.Slug]
		if !ok || previous.ID != n.ID || previous.Role != n.Role {
			state.ScopeChanged = true
			seeds[n.Slug] = true
			structural[n.Slug] = true
		}
		if !reflect.DeepEqual(previous.CompletionGeneration, n.CompletionGeneration) {
			seeds[n.Slug] = true
			structural[n.Slug] = true
		}
	}
	oldParents, newParents := map[string][]string{}, map[string][]string{}
	for _, relation := range old.Relations {
		if relation.Present && relation.Type == "COMPOSES" {
			oldParents[relation.ToSlug] = append(oldParents[relation.ToSlug], relation.FromSlug)
		}
	}
	for _, relation := range state.References.Relations {
		if relation.Present && relation.Type == "COMPOSES" {
			newParents[relation.ToSlug] = append(newParents[relation.ToSlug], relation.FromSlug)
		}
	}
	// Compare actual inherited source membership, not edge counts or event generations.
	// Removing an alternate path with the same sources requires a new root opinion,
	// but does not invent a changed obligation for each leaf.
	for _, o := range state.Obligations {
		if !o.Effective || o.Role != storage.SpecRoleWork {
			continue
		}
		previous, _ := summaryReach(o.Slug, oldParents)
		current, _ := summaryReach(o.Slug, newParents)
		for slug := range previous {
			if _, ok := before[slug]; !ok {
				delete(previous, slug)
			}
		}
		for slug := range current {
			if _, ok := after[slug]; !ok {
				delete(current, slug)
			}
		}
		if !reflect.DeepEqual(previous, current) {
			seeds[o.Slug] = true
			structural[o.Slug] = true
		}
	}
	seedSlugs := make([]string, 0, len(seeds))
	for slug := range seeds {
		seedSlugs = append(seedSlugs, slug)
	}
	slices.Sort(seedSlugs)
	impacts, err := s.requirementChangeCandidates(ctx, seedSlugs)
	if err != nil {
		return err
	}
	affected := map[string]bool{}
	mustReview := map[string]bool{}
	requiredSources := map[string][]storage.ReviewSource{}
	for seed, slugs := range impacts {
		for _, slug := range slugs {
			affected[slug] = true
			if structural[seed] {
				mustReview[slug] = true
			}
			requiredSources[slug] = append(requiredSources[slug], sources[seed]...)
		}
	}
	for _, o := range state.Obligations {
		if !o.Effective || o.Role != storage.SpecRoleWork || !affected[o.Slug] {
			continue
		}
		resolved := false
		if !mustReview[o.Slug] && !o.PendingReview && !slices.Contains(old.DispositionIDs, o.DispositionID) && len(requiredSources[o.Slug]) > 0 {
			for i := range state.Dispositions {
				d := &state.Dispositions[i]
				if d.ID != o.DispositionID || (d.Disposition != "retain" && d.Disposition != "adjust") {
					continue
				}
				resolved = true
				for i := range requiredSources[o.Slug] {
					source := &requiredSources[o.Slug][i]
					if !slices.Contains(d.AfterSources, *source) {
						resolved = false
						break
					}
				}
			}
		}
		if !resolved {
			state.ReviewCandidates = append(state.ReviewCandidates, o.Slug)
		}
	}
	slices.SortFunc(state.ChangedSources, func(a, b storage.SummarySourceChange) int {
		if a.Slug != b.Slug {
			return strings.Compare(a.Slug, b.Slug)
		}
		return strings.Compare(a.Field, b.Field)
	})
	return nil
}

func summaryCurrentSource(g *summaryGraph, source *storage.ReviewSource) bool {
	if source.Kind != "specgraph" {
		return true
	}
	if node, ok := g.nodes[source.SpecSlug]; ok {
		return node.SourceRefs[source.Field] == source.ChangeID
	}
	if decision, ok := g.decisions[source.SpecSlug]; ok {
		return decision.SourceRefs[source.Field] == source.ChangeID
	}
	return false
}
