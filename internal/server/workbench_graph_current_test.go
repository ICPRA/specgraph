// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package server

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/specgraph/specgraph/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestProjectCurrentGraphPaging(t *testing.T) {
	graph := &storage.FullGraph{}
	for i := 104; i >= 0; i-- {
		graph.Nodes = append(graph.Nodes, storage.GraphNode{Slug: fmt.Sprintf("node-%03d", i), Label: storage.NodeLabelSpec, Stage: "approved", Intent: "must stay private", Priority: "p2"})
		graph.Edges = append(graph.Edges, &storage.Edge{FromID: fmt.Sprintf("node-%03d", i), ToID: "parent", EdgeType: storage.EdgeTypeComposes})
	}
	first := projectCurrentGraph(graph, 0)
	require.Len(t, first.Nodes, 100)
	require.Len(t, first.Edges, 100)
	require.Equal(t, "node-000", first.Nodes[0].Slug)
	require.Equal(t, "node-099", first.Edges[99].From)
	require.Equal(t, 105, first.TotalNodes)
	require.Equal(t, 105, first.TotalEdges)
	require.True(t, first.HasMore)
	require.Equal(t, 100, *first.NextOffset)
	second := projectCurrentGraph(graph, *first.NextOffset)
	require.Len(t, second.Nodes, 5)
	require.Len(t, second.Edges, 5)
	require.Equal(t, "node-104", second.Nodes[4].Slug)
	require.False(t, second.HasMore)
	require.Nil(t, second.NextOffset)
	beyond := projectCurrentGraph(graph, 2147483647)
	require.Empty(t, beyond.Nodes)
	require.Empty(t, beyond.Edges)
	require.False(t, beyond.HasMore)
	encoded, err := json.Marshal(first)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "must stay private")
	encoded, err = json.Marshal(beyond)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"nodes":[]`)
	require.Contains(t, string(encoded), `"edges":[]`)
	unequal := &storage.FullGraph{Nodes: graph.Nodes[:2], Edges: graph.Edges}
	require.True(t, projectCurrentGraph(unequal, 0).HasMore)
	tail := projectCurrentGraph(unequal, 100)
	require.Empty(t, tail.Nodes)
	require.Len(t, tail.Edges, 5)
	require.False(t, tail.HasMore)
}
