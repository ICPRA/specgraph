// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package server

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
)

func TestNodeOwnerRequiresExplicitExpectedOwner(t *testing.T) {
	ctx := auth.WithIdentity(context.Background(), &auth.Identity{UserID: "human", UserKind: storage.KindHuman})
	for _, operation := range []string{"node-owner-take-begin", "node-owner-return"} {
		_, err := ExecuteNodeOwnershipCommand(ctx, nil, "fixture", operation,
			strings.NewReader(`{"taskSlug":"task","expectedVersion":1,"idempotencyKey":"key","reason":"Named reason"}`))
		if !errors.Is(err, storage.ErrInvalidNodeOwnership) {
			t.Fatalf("%s omission error = %v", operation, err)
		}
	}
	_, err := ExecuteLocalWorkbenchKnowledge(context.Background(), nil, "", "node-owner-read",
		strings.NewReader(`{"environment_id":"local","native_project_id":"native"}`))
	if err == nil {
		t.Fatal("node owner read without taskSlug accepted")
	}
}
