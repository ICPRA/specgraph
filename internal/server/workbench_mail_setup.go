// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"log/slog"

	"connectrpc.com/connect"
	specv1 "github.com/specgraph/specgraph/gen/specgraph/v1"
	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
)

// CreateLocalMailCredential requires serviceaccount.manage and apikey.manage.
// Its caller keeps the returned plaintext inside the local initialization flow.
func CreateLocalMailCredential(ctx context.Context, users storage.UsersBackend, owner string) (map[string]string, error) {
	handler := &IdentityHandler{users: users, logger: slog.Default()}
	account, err := handler.CreateServiceAccount(ctx, connect.NewRequest(&specv1.CreateServiceAccountRequest{
		DisplayName: "VACPMS local mail", Role: string(auth.RoleReader), OwnerUserId: owner,
	}))
	if err != nil {
		return nil, err
	}
	key, err := handler.CreateAPIKey(ctx, connect.NewRequest(&specv1.CreateAPIKeyRequest{
		UserId: account.Msg.User.Id, Label: "VACPMS local mail",
	}))
	if err != nil {
		return nil, err
	}
	return map[string]string{"accountId": account.Msg.User.Id, "keyId": key.Msg.Key.Id, "token": key.Msg.Plaintext}, nil
}
