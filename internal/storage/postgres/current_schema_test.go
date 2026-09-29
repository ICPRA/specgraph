// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration || schema_init

package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	dockercontainer "github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestCurrentSchemaInitializationAndVersionBoundary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "pgvector/pgvector:pg18", ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{"POSTGRES_USER": "schema_check", "POSTGRES_PASSWORD": "schema_check", "POSTGRES_DB": "schema_check"},
			HostConfigModifier: func(config *dockercontainer.HostConfig) {
				config.PortBindings = nat.PortMap{"5432/tcp": []nat.PortBinding{{HostIP: "127.0.0.1"}}}
				config.Tmpfs = map[string]string{"/var/lib/postgresql": "rw"}
			},
			WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60 * time.Second),
		}, Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		require.NoError(t, c.Terminate(cleanup))
		t.Logf("terminated owned baseline check container %s", c.GetContainerID())
	})
	ports, err := c.Ports(ctx)
	require.NoError(t, err)
	require.Len(t, ports["5432/tcp"], 1)
	require.Equal(t, "127.0.0.1", ports["5432/tcp"][0].HostIP)
	port, err := c.MappedPort(ctx, "5432/tcp")
	require.NoError(t, err)
	url := fmt.Sprintf("postgres://schema_check:schema_check@127.0.0.1:%s/schema_check?sslmode=disable", port.Port())
	connection, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connection.Close(context.Background())) })
	for _, open := range []func(context.Context, string) (*Store, error){OpenExisting, OpenExistingReadOnly} {
		_, err := open(ctx, url)
		require.ErrorIs(t, err, ErrSchemaVersionMismatch)
	}
	var exists bool
	require.NoError(t, connection.QueryRow(ctx, `SELECT to_regclass('public.goose_db_version') IS NOT NULL`).Scan(&exists))
	require.False(t, exists, "existing-database open must never initialize a version table")
	s, err := New(ctx, url, WithProject("current-schema"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	var version int64
	require.NoError(t, connection.QueryRow(ctx, businessSchemaVersionSQL).Scan(&version))
	require.Equal(t, businessSchemaVersion, version)
	reopened, err := New(ctx, url, WithProject("current-schema"))
	require.NoError(t, err)
	require.NoError(t, reopened.Close(ctx))
	for _, open := range []func(context.Context, string) (*Store, error){OpenExisting, OpenExistingReadOnly} {
		store, err := open(ctx, url)
		require.NoError(t, err)
		_, err = store.GetProject(ctx, "current-schema")
		require.NoError(t, err)
		require.NoError(t, store.Close(ctx))
	}
	var markers int
	require.NoError(t, connection.QueryRow(ctx, `SELECT count(*) FROM public.goose_db_version`).Scan(&markers))
	require.Equal(t, 2, markers, "zero marker plus one current baseline; reopening adds no migration history")
	require.NoError(t, connection.QueryRow(ctx, `SELECT to_regclass('public.goose_db_version_auth') IS NOT NULL`).Scan(&exists))
	require.False(t, exists, "business initialization does not initialize the separate auth owner")
	_, err = connection.Exec(ctx, `UPDATE public.goose_db_version SET version_id=25 WHERE version_id=$1`, businessSchemaVersion)
	require.NoError(t, err)
	const observed = `SELECT jsonb_agg(to_jsonb(v) ORDER BY id)::text FROM public.goose_db_version v`
	var before, after string
	require.NoError(t, connection.QueryRow(ctx, observed).Scan(&before))
	_, err = New(ctx, url, WithProject("must-not-create"))
	require.ErrorIs(t, err, ErrSchemaVersionMismatch)
	require.Contains(t, err.Error(), "rebuild")
	for _, open := range []func(context.Context, string) (*Store, error){OpenExisting, OpenExistingReadOnly} {
		_, err := open(ctx, url)
		require.ErrorIs(t, err, ErrSchemaVersionMismatch)
	}
	require.NoError(t, connection.QueryRow(ctx, observed).Scan(&after))
	require.Equal(t, before, after, "old-version rejection must not update migration state")
	require.NoError(t, connection.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM projects WHERE slug='must-not-create')`).Scan(&exists))
	require.False(t, exists)
}
