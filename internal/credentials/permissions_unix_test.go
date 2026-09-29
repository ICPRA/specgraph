//go:build !windows

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package credentials_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/specgraph/specgraph/internal/credentials"
)

func TestCheckPermissions(t *testing.T) {
	dir := t.TempDir()
	for _, mode := range []os.FileMode{0o600, 0o400, 0o644} {
		path := filepath.Join(dir, mode.String()+".yaml")
		require.NoError(t, os.WriteFile(path, []byte("servers: {}\n"), mode))
		if mode&0o077 == 0 {
			require.Empty(t, credentials.CheckPermissions(path))
		} else {
			require.NotEmpty(t, credentials.CheckPermissions(path))
		}
	}
	require.Empty(t, credentials.CheckPermissions(filepath.Join(dir, "absent.yaml")))
	path := filepath.Join(dir, "saved.yaml")
	require.NoError(t, (&credentials.File{}).Save(path))
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	require.Contains(t, credentials.CheckPermissions(string([]byte{'x', 0})), "cannot check")
}
