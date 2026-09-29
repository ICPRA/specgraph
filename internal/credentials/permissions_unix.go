//go:build !windows

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package credentials

import (
	"fmt"
	"os"
)

func createPrivateTemp(dir string) (*os.File, error) {
	file, err := os.CreateTemp(dir, ".credentials-*.yaml.tmp")
	if err != nil {
		return nil, fmt.Errorf("create private temp file: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()           //nolint:errcheck // preserve chmod error
		_ = os.Remove(file.Name()) //nolint:errcheck // best-effort cleanup
		return nil, fmt.Errorf("chmod private temp file: %w", err)
	}
	return file, nil
}

// CheckPermissions warns about group/other access or an unsuccessful check.
// Missing files and owner-only modes, including 0400, are accepted.
func CheckPermissions(path string) string {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return ""
	}
	if err != nil {
		return fmt.Sprintf("warning: cannot check credentials file %s permissions: %v", path, err)
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		return fmt.Sprintf(
			"warning: credentials file %s has permissions %04o; recommend 0600 (chmod 600 %q)",
			path, perm, path,
		)
	}
	return ""
}
