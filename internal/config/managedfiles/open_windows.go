// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build windows

package managedfiles

import (
	"fmt"
	"os"
)

// readFileNoFollow falls back to os.ReadFile on Windows; symlink rejection
// is handled by the rejectSymlinkComponents walk only. Documented in doc.go
// as best-effort, not a security boundary.
func readFileNoFollow(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return data, nil
}

// noFollowIsSymlink is always false on Windows.
func noFollowIsSymlink(_ error) bool { return false }
