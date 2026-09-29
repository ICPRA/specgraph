// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build !darwin && !linux

package service

import "errors"

// Windows has no user-level service manager integration yet; the specgraph
// CLI runs as a foreground process (or under a Windows service wrapper).
var errUnsupported = errors.New("specgraph service management is not supported on this platform; run `specgraph serve` in the foreground")

func generate(_ string, _ Config) (string, error) {
	return "", errUnsupported
}

func install(_ string) error {
	return errUnsupported
}

func uninstall(_ string) error {
	return errUnsupported
}

func stop() error {
	return errUnsupported
}

func isInstalled() bool {
	return false
}
