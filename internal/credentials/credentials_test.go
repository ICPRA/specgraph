// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package credentials_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/specgraph/specgraph/internal/credentials"
)

func TestSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "credentials.yaml")

	f := &credentials.File{}
	f.Upsert("https://api.example.com", credentials.ServerCreds{
		Token: "tok-123",
		Label: "prod",
	})

	if err := f.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}

	if warning := credentials.CheckPermissions(path); warning != "" {
		t.Fatal(warning)
	}

	loaded, err := credentials.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := loaded.TokenFor("https://api.example.com"); got != "tok-123" {
		t.Fatalf("TokenFor = %q, want tok-123", got)
	}
}

func TestUpsertPreservesOtherServers(t *testing.T) {
	f := &credentials.File{}
	f.Upsert("https://a.example.com", credentials.ServerCreds{Token: "tok-a"})
	f.Upsert("https://b.example.com", credentials.ServerCreds{Token: "tok-b"})

	// Overwrite a without disturbing b.
	f.Upsert("https://a.example.com", credentials.ServerCreds{Token: "tok-a2"})

	if got := f.TokenFor("https://a.example.com"); got != "tok-a2" {
		t.Fatalf("TokenFor(a) = %q, want tok-a2", got)
	}
	if got := f.TokenFor("https://b.example.com"); got != "tok-b" {
		t.Fatalf("TokenFor(b) = %q, want tok-b", got)
	}
}

func TestLoadMissingFileIsEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "does-not-exist.yaml")

	f, err := credentials.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if f == nil {
		t.Fatal("Load returned nil File")
	}
	if got := f.TokenFor("https://anything"); got != "" {
		t.Fatalf("TokenFor on empty = %q, want empty", got)
	}
}

func TestLoadOldShapeYieldsNoServers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "credentials.yaml")

	old := "api_keys:\n  - id: foo\n    key: sg_oldkey\n"
	if err := os.WriteFile(path, []byte(old), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	f, err := credentials.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := f.TokenFor("https://anything"); got != "" {
		t.Fatalf("TokenFor on old-shape = %q, want empty", got)
	}
}

func TestTokenForNormalizesTrailingSlash(t *testing.T) {
	f := &credentials.File{}
	f.Upsert("https://api.example.com/", credentials.ServerCreds{Token: "tok-x"})

	if got := f.TokenFor("https://api.example.com"); got != "tok-x" {
		t.Fatalf("TokenFor(no slash) = %q, want tok-x", got)
	}
	if got := f.TokenFor("https://api.example.com/"); got != "tok-x" {
		t.Fatalf("TokenFor(slash) = %q, want tok-x", got)
	}
}

func TestSaveReplacePreservesOtherServers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credentials.yaml")
	f := &credentials.File{}
	f.Upsert("https://a.example.com", credentials.ServerCreds{Token: "old-a"})
	f.Upsert("https://b.example.com", credentials.ServerCreds{Token: "tok-b"})
	if err := f.Save(path); err != nil {
		t.Fatal(err)
	}
	loaded, err := credentials.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	loaded.Upsert("https://a.example.com", credentials.ServerCreds{Token: "new-a"})
	if err = loaded.Save(path); err != nil {
		t.Fatal(err)
	}
	loaded, err = credentials.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.TokenFor("https://a.example.com") != "new-a" || loaded.TokenFor("https://b.example.com") != "tok-b" {
		t.Fatalf("replacement lost credentials: %+v", loaded.Servers)
	}
	if warning := credentials.CheckPermissions(path); warning != "" {
		t.Fatal(warning)
	}
}
