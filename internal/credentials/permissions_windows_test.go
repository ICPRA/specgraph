// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package credentials

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
)

// setTestDACL only changes a newly created fixture and restores it before cleanup.
func setTestDACL(t *testing.T, path, sddl string) {
	t.Helper()
	original, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	require.NoError(t, err)
	originalDACL, _, err := original.DACL()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT,
			windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
			nil, nil, originalDACL, nil))
		runtime.KeepAlive(original)
	})
	sd, err := windows.SecurityDescriptorFromString(sddl)
	require.NoError(t, err)
	dacl, _, err := sd.DACL()
	require.NoError(t, err)
	require.NoError(t, windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil, nil, dacl, nil))
	runtime.KeepAlive(sd)
}

func TestWindowsPrivateCreationAndReplacement(t *testing.T) {
	sid, err := currentUserSID()
	require.NoError(t, err)
	dir := t.TempDir()
	setTestDACL(t, dir, "D:P(A;;FA;;;"+sid.String()+")(A;OICI;FR;;;WD)")
	path := filepath.Join(dir, "credentials.yaml")
	require.NoError(t, os.WriteFile(path, []byte("old fixture"), 0o600))
	require.NotEmpty(t, CheckPermissions(path), "fixture must inherit the permissive parent ACE")
	f := &File{}
	f.Upsert("https://example.com", ServerCreds{Token: "fixture-token"})
	require.NoError(t, f.Save(path))
	require.Empty(t, CheckPermissions(path), "replacement must retain the private temp ACL")
	loaded, err := Load(path)
	require.NoError(t, err)
	require.Equal(t, "fixture-token", loaded.TokenFor("https://example.com"))
	require.NoError(t, f.Save(path), "private file must remain replaceable")
	require.Empty(t, CheckPermissions(path))
	sd, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	require.NoError(t, err)
	control, _, err := sd.Control()
	require.NoError(t, err)
	require.NotZero(t, control&windows.SE_DACL_PROTECTED)
}

func TestWindowsCheckPermissions(t *testing.T) {
	sid, err := currentUserSID()
	require.NoError(t, err)
	for _, tc := range []struct {
		name, dacl string
		private    bool
	}{
		{"owner-read-only", "D:P(A;;FR;;;" + sid.String() + ")", true},
		{"multiple-owner-allow", "D:P(A;;FR;;;" + sid.String() + ")(A;;FW;;;" + sid.String() + ")", true},
		{"deny-other", "D:P(D;;FR;;;WD)(A;;FA;;;" + sid.String() + ")", true},
		{"other-allow", "D:P(A;;FA;;;" + sid.String() + ")(A;;FR;;;WD)", false},
		{"system-allow", "D:P(A;;FA;;;" + sid.String() + ")(A;;FR;;;SY)", false},
		{"null-dacl", "D:NO_ACCESS_CONTROL", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "credentials.yaml")
			require.NoError(t, (&File{}).Save(path))
			setTestDACL(t, path, tc.dacl)
			if tc.private {
				require.Empty(t, CheckPermissions(path))
			} else {
				require.NotEmpty(t, CheckPermissions(path))
			}
		})
	}
	require.Empty(t, CheckPermissions(filepath.Join(t.TempDir(), "missing.yaml")))
	require.Contains(t, CheckPermissions(string([]byte{'x', 0})), "cannot check")
	require.Contains(t, CheckPermissions(t.TempDir()), "not a regular file")
}

func TestWindowsUnknownDescriptors(t *testing.T) {
	sid, err := currentUserSID()
	require.NoError(t, err)
	for _, sddl := range []string{
		"O:SYD:P(A;;FA;;;" + sid.String() + ")",
		"O:" + sid.String(),
		"O:" + sid.String() + "D:P(OA;;FR;00000000-0000-0000-0000-000000000001;;" + sid.String() + ")",
	} {
		sd, err := windows.SecurityDescriptorFromString(sddl)
		require.NoError(t, err)
		require.NotEmpty(t, privateACLReason(sd, sid))
	}
	require.NotEmpty(t, privateACLReason(nil, sid))
}

func TestWindowsSaveCreationFailureDoesNotWriteSecret(t *testing.T) {
	sid, err := currentUserSID()
	require.NoError(t, err)
	dir := t.TempDir()
	path := filepath.Join(dir, "credentials.yaml")
	require.NoError(t, (&File{}).Save(path))
	require.NoError(t, os.WriteFile(path, []byte("original fixture"), 0o600))
	// FILE_ADD_FILE is denied on this fixture directory, not on the user profile.
	setTestDACL(t, dir, "D:P(D;;0x2;;;"+sid.String()+")(A;;FA;;;"+sid.String()+")")
	f := &File{}
	f.Upsert("https://example.com", ServerCreds{Token: "must-not-be-written"}) //nolint:gosec // fake token proves failed creation never writes it
	require.ErrorContains(t, f.Save(path), "create private temp file")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "original fixture", string(data))
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

func TestWindowsRenameFailureCleansPrivateTemp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "credentials.yaml")
	require.NoError(t, (&File{}).Save(path))
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	name, err := windows.UTF16PtrFromString(path)
	require.NoError(t, err)
	handle, err := windows.CreateFile(name, windows.GENERIC_READ, windows.FILE_SHARE_READ,
		nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, windows.CloseHandle(handle)) }()
	f := &File{}
	f.Upsert("https://example.com", ServerCreds{Token: "replacement fixture"})
	require.ErrorContains(t, f.Save(path), "rename credentials file")
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
}
