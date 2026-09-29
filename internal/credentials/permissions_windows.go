// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package credentials

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"unsafe"

	"golang.org/x/sys/windows"
)

func currentUserSID() (*windows.SID, error) {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return nil, fmt.Errorf("get process token user: %w", err)
	}
	sid, err := user.User.Sid.Copy()
	runtime.KeepAlive(user)
	if err != nil {
		return nil, fmt.Errorf("copy process user SID: %w", err)
	}
	return sid, nil
}

func createPrivateTemp(dir string) (*os.File, error) {
	sid, err := currentUserSID()
	if err != nil {
		return nil, err
	}
	// The protected DACL is installed at creation, before any secret is written.
	sd, err := windows.SecurityDescriptorFromString("O:" + sid.String() + "D:P(A;;FA;;;" + sid.String() + ")")
	if err != nil {
		return nil, fmt.Errorf("build private security descriptor: %w", err)
	}
	var random [16]byte
	if _, err = rand.Read(random[:]); err != nil {
		return nil, fmt.Errorf("generate private temp name: %w", err)
	}
	name := filepath.Join(dir, ".credentials-"+hex.EncodeToString(random[:])+".yaml.tmp")
	nameUTF16, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return nil, fmt.Errorf("encode private temp path: %w", err)
	}
	sa := windows.SecurityAttributes{
		Length: uint32(unsafe.Sizeof(windows.SecurityAttributes{})), SecurityDescriptor: sd,
	}
	handle, err := windows.CreateFile(nameUTF16, windows.GENERIC_READ|windows.GENERIC_WRITE,
		0, &sa, windows.CREATE_NEW, windows.FILE_ATTRIBUTE_NORMAL, 0)
	runtime.KeepAlive(sd)
	if err != nil {
		return nil, fmt.Errorf("create private temp file: %w", err)
	}
	file := os.NewFile(uintptr(handle), name)
	// Filesystems that do not enforce the supplied ACL must fail before writing.
	var flags uint32
	if err = windows.GetVolumeInformationByHandle(handle, nil, 0, nil, nil, &flags, nil, 0); err != nil {
		_ = file.Close()    //nolint:errcheck // preserve volume query error
		_ = os.Remove(name) //nolint:errcheck // best-effort cleanup
		return nil, fmt.Errorf("verify filesystem ACL support: %w", err)
	}
	if flags&windows.FILE_PERSISTENT_ACLS == 0 {
		_ = file.Close()    //nolint:errcheck // preserve security failure
		_ = os.Remove(name) //nolint:errcheck // best-effort cleanup
		return nil, fmt.Errorf("verify filesystem ACL support: filesystem does not support persistent ACLs")
	}
	actual, err := windows.GetSecurityInfo(handle, windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		_ = file.Close()    //nolint:errcheck // preserve security query error
		_ = os.Remove(name) //nolint:errcheck // best-effort cleanup
		return nil, fmt.Errorf("verify private temp security: %w", err)
	}
	control, _, err := actual.Control()
	if err != nil {
		_ = file.Close()    //nolint:errcheck // preserve security query error
		_ = os.Remove(name) //nolint:errcheck // best-effort cleanup
		return nil, fmt.Errorf("verify private temp DACL control: %w", err)
	}
	if control&windows.SE_DACL_PROTECTED == 0 {
		_ = file.Close()    //nolint:errcheck // preserve security failure
		_ = os.Remove(name) //nolint:errcheck // best-effort cleanup
		return nil, fmt.Errorf("verify private temp security: DACL is not protected")
	}
	if reason := privateACLReason(actual, sid); reason != "" {
		_ = file.Close()    //nolint:errcheck // preserve security failure
		_ = os.Remove(name) //nolint:errcheck // best-effort cleanup
		return nil, fmt.Errorf("verify private temp security: %s", reason)
	}
	return file, nil
}

// privateACLReason accepts ordinary file ACEs only. It does not resolve group
// membership or conditional/object ACEs, so those cannot produce a safe verdict.
func privateACLReason(sd *windows.SECURITY_DESCRIPTOR, sid *windows.SID) string {
	if sd == nil {
		return "cannot confirm access: no security descriptor"
	}
	owner, _, err := sd.Owner()
	if err != nil || owner == nil {
		return fmt.Sprintf("cannot confirm owner: %v", err)
	}
	if !owner.Equals(sid) {
		return "owner is not the current user"
	}
	dacl, _, err := sd.DACL()
	if err != nil {
		return fmt.Sprintf("cannot confirm access: missing or unreadable DACL: %v", err)
	}
	if dacl == nil {
		return "NULL DACL allows access to everyone"
	}
	for i := uint32(0); i < uint32(dacl.AceCount); i++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(dacl, i, &ace); err != nil {
			return fmt.Sprintf("cannot confirm access: read ACE: %v", err)
		}
		if ace.Header.AceFlags&windows.INHERIT_ONLY_ACE != 0 {
			continue
		}
		switch ace.Header.AceType {
		case windows.ACCESS_DENIED_ACE_TYPE:
			// A deny ACE cannot grant another principal access.
		case windows.ACCESS_ALLOWED_ACE_TYPE:
			trustee := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
			if ace.Mask != 0 && !trustee.Equals(sid) {
				return "cannot confirm owner-only access: DACL allows access to another SID (" + trustee.String() + ")"
			}
		default:
			return fmt.Sprintf("cannot confirm access: unsupported ACE type %d", ace.Header.AceType)
		}
	}
	return ""
}

// CheckPermissions checks the actual owner and DACL, not Windows' synthesized
// Unix mode bits. Missing files are silent; failed or uncertain checks warn.
func CheckPermissions(path string) string {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return ""
	}
	if err != nil {
		return fmt.Sprintf("warning: cannot check credentials file %s permissions: %v", path, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Sprintf("warning: cannot check credentials file %s permissions: not a regular file", path)
	}
	sid, err := currentUserSID()
	if err != nil {
		return fmt.Sprintf("warning: cannot check credentials file %s permissions: %v", path, err)
	}
	sd, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		return fmt.Sprintf("warning: cannot check credentials file %s permissions: %v", path, err)
	}
	if reason := privateACLReason(sd, sid); reason != "" {
		return fmt.Sprintf("warning: credentials file %s: %s; restrict its Windows ACL to the current user (0600 equivalent)", path, reason)
	}
	return ""
}
