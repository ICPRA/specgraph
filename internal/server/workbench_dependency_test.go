// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeDependencyEdit(t *testing.T) {
	const valid = `{"prerequisite":"upstream","expected_version":2,"expected_prerequisite_version":3,"expected_revision":"0","reason":"Required input","idempotency_key":"edit-1"}`
	for _, tc := range []struct {
		name, body string
		valid      bool
	}{
		{"valid", valid, true},
		{"numeric revision", strings.Replace(valid, `"expected_revision":"0"`, `"expected_revision":0`, 1), false},
		{"missing revision", strings.Replace(valid, `"expected_revision":"0",`, "", 1), false},
		{"negative revision", strings.Replace(valid, `"expected_revision":"0"`, `"expected_revision":"-1"`, 1), false},
		{"duplicate", strings.Replace(valid, `"reason":"Required input"`, `"reason":"Required input","reason":"Other"`, 1), false},
		{"forged actor", strings.TrimSuffix(valid, "}") + `,"actor":"admin"}`, false},
		{"empty reason", strings.Replace(valid, "Required input", "  ", 1), false},
		{"null version", strings.Replace(valid, `"expected_version":2`, `"expected_version":null`, 1), false},
		{"extra document", valid + `{}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tc.body))
			req, err := decodeDependencyEdit(httptest.NewRecorder(), r)
			if !tc.valid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, int64(0), req.ExpectedRevision)
			require.Equal(t, "upstream", req.Prerequisite)
		})
	}
}
