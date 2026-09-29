// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDecodeManualCompletion(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		valid      bool
	}{
		{"valid", `{"expected_version":1,"idempotency_key":"manual-1","note":"human assertion"}`, true},
		{"missing version", `{"idempotency_key":"x"}`, false},
		{"fractional version", `{"expected_version":1.5,"idempotency_key":"x"}`, false},
		{"overflow", `{"expected_version":2147483648,"idempotency_key":"x"}`, false},
		{"actor injection", `{"expected_version":1,"idempotency_key":"x","actor":"admin"}`, false},
		{"bad key", `{"expected_version":1,"idempotency_key":"a b"}`, false},
		{"trailing JSON", `{"expected_version":1,"idempotency_key":"x"}{}`, false},
		{"null", `null`, false},
		{"null note", `{"expected_version":1,"idempotency_key":"x","note":null}`, false},
		{"case alias", `{"Expected_Version":1,"idempotency_key":"x"}`, false},
		{"duplicate", `{"expected_version":1,"expected_version":2,"idempotency_key":"x"}`, false},
		{"long note", `{"expected_version":1,"idempotency_key":"x","note":"` + strings.Repeat("a", 4001) + `"}`, false},
		{"large body", strings.Repeat(" ", 32769) + `{}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeManualCompletion(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tc.body)))
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v err=%v", tc.valid, err)
			}
		})
	}
}
