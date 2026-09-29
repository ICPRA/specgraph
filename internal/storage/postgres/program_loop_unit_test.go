// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestProgramLoopEventScanPreservesNoRows(t *testing.T) {
	_, err := scanProgramLoopEvent(workbenchReadRow{err: pgx.ErrNoRows})
	require.ErrorIs(t, err, pgx.ErrNoRows)
	require.ErrorContains(t, err, "scan program loop event")
}
