// Copyright (c) 2026 PGSTY
//
// This file is part of SILO Object Storage.
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/minio/minio/internal/hash"
)

func TestChecksumMismatchAPIMessage(t *testing.T) {
	for _, algorithm := range []string{"CRC32", "CRC32C", "SHA1", "SHA256", "CRC64NVME", ""} {
		t.Run(algorithm, func(t *testing.T) {
			mismatch := hash.ChecksumMismatch{Algorithm: algorithm, Want: "expected", Got: "actual"}
			want := errorCodes.ToAPIErr(ErrContentChecksumMismatch).Description
			if algorithm != "" {
				want = fmt.Sprintf("The %s you specified did not match the calculated checksum.", algorithm)
			}
			for _, err := range []error{mismatch, fmt.Errorf("upload: %w", mismatch)} {
				got := toAPIError(t.Context(), err)
				if got.HTTPStatusCode != http.StatusBadRequest || got.Code != "XAmzContentChecksumMismatch" || got.Description != want {
					t.Fatalf("toAPIError(%v) = %+v, want 400 XAmzContentChecksumMismatch: %s", err, got, want)
				}
			}
		})
	}
}
