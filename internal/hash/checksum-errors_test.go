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

package hash

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestChecksumMismatchAlgorithm(t *testing.T) {
	content := []byte("actual content")
	for _, typ := range []ChecksumType{ChecksumCRC32, ChecksumCRC32C, ChecksumSHA1, ChecksumSHA256, ChecksumCRC64NVME} {
		t.Run(typ.String(), func(t *testing.T) {
			wrong := NewChecksumFromData(typ, []byte("wrong content"))
			check := func(t *testing.T, err error) {
				t.Helper()
				var mismatch ChecksumMismatch
				if !errors.As(err, &mismatch) || mismatch.Algorithm != typ.String() {
					t.Fatalf("error=%+v, want %s checksum mismatch", err, typ)
				}
			}
			check(t, wrong.Matches(content, 0))
			parts := NewChecksumFromData(typ, content)
			parts.WantParts = 2
			check(t, parts.Matches(content, 1))
			for _, mode := range []string{"header", "trailer", "malformed-trailer"} {
				t.Run(mode, func(t *testing.T) {
					reader, err := NewReader(t.Context(), bytes.NewReader(content), int64(len(content)), "", "", int64(len(content)))
					if err != nil {
						t.Fatal(err)
					}
					req := httptest.NewRequest(http.MethodPut, "http://localhost/bucket/object", nil)
					if mode == "header" {
						req.Header.Set(typ.Key(), wrong.Encoded)
					} else {
						req.Header.Set("x-amz-trailer", typ.Key())
						req.Trailer = make(http.Header)
						value := wrong.Encoded
						if mode == "malformed-trailer" {
							value = "!invalid-base64!"
						}
						req.Trailer.Set(typ.Key(), value)
					}
					if err = reader.AddChecksum(req, false); err != nil {
						t.Fatal(err)
					}
					_, err = io.ReadAll(reader)
					check(t, err)
				})
			}
		})
	}
}
