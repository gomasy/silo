// Copyright (c) 2026 Feng Ruohang
//
// This file is part of Silo Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"encoding/base64"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	xhttp "github.com/minio/minio/internal/http"
)

func TestPutOptsFromHeadersReplicationTimestamps(t *testing.T) {
	stamp := time.Date(2026, 9, 15, 1, 2, 3, 123456789, time.UTC)
	context := base64.StdEncoding.EncodeToString([]byte(`{"purpose":"tag-replication"}`))
	for _, encryption := range []struct {
		name    string
		headers map[string]string
	}{
		{name: "none"},
		{name: "SSE-S3", headers: map[string]string{xhttp.AmzServerSideEncryption: xhttp.AmzEncryptionAES}},
		{name: "SSE-KMS", headers: map[string]string{xhttp.AmzServerSideEncryption: xhttp.AmzEncryptionKMS}},
		{name: "SSE-KMS-context", headers: map[string]string{
			xhttp.AmzServerSideEncryption: xhttp.AmzEncryptionKMS, xhttp.AmzServerSideEncryptionKmsID: "tag-replication-key",
			xhttp.AmzServerSideEncryptionKmsContext: context,
		}},
		{name: "SSE-C", headers: ssecKeyHeaders([]byte("01234567890123456789012345678901"), false)},
	} {
		t.Run(encryption.name, func(t *testing.T) {
			for _, trusted := range []bool{false, true} {
				t.Run("trusted="+strconv.FormatBool(trusted), func(t *testing.T) {
					for _, tagging := range []struct {
						name, header string
						want         time.Time
						invalid      bool
					}{
						{name: "absent"},
						{name: "nanoseconds", header: stamp.Format(time.RFC3339Nano), want: stamp},
						{name: "offset-whitespace", header: " " + stamp.In(time.FixedZone("UTC+8", 8*60*60)).Format(time.RFC3339Nano) + " ", want: stamp},
						{name: "invalid", header: "not-a-timestamp", invalid: true},
					} {
						t.Run(tagging.name, func(t *testing.T) {
							for _, metadata := range []map[string]string{nil, {"x-amz-meta-test": "kept"}} {
								hdr := make(http.Header)
								wantEncryption := make(http.Header)
								for key, value := range encryption.headers {
									hdr.Set(key, value)
									wantEncryption.Set(key, value)
								}
								hdr.Set(xhttp.MinIOSourceTaggingTimestamp, tagging.header)
								hdr.Set(xhttp.MinIOSourceMTime, stamp.Add(-time.Hour).Format(time.RFC3339Nano))
								hdr.Set(xhttp.MinIOSourceObjectRetentionTimestamp, stamp.Add(-time.Minute).Format(time.RFC3339Nano))
								hdr.Set(xhttp.MinIOSourceObjectLegalHoldTimestamp, stamp.Add(-time.Second).Format(time.RFC3339Nano))
								hdr.Set(xhttp.MinIOSourceETag, "source-etag")
								opts, err := putOptsFromHeaders(t.Context(), hdr, metadata, trusted)
								if trusted && tagging.invalid {
									if err == nil || !strings.Contains(err.Error(), xhttp.MinIOSourceTaggingTimestamp) {
										t.Fatalf("malformed trusted timestamp: got %v", err)
									}
									continue
								}
								if err != nil {
									t.Fatal(err)
								}
								wantTag, wantMTime, wantRetention, wantLegalhold, wantETag := time.Time{}, time.Time{}, time.Time{}, time.Time{}, ""
								if trusted {
									wantTag, wantMTime = tagging.want, stamp.Add(-time.Hour)
									wantRetention, wantLegalhold, wantETag = stamp.Add(-time.Minute), stamp.Add(-time.Second), "source-etag"
								}
								if !opts.ReplicationSourceTaggingTimestamp.Equal(wantTag) {
									t.Errorf("tag timestamp=%s, want %s", opts.ReplicationSourceTaggingTimestamp, wantTag)
								}
								if !opts.MTime.Equal(wantMTime) || !opts.ReplicationSourceRetentionTimestamp.Equal(wantRetention) ||
									!opts.ReplicationSourceLegalholdTimestamp.Equal(wantLegalhold) || opts.PreserveETag != wantETag || opts.ReplicationRequest != trusted {
									t.Error("other source fields did not preserve the replication trust boundary")
								}
								if opts.UserDefined == nil || (metadata != nil && !reflect.DeepEqual(opts.UserDefined, metadata)) {
									t.Errorf("metadata=%v, want nonnil map preserving %v", opts.UserDefined, metadata)
								}
								gotEncryption := make(http.Header)
								if opts.ServerSideEncryption != nil {
									opts.ServerSideEncryption.Marshal(gotEncryption)
								}
								if !reflect.DeepEqual(gotEncryption, wantEncryption) {
									t.Errorf("SSE headers=%v, want %v", gotEncryption, wantEncryption)
								}
							}
						})
					}
				})
			}
		})
	}
}
