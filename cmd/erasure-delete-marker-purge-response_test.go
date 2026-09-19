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
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/bucket/replication"
	xhttp "github.com/minio/minio/internal/http"
)

// An exact-version DELETE without a replication decision physically purges
// the version. Its response must describe the stored version: a data version
// is not a delete marker even while pending-purge metadata makes the lookup
// expose it as deleted, and a stored marker stays a marker.
func TestDeleteMarkerPurgeResponseIdentity(t *testing.T) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, endpoints: []string{"DeleteObject"}, objAPITest: func(obj ObjectLayer, backend, bucket string, router http.Handler, creds auth.Credentials, t *testing.T) {
		defer replicationTestCapacity(obj)()
		ctx := t.Context()
		if _, err := globalBucketMetadataSys.Update(ctx, bucket, bucketVersioningConfig, enabledBucketVersioningConfig); err != nil {
			t.Fatal(err)
		}
		// Seed the purge state the DELETE handler would record for a target.
		arn := "arn:minio:replication::" + mustGetUUID() + ":bucket"
		pendingPurge := ReplicationState{
			VersionPurgeStatusInternal: arn + "=PENDING;",
			PurgeTargets:               map[string]VersionPurgeStatusType{arn: replication.VersionPurgePending},
		}
		for _, tc := range []struct {
			name           string
			marker, purged bool
		}{
			{name: "data"},
			{name: "data-pending-purge", purged: true},
			{name: "marker", marker: true},
			{name: "marker-pending-purge", marker: true, purged: true},
		} {
			name := "purge-response-" + mustGetUUID()
			oi, err := obj.PutObject(ctx, bucket, name, mustGetPutObjReader(t, bytes.NewReader([]byte("data")), 4, "", ""), ObjectOptions{Versioned: true})
			if err != nil {
				t.Fatal(err)
			}
			version := oi.VersionID
			if tc.marker {
				opts := ObjectOptions{Versioned: true, VersionID: mustGetUUID(), DeleteMarker: true, MTime: UTCNow(), ReplicationRequest: true}
				opts.SetReplicaStatus(replication.Replica)
				if _, err := obj.DeleteObject(ctx, bucket, name, opts); err != nil {
					t.Fatal(err)
				}
				version = opts.VersionID
			}
			if tc.purged {
				if _, err := obj.DeleteObject(ctx, bucket, name, ObjectOptions{Versioned: true, VersionID: version, DeleteReplication: pendingPurge}); err != nil {
					t.Fatal(err)
				}
			}
			before, err := obj.GetObjectInfo(ctx, bucket, name, ObjectOptions{Versioned: true, VersionID: version})
			if tc.marker || tc.purged {
				if !isErrMethodNotAllowed(err) || !before.DeleteMarker {
					t.Fatalf("%s: seeded version not exposed as deleted: %+v %v", tc.name, before, err)
				}
			} else if err != nil || before.DeleteMarker {
				t.Fatalf("%s: seeded data version: %+v %v", tc.name, before, err)
			}
			if tc.purged && before.VersionPurgeStatus != replication.VersionPurgePending {
				t.Fatalf("%s: purge state not seeded: %+v", tc.name, before)
			}
			req, err := newTestSignedRequestV4(http.MethodDelete, "/"+bucket+"/"+name+"?versionId="+version, 0, nil, creds.AccessKey, creds.SecretKey, nil)
			if err != nil {
				t.Fatal(err)
			}
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusNoContent {
				t.Fatalf("%s: status=%d: %s", tc.name, rec.Code, rec.Body.String())
			}
			// The handler sets these headers by map key, not canonical name.
			if got := rec.Header()[xhttp.AmzVersionID]; len(got) != 1 || got[0] != version {
				t.Fatalf("%s: response version %q, want %q", tc.name, got, version)
			}
			if got := len(rec.Header()[xhttp.AmzDeleteMarker]) == 1 && rec.Header()[xhttp.AmzDeleteMarker][0] == "true"; got != tc.marker {
				t.Fatalf("%s: x-amz-delete-marker=%v for a stored marker=%v", tc.name, got, tc.marker)
			}
			if _, err := obj.GetObjectInfo(ctx, bucket, name, ObjectOptions{Versioned: true, VersionID: version}); !isErrVersionNotFound(err) && !isErrObjectNotFound(err) {
				t.Fatalf("%s: version not purged: %v", tc.name, err)
			}
			t.Logf("%s %s: purged with x-amz-delete-marker=%q", backend, tc.name, rec.Header()[xhttp.AmzDeleteMarker])
		}
	}})
}
