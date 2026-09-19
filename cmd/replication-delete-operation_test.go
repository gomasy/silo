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
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio/internal/bucket/replication"
	xhttp "github.com/minio/minio/internal/http"
)

// Purge uses the version-delete wire form regardless of the in-memory shape.
// Creation status, purge status and successful resync markers are independent.
func TestReplicateDeleteOperationExits(t *testing.T) {
	for _, shape := range []string{"marker-creation", "legacy-marker-purge", "marker-purge", "object-purge"} {
		purge := shape != "marker-creation"
		for _, tc := range []struct {
			name                 string
			creation             replication.StatusType
			priorPurge           VersionPurgeStatusType
			headCode, deleteCode int
			headError            string
			offline, resync      bool
		}{
			{name: "pending-success", creation: replication.Pending, priorPurge: replication.VersionPurgePending, headCode: 404, deleteCode: 204},
			{name: "completed-creation", creation: replication.Completed, priorPurge: replication.VersionPurgePending, headCode: 405, deleteCode: 204},
			{name: "existing-marker", creation: replication.Pending, priorPurge: replication.VersionPurgePending, headCode: 405, deleteCode: 204},
			// Use the quorum S3 code without 503, which is classified as backend-down first.
			{name: "head-read-quorum", creation: replication.Pending, priorPurge: replication.VersionPurgePending, headCode: 400, headError: "SlowDownRead", deleteCode: 204},
			{name: "replica-creation-status", creation: replication.Replica, priorPurge: replication.VersionPurgeFailed, headCode: 405, deleteCode: 204},
			{name: "head-unavailable", creation: replication.Pending, priorPurge: replication.VersionPurgePending, headCode: 503, deleteCode: 204},
			{name: "head-forbidden", creation: replication.Pending, priorPurge: replication.VersionPurgePending, headCode: 403, deleteCode: 204},
			{name: "delete-forbidden", creation: replication.Pending, priorPurge: replication.VersionPurgePending, headCode: 404, deleteCode: 403},
			{name: "delete-method-rejected", creation: replication.Pending, priorPurge: replication.VersionPurgePending, headCode: 404, deleteCode: 405},
			{name: "delete-unavailable", creation: replication.Pending, priorPurge: replication.VersionPurgePending, headCode: 404, deleteCode: 503},
			{name: "offline", creation: replication.Pending, priorPurge: replication.VersionPurgePending, headCode: 404, deleteCode: 204, offline: true},
			{name: "retry-failed", creation: replication.Completed, priorPurge: replication.VersionPurgeFailed, headCode: 404, deleteCode: 204},
			{name: "purge-complete", creation: replication.Pending, priorPurge: replication.VersionPurgeComplete, headCode: 404, deleteCode: 403},
			{name: "resync-success", creation: replication.Completed, priorPurge: replication.VersionPurgePending, headCode: 405, deleteCode: 204, resync: true},
			{name: "resync-already-purged", creation: replication.Pending, priorPurge: replication.VersionPurgeComplete, headCode: 404, deleteCode: 403, resync: true},
			{name: "resync-failure", creation: replication.Completed, priorPurge: replication.VersionPurgePending, headCode: 404, deleteCode: 403, resync: true},
		} {
			t.Run(shape+"/"+tc.name, func(t *testing.T) {
				version := mustGetUUID()
				var heads, deletes atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Query().Get("versionId") != version {
						t.Errorf("wrong request version: %s", r.URL)
					}
					switch r.Method {
					case http.MethodHead:
						heads.Add(1)
						w.Header().Set(xhttp.AmzVersionID, version)
						w.Header().Set(xhttp.LastModified, time.Now().UTC().Format(http.TimeFormat))
						if tc.headCode == 405 {
							w.Header().Set(xhttp.AmzDeleteMarker, "true")
						}
						if tc.headError != "" {
							w.Header().Set("x-minio-error-code", tc.headError)
						}
						w.WriteHeader(tc.headCode)
					case http.MethodDelete:
						deletes.Add(1)
						if got := r.Header.Get(xhttp.MinIOSourceDeleteMarker) == "true"; got == purge {
							t.Errorf("source delete-marker header=%v, purge=%v", got, purge)
						}
						w.WriteHeader(tc.deleteCode)
						if tc.deleteCode != 204 {
							fmt.Fprintf(w, `<Error><Code>%s</Code><Message>injected rejection</Message></Error>`, map[int]string{403: "AccessDenied", 405: "MethodNotAllowed", 503: "ServiceUnavailable"}[tc.deleteCode])
						}
					default:
						t.Errorf("unexpected method %s", r.Method)
					}
				}))
				defer server.Close()
				client, err := minio.New(strings.TrimPrefix(server.URL, "http://"), &minio.Options{Region: "us-east-1", MaxRetries: 1})
				if err != nil {
					t.Fatal(err)
				}
				old := globalBucketTargetSys
				globalBucketTargetSys = &BucketTargetSys{hc: map[string]epHealth{client.EndpointURL().Host: {Online: !tc.offline}}}
				defer func() { globalBucketTargetSys = old }()
				d := DeletedObjectReplicationInfo{Bucket: "source", DeletedObject: DeletedObject{ObjectName: "marker", DeleteMarker: shape != "object-purge"}}
				if shape == "marker-creation" || shape == "legacy-marker-purge" {
					d.DeleteMarkerVersionID = version
				} else {
					d.VersionID = version
				}
				d.ReplicationState.Targets = map[string]replication.StatusType{"arn1": tc.creation}
				d.ReplicationState.ResetStatusesMap = map[string]string{"arn1": "previous-reset"}
				if purge {
					d.ReplicationState.PurgeTargets = map[string]VersionPurgeStatusType{"arn1": tc.priorPurge}
				}
				if tc.resync {
					d.OpType = replication.ExistingObjectReplicationType
				}
				got := replicateDeleteToTarget(t.Context(), d, &TargetClient{Client: client, ARN: "arn1", Bucket: "target", ResetID: "current-reset"})
				var wantCreation replication.StatusType
				var wantPurge VersionPurgeStatusType
				var wantHeads, wantDeletes int32
				success := false
				if purge {
					wantCreation = ""
					wantPurge = replication.VersionPurgeComplete
					switch {
					case tc.priorPurge == replication.VersionPurgeComplete:
						success = true
					case tc.offline:
						wantPurge = replication.VersionPurgeFailed
					default:
						wantDeletes = 1
						success = tc.deleteCode == 204
						if !success {
							wantPurge = replication.VersionPurgeFailed
						}
					}
				} else {
					switch {
					case tc.creation == replication.Completed && !tc.resync:
						success = true
					case tc.offline:
					default:
						wantHeads = 1
						switch tc.headCode {
						case 405:
							success = true
						case 403, 503:
						default:
							wantDeletes = 1
							success = tc.deleteCode == 204
						}
					}
					if success {
						wantCreation = replication.Completed
					} else {
						wantCreation = replication.Failed
					}
				}
				if got.PrevReplicationStatus != tc.creation {
					t.Error("previous creation state changed")
				}
				if got.ReplicationStatus != wantCreation || got.VersionPurgeStatus != wantPurge {
					t.Errorf("status=%+v, want creation=%s purge=%s", got, wantCreation, wantPurge)
				}
				if heads.Load() != wantHeads || deletes.Load() != wantDeletes {
					t.Errorf("HEAD/DELETE=%d/%d, want %d/%d", heads.Load(), deletes.Load(), wantHeads, wantDeletes)
				}
				if (got.Err != nil) == success {
					t.Errorf("success=%v, error=%v", success, got.Err)
				}
				if tc.resync && success {
					if !strings.HasSuffix(got.ResyncTimestamp, ";current-reset") {
						t.Errorf("successful resync missing reset: %q", got.ResyncTimestamp)
					}
				} else if got.ResyncTimestamp != "previous-reset" {
					t.Errorf("unexpected reset: %q", got.ResyncTimestamp)
				}
			})
		}
	}
}

func TestReplicateDeletePurgeMissingTargetState(t *testing.T) {
	// Classification belongs to the operation, even if this target has no
	// previous purge entry (another target supplies the tracked purge state).
	var deletes atomic.Int32
	version := mustGetUUID()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Query().Get("versionId") != version || r.Header.Get(xhttp.MinIOSourceDeleteMarker) == "true" {
			t.Errorf("wrong purge request: %s %s %v", r.Method, r.URL, r.Header)
		}
		deletes.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, err := minio.New(strings.TrimPrefix(server.URL, "http://"), &minio.Options{Region: "us-east-1", MaxRetries: 1})
	if err != nil {
		t.Fatal(err)
	}
	old := globalBucketTargetSys
	globalBucketTargetSys = &BucketTargetSys{hc: map[string]epHealth{client.EndpointURL().Host: {Online: true}}}
	defer func() { globalBucketTargetSys = old }()
	d := DeletedObjectReplicationInfo{Bucket: "source", DeletedObject: DeletedObject{ObjectName: "marker", DeleteMarker: true, DeleteMarkerVersionID: version, ReplicationState: ReplicationState{Targets: map[string]replication.StatusType{"arn1": replication.Completed}, PurgeTargets: map[string]VersionPurgeStatusType{"arn2": replication.VersionPurgePending}}}}
	got := replicateDeleteToTarget(t.Context(), d, &TargetClient{Client: client, ARN: "arn1", Bucket: "target", ResetID: "reset"})
	if deletes.Load() != 1 || got.VersionPurgeStatus != replication.VersionPurgeComplete || got.ReplicationStatus != "" {
		t.Fatalf("operation misclassified: %+v, deletes=%d", got, deletes.Load())
	}
	got.ResyncTimestamp = "resync;reset"
	state := getReplicationState(replicatedInfos{Targets: []replicatedTargetInfo{got}}, ReplicationState{}, "")
	if state.ResetStatusesMap[targetResetHeader("arn1")] != got.ResyncTimestamp {
		t.Fatal("resync timestamp not preserved with nil reset map")
	}
}
