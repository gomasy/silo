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
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/bucket/replication"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/once"
)

type staleCreationCase struct {
	name                             string
	purged, pendingPurge, unreadable bool
}

// A queued delete-marker creation must be checked against the source marker
// under the replication lock before it is sent. Between queueing (DELETE
// handler, GET/HEAD/LIST heal, scanner, MRF) and sending, a user purge of the
// same marker can complete and reach the targets; the stale creation would
// then recreate the marker there.
func TestReplicationDeleteMarkerCreationRevalidated(t *testing.T) {
	for _, tc := range []staleCreationCase{
		{name: "current"},
		{name: "purged", purged: true},
		{name: "purge-pending", pendingPurge: true},
		{name: "unreadable", unreadable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, endpoints: []string{"DeleteObject"}, objAPITest: func(obj ObjectLayer, backend, bucket string, router http.Handler, creds auth.Credentials, t *testing.T) {
				testReplicationDeleteMarkerCreationRevalidated(t, obj, backend, bucket, router, creds, tc)
			}})
		})
	}
}

type staleCreationTarget struct {
	arn, bucket string
	creations   atomic.Int32
	purges      atomic.Int32
}

func testReplicationDeleteMarkerCreationRevalidated(t *testing.T, obj ObjectLayer, backend, bucket string, router http.Handler, creds auth.Credentials, tc staleCreationCase) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	defer replicationTestCapacity(obj)()
	stats := NewReplicationStats(ctx, nil)
	oldStats := globalReplicationStats.Swap(stats)
	defer globalReplicationStats.Store(oldStats)
	oldPool := globalReplicationPool
	defer func() { globalReplicationPool = oldPool }()
	const name = "marker"
	if _, err := globalBucketMetadataSys.Update(ctx, bucket, bucketVersioningConfig, enabledBucketVersioningConfig); err != nil {
		t.Fatal(err)
	}
	if _, err := obj.PutObject(ctx, bucket, name, mustGetPutObjReader(t, bytes.NewReader([]byte("data")), 4, "", ""), ObjectOptions{Versioned: true}); err != nil {
		t.Fatal(err)
	}
	target := &staleCreationTarget{arn: "arn:minio:replication::" + mustGetUUID() + ":bucket", bucket: getRandomBucketName()}
	if err := obj.MakeBucket(ctx, target.bucket, MakeBucketOptions{VersioningEnabled: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := obj.PutObject(ctx, target.bucket, name, mustGetPutObjReader(t, bytes.NewReader([]byte("data")), 4, "", ""), ObjectOptions{Versioned: true}); err != nil {
		t.Fatal(err)
	}
	// The remote behaves like a real receiver: a marker lookup and a
	// replicated DELETE applied to the target bucket on the same fixture.
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		opts := ObjectOptions{VersionID: r.URL.Query().Get("versionId"), Versioned: true}
		switch r.Method {
		case http.MethodHead:
			oi, err := obj.GetObjectInfo(r.Context(), target.bucket, name, opts)
			if oi.DeleteMarker {
				w.Header().Set(xhttp.AmzDeleteMarker, "true")
				w.Header().Set(xhttp.AmzVersionID, oi.VersionID)
			}
			if err != nil {
				writeErrorResponseHeadersOnly(w, toAPIError(r.Context(), err))
				return
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			opts.DeleteMarker = r.Header.Get(xhttp.MinIOSourceDeleteMarker) == "true"
			if opts.DeleteMarker {
				target.creations.Add(1)
			} else {
				target.purges.Add(1)
			}
			opts.ReplicationRequest = true
			opts.SetReplicaStatus(replication.Replica)
			if _, err := obj.DeleteObject(r.Context(), target.bucket, name, opts); err != nil && !isErrVersionNotFound(err) && !isErrObjectNotFound(err) {
				writeErrorResponse(r.Context(), w, toAPIError(r.Context(), err), r.URL)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected remote method %s", r.Method)
		}
	}))
	defer remote.Close()
	client, err := minio.New(strings.TrimPrefix(remote.URL, "http://"), &minio.Options{Region: "us-east-1", MaxRetries: 1})
	if err != nil {
		t.Fatal(err)
	}
	globalBucketTargetSys.Lock()
	globalBucketTargetSys.arnRemotesMap[target.arn] = arnTarget{Client: &TargetClient{Client: client, ARN: target.arn, Bucket: target.bucket}, lastRefresh: UTCNow()}
	globalBucketTargetSys.targetsMap[bucket] = append(globalBucketTargetSys.targetsMap[bucket], madmin.BucketTarget{Arn: target.arn, TargetBucket: target.bucket})
	globalBucketTargetSys.Unlock()
	globalBucketTargetSys.hMutex.Lock()
	globalBucketTargetSys.hc[client.EndpointURL().Host] = epHealth{Online: true}
	globalBucketTargetSys.hMutex.Unlock()
	rule := configs[0].Rules[0]
	rule.Destination = replication.Destination{ARN: target.arn, Bucket: target.bucket}
	cfg := replication.Config{RoleArn: target.arn, Rules: []replication.Rule{rule}}
	meta, err := globalBucketMetadataSys.Get(bucket)
	if err != nil {
		t.Fatal(err)
	}
	meta.replicationConfig = &cfg
	globalBucketMetadataSys.Set(bucket, meta)
	p := &ReplicationPool{ctx: ctx, objLayer: obj, workers: []chan ReplicationWorkerOperation{make(chan ReplicationWorkerOperation, 8)}, stats: stats, mrfSaveCh: make(chan MRFReplicateEntry, 8)}
	globalReplicationPool = once.NewSingleton[ReplicationPool]()
	globalReplicationPool.Set(p)

	// The real DELETE handler creates the pending marker and queues the
	// creation task that a worker would later run.
	req, err := newTestSignedRequestV4(http.MethodDelete, "/"+bucket+"/"+name, 0, nil, creds.AccessKey, creds.SecretKey, nil)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("source DELETE status %d: %s", w.Code, w.Body)
	}
	var creation DeletedObjectReplicationInfo
	select {
	case op := <-p.workers[0]:
		creation = op.(DeletedObjectReplicationInfo)
	case <-time.After(3 * time.Second):
		t.Fatal("handler queued no creation task")
	}
	version := creation.DeleteMarkerVersionID
	if version == "" || creation.VersionID != "" || !creation.DeleteMarker {
		t.Fatalf("handler queued a non-creation task: %+v", creation)
	}
	before, err := obj.GetObjectInfo(ctx, bucket, name, ObjectOptions{VersionID: version, Versioned: true})
	if !isErrMethodNotAllowed(err) || !before.DeleteMarker || before.ReplicationStatus != replication.Pending {
		t.Fatalf("source marker not pending: %+v %v", before, err)
	}

	// Meanwhile the user purges the marker through another path. The purge is
	// either complete (version gone) or still pending on the source.
	switch {
	case tc.purged:
		if _, err := obj.DeleteObject(ctx, bucket, name, ObjectOptions{VersionID: version, Versioned: true}); err != nil {
			t.Fatal(err)
		}
		if _, err := obj.GetObjectInfo(ctx, bucket, name, ObjectOptions{VersionID: version, Versioned: true}); !isErrVersionNotFound(err) && !isErrObjectNotFound(err) {
			t.Fatalf("source purge left the marker: %v", err)
		}
	case tc.pendingPurge:
		opts := ObjectOptions{VersionID: version, Versioned: true, DeleteReplication: ReplicationState{
			ReplicateDecisionStr:       creation.ReplicationState.ReplicateDecisionStr,
			VersionPurgeStatusInternal: target.arn + "=PENDING;",
			PurgeTargets:               map[string]VersionPurgeStatusType{target.arn: replication.VersionPurgePending},
		}}
		if _, err := obj.DeleteObject(ctx, bucket, name, opts); err != nil {
			t.Fatal(err)
		}
		oi, err := obj.GetObjectInfo(ctx, bucket, name, ObjectOptions{VersionID: version, Versioned: true})
		if !isErrMethodNotAllowed(err) || !oi.DeleteMarker || oi.VersionPurgeStatus != replication.VersionPurgePending {
			t.Fatalf("source marker not pending purge: %+v %v", oi, err)
		}
	}
	source := obj
	if tc.unreadable {
		source = staleLookupLayer{ObjectLayer: obj}
	}
	pending, _ := obj.GetObjectInfo(ctx, bucket, name, ObjectOptions{VersionID: version, Versioned: true})

	result := replicateDelete(ctx, creation, source)

	after, aerr := obj.GetObjectInfo(ctx, bucket, name, ObjectOptions{VersionID: version, Versioned: true})
	_, terr := obj.GetObjectInfo(ctx, target.bucket, name, ObjectOptions{VersionID: version, Versioned: true})
	targetHasMarker := isErrMethodNotAllowed(terr)
	switch {
	case tc.purged, tc.pendingPurge, tc.unreadable:
		if len(result.Targets) != 0 || target.creations.Load() != 0 || target.purges.Load() != 0 {
			t.Fatalf("%s: stale creation was sent: result=%+v creations=%d purges=%d", tc.name, result, target.creations.Load(), target.purges.Load())
		}
		if targetHasMarker {
			t.Fatalf("%s: target marker recreated from the stale creation", tc.name)
		}
		if tc.purged {
			if !isErrVersionNotFound(aerr) && !isErrObjectNotFound(aerr) {
				t.Fatalf("stale creation resurrected the source marker: %+v %v", after, aerr)
			}
		} else if !isErrMethodNotAllowed(aerr) || !after.DeleteMarker ||
			after.ReplicationStatusInternal != pending.ReplicationStatusInternal ||
			after.VersionPurgeStatusInternal != pending.VersionPurgeStatusInternal ||
			after.UserDefined[ReservedMetadataPrefixLower+ReplicationTimestamp] != pending.UserDefined[ReservedMetadataPrefixLower+ReplicationTimestamp] {
			t.Fatalf("skipped creation rewrote the source marker: before=%+v after=%+v %v", pending, after, aerr)
		}
		if tc.unreadable {
			select {
			case entry := <-p.mrfSaveCh:
				if entry.versionID != version || entry.RetryCount != 1 || entry.Bucket != bucket || entry.Object != name {
					t.Fatalf("unverified creation queued wrong MRF entry: %+v", entry)
				}
			default:
				t.Fatal("unverified source read did not queue a retry")
			}
		}
		if len(p.mrfSaveCh) != 0 {
			t.Fatalf("%s: unexpected MRF entries queued: %d", tc.name, len(p.mrfSaveCh))
		}
	default:
		if result.ReplicationStatus() != replication.Completed || target.creations.Load() != 1 || !targetHasMarker {
			t.Fatalf("current creation not replicated: result=%+v creations=%d targetMarker=%v", result, target.creations.Load(), targetHasMarker)
		}
		if !isErrMethodNotAllowed(aerr) || !after.DeleteMarker || replicationStatusesMap(after.ReplicationStatusInternal)[target.arn] != replication.Completed {
			t.Fatalf("source creation state not completed: %+v %v", after, aerr)
		}
		if len(p.mrfSaveCh) != 0 {
			t.Fatal("completed creation queued MRF work")
		}
	}
	t.Logf("%s: %s checked; creations=%d purges=%d", backend, tc.name, target.creations.Load(), target.purges.Load())
}

// staleLookupLayer cannot confirm the source marker: the read fails without
// saying whether the version is present.
type staleLookupLayer struct{ ObjectLayer }

func (staleLookupLayer) GetObjectInfo(context.Context, string, string, ObjectOptions) (ObjectInfo, error) {
	return ObjectInfo{}, InsufficientReadQuorum{}
}
