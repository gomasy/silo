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

func r5ReplicationFixture(t *testing.T, obj ObjectLayer, bucket string, client *minio.Client) (chan ReplicationWorkerOperation, func()) {
	t.Helper()
	const arn = "arn:minio:replication::af470089-d354-4473-934c-9e1f52f6da89:bucket"
	target := &TargetClient{Client: client, ARN: arn, Bucket: bucket}
	globalBucketTargetSys.arnRemotesMap[arn] = arnTarget{Client: target, lastRefresh: UTCNow()}
	globalBucketTargetSys.targetsMap[bucket] = []madmin.BucketTarget{{Arn: arn, TargetBucket: bucket}}
	meta, err := globalBucketMetadataSys.Get(bucket)
	if err != nil {
		t.Fatal(err)
	}
	cfg := configs[0]
	cfg.RoleArn = arn
	meta.replicationConfig = &cfg
	globalBucketMetadataSys.Set(bucket, meta)
	worker := make(chan ReplicationWorkerOperation, 10)
	previous := globalReplicationPool
	globalReplicationPool = once.NewSingleton[ReplicationPool]()
	globalReplicationPool.Set(&ReplicationPool{ctx: t.Context(), objLayer: obj, workers: []chan ReplicationWorkerOperation{worker}, stats: globalReplicationStats.Load(), mrfSaveCh: make(chan MRFReplicateEntry, 10)})
	return worker, func() { globalReplicationPool = previous }
}

func TestTaggingReplicationSenderRetryAndAcknowledgment(t *testing.T) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: func(obj ObjectLayer, instance, bucket string, router http.Handler, cred auth.Credentials, t *testing.T) {
		defer r5Capacity(obj.(*erasureServerPools))()
		if _, err := globalBucketMetadataSys.Update(t.Context(), bucket, bucketVersioningConfig, enabledBucketVersioningConfig); err != nil {
			t.Fatal(err)
		}
		const name = "tagging-old-queue"
		oi, err := obj.PutObject(t.Context(), bucket, name, mustGetPutObjReader(t, strings.NewReader("data"), 4, "", ""), ObjectOptions{Versioned: true})
		if err != nil {
			t.Fatal(err)
		}
		requests := make(chan http.Header, 4)
		var attempts atomic.Int32
		peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set(xhttp.AmzVersionID, oi.VersionID)
			w.Header().Set(xhttp.ETag, "\""+oi.ETag+"\"")
			w.Header().Set(xhttp.LastModified, oi.ModTime.Format(http.TimeFormat))
			w.Header().Set(xhttp.ContentType, oi.ContentType)
			if r.Method == http.MethodHead {
				w.Header().Set(xhttp.ContentLength, "4")
				w.WriteHeader(http.StatusOK)
				return
			}
			requests <- r.Header.Clone()
			w.Header().Set(xhttp.ContentType, "application/xml")
			if attempts.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				w.Write([]byte(`<Error><Code>SlowDown</Code><Message>retry fixture</Message></Error>`))
				return
			}
			w.Write([]byte("<CopyObjectResult><LastModified>" + oi.ModTime.Format(time.RFC3339Nano) + "</LastModified><ETag>\"" + oi.ETag + "\"</ETag></CopyObjectResult>"))
		}))
		defer peer.Close()
		client, err := minio.New(strings.TrimPrefix(peer.URL, "http://"), &minio.Options{Region: "us-east-1", MaxRetries: 1})
		if err != nil {
			t.Fatal(err)
		}
		worker, cleanup := r5ReplicationFixture(t, obj, bucket, client)
		defer cleanup()
		body := `<Tagging><TagSet><Tag><Key>key</Key><Value>queued</Value></Tag></TagSet></Tagging>`
		w := r5Request(t, router, cred, http.MethodPut, "/"+bucket+"/"+name+"?tagging&versionId="+oi.VersionID, body, nil)
		if w.Code != http.StatusOK || len(worker) != 1 {
			t.Fatalf("tagging PUT: %d %s queued=%d", w.Code, w.Body.String(), len(worker))
		}
		old := (<-worker).(ReplicateObjectInfo)
		// Delete through the actual handler before processing the old task.
		w = r5Request(t, router, cred, http.MethodDelete, "/"+bucket+"/"+name+"?tagging&versionId="+oi.VersionID, "", nil)
		if w.Code != http.StatusNoContent || len(worker) != 1 {
			t.Fatalf("tagging DELETE: %d %s queued=%d", w.Code, w.Body.String(), len(worker))
		}
		deleted, err := obj.GetObjectInfo(t.Context(), bucket, name, ObjectOptions{VersionID: oi.VersionID})
		if err != nil {
			t.Fatal(err)
		}
		stamp := deleted.UserDefined[r5TagStamp]
		for attempt := 0; attempt < 2; attempt++ {
			result := replicateObject(t.Context(), old, obj)
			want := replication.Failed
			if attempt == 1 {
				want = replication.Completed
			}
			if result.ReplicationStatus() != want {
				t.Fatalf("attempt %d result=%+v want %s", attempt, result, want)
			}
			if len(result.Targets) != 1 || result.Targets[0].ReplicationAction != replicateMetadata || (result.Targets[0].Err != nil) != (attempt == 0) {
				t.Fatalf("attempt %d reported wrong action/error: %+v", attempt, result)
			}
			r5Stored(t, obj, bucket, name, oi.VersionID, "", stamp)
			select {
			case h := <-requests:
				if h.Get(xhttp.MinIOSourceTaggingTimestamp) != stamp || h.Get(xhttp.AmzObjectTagging) != "" || h.Get(xhttp.AmzTagDirective) != "REPLACE" || h.Get(xhttp.AmzMetadataDirective) != "" {
					t.Fatalf("sender did not carry current deletion: %v", h)
				}
				t.Logf("%s attempt %d sent tags=%q timestamp=%s status=%s", instance, attempt, h.Get(xhttp.AmzObjectTagging), stamp, want)
			default:
				t.Fatal("no metadata COPY sent for same-empty target")
			}
		}
		// With a real outgoing rule enabled, a signed incoming replica COPY
		// must not queue another outgoing event and create a feedback loop.
		before := len(worker)
		r5Receive(t, obj, router, cred, bucket, "copy", deleted, stamp, nil)
		if len(worker) != before {
			t.Fatal("incoming replica COPY scheduled another outgoing event")
		}
		// The metadata sender must fail malformed stored revisions before COPY,
		// just as the full retransmission option builder does.
		_, err = obj.PutObjectTags(t.Context(), bucket, name, "", ObjectOptions{VersionID: oi.VersionID, UserDefined: map[string]string{r5TagStamp: "invalid"}})
		if err != nil {
			t.Fatal(err)
		}
		target := globalBucketTargetSys.GetRemoteTargetClient(bucket, globalBucketTargetSys.targetsMap[bucket][0].Arn)
		invalid := old.replicateAll(t.Context(), obj, target)
		if invalid.ReplicationStatus != replication.Failed || invalid.Err == nil || len(requests) != 0 {
			t.Fatalf("invalid timestamp was not rejected before send: %+v", invalid)
		}
	}})
}
