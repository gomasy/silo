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
	"encoding/json"
	"fmt"
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
	"github.com/minio/minio/internal/logger"
	loghttp "github.com/minio/minio/internal/logger/target/http"
	"github.com/minio/minio/internal/once"
	xnet "github.com/pgsty/silo-pkg/v3/net"
)

type markerRecoveryCase struct {
	name                                                                                                          string
	legacy, creation, lostReply, partial, exhaust, lockFirst, invalidMRF, replicaSource, offline, unrecordedPurge bool
	targets                                                                                                       int
}

func TestReplicationMRFMarkerRecovery(t *testing.T) {
	for _, tc := range []markerRecoveryCase{
		{name: "canonical", targets: 1},
		{name: "lock-failure", lockFirst: true, targets: 1},
		{name: "invalid-MRF-metadata", invalidMRF: true, targets: 1},
		{name: "legacy", legacy: true, targets: 1},
		{name: "unrecorded-purge", unrecordedPurge: true, targets: 2},
		{name: "two-targets", legacy: true, targets: 2},
		{name: "two-targets-offline", offline: true, targets: 2},
		{name: "replica-source", replicaSource: true, targets: 1},
		{name: "lost-reply-canonical", lostReply: true, targets: 1},
		{name: "lost-reply-legacy", legacy: true, lostReply: true, targets: 1},
		{name: "creation", creation: true, targets: 1},
		{name: "partial-creation-block", partial: true, targets: 2},
		{name: "retry-budget-and-scanner", exhaust: true, targets: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, endpoints: []string{"DeleteObject"}, objAPITest: func(obj ObjectLayer, backend, bucket string, router http.Handler, creds auth.Credentials, t *testing.T) {
				testReplicationMRFMarkerRecovery(t, obj, backend, bucket, router, creds, tc)
			}})
		})
	}
}

type markerRecoveryTarget struct {
	arn, bucket       string
	client            *minio.Client
	reject, loseReply atomic.Bool
	deletes           atomic.Int32
}

// Only adapt host capacity accounting, using the existing real-disk adapter.
// Reads, object metadata, MRF files and writes all still use the fixture disks.
func replicationTestCapacity(obj ObjectLayer) func() {
	var restore []func()
	for _, pool := range obj.(*erasureServerPools).serverPools {
		pool.erasureDisksMu.Lock()
		for _, set := range pool.sets {
			original := pool.erasureDisks[set.setIndex]
			disks := append([]StorageAPI(nil), original...)
			for i, disk := range disks {
				if disk != nil {
					disks[i] = tagTestCapacityDisk{StorageAPI: disk}
				}
			}
			// GetDisks copies this list under the same mutex. Keep its function
			// stable while background IAM scans use the fixture, both when
			// installing the adapter and when restoring the original disks.
			pool.erasureDisks[set.setIndex] = disks
			restore = append(restore, func() {
				pool.erasureDisksMu.Lock()
				pool.erasureDisks[set.setIndex] = original
				pool.erasureDisksMu.Unlock()
			})
		}
		pool.erasureDisksMu.Unlock()
	}
	return func() {
		for _, fn := range restore {
			fn()
		}
	}
}

func TestReplicationCapacityConcurrentIAM(t *testing.T) {
	z, _ := consistencyPools(t)
	if _, _, err := initAPIHandlerTest(t.Context(), z, nil, MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	iam := globalIAMSys
	started, finished := make(chan struct{}), make(chan error, 1)
	go func() {
		close(started)
		for range 20 {
			if err := iam.Load(ctx, false); err != nil {
				finished <- err
				return
			}
		}
		finished <- nil
	}()
	<-started
	for range 5000 {
		replicationTestCapacity(z)()
	}
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
}

func testReplicationMRFMarkerRecovery(t *testing.T, obj ObjectLayer, backend, bucket string, router http.Handler, creds auth.Credentials, tc markerRecoveryCase) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	defer replicationTestCapacity(obj)()
	stats := NewReplicationStats(ctx, nil)
	oldStats := globalReplicationStats.Swap(stats)
	defer globalReplicationStats.Store(oldStats)
	oldPool := globalReplicationPool
	defer func() { globalReplicationPool = oldPool }()
	const name = "marker"
	version := mustGetUUID()
	creationTime := UTCNow().Add(-time.Hour).Truncate(time.Second)
	if _, err := globalBucketMetadataSys.Update(ctx, bucket, bucketVersioningConfig, enabledBucketVersioningConfig); err != nil {
		t.Fatal(err)
	}
	if _, err := obj.PutObject(ctx, bucket, name, mustGetPutObjReader(t, bytes.NewReader([]byte("data")), 4, "", ""), ObjectOptions{Versioned: true}); err != nil {
		t.Fatal(err)
	}
	var targets []*markerRecoveryTarget
	cfg := replication.Config{}
	creationStates := make(map[string]replication.StatusType)
	var creationInternal string
	for i := 0; i < tc.targets; i++ {
		target := &markerRecoveryTarget{arn: "arn:minio:replication::" + mustGetUUID() + ":bucket", bucket: getRandomBucketName()}
		if err := obj.MakeBucket(ctx, target.bucket, MakeBucketOptions{}); err != nil {
			t.Fatal(err)
		}
		if _, err := obj.PutObject(ctx, target.bucket, name, mustGetPutObjReader(t, bytes.NewReader([]byte("data")), 4, "", ""), ObjectOptions{Versioned: true}); err != nil {
			t.Fatal(err)
		}
		if !tc.creation {
			opts := ObjectOptions{VersionID: version, Versioned: true, DeleteMarker: true, ReplicationRequest: true, MTime: creationTime}
			opts.SetReplicaStatus(replication.Replica)
			if _, err := obj.DeleteObject(ctx, target.bucket, name, opts); err != nil {
				t.Fatal(err)
			}
		}
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
				target.deletes.Add(1)
				if got := r.Header.Get(xhttp.MinIOSourceDeleteMarker) == "true"; got != tc.creation {
					t.Errorf("purge/creation wire flag=%v creation=%v", got, tc.creation)
				}
				if opts.VersionID == "" {
					t.Error("missing remote versionId")
				}
				if target.reject.Load() {
					w.WriteHeader(http.StatusForbidden)
					fmt.Fprint(w, `<Error><Code>AccessDenied</Code></Error>`)
					return
				}
				opts.DeleteMarker = r.Header.Get(xhttp.MinIOSourceDeleteMarker) == "true"
				opts.ReplicationRequest = true
				opts.SetReplicaStatus(replication.Replica)
				_, err := obj.DeleteObject(r.Context(), target.bucket, name, opts)
				if err != nil && !isErrVersionNotFound(err) && !isErrObjectNotFound(err) {
					writeErrorResponse(r.Context(), w, toAPIError(r.Context(), err), r.URL)
					return
				}
				if target.loseReply.Swap(false) {
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					conn.Close()
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
		target.client = client
		globalBucketTargetSys.Lock()
		globalBucketTargetSys.arnRemotesMap[target.arn] = arnTarget{Client: &TargetClient{Client: client, ARN: target.arn, Bucket: target.bucket}, lastRefresh: UTCNow()}
		globalBucketTargetSys.targetsMap[bucket] = append(globalBucketTargetSys.targetsMap[bucket], madmin.BucketTarget{Arn: target.arn, TargetBucket: target.bucket})
		globalBucketTargetSys.Unlock()
		globalBucketTargetSys.hMutex.Lock()
		globalBucketTargetSys.hc[client.EndpointURL().Host] = epHealth{Online: true}
		globalBucketTargetSys.hMutex.Unlock()
		rule := configs[0].Rules[0]
		rule.Priority = i + 1
		rule.Destination = replication.Destination{ARN: target.arn, Bucket: target.bucket}
		cfg.Rules = append(cfg.Rules, rule)
		creationStates[target.arn] = replication.Completed
		creationInternal += target.arn + "=COMPLETED;"
		targets = append(targets, target)
	}
	if tc.targets == 1 {
		cfg.RoleArn = targets[0].arn
	}
	if !tc.creation {
		opts := ObjectOptions{VersionID: version, Versioned: true, DeleteMarker: true, MTime: creationTime, DeleteReplication: ReplicationState{Targets: creationStates, ReplicationStatusInternal: creationInternal, ReplicationTimeStamp: creationTime}}
		if tc.replicaSource {
			opts.DeleteReplication = ReplicationState{}
			opts.SetReplicaStatus(replication.Replica)
		}
		if _, err := obj.DeleteObject(ctx, bucket, name, opts); err != nil {
			t.Fatal(err)
		}
	}
	meta, err := globalBucketMetadataSys.Get(bucket)
	if err != nil {
		t.Fatal(err)
	}
	meta.replicationConfig = &cfg
	globalBucketMetadataSys.Set(bucket, meta)
	newPool := func() *ReplicationPool {
		p := &ReplicationPool{ctx: ctx, objLayer: obj, workers: []chan ReplicationWorkerOperation{make(chan ReplicationWorkerOperation, 8)}, stats: stats, mrfSaveCh: make(chan MRFReplicateEntry, 8)}
		globalReplicationPool = once.NewSingleton[ReplicationPool]()
		globalReplicationPool.Set(p)
		return p
	}
	p := newPool()
	receive := func() DeletedObjectReplicationInfo {
		select {
		case op := <-p.workers[0]:
			d, ok := op.(DeletedObjectReplicationInfo)
			if !ok {
				t.Fatalf("wrong queued operation %T", op)
			}
			return d
		case <-time.After(3 * time.Second):
			t.Fatal("no marker task from actual MRF/handler/scanner queue")
			return DeletedObjectReplicationInfo{}
		}
	}
	uri := "/" + bucket + "/" + name
	if !tc.creation {
		uri += "?versionId=" + version
	}
	req, err := newTestSignedRequestV4(http.MethodDelete, uri, 0, nil, creds.AccessKey, creds.SecretKey, nil)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("source DELETE status %d: %s", w.Code, w.Body)
	}
	deletion := receive()
	if tc.creation {
		version = deletion.DeleteMarkerVersionID
	} else if deletion.VersionID != version || deletion.DeleteMarkerVersionID != "" {
		t.Fatalf("current producer emitted noncanonical purge: %+v", deletion)
	}
	if tc.unrecordedPurge {
		// Exercise a task carrying purge state while the disk marker still has
		// only creation metadata. Recreate only this isolated fixture marker.
		if _, err := obj.DeleteObject(ctx, bucket, name, ObjectOptions{VersionID: version, Versioned: true}); err != nil {
			t.Fatal(err)
		}
		opts := ObjectOptions{VersionID: version, Versioned: true, DeleteMarker: true, MTime: creationTime, DeleteReplication: ReplicationState{Targets: creationStates, ReplicationStatusInternal: creationInternal, ReplicationTimeStamp: creationTime}}
		if _, err := obj.DeleteObject(ctx, bucket, name, opts); err != nil {
			t.Fatal(err)
		}
	}
	if tc.legacy {
		deletion.VersionID, deletion.DeleteMarkerVersionID = "", version
	}
	if tc.partial {
		deletion.TargetArn = targets[len(targets)-1].arn
	}
	if tc.invalidMRF {
		testReplicationMRFInvalidLookups(ctx, t, obj, p, newPool, deletion)
		return
	}
	var auditStatuses <-chan string
	if tc.name == "canonical" {
		var stopAudit func()
		auditStatuses, stopAudit = replicationTestAudit(ctx, t, bucket)
		defer stopAudit()
	}
	assertAudit := func(want string) {
		if auditStatuses == nil {
			return
		}
		select {
		case got := <-auditStatuses:
			if got != want {
				t.Fatalf("replication audit status=%q want %q", got, want)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("missing replication audit event")
		}
	}
	deletion.OpType = replication.HealReplicationType // Exercise operation-status statistics too.
	failing := targets[len(targets)-1]
	failing.reject.Store(!tc.lostReply)
	failing.loseReply.Store(tc.lostReply)
	if tc.offline {
		globalBucketTargetSys.hMutex.Lock()
		globalBucketTargetSys.hc[failing.client.EndpointURL().Host] = epHealth{Online: false}
		globalBucketTargetSys.hMutex.Unlock()
	}
	before, _ := obj.GetObjectInfo(ctx, bucket, name, ObjectOptions{VersionID: version})
	beforeCreation := before.ReplicationStatusInternal
	beforeStamp := before.UserDefined[ReservedMetadataPrefixLower+ReplicationTimestamp]
	beforeReplica := before.UserDefined[ReservedMetadataPrefixLower+ReplicaStatus]
	beforeReplicaStamp := before.UserDefined[ReservedMetadataPrefixLower+ReplicaTimestamp]
	if !tc.creation && !tc.replicaSource && (len(replicationStatusesMap(beforeCreation)) != tc.targets || beforeStamp == "") {
		t.Fatalf("missing seeded creation block: %+v", before)
	}
	if tc.replicaSource && (beforeReplica != "REPLICA" || beforeReplicaStamp == "") {
		t.Fatalf("missing replica block: %+v", before)
	}
	updateObj := obj
	if !tc.creation {
		updateObj = markerPurgeUpdateLayer{ObjectLayer: obj, t: t}
	}
	rounds := 2
	if tc.lostReply {
		rounds = 1
	}
	if tc.exhaust {
		rounds = mrfRetryLimit + 1
	}
	for round := 1; round <= rounds; round++ {
		callObj := updateObj
		if tc.lockFirst && round == 1 {
			callObj = markerLockFailureLayer{ObjectLayer: updateObj}
		}
		result := replicateDelete(ctx, deletion, callObj)
		switch {
		case tc.lockFirst && round == 1:
			if len(result.Targets) != 0 {
				t.Fatal("lock failure attempted a target")
			}
		case tc.creation:
			if result.ReplicationStatus() != replication.Failed {
				t.Fatalf("creation result: %+v", result)
			}
		case result.VersionPurgeStatus() != replication.VersionPurgeFailed:
			t.Fatalf("purge failure result: %+v", result)
		}
		assertAudit("FAILED")
		if round == 1 && !tc.lockFirst && !tc.creation {
			stats.RLock()
			failed := stats.Cache[bucket].Stats[failing.arn].FailStats.SinceUptime
			stats.RUnlock()
			if failed.Count != 1 || failed.Bytes != 0 {
				t.Fatalf("purge failure stats=%+v, want count 1 and zero bytes", failed)
			}
		}
		oi, err := obj.GetObjectInfo(ctx, bucket, name, ObjectOptions{VersionID: version})
		if !isErrMethodNotAllowed(err) || !oi.DeleteMarker {
			t.Fatalf("source marker metadata/405 missing: %+v %v", oi, err)
		}
		if !tc.creation && (oi.ReplicationStatusInternal != beforeCreation || oi.UserDefined[ReservedMetadataPrefixLower+ReplicationTimestamp] != beforeStamp) {
			t.Fatalf("purge rewrote creation block: before=%q/%q after=%q/%q", beforeCreation, beforeStamp, oi.ReplicationStatusInternal, oi.UserDefined[ReservedMetadataPrefixLower+ReplicationTimestamp])
		}
		if !tc.creation && (oi.UserDefined[ReservedMetadataPrefixLower+ReplicaStatus] != beforeReplica || oi.UserDefined[ReservedMetadataPrefixLower+ReplicaTimestamp] != beforeReplicaStamp) {
			t.Fatal("purge rewrote replica block")
		}
		if tc.targets == 2 && !tc.partial {
			purges := versionPurgeStatusesMap(oi.VersionPurgeStatusInternal)
			if purges[targets[0].arn] != replication.VersionPurgeComplete || purges[failing.arn] != replication.VersionPurgeFailed {
				t.Fatalf("incorrect persisted target states: %v", purges)
			}
			if targets[0].deletes.Load() != 1 {
				t.Fatalf("successful target resent %d times", targets[0].deletes.Load())
			}
		}
		if tc.partial {
			select {
			case <-p.mrfSaveCh:
			default:
				t.Fatal("partial failure missing MRF")
			}
			dsc := deletion.ReplicationState.ReplicateDecisionStr
			deletion.ReplicationState = oi.ReplicationState()
			deletion.ReplicationState.ReplicateDecisionStr = dsc
			continue
		}
		if tc.exhaust && round > mrfRetryLimit {
			if len(p.mrfSaveCh) != 0 || atomic.LoadUint64(&stats.mrfStats.TotalDroppedCount) != 1 {
				t.Fatalf("retry budget not applied: queued=%d drops=%d", len(p.mrfSaveCh), stats.mrfStats.TotalDroppedCount)
			}
			break
		}
		var entry MRFReplicateEntry
		select {
		case entry = <-p.mrfSaveCh:
		default:
			t.Fatal("failure did not enter MRF")
		}
		if entry.RetryCount != round || entry.versionID != version {
			t.Fatalf("MRF entry lost identity/budget: %+v, round=%d", entry, round)
		}
		p.saveMRFEntries(ctx, map[string]MRFReplicateEntry{entry.versionID: entry})
		record, err := p.loadMRF()
		if err != nil || len(record.Entries) != 1 {
			t.Fatalf("disk MRF missing: %+v %v", record, err)
		}
		if got := record.Entries[version]; got.RetryCount != round || got.Object != name || got.Bucket != bucket {
			t.Fatalf("disk MRF mismatch: %+v", got)
		}
		p.saveMRFEntries(ctx, record.Entries) // loadMRF consumes the file; each replay uses a fresh disk record.
		p = newPool()                         // no in-memory entries carried to the replacement pool.
		if err := p.queueMRFHeal(); err != nil {
			t.Fatal(err)
		}
		deletion = receive()
		if deletion.RetryCount != round {
			t.Fatalf("MRF retry count=%d want %d", deletion.RetryCount, round)
		}
		if !tc.creation && (deletion.VersionID != version || deletion.DeleteMarkerVersionID != "") {
			t.Fatalf("MRF emitted wrong purge: %+v", deletion)
		}
	}
	if tc.partial {
		return
	}
	failing.reject.Store(false)
	globalBucketTargetSys.hMutex.Lock()
	globalBucketTargetSys.hc[failing.client.EndpointURL().Host] = epHealth{Online: true}
	globalBucketTargetSys.hMutex.Unlock()
	if tc.exhaust {
		oi, _ := obj.GetObjectInfo(ctx, bucket, name, ObjectOptions{VersionID: version})
		QueueReplicationHeal(ctx, bucket, oi, 0) // Separately prove the existing scanner fallback after MRF exhaustion.
		deletion = receive()
		if deletion.RetryCount != 0 {
			t.Fatal("scanner did not start a fresh retry budget")
		}
	}
	result := replicateDelete(ctx, deletion, updateObj)
	assertAudit("COMPLETED")
	if tc.creation {
		if result.ReplicationStatus() != replication.Completed {
			t.Fatalf("creation recovery failed: %+v", result)
		}
	} else if result.VersionPurgeStatus() != replication.VersionPurgeComplete {
		t.Fatalf("purge recovery failed: %+v", result)
	}
	for _, b := range append([]string{bucket}, func() []string {
		var b []string
		for _, target := range targets {
			b = append(b, target.bucket)
		}
		return b
	}()...) {
		oi, err := obj.GetObjectInfo(ctx, b, name, ObjectOptions{VersionID: version})
		if tc.creation {
			if !oi.DeleteMarker || !isErrMethodNotAllowed(err) {
				t.Fatalf("creation missing in %s: %+v %v", b, oi, err)
			}
		} else if !isErrVersionNotFound(err) && !isErrObjectNotFound(err) {
			t.Fatalf("purge left marker in %s: %+v %v", b, oi, err)
		}
	}
	if !tc.creation {
		// Re-deliver an ambiguous purge directly. An absent marker must stay absent.
		duplicate := deletion
		duplicate.VersionID, duplicate.DeleteMarkerVersionID = "", version
		duplicate.ReplicationState.PurgeTargets = map[string]VersionPurgeStatusType{failing.arn: replication.VersionPurgePending}
		duplicate.ReplicationState.VersionPurgeStatusInternal = ""
		got := replicateDeleteToTarget(ctx, duplicate, &TargetClient{Client: failing.client, ARN: failing.arn, Bucket: failing.bucket})
		if got.VersionPurgeStatus != replication.VersionPurgeComplete {
			t.Fatalf("duplicate purge: %+v", got)
		}
		if oi, err := obj.GetObjectInfo(ctx, failing.bucket, name, ObjectOptions{VersionID: version}); !isErrVersionNotFound(err) && !isErrObjectNotFound(err) {
			t.Fatalf("duplicate recreated marker: %+v %v", oi, err)
		}
	}
	if len(p.mrfSaveCh) != 0 || len(p.workers[0]) != 0 {
		t.Fatalf("completion left queued work: mrf=%d worker=%d", len(p.mrfSaveCh), len(p.workers[0]))
	}
	if !tc.creation && !tc.exhaust && !tc.lostReply {
		stats.RLock()
		got := stats.Cache[bucket].Stats[failing.arn].ReplicatedCount
		size := stats.Cache[bucket].Stats[failing.arn].ReplicatedSize
		stats.RUnlock()
		if got != 1 || size != 0 {
			t.Fatalf("purge completion statistic=%d want 1", got)
		}
	}
	t.Logf("%s: %s recovered; %d target(s), persisted MRF, source/target metadata checked", backend, tc.name, tc.targets)
}

func TestReplicationDeleteQueueFullRetryBudget(t *testing.T) {
	stats := NewReplicationStats(t.Context(), nil)
	p := &ReplicationPool{ctx: t.Context(), objLayer: &replicationMRFTestObjectLayer{}, workers: []chan ReplicationWorkerOperation{make(chan ReplicationWorkerOperation)}, stats: stats, mrfSaveCh: make(chan MRFReplicateEntry, 1), priority: "slow"}
	d := DeletedObjectReplicationInfo{Bucket: "bucket", DeletedObject: DeletedObject{ObjectName: "marker", VersionID: mustGetUUID()}, RetryCount: 2}
	p.queueReplicaDeleteTask(d)
	if got := <-p.mrfSaveCh; got.RetryCount != 3 {
		t.Fatalf("queue-full retry count=%d", got.RetryCount)
	}
	d.RetryCount = mrfRetryLimit
	p.queueReplicaDeleteTask(d)
	if len(p.mrfSaveCh) != 0 || atomic.LoadUint64(&stats.mrfStats.TotalDroppedCount) != 1 {
		t.Fatal("queue-full retry exceeded budget without a visible drop")
	}
}

type markerLockFailureLayer struct{ ObjectLayer }

func (markerLockFailureLayer) NewNSLock(string, ...string) RWLocker { return markerFailedLock{} }

type markerFailedLock struct{ RWLocker }

func (markerFailedLock) GetLock(context.Context, *dynamicTimeout) (LockContext, error) {
	return LockContext{}, context.DeadlineExceeded
}

type markerLookupLayer struct {
	ObjectLayer
	mutate   func(ObjectInfo, error) (ObjectInfo, error)
	lookedUp chan struct{}
}

func (l markerLookupLayer) GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	oi, err := l.ObjectLayer.GetObjectInfo(ctx, bucket, object, opts)
	defer close(l.lookedUp)
	return l.mutate(oi, err)
}

func testReplicationMRFInvalidLookups(ctx context.Context, t *testing.T, obj ObjectLayer, p *ReplicationPool, newPool func() *ReplicationPool, d DeletedObjectReplicationInfo) {
	for _, tc := range []struct {
		name   string
		mutate func(ObjectInfo, error) (ObjectInfo, error)
	}{
		{"empty-info", func(_ ObjectInfo, e error) (ObjectInfo, error) { return ObjectInfo{}, e }},
		{"not-a-marker", func(o ObjectInfo, e error) (ObjectInfo, error) { o.DeleteMarker = false; return o, e }},
		{"wrong-version", func(o ObjectInfo, e error) (ObjectInfo, error) { o.VersionID = mustGetUUID(); return o, e }},
		{"wrong-bucket", func(o ObjectInfo, e error) (ObjectInfo, error) { o.Bucket = "different-bucket"; return o, e }},
		{"wrong-object", func(o ObjectInfo, e error) (ObjectInfo, error) { o.Name = "different-object"; return o, e }},
		{"zero-modtime", func(o ObjectInfo, e error) (ObjectInfo, error) { o.ModTime = time.Time{}; return o, e }},
		{"missing", func(o ObjectInfo, _ error) (ObjectInfo, error) {
			return o, ObjectNotFound{Bucket: o.Bucket, Object: o.Name}
		}},
		{"read-error", func(o ObjectInfo, _ error) (ObjectInfo, error) { return o, InsufficientReadQuorum{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := d.ToMRFEntry()
			p.saveMRFEntries(ctx, map[string]MRFReplicateEntry{entry.versionID: entry})
			fresh := newPool()
			looked := make(chan struct{})
			fresh.objLayer = markerLookupLayer{ObjectLayer: obj, mutate: tc.mutate, lookedUp: looked}
			if err := fresh.queueMRFHeal(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-looked:
			case <-time.After(3 * time.Second):
				t.Fatal("disk MRF entry was not read")
			}
			select {
			case op := <-fresh.workers[0]:
				t.Fatalf("invalid lookup scheduled %T", op)
			case <-time.After(100 * time.Millisecond):
			}
		})
	}
}

// Capture the actual internal audit event through the supported webhook sink.
func replicationTestAudit(ctx context.Context, t *testing.T, bucket string) (<-chan string, func()) {
	t.Helper()
	if len(logger.AuditTargets()) != 0 {
		t.Fatal("unexpected pre-existing test audit targets")
	}
	statuses := make(chan string, 16)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var entry struct {
			API struct{ Name, Bucket, Status string }
		}
		if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
			t.Error(err)
		} else if entry.API.Name == ReplicateDeleteAPI && entry.API.Bucket == bucket {
			statuses <- entry.API.Status
		}
		w.WriteHeader(http.StatusOK)
	}))
	endpoint, err := xnet.ParseHTTPURL(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if errs := logger.UpdateAuditWebhooks(ctx, map[string]loghttp.Config{"r6": {Enabled: true, Name: "r6", Endpoint: endpoint, BatchSize: 1, QueueSize: 128, MaxRetry: 1, RetryIntvl: time.Millisecond, HTTPTimeout: time.Second}}); len(errs) > 0 {
		t.Fatal(errs)
	}
	targets := logger.AuditTargets()
	return statuses, func() {
		logger.UpdateAuditWebhooks(ctx, nil)
		for _, target := range targets {
			target.Cancel()
		}
		server.Close()
	}
}

type markerPurgeUpdateLayer struct {
	ObjectLayer
	t *testing.T
}

func (l markerPurgeUpdateLayer) DeleteObject(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	l.t.Helper()
	rs := opts.DeleteReplication
	if rs.ReplicationStatusInternal != "" || rs.Targets != nil || rs.ReplicaStatus != "" || !rs.CompositeReplicationStatus().Empty() {
		l.t.Fatalf("purge has a nonempty creation update: %+v", rs)
	}
	if rs.CompositeVersionPurgeStatus().Empty() {
		l.t.Fatal("purge update lost its purge status")
	}
	return l.ObjectLayer.DeleteObject(ctx, bucket, object, opts)
}
