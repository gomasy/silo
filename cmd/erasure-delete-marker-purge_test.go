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
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio/internal/bucket/replication"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/once"
)

func TestDeleteMarkerPurgeIntent(t *testing.T) {
	version := mustGetUUID()
	for _, tc := range []struct {
		name string
		edit func(*ObjectOptions)
		want bool
	}{
		{"ordinary", func(*ObjectOptions) {}, true},
		{"receiver", func(o *ObjectOptions) { o.ReplicationRequest = true; o.SetReplicaStatus(replication.Replica) }, true},
		{"untrusted-replica", func(o *ObjectOptions) { o.SetReplicaStatus(replication.Replica) }, false},
		{"complete-purge", func(o *ObjectOptions) {
			o.DeleteReplication.VersionPurgeStatusInternal = string(replication.VersionPurgeComplete)
		}, true},
		{"pending-purge", func(o *ObjectOptions) { o.DeleteReplication.VersionPurgeStatusInternal = "PENDING" }, false},
		{"failed-purge", func(o *ObjectOptions) { o.DeleteReplication.VersionPurgeStatusInternal = "FAILED" }, false},
		{"unknown-purge", func(o *ObjectOptions) { o.DeleteReplication.VersionPurgeStatusInternal = "future-state" }, false},
		{"pending-creation", func(o *ObjectOptions) { o.DeleteReplication.ReplicationStatusInternal = "PENDING" }, false},
		{"failed-creation", func(o *ObjectOptions) { o.DeleteReplication.ReplicationStatusInternal = "FAILED" }, false},
		{"completed-creation", func(o *ObjectOptions) { o.DeleteReplication.ReplicationStatusInternal = "COMPLETED" }, false},
		{"create-marker", func(o *ObjectOptions) { o.DeleteMarker = true }, false},
		{"empty", func(o *ObjectOptions) { o.VersionID = "" }, false},
		{"null", func(o *ObjectOptions) { o.VersionID = nullVersionID }, false},
		{"invalid", func(o *ObjectOptions) { o.VersionID = "invalid" }, false},
		{"zero-uuid", func(o *ObjectOptions) { o.VersionID = emptyUUID }, false},
		{"movement", func(o *ObjectOptions) { o.DataMovement = true }, false},
		{"free-version", func(o *ObjectOptions) { o.InclFreeVersions = true }, false},
		{"expiration", func(o *ObjectOptions) { o.Expiration.Expire = true }, false},
		{"transition", func(o *ObjectOptions) { o.Transition.Status = "complete" }, false},
		{"restored-expiration", func(o *ObjectOptions) { o.Transition.ExpireRestored = true }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := ObjectOptions{VersionID: version, Versioned: true}
			tc.edit(&o)
			if got := o.isVersionPurge(); got != tc.want {
				t.Fatalf("purge=%v want=%v", got, tc.want)
			}
		})
	}
}

// Every other StorageAPI method panics through the nil embedding: a proof may
// read metadata, but must never delete, heal, or ask a different storage API.
type absenceProofDisk struct {
	StorageAPI
	t       *testing.T
	err     error
	reads   *atomic.Int32
	deletes *atomic.Int32
}

func (d absenceProofDisk) ReadVersion(_ context.Context, _, _, _, _ string, opts ReadOptions) (FileInfo, error) {
	if opts.ReadData || opts.Healing {
		d.t.Error("absence proof requested data/healing")
	}
	d.reads.Add(1)
	return FileInfo{}, d.err
}

func (d absenceProofDisk) DeleteVersion(_ context.Context, _, _ string, _ FileInfo, _ bool, _ DeleteOptions) error {
	if d.deletes == nil {
		d.t.Error("read-only absence proof attempted a deletion")
	} else {
		d.deletes.Add(1)
	}
	return d.err
}

func TestVersionPurgeAbsenceProof(t *testing.T) {
	for _, tc := range []struct {
		name        string
		errs        []error
		quorum, all bool
	}{
		{"all-absent", []error{errFileNotFound, errFileVersionNotFound, errFileNotFound, errFileVersionNotFound}, true, true},
		{"majority-absent", []error{errFileNotFound, errFileVersionNotFound, errFileNotFound, nil}, true, false},
		{"half-absent", []error{errFileNotFound, errFileVersionNotFound, errDiskNotFound, errDiskNotFound}, false, false},
		{"corrupt", []error{errFileNotFound, errFileVersionNotFound, errFileCorrupt, errFileCorrupt}, false, false},
		{"permission", []error{errFileNotFound, errFileVersionNotFound, errDiskAccessDenied, errVolumeAccessDenied}, false, false},
		{"volume-missing", []error{errFileNotFound, errFileVersionNotFound, errVolumeNotFound, errVolumeNotFound}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var reads atomic.Int32
			disks := make([]StorageAPI, len(tc.errs))
			for i, err := range tc.errs {
				disks[i] = absenceProofDisk{t: t, err: err, reads: &reads}
			}
			er := erasureObjects{getDisks: func() []StorageAPI { return disks }}
			oldQueue := globalMRFState.opCh
			globalMRFState.opCh = make(chan PartialOperation, 1)
			defer func() { globalMRFState.opCh = oldQueue }()
			all, err := er.confirmVersionAbsent(t.Context(), "bucket", "object", mustGetUUID())
			if (err == nil) != tc.quorum || all != tc.all || reads.Load() != int32(len(disks)) || len(globalMRFState.opCh) != 0 {
				t.Fatalf("proof all=%v error=%v reads=%d queued=%d", all, err, reads.Load(), len(globalMRFState.opCh))
			}
		})
	}
}

func TestVersionPurgeAggregation(t *testing.T) {
	for _, pure := range []bool{false, true} {
		for _, fault := range []error{nil, errFileCorrupt, errDiskAccessDenied} {
			t.Run(fmt.Sprintf("purge_%v_fault_%v", pure, fault), func(t *testing.T) {
				var calls atomic.Int32
				disks := make([]StorageAPI, 4)
				for i, err := range []error{nil, errFileNotFound, errFileVersionNotFound, fault} {
					disks[i] = absenceProofDisk{t: t, err: err, deletes: &calls}
				}
				er := erasureObjects{getDisks: func() []StorageAPI { return disks }}
				err := er.deleteObjectVersion(t.Context(), "bucket", "object", FileInfo{VersionID: mustGetUUID()}, false, pure)
				if (err == nil) != pure || calls.Load() != 4 {
					t.Fatalf("aggregation error=%v calls=%d", err, calls.Load())
				}
			})
		}
	}
}

func TestDeleteMarkerPurgeOmittedPool(t *testing.T) {
	z, bucket := consistencyPools(t)
	defer replicationTestCapacity(z)()
	const name = "partially-absent"
	er := z.serverPools[1].getHashedSet(name)
	_, opts := seedPurgeMarker(t, er, bucket, name, false)
	opts.ReplicationRequest = false
	opts.DeleteReplication = ReplicationState{}
	// Pool 0 appears absent at read quorum, but half of it cannot be read.
	missing := z.serverPools[0].getHashedSet(name)
	original := missing.getDisks
	disks := append([]StorageAPI(nil), original()...)
	clear(disks[len(disks)/2:])
	missing.getDisks = func() []StorageAPI { return disks }
	defer func() { missing.getDisks = original }()
	_, err := z.DeleteObject(t.Context(), bucket, name, opts)
	if !isErrWriteQuorum(err) {
		t.Errorf("omitted pool acknowledged deletion: %T %v", err, err)
	}
	if countPurgeMarkers(t, er.getDisks(), bucket, name, opts.VersionID) != 16 {
		t.Fatal("mutated known copies before checking omitted pool")
	}
}

func TestDeleteMarkerPurgeReceivingPool(t *testing.T) {
	z, bucket := consistencyPools(t)
	defer replicationTestCapacity(z)()
	for _, duplicate := range []bool{false, true} {
		t.Run(fmt.Sprintf("duplicate_%v", duplicate), func(t *testing.T) {
			name := "receiver-" + mustGetUUID()
			_, opts := seedPurgeMarker(t, z.serverPools[0].getHashedSet(name), bucket, name, false)
			if duplicate {
				create := opts
				create.DeleteMarker = true
				if _, err := z.serverPools[1].DeleteObject(t.Context(), bucket, name, create); err != nil {
					t.Fatal(err)
				}
			} else {
				// Latest-key routing selects pool 1, but the addressed marker is
				// in pool 0. A replica purge addresses a version, not the latest.
				putConsistencyObject(t, z, bucket, name, 1, "newer-data", ObjectOptions{Versioned: true})
			}
			if _, err := z.DeleteObject(t.Context(), bucket, name, opts); err != nil {
				t.Errorf("replica purge did not find its addressed version: %v", err)
			}
			for i, pool := range z.serverPools {
				if got := countPurgeMarkers(t, pool.getHashedSet(name).getDisks(), bucket, name, opts.VersionID); got != 0 {
					t.Errorf("replica purge left %d marker copies in pool %d", got, i)
				}
			}
		})
	}
}

func TestDeleteMarkerPurgeCallbackIntent(t *testing.T) {
	for _, loseCopies := range []bool{false, true} {
		t.Run(fmt.Sprintf("missing_after_callback_%v", loseCopies), func(t *testing.T) {
			z, bucket := consistencyPools(t)
			defer replicationTestCapacity(z)()
			const name = "callback-pending"
			er := z.serverPools[0].getHashedSet(name)
			_, opts := seedPurgeMarker(t, er, bucket, name, false)
			original := er.getDisks
			all := append([]StorageAPI(nil), original()...)
			defer func() { er.getDisks = original }()
			opts.ReplicationRequest = false
			opts.DeleteReplication = ReplicationState{}
			metadata, retention := 0, 0
			opts.EvalRetentionBypassFn = func(oi ObjectInfo, err error) error {
				retention++
				if !oi.DeleteMarker || !isErrMethodNotAllowed(err) {
					t.Fatalf("retention callback lost marker: %+v %v", oi, err)
				}
				return nil
			}
			opts.EvalMetadataFn = func(*ObjectInfo, error) (ReplicateDecision, error) {
				metadata++
				if loseCopies {
					// Inject a disk-state change after preflight, before the set's
					// metadata-update read. It returns NotFound at read quorum.
					for _, disk := range all[:8] {
						if err := disk.DeleteVersion(t.Context(), bucket, name, FileInfo{VersionID: opts.VersionID}, false, DeleteOptions{}); err != nil {
							t.Fatal(err)
						}
					}
					partial := make([]StorageAPI, len(all))
					copy(partial, all[:8])
					er.getDisks = func() []StorageAPI { return partial }
				}
				decision := ReplicateDecision{}
				decision.Set(newReplicateTargetDecision("arn1", true, false))
				return decision, nil
			}
			_, err := z.DeleteObject(t.Context(), bucket, name, opts)
			if loseCopies {
				if !isErrVersionNotFound(err) && !isErrObjectNotFound(err) {
					t.Errorf("pending update misclassified as physical purge: %T %v", err, err)
				}
			} else if err != nil {
				t.Errorf("pending update failed: %v", err)
			}
			want := 16
			if loseCopies {
				want = 8
			}
			if got := countPurgeMarkers(t, all, bucket, name, opts.VersionID); got != want || metadata != 1 || retention != 1 {
				t.Errorf("pending update: copies=%d want=%d metadata=%d retention=%d", got, want, metadata, retention)
			}
			if !loseCopies {
				fi, err := all[0].ReadVersion(t.Context(), "", bucket, name, opts.VersionID, ReadOptions{})
				if err != nil || fi.VersionPurgeStatus() != replication.VersionPurgePending {
					t.Errorf("pending state not persisted: %+v %v", fi.ReplicationState, err)
				}
			}
		})
	}
}

func TestDeleteMarkerReceivingMetadataUpdate(t *testing.T) {
	z, bucket := consistencyPools(t)
	defer replicationTestCapacity(z)()
	for _, state := range []VersionPurgeStatusType{replication.VersionPurgePending, replication.VersionPurgeFailed} {
		t.Run(string(state), func(t *testing.T) {
			name := "update-" + mustGetUUID()
			_, opts := seedPurgeMarker(t, z.serverPools[0].getHashedSet(name), bucket, name, false)
			create := opts
			create.DeleteMarker = true
			if _, err := z.serverPools[1].DeleteObject(t.Context(), bucket, name, create); err != nil {
				t.Fatal(err)
			}
			opts.DeleteReplication = ReplicationState{VersionPurgeStatusInternal: string(state)}
			if _, err := z.DeleteObject(t.Context(), bucket, name, opts); err != nil {
				t.Fatal(err)
			}
			for i, pool := range z.serverPools {
				if got := countPurgeMarkers(t, pool.getHashedSet(name).getDisks(), bucket, name, opts.VersionID); got != 16 {
					t.Errorf("metadata update removed marker copies in pool %d: %d", i, got)
				}
			}
		})
	}
}

func TestVersionPurgeRetentionGate(t *testing.T) {
	z, er, all, bucket := markerPurgeFixture(t, 4)
	data, _ := seedPurgeMarker(t, er, bucket, "protected-data", true)
	denied := errors.New("retention denied")
	opts := ObjectOptions{Versioned: true, VersionID: data, EvalRetentionBypassFn: func(oi ObjectInfo, err error) error {
		if err != nil || oi.VersionID != data || oi.DeleteMarker {
			t.Errorf("wrong retained version: %+v error=%v", oi, err)
		}
		return denied
	}}
	_, err := z.DeleteObject(t.Context(), bucket, "protected-data", opts)
	if !errors.Is(err, denied) {
		t.Fatalf("retention gate: %v", err)
	}
	for _, disk := range all {
		if _, err := disk.ReadVersion(t.Context(), "", bucket, "protected-data", data, ReadOptions{}); err != nil {
			t.Errorf("protected version changed: %v", err)
		}
	}
}

func markerPurgeFixture(t *testing.T, n int) (*erasureServerPools, *erasureObjects, []StorageAPI, string) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	obj, dirs, err := prepareErasure(ctx, n)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	restore := replicationTestCapacity(obj)
	t.Cleanup(func() {
		cancel()
		restore()
		obj.Shutdown(context.Background())
		removeRoots(dirs)
	})
	bucket := getRandomBucketName()
	if err := obj.MakeBucket(ctx, bucket, MakeBucketOptions{VersioningEnabled: true}); err != nil {
		t.Fatal(err)
	}
	z := obj.(*erasureServerPools)
	er := z.serverPools[0].sets[0]
	original := er.getDisks
	t.Cleanup(func() { er.getDisks = original })
	return z, er, append([]StorageAPI(nil), original()...), bucket
}

func seedPurgeMarker(t *testing.T, er *erasureObjects, bucket, name string, withData bool) (string, ObjectOptions) {
	t.Helper()
	dataVersion := ""
	if withData {
		oi, err := er.PutObject(t.Context(), bucket, name, mustGetPutObjReader(t, bytes.NewReader([]byte("data")), 4, "", ""), ObjectOptions{Versioned: true})
		if err != nil {
			t.Fatal(err)
		}
		dataVersion = oi.VersionID
	}
	opts := ObjectOptions{Versioned: true, VersionID: mustGetUUID(), DeleteMarker: true, ReplicationRequest: true, MTime: UTCNow().Add(-time.Hour), NoAuditLog: true}
	opts.SetReplicaStatus(replication.Replica)
	if _, err := er.DeleteObject(t.Context(), bucket, name, opts); err != nil {
		t.Fatal(err)
	}
	opts.DeleteMarker = false
	return dataVersion, opts
}

func countPurgeMarkers(t *testing.T, disks []StorageAPI, bucket, name, version string) int {
	t.Helper()
	n := 0
	for _, disk := range disks {
		fi, err := disk.ReadVersion(t.Context(), "", bucket, name, version, ReadOptions{})
		if err == nil && fi.Deleted {
			n++
		} else if err != errFileNotFound && err != errFileVersionNotFound {
			t.Fatalf("unexpected disk version: deleted=%v error=%v", fi.Deleted, err)
		}
	}
	return n
}

func TestDeleteMarkerPurgeQuorum(t *testing.T) {
	for _, tc := range []struct{ disks, online int }{{4, 2}, {16, 7}, {16, 8}, {16, 9}} {
		t.Run(fmt.Sprintf("%d_disks_%d_online", tc.disks, tc.online), func(t *testing.T) {
			z, er, all, bucket := markerPurgeFixture(t, tc.disks)
			const name = "marker"
			dataVersion, opts := seedPurgeMarker(t, er, bucket, name, true)
			partial := make([]StorageAPI, len(all))
			copy(partial, all[:tc.online])
			er.getDisks = func() []StorageAPI { return partial }
			_, first := z.DeleteObject(t.Context(), bucket, name, opts)
			_, retry := z.DeleteObject(t.Context(), bucket, name, opts)
			if tc.online <= tc.disks/2 {
				if !isErrWriteQuorum(first) || !isErrWriteQuorum(retry) {
					t.Errorf("below write quorum: first=%T %v, retry=%T %v", first, first, retry, retry)
				}
			} else if first != nil || (retry != nil && !isErrVersionNotFound(retry) && !isErrObjectNotFound(retry)) {
				t.Errorf("write quorum available: first=%v retry=%v", first, retry)
			}
			want := tc.disks - tc.online
			if tc.online < tc.disks/2 {
				want = tc.disks
			}
			if got := countPurgeMarkers(t, all, bucket, name, opts.VersionID); got != want {
				t.Errorf("partial purge markers=%d want=%d", got, want)
			}
			er.getDisks = func() []StorageAPI { return all }
			_, err := z.DeleteObject(t.Context(), bucket, name, opts)
			if err != nil && !isErrVersionNotFound(err) && !isErrObjectNotFound(err) {
				t.Errorf("full-online retry: %v", err)
			}
			if tc.online <= tc.disks/2 {
				if got := countPurgeMarkers(t, all, bucket, name, opts.VersionID); got != 0 {
					t.Errorf("full-online retry recreated/retained %d marker copies", got)
				}
			}
			// A quorum-confirmed missing retry can leave minority residue. Exercise
			// the real dangling-version healer after every disk has returned.
			_, _ = er.HealObject(t.Context(), bucket, name, opts.VersionID, madmin.HealOpts{Remove: true})
			if got := countPurgeMarkers(t, all, bucket, name, opts.VersionID); got != 0 {
				t.Errorf("marker copies after retry and heal=%d", got)
			}
			oi, err := z.GetObjectInfo(t.Context(), bucket, name, ObjectOptions{Versioned: true})
			if err != nil || oi.DeleteMarker || oi.VersionID != dataVersion {
				t.Errorf("underlying version not visible: %+v error=%v", oi, err)
			}
		})
	}
}

func TestDeleteMarkerPurgeMissingKey(t *testing.T) {
	for _, pooled := range []bool{false, true} {
		t.Run(fmt.Sprintf("pooled_%v", pooled), func(t *testing.T) {
			z, er, all, bucket := markerPurgeFixture(t, 4)
			_, opts := seedPurgeMarker(t, er, bucket, "marker-only", false)
			er.getDisks = func() []StorageAPI { return []StorageAPI{all[0], all[1], nil, nil} }
			deleteFn := er.DeleteObject
			if pooled {
				deleteFn = z.DeleteObject
			}
			_, _ = deleteFn(t.Context(), bucket, "marker-only", opts)
			_, err := deleteFn(t.Context(), bucket, "marker-only", opts)
			if !isErrWriteQuorum(err) {
				t.Errorf("missing retry with only 2/4 absence votes returned %T %v", err, err)
			}
		})
	}
}

func TestDeleteMarkerHealReplicationIdentity(t *testing.T) {
	obj, er, all, bucket := markerPurgeFixture(t, 4)
	const name = "healed-marker"
	_, opts := seedPurgeMarker(t, er, bucket, name, true)
	original, err := all[3].ReadVersion(t.Context(), "", bucket, name, opts.VersionID, ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, disk := range all[:2] {
		if err := disk.DeleteVersion(t.Context(), bucket, name, FileInfo{VersionID: opts.VersionID}, false, DeleteOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := er.HealObject(t.Context(), bucket, name, opts.VersionID, madmin.HealOpts{}); err != nil {
		t.Fatal(err)
	}
	for i, disk := range all {
		fi, err := disk.ReadVersion(t.Context(), "", bucket, name, opts.VersionID, ReadOptions{})
		if err != nil || !maps.Equal(fi.Metadata, original.Metadata) {
			t.Errorf("disk %d lost marker metadata: before=%v after=%v error=%v", i, original.Metadata, fi.Metadata, err)
		}
	}
	// Only the healed half remains readable. Read actual disk state before
	// invoking scanner replication, rather than constructing an ObjectInfo.
	er.getDisks = func() []StorageAPI { return []StorageAPI{all[0], all[1], nil, nil} }
	oi, err := er.GetObjectInfo(t.Context(), bucket, name, ObjectOptions{VersionID: opts.VersionID, Versioned: true})
	if !oi.DeleteMarker || !isErrMethodNotAllowed(err) {
		t.Fatalf("healed marker unreadable: %+v %v", oi, err)
	}
	er.getDisks = func() []StorageAPI { return all }
	var outbound atomic.Int32
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			outbound.Add(1)
			if r.Header.Get(xhttp.MinIOSourceDeleteMarker) != "true" || r.URL.Query().Get("versionId") != opts.VersionID {
				t.Errorf("unexpected outbound request: %s %s", r.Method, r.URL)
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer remote.Close()
	client, err := minio.New(strings.TrimPrefix(remote.URL, "http://"), &minio.Options{Region: "us-east-1", MaxRetries: 1})
	if err != nil {
		t.Fatal(err)
	}
	arn := "arn:minio:replication::" + mustGetUUID() + ":bucket"
	target := &TargetClient{Client: client, ARN: arn, Bucket: "target"}
	oldTargets := globalBucketTargetSys
	globalBucketTargetSys = &BucketTargetSys{arnRemotesMap: map[string]arnTarget{arn: {Client: target, lastRefresh: UTCNow()}}, targetsMap: map[string][]madmin.BucketTarget{bucket: {{Arn: arn, TargetBucket: "target"}}}, hc: map[string]epHealth{client.EndpointURL().Host: {Online: true}}}
	defer func() { globalBucketTargetSys = oldTargets }()
	cfg := configs[0]
	cfg.RoleArn = arn
	meta, err := globalBucketMetadataSys.Get(bucket)
	if err != nil {
		t.Fatal(err)
	}
	meta.replicationConfig = &cfg
	globalBucketMetadataSys.Set(bucket, meta)
	worker := make(chan ReplicationWorkerOperation, 1)
	oldPool := globalReplicationPool
	globalReplicationPool = once.NewSingleton[ReplicationPool]()
	globalReplicationPool.Set(&ReplicationPool{ctx: t.Context(), objLayer: obj, workers: []chan ReplicationWorkerOperation{worker}, stats: globalReplicationStats.Load(), mrfSaveCh: make(chan MRFReplicateEntry, 1)})
	defer func() { globalReplicationPool = oldPool }()
	roi := queueReplicationHeal(t.Context(), bucket, oi, replicationConfig{Config: &cfg, remotes: &madmin.BucketTargets{Targets: []madmin.BucketTarget{{Arn: arn, TargetBucket: "target"}}}}, 0)
	select {
	case op := <-worker:
		d := op.(DeletedObjectReplicationInfo)
		result := replicateDeleteToTarget(t.Context(), d, target)
		t.Errorf("healed replica scheduled for creation: version=%s existing=%v result=%+v", d.DeleteMarkerVersionID, roi.ExistingObjResync.mustResync(), result)
	default:
	}
	if outbound.Load() != 0 {
		t.Errorf("healed replica sent %d new marker creations", outbound.Load())
	}
}
