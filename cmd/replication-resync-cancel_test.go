// Copyright (c) 2026 PGSTY
// SPDX-License-Identifier: AGPL-3.0-only

package cmd

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio/internal/once"
)

func TestSiteResyncCancelState(t *testing.T) {
	globalSiteReplicationSys.Lock()
	old := globalSiteReplicationSys.enabled
	globalSiteReplicationSys.enabled = true
	globalSiteReplicationSys.Unlock()
	t.Cleanup(func() {
		globalSiteReplicationSys.Lock()
		globalSiteReplicationSys.enabled = old
		globalSiteReplicationSys.Unlock()
	})
	rs := newSiteResyncStatus("peer", []BucketInfo{{Name: "one"}, {Name: "two"}})
	sm := &siteResyncMetrics{
		resyncStatus:  map[string]SiteResyncStatus{rs.ResyncID: rs.clone()},
		peerResyncMap: map[string]resyncState{"peer": {resyncID: rs.ResyncID}},
	}
	rs.Status = ResyncCanceled
	if err := sm.updateState(rs); err != nil {
		t.Fatal(err)
	}
	got, err := sm.status("peer")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != ResyncCanceled {
		t.Fatalf("cancel returned without updating site state: got %s", got.Status)
	}
	for _, bucket := range []string{"one", "two"} {
		sm.incBucket(resyncOpts{bucket: bucket, resyncID: rs.ResyncID}, ResyncCanceled)
	}
	got, err = sm.status("peer")
	if err != nil {
		t.Fatal(err)
	}
	for bucket, status := range got.BucketStatuses {
		if status != ResyncCanceled {
			t.Errorf("bucket %s = %s, want Canceled", bucket, status)
		}
	}
	// An already-running worker must not resurrect a canceled site.
	sm.incBucket(resyncOpts{bucket: "one", resyncID: rs.ResyncID}, ResyncCompleted)
	got, _ = sm.status("peer")
	if got.Status != ResyncCanceled || got.BucketStatuses["one"] != ResyncCanceled {
		t.Fatalf("late completion overwrote canceled state: %+v", got)
	}
}

// The fixture runs the real resyncBucket control flow with in-memory metadata
// persistence. Walk is deliberately controllable so cancellation does not
// depend on disk speed, network timing or the walker closing its output.
type resyncCancelObjectLayer struct {
	ObjectLayer
	walk  func(context.Context, chan<- itemOrErr[ObjectInfo]) error
	lock  RWLocker
	mu    sync.Mutex
	saved map[string]BucketReplicationResyncStatus
}

func (o *resyncCancelObjectLayer) NewNSLock(bucket string, objects ...string) RWLocker {
	return o.lock
}

func (o *resyncCancelObjectLayer) Walk(ctx context.Context, bucket, prefix string, results chan<- itemOrErr[ObjectInfo], opts WalkOptions) error {
	return o.walk(ctx, results)
}

func (o *resyncCancelObjectLayer) PutObject(_ context.Context, bucket, object string, r *PutObjReader, opts ObjectOptions) (ObjectInfo, error) {
	data, err := io.ReadAll(r)
	if err == nil && o.saved != nil {
		var status BucketReplicationResyncStatus
		_, err = status.UnmarshalMsg(data[4:])
		o.mu.Lock()
		o.saved[object] = status
		o.mu.Unlock()
	}
	return ObjectInfo{}, err
}

func (o *resyncCancelObjectLayer) GetObjectNInfo(_ context.Context, bucket, object string, _ *HTTPRangeSpec, _ http.Header, opts ObjectOptions) (*GetObjectReader, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	status, ok := o.saved[object]
	if !ok {
		return nil, ObjectNotFound{Bucket: bucket, Object: object}
	}
	data := make([]byte, 4)
	binary.LittleEndian.PutUint16(data[:2], resyncMetaFormat)
	binary.LittleEndian.PutUint16(data[2:], resyncMetaVersion)
	data, err := status.MarshalMsg(data)
	if err != nil {
		return nil, err
	}
	return NewGetObjectReaderFromReader(bytes.NewReader(data), ObjectInfo{Size: int64(len(data))}, opts)
}

func TestResyncRecoveryOwnsLeaderContext(t *testing.T) {
	for _, loseLeader := range []bool{false, true} {
		t.Run(fmt.Sprintf("lose_leader_%v", loseLeader), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var walkCtx context.Context
				var output chan<- itemOrErr[ObjectInfo]
				obj := &resyncCancelObjectLayer{saved: make(map[string]BucketReplicationResyncStatus), walk: func(ctx context.Context, ch chan<- itemOrErr[ObjectInfo]) error {
					walkCtx, output = ctx, ch
					return nil
				}}
				s, opts := setupResyncCancelTest(t, obj)
				if err := saveResyncStatus(t.Context(), opts.bucket, s.statusMap[opts.bucket], obj); err != nil {
					t.Fatal(err)
				}
				leaderCtx, lose := context.WithCancel(t.Context())
				defer lose()
				leader := &sharedLock{lockContext: make(chan LockContext, 1)}
				leader.lockContext <- LockContext{ctx: leaderCtx}
				oldLeader := globalLeaderLock
				globalLeaderLock = leader
				defer func() { globalLeaderLock = oldLeader }()
				done := make(chan error, 1)
				go func() { done <- (&ReplicationPool{resyncer: s}).loadResync(t.Context(), []string{opts.bucket}, obj) }()
				synctest.Wait()
				if walkCtx == nil || walkCtx.Err() != nil {
					t.Fatal("recovery canceled its worker at startup")
				}
				select {
				case <-done:
					t.Fatal("recovery released its leader context before the run finished")
				default:
				}
				want := ResyncCompleted
				if loseLeader {
					want = ResyncFailed
					lose()
				} else {
					close(output)
				}
				synctest.Wait()
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				if walkCtx.Err() == nil {
					t.Fatal("finished recovery leaked the Walk context")
				}
				if got := s.statusMap[opts.bucket].TargetsMap[opts.arn].ResyncStatus; got != want {
					t.Fatalf("recovery status = %s, want %s", got, want)
				}
			})
		})
	}
}

func TestResyncCancelRouting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent, stop := context.WithCancel(t.Context())
		defer stop()
		walks := make(chan context.Context, 4)
		obj := &resyncCancelObjectLayer{walk: func(ctx context.Context, output chan<- itemOrErr[ObjectInfo]) error {
			walks <- ctx
			return nil
		}}
		s, opts := setupResyncCancelTest(t, obj)
		add := func(bucket, id string) resyncOpts {
			o := opts
			o.bucket, o.resyncID = bucket, id
			s.Lock()
			m := newBucketResyncStatus(bucket)
			m.TargetsMap[o.arn] = TargetReplicationResyncStatus{ResyncID: id, ResyncStatus: ResyncPending}
			s.statusMap[bucket] = m
			s.Unlock()
			meta, _ := globalBucketMetadataSys.Get(opts.bucket)
			globalBucketMetadataSys.Set(bucket, meta)
			globalBucketTargetSys.Lock()
			globalBucketTargetSys.targetsMap[bucket] = []madmin.BucketTarget{{Arn: o.arn}}
			globalBucketTargetSys.Unlock()
			return o
		}
		run := func(o resyncOpts) chan struct{} {
			done := make(chan struct{})
			go func() { s.resyncBucket(parent, obj, false, o); close(done) }()
			return done
		}
		first := run(opts)
		synctest.Wait()
		firstCtx := <-walks
		queuedOpts := add("queued", opts.resyncID)
		queued := run(queuedOpts)
		otherOpts := add("other", "other-id")
		other := run(otherOpts)
		synctest.Wait()
		s.cancelResyncID(opts.resyncID)
		synctest.Wait()
		for name, done := range map[string]chan struct{}{"active": first, "queued": queued} {
			select {
			case <-done:
			default:
				t.Fatalf("%s matching run survived cancellation", name)
			}
		}
		if !errors.Is(context.Cause(firstCtx), errResyncCanceled) {
			t.Fatal("active Walk did not receive user cancellation")
		}
		select {
		case <-other:
			t.Fatal("unrelated run was canceled")
		default:
		}
		otherCtx := <-walks
		if otherCtx.Err() != nil {
			t.Fatal("unrelated Walk was canceled")
		}
		// There is no token/tombstone left for a subsequently registered run.
		freshOpts := add("fresh", opts.resyncID)
		fresh := run(freshOpts)
		synctest.Wait()
		select {
		case <-fresh:
			t.Fatal("fresh run inherited an old cancellation")
		default:
		}
		stop()
		synctest.Wait()
		<-other
		<-fresh
		s.RLock()
		defer s.RUnlock()
		if len(s.cancelResyncs) != 0 || len(s.workerCh) != 1 {
			t.Fatal("run registrations or worker slot leaked")
		}
		if s.statusMap[queuedOpts.bucket].TargetsMap[opts.arn].ResyncStatus != ResyncCanceled {
			t.Fatal("queued user cancellation was not recorded")
		}
		if s.statusMap[freshOpts.bucket].TargetsMap[opts.arn].ResyncStatus != ResyncPending {
			t.Fatal("shutdown changed a queued run's resumable Pending status")
		}
	})
}

func TestResyncCancellationWinsFinalization(t *testing.T) {
	s, opts := newTestResyncer("finalize-cancel", "arn1")
	obj := &resyncCancelObjectLayer{saved: make(map[string]BucketReplicationResyncStatus)}
	ctx, cancel, registered := s.registerResync(t.Context(), opts)
	if !registered {
		t.Fatal("registration failed")
	}
	defer cancel(nil)
	// Simulate cancellation after the finalizer computed Completed but before
	// it acquired the persistence lock.
	status := finalResyncStatus(ResyncCompleted, context.Cause(ctx))
	s.cancelResyncID(opts.resyncID)
	if got := s.markStatus(status, opts, obj); got != ResyncCanceled {
		t.Fatalf("final status = %s, want Canceled", got)
	}
	for _, saved := range obj.saved {
		if saved.TargetsMap[opts.arn].ResyncStatus != ResyncCanceled {
			t.Fatal("persisted Completed over cancellation")
		}
	}
	if len(obj.saved) != 1 {
		t.Fatal("canceled status was not persisted")
	}
	m := s.statusMap[opts.bucket]
	m.TargetsMap[opts.arn] = TargetReplicationResyncStatus{ResyncID: "new-run", ResyncStatus: ResyncStarted}
	s.statusMap[opts.bucket] = m
	if s.markStatus(ResyncCompleted, opts, obj) != NoResync || s.incStats(TargetReplicationResyncStatus{ReplicatedCount: 1}, opts) {
		t.Fatal("old run changed the replacement run")
	}
	if st := s.statusMap[opts.bucket].TargetsMap[opts.arn]; st.ResyncStatus != ResyncStarted || st.ReplicatedCount != 0 {
		t.Fatalf("replacement state changed: %+v", st)
	}
	delete(s.statusMap, opts.bucket)
	if s.markStatus(ResyncFailed, opts, obj) != NoResync {
		t.Fatal("deleted bucket was recreated")
	}
}

func setupResyncCancelTest(t *testing.T, obj *resyncCancelObjectLayer) (*replicationResyncer, resyncOpts) {
	t.Helper()
	s, opts := newTestResyncer("cancel-bucket", "arn1")
	s.workerCh = make(chan struct{}, 1)
	s.workerCh <- struct{}{}
	cfg := configs[0]
	cfg.RoleArn = opts.arn
	meta := newBucketMetadata(opts.bucket)
	meta.replicationConfig = &cfg
	oldMeta, oldTargets, oldObj := globalBucketMetadataSys, globalBucketTargetSys, newObjectLayerFn()
	oldPool := globalReplicationPool
	oldNotifier := globalEventNotifier
	globalEventNotifier = &EventNotifier{}
	globalReplicationPool = once.NewSingleton[ReplicationPool]()
	globalReplicationPool.Set(&ReplicationPool{})
	globalBucketMetadataSys = NewBucketMetadataSys()
	globalBucketMetadataSys.Set(opts.bucket, meta)
	client, err := minio.New("127.0.0.1:1", &minio.Options{Region: "us-east-1"})
	if err != nil {
		t.Fatal(err)
	}
	globalBucketTargetSys = &BucketTargetSys{
		arnRemotesMap: map[string]arnTarget{opts.arn: {Client: &TargetClient{Client: client, ARN: opts.arn}}},
		targetsMap:    map[string][]madmin.BucketTarget{opts.bucket: {{Arn: opts.arn}}},
	}
	setObjectLayer(obj)
	t.Cleanup(func() {
		globalBucketMetadataSys, globalBucketTargetSys = oldMeta, oldTargets
		globalReplicationPool = oldPool
		globalEventNotifier = oldNotifier
		setObjectLayer(oldObj)
	})
	return s, opts
}

type blockedResyncLock struct {
	RWLocker
	started chan struct{}
}

func (l *blockedResyncLock) GetLock(ctx context.Context, _ *dynamicTimeout) (LockContext, error) {
	close(l.started)
	<-ctx.Done()
	return LockContext{}, ctx.Err()
}

func TestResyncCancelFullWorkerQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		lock := &blockedResyncLock{started: make(chan struct{})}
		produced := 0
		walkerDone := make(chan struct{})
		obj := &resyncCancelObjectLayer{lock: lock}
		s, opts := setupResyncCancelTest(t, obj)
		obj.walk = func(ctx context.Context, output chan<- itemOrErr[ObjectInfo]) error {
			go func() {
				defer close(walkerDone)
				defer close(output)
				for range 120 {
					select {
					case <-ctx.Done():
						return
					case output <- itemOrErr[ObjectInfo]{Item: ObjectInfo{Bucket: opts.bucket, Name: "same-key", VersionID: mustGetUUID(), DeleteMarker: true, ModTime: time.Now()}}:
						produced++
					}
				}
			}()
			return nil
		}
		done := make(chan struct{})
		go func() { s.resyncBucket(ctx, obj, false, opts); close(done) }()
		synctest.Wait()
		select {
		case <-lock.started:
		default:
			t.Fatal("worker did not enter replication")
		}
		if produced < 101 || produced >= 120 {
			t.Fatalf("expected a full 100-entry worker queue to block dispatch; Walk produced %d", produced)
		}
		cancel()
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("canceled dispatcher deadlocked sending to a full worker queue")
		}
		select {
		case <-walkerDone:
		default:
			t.Fatal("canceled Walk producer leaked")
		}
		if len(s.workerCh) != 1 {
			t.Fatal("resync worker slot was not returned")
		}
	})
}

func TestResyncCancelBlockedWalkReceive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent, cancel := context.WithCancel(t.Context())
		defer cancel()
		var output chan<- itemOrErr[ObjectInfo]
		obj := &resyncCancelObjectLayer{walk: func(ctx context.Context, ch chan<- itemOrErr[ObjectInfo]) error {
			output = ch
			return nil
		}}
		s, opts := setupResyncCancelTest(t, obj)
		done := make(chan struct{})
		go func() { s.resyncBucket(parent, obj, false, opts); close(done) }()
		synctest.Wait()
		if output == nil {
			t.Fatal("resync did not reach Walk")
		}
		cancel()
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Error("resync remained blocked on Walk output after cancellation")
		}
		close(output) // release the old implementation on failure
		synctest.Wait()
		if len(s.workerCh) != 1 {
			t.Error("resync worker slot was not released")
		}
	})
}

func TestResyncCancelsOwnedWalkOnError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var walkCtx context.Context
		obj := &resyncCancelObjectLayer{walk: func(ctx context.Context, ch chan<- itemOrErr[ObjectInfo]) error {
			walkCtx = ctx
			return errors.New("injected walk failure")
		}}
		s, opts := setupResyncCancelTest(t, obj)
		s.resyncBucket(t.Context(), obj, false, opts)
		if walkCtx == nil || walkCtx.Err() == nil {
			t.Fatal("resync exit did not cancel the context it passed to Walk")
		}
		if t.Context().Err() != nil {
			t.Fatal("resync canceled its caller's context")
		}
	})
}
