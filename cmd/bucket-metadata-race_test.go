// Copyright (c) 2015-2026 MinIO, Inc.
// Copyright (c) 2026 PGSTY
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package cmd

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio/internal/auth"
)

// ---------------------------------------------------------------------------
// Target 1 (issue #105): a higher-level lifecycle XML merge racing another
// bucket-metadata transition outside BucketMetadataSys.Delete.
//
// Before the fix, PeerBucketLCConfigHandler / healBucketILMExpiry read the
// current lifecycle document without metadata.lock, merged the replicated expiry
// rules with the local transition rules, and then persisted the merged blob with
// BucketMetadataSys.Update. Update re-read the record under metadata.lock but
// overwrote LifecycleConfigXML wholesale with the pre-computed blob, so any
// lifecycle transition change committed between the merge read and the merge
// write was silently lost on disk. BucketMetadataSys.UpdateExpiryLCConfig now
// performs the read, merge, and save under a single metadata.lock. This test
// drives PeerBucketLCConfigHandler and asserts the concurrent change survives.
// ---------------------------------------------------------------------------

type lcMergeWriterKey struct{}

// lcMergeBarrier pauses the merge writer (context value "M") exactly when it
// tries to take metadata.lock for its persisting Update. By that point the
// merge has already read the stale lifecycle document, so the test can commit a
// concurrent transition change before releasing the merge write.
type lcMergeBarrier struct {
	ObjectLayer
	bucket   string
	mAtLock  chan struct{}
	mProceed chan struct{}
	mOnce    sync.Once
}

func (o *lcMergeBarrier) metadataLock() string {
	return pathJoin(bucketMetaPrefix, o.bucket, "metadata.lock")
}

func (o *lcMergeBarrier) NewNSLock(bucket string, objects ...string) RWLocker {
	lock := o.ObjectLayer.NewNSLock(bucket, objects...)
	if bucket != minioMetaBucket || len(objects) != 1 || objects[0] != o.metadataLock() {
		return lock
	}
	return metadataObservedRWLocker{RWLocker: lock, onLock: func(ctx context.Context) {
		if ctx.Value(lcMergeWriterKey{}) != "M" {
			return
		}
		o.mOnce.Do(func() { close(o.mAtLock) })
		select {
		case <-o.mProceed:
		case <-ctx.Done():
		}
	}}
}

func TestLifecycleExpiryMergeRaceLosesConcurrentTransition(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t:          t,
		objAPITest: testLifecycleExpiryMergeRaceLosesConcurrentTransition,
	})
}

func testLifecycleExpiryMergeRaceLosesConcurrentTransition(obj ObjectLayer, instanceType, bucket string,
	_ http.Handler, _ auth.Credentials, t *testing.T,
) {
	// Revision N: a single transition-only rule.
	baseXML := []byte(`<LifecycleConfiguration><Rule><ID>keep</ID><Filter><Prefix>data/</Prefix></Filter><Status>Enabled</Status><Transition><Days>30</Days><StorageClass>WARM</StorageClass></Transition></Rule></LifecycleConfiguration>`)
	if _, err := globalBucketMetadataSys.Update(t.Context(), bucket, bucketLifecycleConfig, baseXML); err != nil {
		t.Fatalf("%s: seed lifecycle: %v", instanceType, err)
	}

	// A replicated expiry-only rule arriving from a peer site.
	expXML := `<LifecycleConfiguration><Rule><ID>expire</ID><Filter><Prefix>tmp/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>7</Days></Expiration></Rule></LifecycleConfiguration>`
	expLCConfig := base64.StdEncoding.EncodeToString([]byte(expXML))

	previousObjectAPI := newObjectLayerFn()
	barrier := &lcMergeBarrier{
		ObjectLayer: obj,
		bucket:      bucket,
		mAtLock:     make(chan struct{}),
		mProceed:    make(chan struct{}),
	}
	setObjectLayer(barrier)
	defer setObjectLayer(previousObjectAPI)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	mCtx := context.WithValue(ctx, lcMergeWriterKey{}, "M")

	mDone := make(chan error, 1)
	go func() {
		mDone <- globalSiteReplicationSys.PeerBucketLCConfigHandler(mCtx, bucket, &expLCConfig, UTCNow())
	}()

	// Wait until the merge writer has read revision N and is about to persist.
	select {
	case <-barrier.mAtLock:
	case err := <-mDone:
		t.Fatalf("%s: merge writer finished before persisting: %v", instanceType, err)
	case <-ctx.Done():
		t.Fatalf("%s: merge writer never reached metadata.lock: %v", instanceType, ctx.Err())
	}

	// Concurrent local lifecycle transition change commits revision N+1.
	concurrentXML := []byte(`<LifecycleConfiguration><Rule><ID>keep</ID><Filter><Prefix>data/</Prefix></Filter><Status>Enabled</Status><Transition><Days>10</Days><StorageClass>COLD</StorageClass></Transition></Rule></LifecycleConfiguration>`)
	if _, err := globalBucketMetadataSys.Update(ctx, bucket, bucketLifecycleConfig, concurrentXML); err != nil {
		t.Fatalf("%s: concurrent transition update: %v", instanceType, err)
	}

	// Release the merge write so it lands after the concurrent commit.
	close(barrier.mProceed)
	if err := <-mDone; err != nil {
		t.Fatalf("%s: merge writer failed: %v", instanceType, err)
	}

	// The persisted lifecycle must contain both the replicated expiry rule and
	// the concurrent transition change.
	cfg, _, err := globalBucketMetadataSys.GetLifecycleConfig(bucket)
	if err != nil {
		t.Fatalf("%s: read merged lifecycle: %v", instanceType, err)
	}
	var keep, expire bool
	for i := range cfg.Rules {
		switch cfg.Rules[i].ID {
		case "keep":
			keep = true
			if cfg.Rules[i].Transition.Days != 10 || cfg.Rules[i].Transition.StorageClass != "COLD" {
				t.Fatalf("%s: lifecycle merge overwrote the concurrent transition change: got Days=%d StorageClass=%q, want Days=10 StorageClass=COLD",
					instanceType, cfg.Rules[i].Transition.Days, cfg.Rules[i].Transition.StorageClass)
			}
		case "expire":
			expire = true
		}
	}
	if !expire {
		t.Fatalf("%s: merged lifecycle dropped the replicated expiry rule: %+v", instanceType, cfg.Rules)
	}
	if !keep {
		t.Fatalf("%s: merged lifecycle dropped the transition rule entirely: %+v", instanceType, cfg.Rules)
	}
}

// ---------------------------------------------------------------------------
// Target 2 (issue #105): DeleteBucket racing an in-flight metadata writer must
// not resurrect a ghost .metadata.bin record.
//
// Before the fix, erasureServerPools.DeleteBucket took only <bucket>.lck and
// purged the metadata prefix while config writers (updateAndParse) took only
// metadata.lock, so a writer already MID-SAVE (holding metadata.lock, past
// saveMetadata's existence recheck) could persist .metadata.bin after the purge.
// DeleteBucket now takes metadata.lock before deleting, so it waits for that
// writer and then purges whatever the writer wrote.
//
// This test isolates the DeleteBucket-lock fix specifically: the writer holds
// metadata.lock and is paused at the .metadata.bin PutObject, so the earlier
// saveMetadata existence recheck cannot save it — only serializing the delete
// behind the writer can. Removing just DeleteBucket's metadata.lock (keeping the
// recheck) therefore makes this test fail. The delete's ACTUAL metadata.lock
// attempt is observed with lockBucketMetadataAcquireHook (its lock is taken
// through the erasureServerPools receiver, invisible to the object-layer
// barrier), so the handshake is deterministic with no timing assumption.
// ---------------------------------------------------------------------------

func TestDeleteBucketResurrectsGhostMetadata(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t:          t,
		objAPITest: testDeleteBucketResurrectsGhostMetadata,
	})
}

func testDeleteBucketResurrectsGhostMetadata(obj ObjectLayer, instanceType, bucket string,
	_ http.Handler, _ auth.Credentials, t *testing.T,
) {
	previousObjectAPI := newObjectLayerFn()
	// Writer A holds metadata.lock and pauses at the .metadata.bin PutObject,
	// i.e. already past saveMetadata's existence recheck and mid-save.
	barrier := &metadataRMWBarrierObjectLayer{
		ObjectLayer:  obj,
		bucket:       bucket,
		aReady:       make(chan struct{}),
		aRelease:     make(chan struct{}),
		bLockAttempt: make(chan struct{}),
	}
	setObjectLayer(barrier)
	defer setObjectLayer(previousObjectAPI)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	aCtx := context.WithValue(ctx, metadataRMWWriterKey{}, "A")

	policyJSON := []byte(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::` + bucket + `/*"}]}`)
	aReleased := sync.OnceFunc(func() { close(barrier.aRelease) })
	defer aReleased()
	aDone := make(chan error, 1)
	go func() {
		_, err := globalBucketMetadataSys.Update(aCtx, bucket, bucketPolicyConfig, policyJSON)
		aDone <- err
	}()

	select {
	case <-barrier.aReady:
	case err := <-aDone:
		t.Fatalf("%s: writer A finished before persisting: %v", instanceType, err)
	case <-ctx.Done():
		t.Fatalf("%s: writer A never reached metadata save: %v", instanceType, ctx.Err())
	}

	// A now holds metadata.lock mid-save. Observe DeleteBucket's ACTUAL
	// metadata.lock attempt via the acquire hook, set only now so A's earlier
	// acquisition does not trip it.
	delAtLock := make(chan struct{})
	var once sync.Once
	hook := func(b string) {
		if b == bucket {
			once.Do(func() { close(delAtLock) })
		}
	}
	lockBucketMetadataAcquireHook.Store(&hook)
	defer lockBucketMetadataAcquireHook.Store(nil)

	delDone := make(chan error, 1)
	go func() {
		delDone <- obj.DeleteBucket(ctx, bucket, DeleteBucketOptions{Force: true})
	}()

	select {
	case <-delAtLock:
		// Fixed tree: DeleteBucket reached metadata.lock and blocks on A. Release
		// A so it finishes its save and unlocks; the delete then acquires the
		// lock and purges the record A wrote.
		aReleased()
		if err := <-aDone; err != nil {
			t.Fatalf("%s: writer A save failed while holding metadata.lock: %v", instanceType, err)
		}
		if err := <-delDone; err != nil {
			t.Fatalf("%s: delete bucket: %v", instanceType, err)
		}
	case err := <-delDone:
		// Broken tree: DeleteBucket purged without taking metadata.lock. Release
		// A so its mid-save PutObject recreates .metadata.bin (the ghost).
		if err != nil {
			t.Fatalf("%s: delete bucket: %v", instanceType, err)
		}
		aReleased()
		if err := <-aDone; err != nil {
			t.Fatalf("%s: writer A save failed: %v", instanceType, err)
		}
	}

	// After DeleteBucket, no .metadata.bin record may remain on disk.
	if meta, err := readBucketMetadata(ctx, obj, bucket); err == nil {
		t.Fatalf("%s: ghost .metadata.bin resurrected after DeleteBucket: name=%q created=%s policyLen=%d",
			instanceType, meta.Name, meta.Created, len(meta.PolicyConfigJSON))
	} else if !errors.Is(err, errConfigNotFound) && !isErrBucketNotFound(err) && !errors.Is(err, errVolumeNotFound) {
		t.Fatalf("%s: unexpected error reading deleted bucket metadata: %v", instanceType, err)
	}
}
