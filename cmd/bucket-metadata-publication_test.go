// Copyright 2026 PGSTY contributors.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio/internal/auth"
)

func TestDeleteBucketMetadataLockCancellation(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: testDeleteBucketMetadataLockCancellation})
}

func testDeleteBucketMetadataLockCancellation(obj ObjectLayer, instanceType, bucket string, _ http.Handler, _ auth.Credentials, t *testing.T) {
	_, unlock, err := lockBucketMetadata(t.Context(), obj, bucket)
	if err != nil {
		t.Fatal(err)
	}
	release := sync.OnceFunc(unlock)
	defer release()

	// Observe DeleteBucket's ACTUAL metadata.lock attempt. Set the hook after
	// our own acquisition above so it only trips on the delete.
	delAtLock := make(chan struct{})
	var once sync.Once
	hook := func(b string) {
		if b == bucket {
			once.Do(func() { close(delAtLock) })
		}
	}
	lockBucketMetadataAcquireHook.Store(&hook)
	defer lockBucketMetadataAcquireHook.Store(nil)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- obj.DeleteBucket(ctx, bucket, DeleteBucketOptions{Force: true, NoLock: true}) }()

	select {
	case <-delAtLock:
		// Fixed tree: the delete reached metadata.lock and is blocking on the
		// lock we hold. Cancel it and confirm it fails WITHOUT deleting, while we
		// still hold the lock (release stays deferred until after the checks).
		cancel()
		if err := <-done; err == nil {
			t.Errorf("%s: canceled deletion succeeded", instanceType)
		}
		if _, err := obj.GetBucketInfo(t.Context(), bucket, BucketOptions{}); err != nil {
			t.Errorf("%s: bucket disappeared while metadata.lock was held: %v", instanceType, err)
		}
		if _, err := readBucketMetadata(t.Context(), obj, bucket); err != nil {
			t.Errorf("%s: canceled deletion removed metadata: %v", instanceType, err)
		}
	case err := <-done:
		// Broken tree: the delete finished without ever taking metadata.lock,
		// i.e. it did not serialize the destructive operation behind the lock.
		t.Errorf("%s: delete bypassed metadata.lock (err=%v)", instanceType, err)
	}
}

func TestQueuedMetadataUpdateAfterDelete(t *testing.T) {
	defer DetectTestLeak(t)()
	for _, expiry := range []bool{false, true} {
		t.Run(fmt.Sprintf("expiry=%v", expiry), func(t *testing.T) {
			ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: func(obj ObjectLayer, instanceType, bucket string, _ http.Handler, _ auth.Credentials, t *testing.T) {
				previous := newObjectLayerFn()
				barrier := &lcMergeBarrier{ObjectLayer: obj, bucket: bucket, mAtLock: make(chan struct{}), mProceed: make(chan struct{})}
				setObjectLayer(barrier)
				defer setObjectLayer(previous)
				release := sync.OnceFunc(func() { close(barrier.mProceed) })
				defer release()
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				ctx = context.WithValue(ctx, lcMergeWriterKey{}, "M")
				done := make(chan error, 1)
				go func() {
					if expiry {
						done <- globalBucketMetadataSys.UpdateExpiryLCConfig(ctx, bucket, nil, UTCNow())
						return
					}
					_, err := globalBucketMetadataSys.Update(ctx, bucket, bucketTaggingConfig, []byte(`<Tagging><TagSet/></Tagging>`))
					done <- err
				}()
				select {
				case <-barrier.mAtLock:
				case <-ctx.Done():
					t.Fatal("writer did not reach metadata.lock")
				}
				if err := obj.DeleteBucket(t.Context(), bucket, DeleteBucketOptions{Force: true}); err != nil {
					t.Fatal(err)
				}
				release()
				if err := <-done; !isErrBucketNotFound(err) {
					t.Errorf("%s: queued update should reject a deleted bucket, got %v", instanceType, err)
				}
				if _, err := readBucketMetadata(t.Context(), obj, bucket); !errors.Is(err, errConfigNotFound) && !isErrBucketNotFound(err) {
					t.Errorf("%s: queued update recreated metadata: %v", instanceType, err)
				}
			}})
		})
	}
}
