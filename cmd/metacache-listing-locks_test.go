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
	"testing"
	"time"
)

// The fallback on pool 1 must contend with the
// pools-layer lock used by ordinary multi-pool writers (anchored at pool 0).
func TestListFallbackMultiPoolReadLock(t *testing.T) {
	z, bucket, _ := listingPoolsBackend(t)
	ctx := t.Context()
	set := z.serverPools[1].sets[0]
	disks := set.getDisks()
	const object = "object"
	body := bytes.Repeat([]byte{'a'}, 8192)
	mtime := time.Now().UTC().Add(-time.Hour)
	put := func(tm time.Time) {
		_, err := set.PutObject(ctx, bucket, object, mustGetPutObjReader(t, bytes.NewReader(body), int64(len(body)), "", ""), ObjectOptions{MTime: tm})
		if err != nil {
			t.Fatal(err)
		}
	}
	put(mtime)
	old := make([][]byte, len(disks))
	for i := range old {
		old[i] = mustReadNullQuorumMeta(t, disks[i], bucket, object)
	}
	put(mtime.Add(time.Minute))
	entries := make(metaCacheEntries, len(disks))
	errs := make([]error, len(disks))
	for i := range 3 {
		entries[i] = metaCacheEntry{name: object, metadata: old[i]}
	}
	for i := 3; i < 8; i++ {
		entries[i] = metaCacheEntry{name: object, metadata: mustReadNullQuorumMeta(t, disks[i], bucket, object)}
	}
	for i := 8; i < len(disks); i++ {
		errs[i] = errDiskNotFound
	}
	resolver := metadataResolutionParams{dirQuorum: 8, objQuorum: 8, bucket: bucket, requestedVersions: 1}
	lock := z.NewNSLock(bucket, object)
	lkctx, err := lock.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		t.Fatal(err)
	}
	locked := true
	defer func() {
		if locked {
			lock.Unlock(lkctx)
		}
	}()
	done := make(chan struct{})
	var entry *metaCacheEntry
	var readErr error
	go func() {
		defer close(done)
		entry, readErr = z.resolveListingGroup(ctx, bucket, []*metaCacheEntry{set.resolveListEntry(entries, errs, &resolver)})
	}()
	waitForListingReadLock(t, z.serverPools[0].sets[0].nsMutex, bucket, object, done)
	lock.Unlock(lkctx)
	locked = false
	<-done
	if readErr != nil || entry == nil || entry.name != object {
		t.Fatalf("fallback after writer unlock: entry=%v err=%v", entry, readErr)
	}
}

// Observe the reader registered on the writer's actual namespace lock before
// releasing the writer. This tests mutual exclusion without a timed sleep.
func waitForListingReadLock(t *testing.T, mutex *nsLockMap, bucket, object string, done <-chan struct{}) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		mutex.lockMapMutex.Lock()
		lock := mutex.lockMap[pathJoin(bucket, object)]
		waiting := lock != nil && lock.ref > 1
		mutex.lockMapMutex.Unlock()
		if waiting {
			return
		}
		select {
		case <-done:
			t.Fatal("listing completed while the writer held its namespace lock")
		case <-timer.C:
			t.Fatal("listing did not wait on the writer's namespace lock")
		case <-tick.C:
		}
	}
}

func listingPoolsBackend(t *testing.T) (*erasureServerPools, string, http.Handler) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	obj, dirs, err := prepareErasurePoolsWithContext(ctx)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	z := obj.(*erasureServerPools)
	previous := newObjectLayerFn()
	setObjectLayer(z)
	t.Cleanup(func() {
		cancel()
		z.Shutdown(context.Background())
		removeRoots(dirs)
		setObjectLayer(previous)
	})
	bucket, router, err := initAPIHandlerTest(ctx, z, nil, MakeBucketOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return z, bucket, router
}
