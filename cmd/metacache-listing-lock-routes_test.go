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
	"context"
	"testing"
	"time"

	"github.com/minio/minio/internal/dsync"
)

// Use the real distributed-lock algorithm with in-process lock servers.
// This is a protocol-level check, not a network or rolling-restart test.
func TestListFallbackDistributedLockRoute(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		multiPool, sharedLockers bool
	}{
		{"multi-pool-shared-lockers", true, true},
		{"multi-pool-disjoint-lockers", true, false},
		{"single-pool-target-set-lockers", false, false},
		{"single-pool-first-set-lockers", false, false},
		{"single-pool-shared-ns-distinct-lockers", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := []dsync.NetLocker{newLocker(), newLocker()}
			b := []dsync.NetLocker{newLocker(), newLocker()}
			if tc.sharedLockers {
				b = a
			}
			set0 := &erasureObjects{nsMutex: newNSLock(true), getLockers: func() ([]dsync.NetLocker, string) { return a, "listing-test" }}
			target := &erasureObjects{nsMutex: newNSLock(true), getLockers: func() ([]dsync.NetLocker, string) { return b, "listing-test" }}
			if tc.name == "single-pool-shared-ns-distinct-lockers" {
				target.nsMutex = set0.nsMutex
			}
			z := &erasureServerPools{serverPools: []*erasureSets{{sets: []*erasureObjects{set0, target}}}}
			if tc.multiPool {
				z.serverPools = []*erasureSets{{sets: []*erasureObjects{set0}}, {sets: []*erasureObjects{target}}}
			}
			writer := target.NewNSLock("bucket", "object")
			if tc.multiPool || tc.name == "single-pool-first-set-lockers" {
				writer = z.NewNSLock("bucket", "object")
			}
			wctx, err := writer.GetLock(t.Context(), globalOperationTimeout)
			if err != nil {
				t.Fatal(err)
			}
			locked := true
			defer func() {
				if locked {
					writer.Unlock(wctx)
				}
			}()
			ctx, cancel := context.WithTimeout(t.Context(), 80*time.Millisecond)
			defer cancel()
			_, unlock, err := z.listingRLock(ctx, "bucket", "object", target)
			if err == nil {
				unlock()
				t.Fatal("fallback read lock acquired while ordinary writer lock is held")
			}
			writer.Unlock(wctx)
			locked = false
			ctx, cancel = context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			_, unlock, err = z.listingRLock(ctx, "bucket", "object", target)
			if err != nil {
				t.Fatalf("fallback read lock failed after writer unlock: %v", err)
			}
			unlock()
		})
	}
}

// Cancellation while waiting for the second domain must release the first.
func TestListingSecondLockCancellationReleasesFirst(t *testing.T) {
	first := &erasureObjects{nsMutex: newNSLock(false)}
	target := &erasureObjects{nsMutex: newNSLock(false)}
	z := &erasureServerPools{serverPools: []*erasureSets{{sets: []*erasureObjects{first, target}}}}
	writer := target.NewNSLock("bucket", "object")
	wctx, err := writer.GetLock(t.Context(), globalOperationTimeout)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Unlock(wctx)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	var lockErr error
	go func() {
		defer close(done)
		_, unlock, err := z.listingRLock(ctx, "bucket", "object", target)
		lockErr = err
		if err == nil {
			unlock()
		}
	}()
	waitForListingReadLock(t, target.nsMutex, "bucket", "object", done)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("second lock did not cancel")
	}
	if lockErr == nil {
		t.Fatal("canceled second lock succeeded")
	}
	poolWriter := z.NewNSLock("bucket", "object")
	ctx, cancel = context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	pctx, err := poolWriter.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		t.Fatalf("first lock leaked after second failed: %v", err)
	}
	poolWriter.Unlock(pctx)
}
