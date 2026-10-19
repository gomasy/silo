// Copyright (c) 2026 Feng Ruohang
//
// This file is part of Silo Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/minio/minio/internal/dsync"
)

func listingLockerTestWrite(t *testing.T, l *localLocker, args dsync.LockArgs) {
	t.Helper()
	if ok, err := l.Lock(t.Context(), args); !ok || err != nil {
		t.Fatalf("Lock = %v, %v", ok, err)
	}
}

func listingLockerTestUnlock(t *testing.T, l *localLocker, args dsync.LockArgs) {
	t.Helper()
	if ok, err := l.Unlock(t.Context(), args); !ok || err != nil {
		t.Fatalf("Unlock = %v, %v", ok, err)
	}
}

func TestLocalLockerListingReverseWaitRechecks(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*testing.T, *localLocker, dsync.LockArgs)
		want   bool
	}{
		{"writer-released", func(t *testing.T, l *localLocker, w dsync.LockArgs) { listingLockerTestUnlock(t, l, w) }, true},
		{"writer-still-held", func(*testing.T, *localLocker, dsync.LockArgs) {}, false},
		{"replacement-writer", func(t *testing.T, l *localLocker, w dsync.LockArgs) {
			listingLockerTestUnlock(t, l, w)
			listingLockerTestWrite(t, l, listingLockerTestArgs("new-writer", "key", "writer"))
		}, false},
		{"ordinary-reader-arrives", func(t *testing.T, l *localLocker, w dsync.LockArgs) {
			listingLockerTestUnlock(t, l, w)
			listingLockerTestRead(t, l, listingLockerTestArgs("ordinary-reader", "key", "ordinary-reader"))
		}, true},
		{"listing-reader-arrives", func(t *testing.T, l *localLocker, w dsync.LockArgs) {
			listingLockerTestUnlock(t, l, w)
			listingLockerTestRead(t, l, listingLockerTestArgs("other-listing", "key", listingLockerTestSource))
		}, true},
		{"force-unlock", func(t *testing.T, l *localLocker, w dsync.LockArgs) {
			if ok, err := l.ForceUnlock(t.Context(), w); !ok || err != nil {
				t.Fatalf("ForceUnlock = %v, %v", ok, err)
			}
		}, true},
		{"expiry", func(_ *testing.T, l *localLocker, _ dsync.LockArgs) { l.expireOldLocks(-time.Minute) }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				l := newLocker()
				writer := listingLockerTestArgs("writer", "key", "writer")
				reader := listingLockerTestArgs("listing", "key", listingLockerTestSource)
				listingLockerTestWrite(t, l, writer)
				var got bool
				var err error
				start := time.Now()
				go func() { got, err = l.RLock(t.Context(), reader) }()
				synctest.Wait()
				if state := listingLockerTestSnapshot(l); state.waiting != 1 {
					t.Fatalf("waiting = %d, want 1", state.waiting)
				}
				listingLockerTestWrite(t, l, listingLockerTestArgs("unrelated", "other-key", "writer"))
				if elapsed := time.Since(start); elapsed != 0 {
					t.Fatalf("unrelated key waited %v", elapsed)
				}
				time.Sleep(time.Millisecond)
				tc.change(t, l, writer)
				time.Sleep(time.Millisecond)
				synctest.Wait()
				if state := listingLockerTestSnapshot(l); got != tc.want || err != nil || state.waiting != 0 {
					t.Fatalf("RLock = %v, %v, waiting = %d; want %v, nil, 0", got, err, state.waiting, tc.want)
				}
				if elapsed := time.Since(start); elapsed != 2*time.Millisecond {
					t.Fatalf("one attempt elapsed %v, want 2ms", elapsed)
				}
				found := false
				for _, holder := range l.DupLockMap()["key"] {
					if holder.UID == reader.UID {
						found = true
						if holder.Writer || holder.Timestamp != UTCNow().UnixNano() || holder.TimeLastRefresh != holder.Timestamp {
							t.Fatalf("invalid granted reader: %+v", holder)
						}
					}
				}
				if found != got {
					t.Fatalf("reader present = %v, granted = %v", found, got)
				}
			})
		})
	}
}

func TestLocalLockerListingReverseWaitCancellation(t *testing.T) {
	for _, when := range []string{"before-call", "during-wait", "deadline"} {
		t.Run(when, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				l := newLocker()
				writer := listingLockerTestArgs("writer", "key", "writer")
				listingLockerTestWrite(t, l, writer)
				ctx, cancel := context.WithCancel(t.Context())
				if when == "deadline" {
					cancel()
					ctx, cancel = context.WithTimeout(t.Context(), time.Millisecond)
				}
				defer cancel()
				if when == "before-call" {
					cancel()
				}
				var got bool
				var err error
				start := time.Now()
				go func() { got, err = l.RLock(ctx, listingLockerTestArgs("listing", "key", listingLockerTestSource)) }()
				synctest.Wait()
				if when != "before-call" {
					if state := listingLockerTestSnapshot(l); state.waiting != 1 {
						t.Fatalf("waiting = %d, want 1", state.waiting)
					}
					time.Sleep(time.Millisecond)
					// Removal must not turn the canceled request into a grant.
					listingLockerTestUnlock(t, l, writer)
					cancel()
				} else {
					listingLockerTestUnlock(t, l, writer)
				}
				synctest.Wait()
				state := listingLockerTestSnapshot(l)
				if got || err == nil || err != ctx.Err() || state != (listingLockerTestState{}) {
					t.Fatalf("canceled RLock = %v, %v; context %v; state %+v", got, err, ctx.Err(), state)
				}
				if elapsed := time.Since(start); elapsed >= 2*time.Millisecond {
					t.Fatalf("canceled request waited for full timer: %v", elapsed)
				}
			})
		})
	}
}

func TestLocalLockerListingReverseWaitExcluded(t *testing.T) {
	for _, source := range []string{"ordinary-reader", "", "[<unknown>:0:()]", "listingRLock", "[metacache-server-pool.go:0:(*erasureServerPools).listingRLock()]"} {
		t.Run(source, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				l := newLocker()
				listingLockerTestWrite(t, l, listingLockerTestArgs("writer", "key", "writer"))
				start := time.Now()
				if ok, err := l.RLock(t.Context(), listingLockerTestArgs("reader", "key", source)); ok || err != nil {
					t.Fatalf("excluded RLock = %v, %v", ok, err)
				}
				if state := listingLockerTestSnapshot(l); time.Since(start) != 0 || state.waiting != 0 || state.uids != 1 {
					t.Fatalf("excluded RLock changed timing or state: %+v", state)
				}
			})
		})
	}
}

func TestLocalLockerListingReverseWaitFastPaths(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := newLocker()
		start := time.Now()
		for i, source := range []string{listingLockerTestSource, "ordinary-reader", ""} {
			listingLockerTestRead(t, l, listingLockerTestArgs(fmt.Sprintf("shared-%d", i), "shared", source))
			listingLockerTestRead(t, l, listingLockerTestArgs(fmt.Sprintf("free-%d", i), fmt.Sprintf("free-%d", i), source))
		}
		if state := listingLockerTestSnapshot(l); time.Since(start) != 0 || state.waiting != 0 || state.uids != 6 {
			t.Fatalf("uncontended readers changed timing or state: %+v", state)
		}
	})
}

func TestLocalLockerListingReverseWaitOverloaded(t *testing.T) {
	for _, when := range []string{"before-call", "during-wait"} {
		t.Run(when, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				l := newLocker()
				writer := listingLockerTestArgs("writer", "key", "writer")
				listingLockerTestWrite(t, l, writer)
				defer l.waitMutex.Store(0)
				if when == "before-call" {
					l.waitMutex.Store(lockMutexWaitLimit + 1)
				}
				var got bool
				var err error
				go func() {
					got, err = l.RLock(t.Context(), listingLockerTestArgs("listing", "key", listingLockerTestSource))
				}()
				synctest.Wait()
				if when == "during-wait" {
					if state := listingLockerTestSnapshot(l); state.waiting != 1 {
						t.Fatalf("waiting = %d, want 1", state.waiting)
					}
					listingLockerTestUnlock(t, l, writer)
					l.waitMutex.Store(lockMutexWaitLimit + 1)
					time.Sleep(2 * time.Millisecond)
					synctest.Wait()
				} else {
					listingLockerTestUnlock(t, l, writer)
				}
				if state := listingLockerTestSnapshot(l); got || err != nil || state != (listingLockerTestState{}) || l.locksOverloaded.Load() != 1 {
					t.Fatalf("overloaded RLock = %v, %v; state %+v; overloads %d", got, err, state, l.locksOverloaded.Load())
				}
			})
		})
	}
}

func TestLocalLockerListingWaitSharedAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := newLocker()
		listingLockerTestRead(t, l, listingLockerTestArgs("reader", "read-key", listingLockerTestSource))
		listingLockerTestWrite(t, l, listingLockerTestArgs("writer", "write-key", "writer"))
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		results := make(chan error, listingLockWaitLimit)
		for i := range listingLockWaitLimit {
			go func() {
				var ok bool
				var err error
				if i%2 == 0 {
					ok, err = l.Lock(ctx, listingLockerTestArgs(fmt.Sprintf("writer-%d", i), "read-key", "writer"))
				} else {
					ok, err = l.RLock(ctx, listingLockerTestArgs(fmt.Sprintf("listing-%d", i), "write-key", listingLockerTestSource))
				}
				if ok {
					err = fmt.Errorf("granted over a conflicting holder")
				}
				results <- err
			}()
		}
		synctest.Wait()
		if state := listingLockerTestSnapshot(l); state.waiting != listingLockWaitLimit {
			t.Fatalf("combined waiting = %d, want %d", state.waiting, listingLockWaitLimit)
		}
		start := time.Now()
		if ok, err := l.Lock(ctx, listingLockerTestArgs("excess-writer", "read-key", "writer")); ok || err != nil {
			t.Fatalf("excess Lock = %v, %v", ok, err)
		}
		if ok, err := l.RLock(ctx, listingLockerTestArgs("excess-listing", "write-key", listingLockerTestSource)); ok || err != nil {
			t.Fatalf("excess RLock = %v, %v", ok, err)
		}
		listingLockerTestWrite(t, l, listingLockerTestArgs("unrelated-writer", "unrelated-write", "writer"))
		listingLockerTestRead(t, l, listingLockerTestArgs("unrelated-listing", "unrelated-read", listingLockerTestSource))
		if state := listingLockerTestSnapshot(l); time.Since(start) != 0 || state.waiting != listingLockWaitLimit || state.uids != 4 {
			t.Fatalf("combined cap changed immediate paths: %+v", state)
		}
		cancel()
		synctest.Wait()
		for range listingLockWaitLimit {
			if err := <-results; err != context.Canceled {
				t.Fatalf("canceled waiter: %v", err)
			}
		}
		if state := listingLockerTestSnapshot(l); state.waiting != 0 || state.uids != 4 {
			t.Fatalf("combined wait leaked: %+v", state)
		}
	})
}

type listingReverseCountLocker struct {
	*localLocker
	calls  atomic.Int32
	grants atomic.Int32
	reject bool
}

func (l *listingReverseCountLocker) RLock(ctx context.Context, args dsync.LockArgs) (bool, error) {
	l.calls.Add(1)
	if l.reject {
		return false, nil
	}
	ok, err := l.localLocker.RLock(ctx, args)
	if ok {
		l.grants.Add(1)
	}
	return ok, err
}

func TestDistributedListingReverseWait(t *testing.T) {
	for _, tc := range []struct {
		name             string
		free, held       int
		immediateRejects int
		cancel           bool
		want             bool
	}{
		{name: "all-writers-release", want: true},
		{name: "two-writers-remain", held: 2, want: true},
		{name: "three-writers-remain", held: 3},
		{name: "partial-grant-then-quorum", free: 1, want: true},
		{name: "partial-grant-without-quorum", free: 1, held: 3},
		{name: "late-grant-after-rejection", immediateRejects: 3},
		{name: "partial-and-late-grants-after-cancel", free: 1, cancel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				clients := make([]dsync.NetLocker, 4)
				writer := listingLockerTestArgs("writer", "key", "writer")
				for i := range clients {
					l := &listingReverseCountLocker{localLocker: newLocker(), reject: i < tc.immediateRejects}
					clients[i] = l
					if i >= tc.free {
						listingLockerTestWrite(t, l.localLocker, writer)
					}
				}
				ds := &dsync.Dsync{GetLockers: func() ([]dsync.NetLocker, string) { return clients, writer.Owner }, Timeouts: dsync.DefaultTimeouts}
				dm := dsync.NewDRWMutex(ds, "key")
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				result := make(chan bool, 1)
				go func() {
					result <- dm.GetRLock(ctx, nil, "listing", listingLockerTestSource, dsync.Options{Timeout: time.Second, RetryInterval: -1})
				}()
				synctest.Wait()
				for i, c := range clients {
					l := c.(*listingReverseCountLocker)
					if i < tc.free && l.grants.Load() != 1 {
						t.Fatal("uncontended partial grant did not arrive immediately")
					}
					if i >= tc.free && !l.reject && listingLockerTestSnapshot(l.localLocker).waiting != 1 {
						t.Fatal("conflicting reader did not wait in its first RPC")
					}
				}
				earlyResult := tc.immediateRejects == 3 || tc.cancel
				if tc.cancel {
					cancel()
					synctest.Wait()
				}
				if earlyResult {
					select {
					case got := <-result:
						if got {
							t.Fatal("attempt succeeded before delayed replies")
						}
					default:
						t.Fatal("attempt did not return before delayed replies")
					}
					for i := range tc.free {
						if state := listingLockerTestSnapshot(clients[i].(*listingReverseCountLocker).localLocker); state.uids != 0 {
							t.Fatal("early partial grant was not released")
						}
					}
				}
				time.Sleep(time.Millisecond)
				for i, c := range clients {
					if i >= tc.free+tc.held {
						listingLockerTestUnlock(t, c.(*listingReverseCountLocker).localLocker, writer)
					}
				}
				time.Sleep(time.Millisecond)
				synctest.Wait()
				if !earlyResult {
					if got := <-result; got != tc.want {
						t.Fatalf("GetRLock = %v, want %v", got, tc.want)
					}
				}
				for i, c := range clients {
					l := c.(*listingReverseCountLocker)
					if l.calls.Load() != 1 || listingLockerTestSnapshot(l.localLocker).waiting != 0 {
						t.Fatalf("peer %d calls = %d or leaked admission", i, l.calls.Load())
					}
					wantGrant := !l.reject && (i < tc.free || i >= tc.free+tc.held)
					if (l.grants.Load() == 1) != wantGrant {
						t.Fatalf("peer %d grants = %d, want granted = %v", i, l.grants.Load(), wantGrant)
					}
					if !tc.want {
						for _, holder := range l.DupLockMap()["key"] {
							if holder.UID == "listing" {
								t.Fatalf("peer %d leaked partial or late read grant", i)
							}
						}
					}
				}
				if tc.want {
					dm.RUnlock(t.Context())
					synctest.Wait()
				}
				for i, c := range clients {
					l := c.(*listingReverseCountLocker)
					if i >= tc.free && i < tc.free+tc.held {
						listingLockerTestUnlock(t, l.localLocker, writer)
					}
					if state := listingLockerTestSnapshot(l.localLocker); state != (listingLockerTestState{}) {
						t.Fatalf("peer %d final state is not empty: %+v", i, state)
					}
				}
			})
		})
	}
}
