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
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/minio/minio/internal/dsync"
)

const listingLockerTestSource = "[metacache-server-pool.go:274:(*erasureServerPools).listingRLock()]"

func listingLockerTestArgs(uid, resource, source string) dsync.LockArgs {
	quorum := 3
	return dsync.LockArgs{UID: uid, Resources: []string{resource}, Source: source, Owner: "listing-wait-test", Quorum: &quorum}
}

func listingLockerTestRead(t *testing.T, l *localLocker, args dsync.LockArgs) {
	t.Helper()
	if ok, err := l.RLock(t.Context(), args); !ok || err != nil {
		t.Fatalf("RLock = %v, %v", ok, err)
	}
}

func listingLockerTestRUnlock(t *testing.T, l *localLocker, args dsync.LockArgs) {
	t.Helper()
	if ok, err := l.RUnlock(t.Context(), args); !ok || err != nil {
		t.Fatalf("RUnlock = %v, %v", ok, err)
	}
}

type listingLockerTestState struct {
	waiting, resources, uids int
}

func listingLockerTestSnapshot(l *localLocker) listingLockerTestState {
	defer l.getMutex()()
	return listingLockerTestState{waiting: l.listingWaiters, resources: len(l.lockMap), uids: len(l.lockUID)}
}

func TestLocalLockerListingSource(t *testing.T) {
	for _, source := range []string{
		"", "listingRLock", "[<unknown>:0:()]",
		strings.Replace(listingLockerTestSource, ":274:", ":", 1),
		strings.Replace(listingLockerTestSource, "274", "", 1),
		strings.Replace(listingLockerTestSource, "274", "0", 1),
		strings.Replace(listingLockerTestSource, "274", "1x", 1),
		strings.Replace(listingLockerTestSource, "274", "-1", 1),
		strings.Replace(listingLockerTestSource, "listingRLock", "GetObjectNInfo", 1),
		strings.Replace(listingLockerTestSource, "listingRLock", "listingRLock.func1", 1),
		strings.Replace(listingLockerTestSource, "metacache-server-pool.go", "other.go", 1),
		listingLockerTestSource + "trailing",
	} {
		if isListingReadLock([]lockRequesterInfo{{Source: source}}) {
			t.Errorf("unrecognized source accepted: %q", source)
		}
	}
	if isListingReadLock(nil) || isListingReadLock([]lockRequesterInfo{{Source: listingLockerTestSource, Writer: true}}) {
		t.Fatal("empty or write holders accepted")
	}
	if isListingReadLock([]lockRequesterInfo{{Source: listingLockerTestSource}, {Source: "ordinary-reader"}}) {
		t.Fatal("mixed readers accepted")
	}

	// Check the real runtime-generated source, not only a hand-written label.
	synctest.Test(t, func(t *testing.T) {
		lockers := []dsync.NetLocker{newLocker(), newLocker(), newLocker(), newLocker()}
		set := &erasureObjects{nsMutex: newNSLock(true), getLockers: func() ([]dsync.NetLocker, string) { return lockers, "listing-source-test" }}
		z := &erasureServerPools{serverPools: []*erasureSets{{sets: []*erasureObjects{set}}, {sets: []*erasureObjects{set}}}}
		_, unlock, err := z.listingRLock(t.Context(), "bucket", "object", set)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range lockers {
			l := c.(*localLocker)
			for _, holders := range l.DupLockMap() {
				if !isListingReadLock(holders) {
					t.Errorf("actual listingRLock source was not recognized: %#v", holders)
				}
			}
		}
		unlock()
		synctest.Wait()
	})
}

func FuzzListingLockSource(f *testing.F) {
	f.Add(listingLockerTestSource)
	f.Add("[metacache-server-pool.go:(*erasureServerPools).listingRLock()]")
	f.Add("")
	f.Fuzz(func(t *testing.T, source string) {
		// Source is diagnostic text from a peer, including older versions. No
		// malformed string may panic or make a writer eligible for this wait.
		isListingReadLock([]lockRequesterInfo{{Source: source}})
		if isListingReadLock([]lockRequesterInfo{{Source: source, Writer: true}}) {
			t.Fatal("writer was accepted as a listing reader")
		}
	})
}

func TestLocalLockerListingWaitRechecks(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*testing.T, *localLocker, dsync.LockArgs)
		want   bool
	}{
		{"reader-released", func(t *testing.T, l *localLocker, r dsync.LockArgs) { listingLockerTestRUnlock(t, l, r) }, true},
		{"reader-still-held", func(*testing.T, *localLocker, dsync.LockArgs) {}, false},
		{"one-of-two-readers-remains", func(t *testing.T, l *localLocker, r dsync.LockArgs) {
			other := r
			other.UID = "second-reader"
			listingLockerTestRead(t, l, other)
			listingLockerTestRUnlock(t, l, r)
		}, false},
		{"ordinary-reader-arrives", func(t *testing.T, l *localLocker, r dsync.LockArgs) {
			listingLockerTestRUnlock(t, l, r)
			listingLockerTestRead(t, l, listingLockerTestArgs("new-reader", "key", "ordinary-reader"))
		}, false},
		{"other-writer-wins", func(t *testing.T, l *localLocker, r dsync.LockArgs) {
			listingLockerTestRUnlock(t, l, r)
			if ok, err := l.Lock(t.Context(), listingLockerTestArgs("other-writer", "key", "writer")); !ok || err != nil {
				t.Fatalf("competing Lock = %v, %v", ok, err)
			}
		}, false},
		{"force-unlock", func(t *testing.T, l *localLocker, r dsync.LockArgs) {
			if ok, err := l.ForceUnlock(t.Context(), r); !ok || err != nil {
				t.Fatalf("ForceUnlock = %v, %v", ok, err)
			}
		}, true},
		{"expiry", func(_ *testing.T, l *localLocker, _ dsync.LockArgs) { l.expireOldLocks(-time.Minute) }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				l := newLocker()
				reader := listingLockerTestArgs("reader", "key", listingLockerTestSource)
				listingLockerTestRead(t, l, reader)
				var got bool
				var err error
				start := time.Now()
				go func() { got, err = l.Lock(t.Context(), listingLockerTestArgs("writer", "key", "writer")) }()
				synctest.Wait()
				if state := listingLockerTestSnapshot(l); state.waiting != 1 {
					t.Fatalf("waiting = %d, want 1", state.waiting)
				}
				// The global mutex must be released throughout the timer wait.
				if ok, err := l.Lock(t.Context(), listingLockerTestArgs("unrelated", "other-key", "writer")); !ok || err != nil {
					t.Fatalf("unrelated key blocked: %v, %v", ok, err)
				}
				if elapsed := time.Since(start); elapsed != 0 {
					t.Fatalf("unrelated key waited %v", elapsed)
				}
				time.Sleep(time.Millisecond)
				tc.change(t, l, reader)
				time.Sleep(time.Millisecond)
				synctest.Wait()
				if state := listingLockerTestSnapshot(l); got != tc.want || err != nil || state.waiting != 0 {
					t.Fatalf("Lock = %v, %v, waiting = %d; want %v, nil, 0", got, err, state.waiting, tc.want)
				}
				if elapsed := time.Since(start); elapsed != 2*time.Millisecond {
					t.Fatalf("one attempt elapsed %v, want 2ms", elapsed)
				}
			})
		})
	}
}

func TestLocalLockerListingWaitCancellation(t *testing.T) {
	for _, when := range []string{"before-call", "during-wait", "deadline"} {
		t.Run(when, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				l := newLocker()
				reader := listingLockerTestArgs("reader", "key", listingLockerTestSource)
				listingLockerTestRead(t, l, reader)
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
				go func() { got, err = l.Lock(ctx, listingLockerTestArgs("writer", "key", "writer")) }()
				synctest.Wait()
				if when != "before-call" {
					if state := listingLockerTestSnapshot(l); state.waiting != 1 {
						t.Fatalf("waiting = %d", state.waiting)
					}
					time.Sleep(time.Millisecond)
					cancel()
				}
				synctest.Wait()
				if state := listingLockerTestSnapshot(l); got || err != ctx.Err() || err == nil || state.waiting != 0 {
					t.Fatalf("canceled Lock = %v, %v; context %v; waiting = %d", got, err, ctx.Err(), state.waiting)
				}
				listingLockerTestRUnlock(t, l, reader)
				if state := listingLockerTestSnapshot(l); state.resources != 0 || state.uids != 0 {
					t.Fatal("canceled wait left a lock")
				}
			})
		})
	}
}

func TestLocalLockerListingWaitExcluded(t *testing.T) {
	for _, kind := range []string{"ordinary-reader", "unknown-reader", "mixed-readers", "writer", "bulk", "overloaded"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				l := newLocker()
				reader := listingLockerTestArgs("holder", "key", listingLockerTestSource)
				if kind == "ordinary-reader" {
					reader.Source = "[erasure-object.go:100:(*erasureObjects).GetObjectNInfo()]"
				}
				if kind == "unknown-reader" {
					reader.Source = ""
				}
				if kind == "writer" {
					if ok, err := l.Lock(t.Context(), reader); !ok || err != nil {
						t.Fatalf("initial write = %v, %v", ok, err)
					}
				} else {
					listingLockerTestRead(t, l, reader)
				}
				if kind == "mixed-readers" {
					listingLockerTestRead(t, l, listingLockerTestArgs("ordinary", "key", "ordinary-reader"))
				}
				writer := listingLockerTestArgs("writer", "key", "writer")
				if kind == "bulk" {
					writer.Resources = []string{"key", "other-key"}
				}
				if kind == "overloaded" {
					l.waitMutex.Store(lockMutexWaitLimit + 1)
				}
				start := time.Now()
				if ok, err := l.Lock(t.Context(), writer); ok || err != nil {
					t.Fatalf("conflicting Lock = %v, %v", ok, err)
				}
				state := listingLockerTestSnapshot(l)
				if elapsed := time.Since(start); elapsed != 0 || state.waiting != 0 {
					t.Fatalf("excluded path waited %v; waiters = %d", elapsed, state.waiting)
				}
				if _, ok := l.DupLockMap()["other-key"]; ok {
					t.Fatal("bulk request partially acquired an unrelated key")
				}
			})
		})
	}
}

func TestLocalLockerListingWaitAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := newLocker()
		reader := listingLockerTestArgs("reader", "key", listingLockerTestSource)
		listingLockerTestRead(t, l, reader)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		results := make(chan error, listingLockWaitLimit)
		for i := range listingLockWaitLimit {
			go func() {
				ok, err := l.Lock(ctx, listingLockerTestArgs(fmt.Sprintf("writer-%d", i), "key", "writer"))
				if ok {
					err = fmt.Errorf("granted over a live reader")
				}
				results <- err
			}()
		}
		synctest.Wait()
		if state := listingLockerTestSnapshot(l); state.waiting != listingLockWaitLimit {
			t.Fatalf("waiting = %d, want %d", state.waiting, listingLockWaitLimit)
		}
		start := time.Now()
		if ok, err := l.Lock(ctx, listingLockerTestArgs("excess", "key", "writer")); ok || err != nil {
			t.Fatalf("excess waiter = %v, %v", ok, err)
		}
		if state := listingLockerTestSnapshot(l); time.Since(start) != 0 || state.waiting != listingLockWaitLimit {
			t.Fatal("excess waiter consumed time or admission")
		}
		if ok, err := l.Lock(ctx, listingLockerTestArgs("unrelated", "other-key", "writer")); !ok || err != nil {
			t.Fatalf("admission limit rejected an uncontended key: %v, %v", ok, err)
		}
		cancel()
		synctest.Wait()
		for range listingLockWaitLimit {
			if err := <-results; err != context.Canceled {
				t.Fatalf("canceled waiter: %v", err)
			}
		}
		if state := listingLockerTestSnapshot(l); state.waiting != 0 {
			t.Fatalf("admission leaked: %d", state.waiting)
		}
	})
}

func TestLocalLockerListingWaitContended(t *testing.T) {
	for _, n := range []int{2, 8, 32} {
		t.Run(fmt.Sprintf("%d-writers", n), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				l := newLocker()
				reader := listingLockerTestArgs("reader", "key", listingLockerTestSource)
				listingLockerTestRead(t, l, reader)
				results := make(chan bool, n)
				for i := range n {
					go func() {
						ok, err := l.Lock(t.Context(), listingLockerTestArgs(fmt.Sprintf("writer-%d", i), "key", "writer"))
						if err != nil {
							t.Errorf("Lock: %v", err)
						}
						results <- ok
					}()
				}
				synctest.Wait()
				if state := listingLockerTestSnapshot(l); state.waiting != n {
					t.Fatalf("waiting = %d, want %d", state.waiting, n)
				}
				time.Sleep(time.Millisecond)
				listingLockerTestRUnlock(t, l, reader)
				time.Sleep(time.Millisecond)
				synctest.Wait()
				granted := 0
				for range n {
					if <-results {
						granted++
					}
				}
				if state := listingLockerTestSnapshot(l); granted != 1 || state.uids != 1 || state.waiting != 0 {
					t.Fatalf("grants = %d, indexed = %d, waiting = %d", granted, state.uids, state.waiting)
				}
			})
		})
	}
}

type listingCountLocker struct {
	*localLocker
	calls atomic.Int32
}

func (l *listingCountLocker) Lock(ctx context.Context, args dsync.LockArgs) (bool, error) {
	l.calls.Add(1)
	return l.localLocker.Lock(ctx, args)
}

// Exercise real DRWMutex quorum and late-response cleanup. The mixed case with
// two immediate rejections fails before the upgraded lockers finish waiting.
func TestDistributedListingWait(t *testing.T) {
	for _, ordinaryReaders := range []int{0, 1, 2} {
		t.Run(fmt.Sprintf("%d-immediate-rejections", ordinaryReaders), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				clients := make([]dsync.NetLocker, 4)
				reader := listingLockerTestArgs("reader", "key", listingLockerTestSource)
				for i := range clients {
					l := &listingCountLocker{localLocker: newLocker()}
					clients[i] = l
					r := reader
					if i < ordinaryReaders {
						r.Source = "old-node-unknown-source"
					}
					listingLockerTestRead(t, l.localLocker, r)
				}
				ds := &dsync.Dsync{GetLockers: func() ([]dsync.NetLocker, string) { return clients, reader.Owner }, Timeouts: dsync.DefaultTimeouts}
				dm := dsync.NewDRWMutex(ds, "key")
				var got bool
				go func() {
					got = dm.GetLock(t.Context(), nil, "writer", "writer", dsync.Options{Timeout: time.Second, RetryInterval: -1})
				}()
				synctest.Wait()
				time.Sleep(time.Millisecond)
				for _, c := range clients {
					listingLockerTestRUnlock(t, c.(*listingCountLocker).localLocker, reader)
				}
				time.Sleep(time.Millisecond)
				synctest.Wait()
				if got != (ordinaryReaders < 2) {
					t.Fatalf("GetLock = %v", got)
				}
				granted := 0
				for _, c := range clients {
					l := c.(*listingCountLocker)
					if l.calls.Load() != 1 {
						t.Fatalf("lock calls = %d, want one round", l.calls.Load())
					}
					state := listingLockerTestSnapshot(l.localLocker)
					if state.resources != 0 {
						granted++
					}
					if state.waiting != 0 {
						t.Fatal("wait admission leaked")
					}
				}
				if got && granted < 3 {
					t.Fatalf("writer accepted without quorum: %d", granted)
				}
				if !got && granted != 0 {
					t.Fatalf("late grants leaked after rejected attempt: %d", granted)
				}
				if got {
					dm.Unlock(t.Context())
					synctest.Wait()
				}
				for _, c := range clients {
					l := c.(*listingCountLocker)
					if state := listingLockerTestSnapshot(l.localLocker); state.resources != 0 || state.uids != 0 {
						t.Fatal("final lock state not empty")
					}
				}
			})
		})
	}
}
