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

package dsync

import (
	"context"
	"math/rand"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// retryTestLocker deliberately leaves unexpected methods unimplemented.
// These tests exercise the real DRWMutex retry and cleanup code without HTTP.
type retryTestLocker struct {
	NetLocker
	acquire func(context.Context, LockArgs) (bool, error)
	release func(context.Context, LockArgs) (bool, error)
}

func (l retryTestLocker) Lock(ctx context.Context, args LockArgs) (bool, error) {
	return l.acquire(ctx, args)
}

func (l retryTestLocker) RLock(ctx context.Context, args LockArgs) (bool, error) {
	return l.acquire(ctx, args)
}

func (l retryTestLocker) Unlock(ctx context.Context, args LockArgs) (bool, error) {
	return l.release(ctx, args)
}

func (l retryTestLocker) RUnlock(ctx context.Context, args LockArgs) (bool, error) {
	return l.release(ctx, args)
}

func (l retryTestLocker) String() string { return "retry-test-locker" }

func newRetryTestMutex(lockers []NetLocker) *DRWMutex {
	dm := NewDRWMutex(&Dsync{
		GetLockers: func() ([]NetLocker, string) { return lockers, "retry-test-owner" },
		Timeouts:   DefaultTimeouts,
	}, "retry-test-resource")
	dm.rng = rand.New(&lockedRandSource{src: rand.NewSource(1)})
	return dm
}

func retryTestAcquire(ctx context.Context, dm *DRWMutex, read bool, opts Options) bool {
	if read {
		return dm.GetRLock(ctx, nil, "retry-test-uid", "retry-test", opts)
	}
	return dm.GetLock(ctx, nil, "retry-test-uid", "retry-test", opts)
}

func TestDRWMutexRetryCancellation(t *testing.T) {
	for _, read := range []bool{false, true} {
		for _, opts := range []Options{
			{Timeout: time.Hour},
			{Timeout: time.Hour, RetryInterval: time.Hour},
		} {
			synctest.Test(t, func(t *testing.T) {
				var calls atomic.Int64
				lockers := make([]NetLocker, 4)
				for i := range lockers {
					lockers[i] = retryTestLocker{acquire: func(context.Context, LockArgs) (bool, error) {
						calls.Add(1)
						return false, nil
					}}
				}
				dm := newRetryTestMutex(lockers)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan bool, 1)
				go func() {
					done <- retryTestAcquire(ctx, dm, read, opts)
				}()
				synctest.Wait()
				if got := calls.Load(); got != 4 {
					t.Fatalf("first attempt made %d RPCs, want 4", got)
				}
				cancel()
				synctest.Wait()
				select {
				case locked := <-done:
					if locked {
						t.Fatal("canceled waiter acquired lock")
					}
				default:
					t.Errorf("read=%v opts=%+v: cancellation did not interrupt retry wait", read, opts)
					<-done
				}
				time.Sleep(time.Second)
				synctest.Wait()
				if got := calls.Load(); got != 4 {
					t.Fatalf("canceled waiter issued a new RPC attempt: %d calls", got)
				}
			})
		}
	}
}

func TestDRWMutexRetryDeadline(t *testing.T) {
	for _, interval := range []time.Duration{0, time.Hour} {
		synctest.Test(t, func(t *testing.T) {
			locker := retryTestLocker{acquire: func(context.Context, LockArgs) (bool, error) {
				return false, nil
			}}
			dm := newRetryTestMutex([]NetLocker{locker})
			start := time.Now()
			if retryTestAcquire(t.Context(), dm, false, Options{Timeout: 10 * time.Millisecond, RetryInterval: interval}) {
				t.Fatal("unavailable lock was acquired")
			}
			if elapsed := time.Since(start); elapsed != 10*time.Millisecond {
				t.Fatalf("retry wait overran deadline: %v", elapsed)
			}
		})
	}
}

func TestDRWMutexCanceledPartialAndLateGrants(t *testing.T) {
	for _, read := range []bool{false, true} {
		for _, slowRelease := range []bool{false, true} {
			synctest.Test(t, func(t *testing.T) {
				lateGrant := make(chan struct{})
				releaseGate := make(chan struct{})
				if !slowRelease {
					close(releaseGate)
				}
				var calls, releases atomic.Int64
				var held [4]atomic.Bool
				lockers := make([]NetLocker, len(held))
				for i := range lockers {
					lockers[i] = retryTestLocker{
						acquire: func(ctx context.Context, _ LockArgs) (bool, error) {
							calls.Add(1)
							if i > 0 {
								<-lateGrant
							}
							if ctx.Err() != nil {
								t.Error("NetLocker acquisition unexpectedly inherits caller cancellation")
							}
							held[i].Store(true)
							return true, nil
						},
						release: func(context.Context, LockArgs) (bool, error) {
							if i == 0 {
								<-releaseGate
							}
							held[i].Store(false)
							releases.Add(1)
							return true, nil
						},
					}
				}
				dm := newRetryTestMutex(lockers)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan bool, 1)
				go func() { done <- retryTestAcquire(ctx, dm, read, Options{Timeout: time.Hour}) }()
				synctest.Wait()
				if !held[0].Load() || calls.Load() != 4 {
					t.Fatal("partial acquisition was not established")
				}
				cancel()
				synctest.Wait()
				if slowRelease {
					// Cancellation of retry sleep is not a whole-call bound:
					// synchronous cleanup of a partial grant can still block.
					select {
					case <-done:
						t.Fatal("acquisition returned before partial-grant cleanup completed")
					default:
					}
					close(releaseGate)
					synctest.Wait()
				}
				select {
				case locked := <-done:
					if locked {
						t.Fatal("canceled partial acquisition returned success")
					}
				default:
					t.Error("acquisition did not return after partial-grant cleanup")
					<-done
				}
				if held[0].Load() || releases.Load() != 1 {
					t.Fatal("partial grant was not released")
				}
				close(lateGrant)
				synctest.Wait()
				for i := range held {
					if held[i].Load() {
						t.Errorf("late grant on node %d was not released", i)
					}
				}
				if calls.Load() != 4 || releases.Load() != 4 {
					t.Fatalf("calls=%d releases=%d, want 4/4", calls.Load(), releases.Load())
				}
			})
		}
	}
}
