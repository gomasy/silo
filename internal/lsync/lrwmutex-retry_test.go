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

package lsync

import (
	"context"
	"testing"
	"testing/synctest"
	"time"
)

func TestLRWMutexRetryCancellation(t *testing.T) {
	for _, read := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			lm := NewLRWMutex()
			lm.Lock()
			defer lm.Unlock()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan bool, 1)
			go func() {
				if read {
					done <- lm.GetRLock(ctx, "waiter", "retry-test", time.Hour)
				} else {
					done <- lm.GetLock(ctx, "waiter", "retry-test", time.Hour)
				}
			}()
			synctest.Wait()
			cancel()
			synctest.Wait()
			select {
			case locked := <-done:
				if locked {
					t.Fatal("canceled waiter acquired lock")
				}
			default:
				t.Error("cancellation did not interrupt local retry wait")
				<-done
			}
		})
	}
}

func TestLRWMutexRetryDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lm := NewLRWMutex()
		lm.Lock()
		defer lm.Unlock()
		start := time.Now()
		if lm.GetLock(t.Context(), "waiter", "retry-test", time.Millisecond) {
			t.Fatal("unavailable lock was acquired")
		}
		if elapsed := time.Since(start); elapsed != time.Millisecond {
			t.Fatalf("local retry wait overran deadline: %v", elapsed)
		}
	})
}
