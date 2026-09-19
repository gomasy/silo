// Copyright (c) 2026 PGSTY
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"context"
	"testing"
	"time"
)

func TestIAMHealingResumesAfterLeadershipLoss(t *testing.T) {
	previous := globalLeaderLock
	locks := make(chan LockContext)
	globalLeaderLock = &sharedLock{lockContext: locks}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	c := &SiteReplicationSys{}
	go func() { c.startHealRoutine(ctx, nil); close(done) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case locks <- LockContext{ctx: ctx}:
		}
		<-done
		globalLeaderLock = previous
	})
	first, loseFirst := context.WithCancel(ctx)
	defer loseFirst()
	select {
	case locks <- LockContext{ctx: first}:
	case <-time.After(time.Second):
		t.Fatal("healer did not acquire its first leader context")
	}
	loseFirst() // A transient quorum loss cancels the distributed lease.
	select {
	case <-done:
		t.Fatal("healer permanently exited after temporary leadership loss")
	case locks <- LockContext{ctx: ctx}:
	case <-time.After(time.Second):
		t.Fatal("healer did not wait for reacquired leadership")
	}
	// Shutdown must also interrupt the wait for leadership after lease loss.
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("healer did not stop with its owning context")
	}
}

func TestIAMHealingLeadershipWaitCancels(t *testing.T) {
	previous := globalLeaderLock
	locks := make(chan LockContext)
	globalLeaderLock = &sharedLock{lockContext: locks}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	c := &SiteReplicationSys{}
	go func() { c.startHealRoutine(ctx, nil); close(done) }()
	cancel()
	t.Cleanup(func() {
		select {
		case <-done:
		case locks <- LockContext{ctx: ctx}:
		}
		<-done
		globalLeaderLock = previous
	})
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("healer ignored shutdown while waiting for leadership")
	}
}
