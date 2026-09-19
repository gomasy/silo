// Copyright (c) 2015-2026 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
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

package logger

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	types "github.com/minio/minio/internal/logger/target/loggertypes"
)

// fakeTarget is a minimal Target used to exercise the registry.
type fakeTarget struct {
	name  string
	kind  types.TargetType
	inits atomic.Int32
	init  func() error
}

func (f *fakeTarget) String() string           { return f.name }
func (f *fakeTarget) Endpoint() string         { return "" }
func (f *fakeTarget) Stats() types.TargetStats { return types.TargetStats{} }
func (f *fakeTarget) Init(context.Context) error {
	f.inits.Add(1)
	if f.init != nil {
		return f.init()
	}
	return nil
}
func (f *fakeTarget) IsOnline(context.Context) bool   { return true }
func (f *fakeTarget) Cancel()                         {}
func (f *fakeTarget) Send(context.Context, any) error { return nil }
func (f *fakeTarget) Type() types.TargetType          { return f.kind }

// swapSystemTargets isolates the package-level registry for a test.
func swapSystemTargets(t *testing.T) {
	t.Helper()
	prevTargets, prevConsole := systemTargets, consoleTgt.Load()
	systemTargets = newTargetsList()
	consoleTgt.Store(nil)
	t.Cleanup(func() {
		systemTargets = prevTargets
		consoleTgt.Store(prevConsole)
	})
}

// AddSystemTarget must not register the same target twice: the console
// logger is re-added on every console-log subscription, and duplicates
// surface as identical series in the /logger/webhook metrics collector.
func TestAddSystemTargetIdempotent(t *testing.T) {
	swapSystemTargets(t)
	ctx := context.Background()

	console := &fakeTarget{name: "console+http", kind: types.TargetConsole}
	for range 3 {
		if err := AddSystemTarget(ctx, console); err != nil {
			t.Fatalf("AddSystemTarget: %v", err)
		}
	}
	if got := len(SystemTargets()); got != 1 {
		t.Fatalf("expected 1 system target after repeated add, got %d", got)
	}
	if got := console.inits.Load(); got != 1 {
		t.Fatalf("expected Init to run once, ran %d times", got)
	}
	if got := consoleTgt.Load(); got == nil || *got != console {
		t.Fatalf("consoleTgt not set to the console target")
	}

	other := &fakeTarget{name: "other", kind: types.TargetHTTP}
	if err := AddSystemTarget(ctx, other); err != nil {
		t.Fatalf("AddSystemTarget: %v", err)
	}
	if got := len(SystemTargets()); got != 2 {
		t.Fatalf("expected 2 distinct system targets, got %d", got)
	}
}

func TestAddSystemTargetConcurrent(t *testing.T) {
	swapSystemTargets(t)
	ctx := context.Background()

	console := &fakeTarget{name: "console+http", kind: types.TargetConsole}
	console.init = func() error {
		// Initialization is allowed to inspect or write to the existing logger.
		_ = SystemTargets()
		consoleLogIf(ctx, "test", errors.New("initializing logger"))
		return nil
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := AddSystemTarget(ctx, console); err != nil {
				t.Errorf("AddSystemTarget: %v", err)
			}
			consoleLogIf(ctx, "test", errors.New("concurrent logger"))
		}()
	}
	close(start)
	wg.Wait()

	if got := len(SystemTargets()); got != 1 {
		t.Fatalf("expected 1 system target after concurrent adds, got %d", got)
	}
	if got := console.inits.Load(); got != 1 {
		t.Fatalf("expected one concurrent initialization, got %d", got)
	}
}

func TestAddSystemTargetRetriesFailedInit(t *testing.T) {
	swapSystemTargets(t)
	ctx := context.Background()
	initErr := errors.New("target unavailable")
	target := &fakeTarget{name: "console", kind: types.TargetConsole}
	target.init = func() error { return initErr }
	if err := AddSystemTarget(ctx, target); !errors.Is(err, initErr) {
		t.Fatalf("expected initialization error, got %v", err)
	}
	if len(SystemTargets()) != 0 || consoleTgt.Load() != nil {
		t.Fatal("failed initialization published a target")
	}
	target.init = nil
	if err := AddSystemTarget(ctx, target); err != nil {
		t.Fatal(err)
	}
	if got := len(SystemTargets()); got != 1 || target.inits.Load() != 2 {
		t.Fatalf("retry: targets=%d, initializations=%d", got, target.inits.Load())
	}
}
