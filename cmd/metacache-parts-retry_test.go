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
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio/internal/grid"
)

// Read real cache objects and use an actual peer RPC, counting only the block
// under test. Gates control publication, never replace quorum or EOF decisions.
type listingCachePartsProbe struct {
	state    atomic.Pointer[metacache]
	rpcFails atomic.Bool
	reads    atomic.Int64
	mu       sync.Mutex
	probes   []int
	checks   []time.Time
	onProbe  func(int) bool
	onCheck  func(int)
	target   string
	readErr  error
}

func (p *listingCachePartsProbe) setState(o listPathOptions, failed bool) {
	m := o.newMetacache()
	m.lastUpdate = time.Now()
	if failed {
		m.status, m.error = scanStateError, "cache producer failed"
	} else {
		m.status = scanStateStarted
	}
	p.state.Store(&m)
}

func (p *listingCachePartsProbe) snapshot() ([]int, []time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.probes), slices.Clone(p.checks)
}

type listingCachePartsDisk struct {
	StorageAPI
	index int
	probe *listingCachePartsProbe
}

func (d *listingCachePartsDisk) ReadXL(ctx context.Context, bucket, object string, readData bool) (RawFileInfo, error) {
	if bucket == minioMetaBucket && object == d.probe.target {
		d.probe.reads.Add(1)
		if d.probe.readErr != nil {
			return RawFileInfo{}, d.probe.readErr
		}
	}
	return d.StorageAPI.ReadXL(ctx, bucket, object, readData)
}

func (d *listingCachePartsDisk) ReadVersion(ctx context.Context, origVolume, volume, path, versionID string, opts ReadOptions) (FileInfo, error) {
	if volume == minioMetaBucket && path == d.probe.target {
		d.probe.mu.Lock()
		d.probe.probes = append(d.probe.probes, d.index)
		d.probe.mu.Unlock()
		if d.probe.onProbe != nil && d.probe.onProbe(d.index) {
			return FileInfo{}, errFileNotFound
		}
	}
	return d.StorageAPI.ReadVersion(ctx, origVolume, volume, path, versionID, opts)
}

func listingCachePartsBackend(t *testing.T, part int) (*erasureObjects, listPathOptions, *listingCachePartsProbe, func([]StorageAPI)) {
	t.Helper()
	z, bucket, _ := nullQuorumBackend(t)
	set := z.serverPools[0].sets[0]
	o := listPathOptions{ID: mustGetUUID(), Bucket: bucket, Limit: 100, Recursive: true, IncludeDirectories: true}
	p := &listingCachePartsProbe{target: o.objectPath(part)}
	p.setState(o, false)
	disks, switchDisks := installListingDiskSwitch(set)
	wrapped := slices.Clone(disks)
	for i, disk := range disks {
		wrapped[i] = &listingCachePartsDisk{StorageAPI: disk, index: i, probe: p}
	}
	switchDisks(wrapped)

	tg, err := grid.SetupTestGrid(2)
	if err != nil {
		t.Fatal(err)
	}
	previous := globalNotificationSys
	t.Cleanup(func() {
		globalNotificationSys = previous
		tg.Cleanup()
	})
	if err := getMetacacheListingRPC.Register(tg.Managers[1], func(got *listPathOptions) (*metacache, *grid.RemoteErr) {
		if got.ID != o.ID || got.Create {
			return nil, grid.NewRemoteErr(errors.New("invalid cache state lookup"))
		}
		p.mu.Lock()
		p.checks = append(p.checks, time.Now())
		n := len(p.checks)
		p.mu.Unlock()
		if p.onCheck != nil {
			p.onCheck(n)
		}
		if p.rpcFails.Load() {
			return nil, grid.NewRemoteErr(errors.New("cache state RPC failed"))
		}
		m := *p.state.Load()
		return &m, nil
	}); err != nil {
		t.Fatal(err)
	}
	client := &peerRESTClient{gridConn: func() *grid.Connection { return tg.Managers[0].Connection(tg.Hosts[1]) }}
	globalNotificationSys = &NotificationSys{peerClients: []*peerRESTClient{client}, allPeerClients: []*peerRESTClient{client}}
	return set, o, p, switchDisks
}

func putListingCachePart(t *testing.T, set *erasureObjects, o listPathOptions, part int, name string, eos bool) {
	t.Helper()
	var buf bytes.Buffer
	w := newMetacacheWriter(&buf, 0)
	if err := w.write(metaCacheEntry{name: name}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	block := metacacheBlock{n: part, First: name, Last: name, EOS: eos}
	_, err := set.putMetacacheObject(t.Context(), o.objectPath(part), mustGetPutObjReader(t, bytes.NewReader(buf.Bytes()), int64(buf.Len()), "", ""), ObjectOptions{UserDefined: block.headerKV()})
	if err != nil {
		t.Fatal(err)
	}
}

func TestStreamMetadataPartsHealthyNoStateRPC(t *testing.T) {
	set, o, p, _ := listingCachePartsBackend(t, 1)
	putListingCachePart(t, set, o, 0, "a/", false)
	putListingCachePart(t, set, o, 1, "b/", true)
	entries, err := set.streamMetadataParts(t.Context(), o)
	if !errors.Is(err, io.EOF) || entries.len() != 2 || entries.o[0].name != "a/" || entries.o[1].name != "b/" {
		t.Fatalf("healthy two-block read: entries=%v err=%v", entries.o, err)
	}
	probes, checks := p.snapshot()
	if len(probes) != 0 || len(checks) != 0 {
		t.Fatalf("healthy read added probes=%v state RPCs=%d", probes, len(checks))
	}
	t.Logf("healthy persisted blocks: state RPCs=0 retry probes=0 target ReadXL=%d", p.reads.Load())
}

func TestStreamMetadataPartsTerminalError(t *testing.T) {
	for _, part := range []int{0, 1} {
		t.Run(fmt.Sprintf("part=%d", part), func(t *testing.T) {
			set, o, p, _ := listingCachePartsBackend(t, part)
			if part > 0 {
				putListingCachePart(t, set, o, 0, "a/", false)
			}
			p.setState(o, true)
			start := time.Now()
			entries, err := set.streamMetadataParts(t.Context(), o)
			elapsed := time.Since(start)
			if err == nil || errors.Is(err, io.EOF) || !strings.Contains(err.Error(), "cache producer failed") || entries.len() != part {
				t.Fatalf("failed cache became successful: entries=%v err=%v", entries.o, err)
			}
			probes, checks := p.snapshot()
			if len(checks) != 1 || len(probes) != 0 || elapsed > time.Second {
				t.Fatalf("terminal error waited for disk retries: elapsed=%v state RPCs=%d probes=%v", elapsed, len(checks), probes)
			}
			t.Logf("terminal part=%d elapsed=%v state RPCs=%d probes=%d target ReadXL=%d", part, elapsed, len(checks), len(probes), p.reads.Load())
		})
	}
}

func TestStreamMetadataPartsLateTerminalError(t *testing.T) {
	set, o, p, _ := listingCachePartsBackend(t, 1)
	putListingCachePart(t, set, o, 0, "a/", false)
	p.onProbe = func(int) bool {
		p.setState(o, true)
		return false
	}
	start := time.Now()
	entries, err := set.streamMetadataParts(t.Context(), o)
	elapsed := time.Since(start)
	probes, checks := p.snapshot()
	if err == nil || errors.Is(err, io.EOF) || !strings.Contains(err.Error(), "cache producer failed") || entries.len() != 1 {
		t.Fatalf("late producer error lost: entries=%v err=%v", entries.o, err)
	}
	if elapsed > time.Second || len(probes) != 2 || !slices.Equal(probes, []int{0, 1}) || len(checks) != 2 {
		t.Fatalf("late error retry work: elapsed=%v probes=%v state RPCs=%d", elapsed, probes, len(checks))
	}
	if checks[1].Sub(checks[0]) < 250*time.Millisecond || p.reads.Load() > 16 {
		t.Fatalf("unbounded retry: state interval=%v target ReadXL=%d", checks[1].Sub(checks[0]), p.reads.Load())
	}
	t.Logf("late terminal elapsed=%v state RPCs=%d probes=%v target ReadXL=%d", elapsed, len(checks), probes, p.reads.Load())
}

type listingCachePartsResult struct {
	entries metaCacheEntriesSorted
	err     error
}

func startListingCacheParts(t *testing.T, set *erasureObjects, o listPathOptions) (<-chan listingCachePartsResult, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan listingCachePartsResult, 1)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		entries, err := set.streamMetadataParts(ctx, o)
		done <- listingCachePartsResult{entries: entries, err: err}
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-joined:
		case <-time.After(8 * time.Second):
			t.Error("cache continuation worker did not finish")
		}
	})
	return done, cancel
}

func receiveListingCacheParts(t *testing.T, done <-chan listingCachePartsResult) listingCachePartsResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(8 * time.Second):
		t.Fatal("cache continuation did not finish")
		return listingCachePartsResult{}
	}
}

func TestStreamMetadataPartsGeneratingThenPublished(t *testing.T) {
	set, o, p, _ := listingCachePartsBackend(t, 1)
	putListingCachePart(t, set, o, 0, "a/", false)
	checked := make(chan struct{})
	p.onCheck = func(n int) {
		if n == 1 {
			close(checked)
		}
	}
	// The first selected replica is absent even after the producer publishes
	// the block elsewhere. Rotation must reach a readable replica.
	p.onProbe = func(index int) bool { return index == 0 }
	done, _ := startListingCacheParts(t, set, o)
	select {
	case <-checked:
	case <-time.After(5 * time.Second):
		t.Fatal("missing block did not check producer state")
	}
	select {
	case result := <-done:
		t.Fatalf("still-generating cache finished early: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}
	putListingCachePart(t, set, o, 1, "b/", true)
	result := receiveListingCacheParts(t, done)
	if !errors.Is(result.err, io.EOF) || result.entries.len() != 2 || result.entries.o[0].name != "a/" || result.entries.o[1].name != "b/" {
		t.Fatalf("published second block lost: entries=%v err=%v", result.entries.o, result.err)
	}
	probes, checks := p.snapshot()
	if len(probes) < 2 || probes[0] != 0 || probes[1] != 1 {
		t.Fatalf("retry did not rotate: probes=%v checks=%d", probes, len(checks))
	}
	t.Logf("later block recovered: state RPCs=%d probes=%v target ReadXL=%d", len(checks), probes, p.reads.Load())
}

func TestStreamMetadataPartsRPCFailure(t *testing.T) {
	set, o, p, _ := listingCachePartsBackend(t, 1)
	putListingCachePart(t, set, o, 0, "a/", false)
	p.rpcFails.Store(true)
	entries, err := set.streamMetadataParts(t.Context(), o)
	if err == nil || errors.Is(err, io.EOF) || !strings.Contains(err.Error(), "cache state RPC failed") || entries.len() != 1 {
		t.Fatalf("RPC failure became successful EOF: entries=%v err=%v", entries.o, err)
	}
	probes, checks := p.snapshot()
	if len(checks) != 1 || len(probes) != 0 {
		t.Fatalf("state RPC failure continued retrying: probes=%v checks=%d", probes, len(checks))
	}
}

func TestStreamMetadataPartsMissingIndex(t *testing.T) {
	set, o, p, _ := listingCachePartsBackend(t, 0)
	putListingCachePart(t, set, o, 0, "a/", false)
	o.Marker = "b/"
	p.setState(o, true)
	entries, err := set.streamMetadataParts(t.Context(), o)
	if err == nil || errors.Is(err, io.EOF) || !strings.Contains(err.Error(), "cache producer failed") || entries.len() != 0 {
		t.Fatalf("unpublished index became successful EOF: entries=%v err=%v", entries.o, err)
	}
	probes, checks := p.snapshot()
	if len(checks) != 1 || len(probes) != 0 {
		t.Fatalf("missing index delayed terminal state: probes=%v checks=%d", probes, len(checks))
	}
}

func TestStreamMetadataPartsOtherErrorRetriesOnce(t *testing.T) {
	set, o, p, _ := listingCachePartsBackend(t, 0)
	p.readErr = errVolumeNotFound
	entries, err := set.streamMetadataParts(t.Context(), o)
	if err == nil || errors.Is(err, io.EOF) || !strings.Contains(err.Error(), "reading first part metadata:") || entries.len() != 0 {
		t.Fatalf("unexpected metadata error result: entries=%v err=%v", entries.o, err)
	}
	probes, checks := p.snapshot()
	if p.reads.Load() != 32 || len(probes) != 0 || len(checks) != 1 {
		t.Fatalf("other metadata error did not get exactly one full retry: ReadXL=%d probes=%v checks=%d", p.reads.Load(), probes, len(checks))
	}
}

func TestStreamMetadataPartsMissingBlockWorkBound(t *testing.T) {
	set, o, p, _ := listingCachePartsBackend(t, 0)
	checked := make(chan struct{})
	p.onCheck = func(n int) {
		if n == 3 {
			close(checked)
		}
	}
	start := time.Now()
	done, cancel := startListingCacheParts(t, set, o)
	select {
	case <-checked:
	case <-time.After(5 * time.Second):
		t.Fatal("missing cache stopped checking producer state")
	}
	cancel()
	result := receiveListingCacheParts(t, done)
	probes, checks := p.snapshot()
	if !errors.Is(result.err, context.Canceled) || result.entries.len() != 0 {
		t.Fatalf("generating cache became successful EOF: %+v", result)
	}
	if len(probes) != 3 || !slices.Equal(probes, []int{0, 1, 2}) || p.reads.Load() != 16 || len(checks) != 3 {
		t.Fatalf("missing block exceeded work bound: probes=%v target ReadXL=%d checks=%d", probes, p.reads.Load(), len(checks))
	}
	for i := 1; i < len(checks); i++ {
		// Receive timestamps allow a small amount of peer scheduling jitter.
		if checks[i].Sub(checks[i-1]) < 240*time.Millisecond {
			t.Fatalf("cache state queried too often: %v", checks)
		}
	}
	t.Logf("still missing for %v: state RPCs=%d probes=%v target ReadXL=%d", time.Since(start), len(checks), probes, p.reads.Load())
}

func TestStreamMetadataPartsCancelRetryWait(t *testing.T) {
	set, o, p, _ := listingCachePartsBackend(t, 1)
	putListingCachePart(t, set, o, 0, "a/", false)
	probed := make(chan struct{})
	p.onProbe = func(index int) bool {
		if index == 1 {
			close(probed)
		}
		return false
	}
	done, cancel := startListingCacheParts(t, set, o)
	select {
	case <-probed:
	case <-time.After(5 * time.Second):
		t.Fatal("retry probe did not run")
	}
	// Let the probe return and enter the regular 250 ms wait before canceling.
	time.Sleep(20 * time.Millisecond)
	start := time.Now()
	cancel()
	result := receiveListingCacheParts(t, done)
	elapsed := time.Since(start)
	if !errors.Is(result.err, context.Canceled) || elapsed > 150*time.Millisecond {
		t.Fatalf("retry wait ignored cancellation: elapsed=%v err=%v", elapsed, result.err)
	}
	t.Logf("cancel during retry wait: elapsed=%v", elapsed)
}

func TestStreamMetadataPartsChangedDisks(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprintf("empty=%v", empty), func(t *testing.T) {
			testStreamMetadataPartsChangedDisks(t, empty)
		})
	}
}

func testStreamMetadataPartsChangedDisks(t *testing.T, empty bool) {
	set, o, p, switchDisks := listingCachePartsBackend(t, 1)
	putListingCachePart(t, set, o, 0, "a/", false)
	disks := set.getDisks()
	probeDone, restore := make(chan struct{}), make(chan struct{})
	p.onProbe = func(index int) bool {
		if index == 0 {
			// A later probe must safely skip an empty/offline disk snapshot.
			if empty {
				switchDisks([]StorageAPI{})
			} else {
				switchDisks([]StorageAPI{nil})
			}
			close(probeDone)
			return true
		}
		return false
	}
	p.onCheck = func(n int) {
		if n == 2 {
			close(restore)
		}
	}
	done, _ := startListingCacheParts(t, set, o)
	select {
	case <-probeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("initial retry probe did not run")
	}
	select {
	case <-restore:
	case <-time.After(5 * time.Second):
		t.Fatal("empty disk snapshot prevented cache state checks")
	}
	switchDisks(disks)
	putListingCachePart(t, set, o, 1, "b/", true)
	result := receiveListingCacheParts(t, done)
	if !errors.Is(result.err, io.EOF) || result.entries.len() != 2 {
		t.Fatalf("changed disks did not recover: entries=%v err=%v", result.entries.o, result.err)
	}
}
