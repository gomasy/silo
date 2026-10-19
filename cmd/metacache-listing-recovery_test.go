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
	"errors"
	"fmt"
	"io"
	"slices"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

type listingRecoveryDisk struct {
	StorageAPI
	reads        *atomic.Int64
	err          error
	emptySuccess bool
}

func (d *listingRecoveryDisk) ReadXL(ctx context.Context, bucket, object string, data bool) (RawFileInfo, error) {
	if d.reads != nil {
		d.reads.Add(1)
	}
	if d.emptySuccess {
		return RawFileInfo{}, nil
	}
	if d.err != nil {
		return RawFileInfo{}, d.err
	}
	return d.StorageAPI.ReadXL(ctx, bucket, object, data)
}

func TestListingRecoveryStrictAbsence(t *testing.T) {
	z, bucket, _ := nullQuorumBackend(t)
	set := z.serverPools[0].sets[0]
	disks, switchDisks := installListingDiskSwitch(set)
	for _, tc := range []struct {
		name                   string
		err                    error
		offline, empty, absent bool
	}{
		{name: "all-native-not-found", absent: true},
		{name: "offline", offline: true},
		{name: "volume-not-found", err: errVolumeNotFound},
		{name: "version-not-found", err: errFileVersionNotFound},
		{name: "corrupt", err: errFileCorrupt},
		{name: "decode-generated-not-found", empty: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			next := slices.Clone(disks)
			next[0] = &listingRecoveryDisk{StorageAPI: disks[0], err: tc.err, emptySuccess: tc.empty}
			if tc.offline {
				next[0] = &offlineNullQuorumDisk{StorageAPI: disks[0]}
			}
			switchDisks(next)
			_, absent, err := set.readListingFileInfo(t.Context(), bucket, "missing")
			if absent != tc.absent || (tc.absent && err != nil) || (!tc.absent && !isErrReadQuorum(err)) {
				t.Fatalf("absent=%v err=%v, want absent=%v", absent, err, tc.absent)
			}
		})
	}
	switchDisks(disks)
}

// The target walk can predate the copy. Only current cross-pool reads can
// distinguish this copy-before-delete schedule from a genuine object deletion.
func TestListingRecoveryCurrentPools(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		target, marker, unknown, stale bool
	}{
		{name: "copied-before-source-delete", target: true},
		{name: "deleted-with-stale-other-walk", stale: true},
		{name: "current-marker-over-stale-live", target: true, marker: true, stale: true},
		{name: "missing-source-unknown-target", unknown: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			z, bucket, _ := listingPoolsBackend(t)
			target, source := z.serverPools[0].sets[0], z.serverPools[1].sets[0]
			targetDisks, switchTarget := installListingDiskSwitch(target)
			sourceDisks, switchSource := installListingDiskSwitch(source)
			const object = "moved"
			body := bytes.Repeat([]byte{'a'}, 8192)
			mtime := time.Now().UTC().Add(-time.Hour)
			put := func(set *erasureObjects) {
				_, err := set.PutObject(t.Context(), bucket, object, mustGetPutObjReader(t, bytes.NewReader(body), int64(len(body)), "", ""), ObjectOptions{MTime: mtime, VersionSuspended: true})
				if err != nil {
					t.Fatal(err)
				}
			}
			put(source)
			var stale *metaCacheEntry
			if tc.stale || tc.target {
				put(target)
				stale = &metaCacheEntry{name: object, metadata: mustReadNullQuorumMeta(t, targetDisks[0], bucket, object)}
			}
			if tc.stale && !tc.target {
				if _, err := target.DeleteObject(t.Context(), bucket, object, ObjectOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			if tc.marker {
				if _, err := target.DeleteObject(t.Context(), bucket, object, ObjectOptions{VersionSuspended: true, DeleteMarker: true, MTime: mtime.Add(time.Minute)}); err != nil {
					t.Fatal(err)
				}
			}
			// Actual copy is committed before actual source cleanup.
			if _, err := source.DeleteObject(t.Context(), bucket, object, ObjectOptions{}); err != nil {
				t.Fatal(err)
			}
			var sourceReads, targetReads atomic.Int64
			nextSource, nextTarget := slices.Clone(sourceDisks), slices.Clone(targetDisks)
			for i := range nextSource {
				nextSource[i] = &listingRecoveryDisk{StorageAPI: sourceDisks[i], reads: &sourceReads}
			}
			for i := range nextTarget {
				nextTarget[i] = &listingRecoveryDisk{StorageAPI: targetDisks[i], reads: &targetReads}
			}
			if tc.unknown {
				nextTarget[0] = &listingRecoveryDisk{StorageAPI: targetDisks[0], reads: &targetReads, err: errDiskNotFound}
			}
			switchSource(nextSource)
			switchTarget(nextTarget)
			group := []*metaCacheEntry{{name: object, listingSet: source}}
			if stale != nil && tc.stale {
				group = append(group, stale)
			}
			entry, err := z.resolveListingGroup(t.Context(), bucket, group)
			if sourceReads.Load() != int64(len(sourceDisks)) || targetReads.Load() != int64(len(targetDisks)) {
				t.Fatalf("reads source=%d target=%d", sourceReads.Load(), targetReads.Load())
			}
			if tc.unknown {
				if entry != nil || !isErrReadQuorum(err) {
					t.Fatalf("unknown became success: entry=%v err=%v", entry, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !tc.target {
				if entry != nil {
					t.Fatalf("stale walk survived current absence: %+v", entry)
				}
				return
			}
			if entry == nil || entry.listingSet != nil {
				t.Fatalf("missing current result: %+v", entry)
			}
			fi, err := entry.fileInfo(bucket)
			if err != nil || fi.Deleted != tc.marker {
				t.Fatalf("current result deleted=%v err=%v, want marker=%v", fi.Deleted, err, tc.marker)
			}
		})
	}
}

func TestListingRecoveryNormalPathDoesNotReadOrLock(t *testing.T) {
	z, bucket, _ := listingPoolsBackend(t)
	const object = "normal"
	set := z.serverPools[1].sets[0]
	body := []byte("normal data")
	if _, err := set.PutObject(t.Context(), bucket, object, mustGetPutObjReader(t, bytes.NewReader(body), int64(len(body)), "", ""), ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	var reads atomic.Int64
	for _, pool := range z.serverPools {
		for _, set := range pool.sets {
			disks, switchDisks := installListingDiskSwitch(set)
			wrapped := slices.Clone(disks)
			for i := range wrapped {
				wrapped[i] = &listingRecoveryDisk{StorageAPI: disks[i], reads: &reads}
			}
			switchDisks(wrapped)
		}
	}
	writer := z.NewNSLock(bucket, object)
	lkctx, err := writer.GetLock(t.Context(), globalOperationTimeout)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Unlock(lkctx)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	out := make(chan metaCacheEntry, 10)
	err = z.listMerged(ctx, listPathOptions{Bucket: bucket, Recursive: true, Limit: 10}, out)
	if err != nil && err != io.EOF {
		t.Fatalf("normal listing waited on writer or failed: %v", err)
	}
	var names []string
	for entry := range out {
		names = append(names, entry.name)
		if entry.listingSet != nil {
			t.Fatal("pending escaped")
		}
	}
	if !slices.Equal(names, []string{object}) || reads.Load() != 0 {
		t.Fatalf("normal names=%v additional ReadXL=%d", names, reads.Load())
	}
}

func TestListingPendingDirectoryCollision(t *testing.T) {
	for _, objectFirst := range []bool{false, true} {
		for _, present := range []bool{true, false} {
			t.Run(fmt.Sprintf("objectFirst=%v/present=%v", objectFirst, present), func(t *testing.T) {
				in := []chan metaCacheEntry{make(chan metaCacheEntry, 2), make(chan metaCacheEntry, 2), make(chan metaCacheEntry, 2)}
				objectIndex := 1
				if objectFirst {
					objectIndex = 0
				}
				in[objectIndex] <- metaCacheEntry{name: "dir/", listingSet: &erasureObjects{}}
				in[1-objectIndex] <- metaCacheEntry{name: "dir/"}
				in[2] <- metaCacheEntry{name: "a", metadata: []byte{1}}
				for _, c := range in {
					close(c)
				}
				out := make(chan metaCacheEntry, 4)
				calls := 0
				err := mergeEntryChannelsWithResolver(t.Context(), in, out, 1, nil, func(group []*metaCacheEntry) (*metaCacheEntry, error) {
					calls++
					if len(group) != 1 || group[0].listingSet == nil {
						t.Fatalf("wrong pending group: %+v", group)
					}
					if !present {
						return nil, nil
					}
					return &metaCacheEntry{name: "dir/", metadata: []byte{1}}, nil
				})
				if err != nil || calls != 1 || len(out) != 2 {
					t.Fatalf("err=%v recoveries=%d outputs=%d", err, calls, len(out))
				}
				first := <-out
				if first.name != "a" {
					t.Fatalf("expected smaller key first, got %+v", first)
				}
				entry := <-out
				if entry.name != "dir/" || entry.listingSet != nil {
					t.Fatalf("unexpected recovered entry: %+v", entry)
				}
				if present && (entry.isDir() || !entry.isObjectDir()) {
					t.Fatalf("object became directory: %+v", entry)
				}
				if !present && (!entry.isDir() || entry.name != "dir/") {
					t.Fatalf("absent pending object dropped live directory prefix: %+v", entry)
				}
			})
		}
	}
}

func TestListingRecoveryRejectsUnexpectedSinglePoolSet(t *testing.T) {
	sets := []*erasureObjects{{}, {}}
	pool := &erasureSets{sets: sets, distributionAlgo: formatErasureVersionV3DistributionAlgoV3}
	z := &erasureServerPools{serverPools: []*erasureSets{pool}}
	for _, object := range []string{"key", "directory/"} {
		target := pool.getHashedSet(encodeDirObject(object))
		wrong := sets[0]
		if wrong == target {
			wrong = sets[1]
		}
		entry, err := z.resolveListingGroup(t.Context(), "bucket", []*metaCacheEntry{{name: object, listingSet: wrong}})
		if entry != nil || !isErrReadQuorum(err) {
			t.Fatalf("unexpected set became success: %v %v", entry, err)
		}
	}
}

func TestListingRecoveryReleasesBeforeOutput(t *testing.T) {
	z, bucket, _ := nullQuorumBackend(t)
	const object = "object/"
	nullQuorumPut(t, z, bucket, object, []byte("data"), ObjectOptions{})
	set := z.serverPools[0].sets[0]
	input := make(chan metaCacheEntry, 1)
	input <- metaCacheEntry{name: object, listingSet: set}
	close(input)
	out := make(chan metaCacheEntry)
	resolved := make(chan struct{})
	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	go func() {
		done <- mergeEntryChannelsWithResolver(ctx, []chan metaCacheEntry{input}, out, 1, nil, func(group []*metaCacheEntry) (*metaCacheEntry, error) {
			entry, err := z.resolveListingGroup(ctx, bucket, group)
			close(resolved)
			return entry, err
		})
	}()
	select {
	case <-resolved:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	lock := set.NewNSLock(bucket, encodeDirObject(object))
	lkctx, err := lock.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		t.Fatalf("writer blocked by output backpressure: %v", err)
	}
	lock.Unlock(lkctx)
	entry := <-out
	if entry.name != object || !entry.isObjectDir() || entry.isDir() || entry.listingSet != nil {
		t.Fatalf("directory object identity lost: %+v", entry)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestListingRecoveryTerminalErrorPrecedesLock(t *testing.T) {
	for _, n := range []int{1, 2} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			inputs := make([]chan metaCacheEntry, n)
			for i := range inputs {
				inputs[i] = make(chan metaCacheEntry, 1)
				inputs[i] <- metaCacheEntry{name: "key", listingSet: &erasureObjects{}}
				close(inputs[i])
			}
			out := make(chan metaCacheEntry, 1)
			want := errors.New("walk failed")
			called := false
			err := mergeEntryChannelsWithResolver(t.Context(), inputs, out, 1, func(int) error { return want }, func([]*metaCacheEntry) (*metaCacheEntry, error) { called = true; return nil, nil })
			if !errors.Is(err, want) || called || len(out) != 0 {
				t.Fatalf("err=%v recovery=%v output=%d", err, called, len(out))
			}
		})
	}
}

func TestListingPendingCannotBeSerialized(t *testing.T) {
	entry := metaCacheEntry{name: "directory/", listingSet: &erasureObjects{}}
	if entry.isDir() || !entry.isObjectDir() {
		t.Fatal("pending object became a directory")
	}
	var buf bytes.Buffer
	w := newMetacacheWriter(&buf, 0)
	if err := w.write(entry); err == nil {
		t.Fatal("pending entry serialized")
	}
	_ = w.Close()
	buf.Reset()
	w = newMetacacheWriter(&buf, 0)
	stream, err := w.stream()
	if err != nil {
		t.Fatal(err)
	}
	stream <- entry
	close(stream)
	if err = w.Close(); err == nil {
		t.Fatal("stream serialized pending entry")
	}
}

func TestListingEntryRepresentationCost(t *testing.T) {
	type before struct {
		_ string    // name
		_ []byte    // metadata
		_ *xlMetaV2 // cached
		_ bool      // reusable
	}
	t.Logf("metaCacheEntry before=%d after=%d bytes", unsafe.Sizeof(before{}), unsafe.Sizeof(metaCacheEntry{}))
}

func BenchmarkListingMergeNormal(b *testing.B) {
	for _, n := range []int{1, 2} {
		for _, resolver := range []bool{false, true} {
			b.Run(fmt.Sprintf("inputs=%d/resolver=%v", n, resolver), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					inputs := make([]chan metaCacheEntry, n)
					for i := range inputs {
						inputs[i] = make(chan metaCacheEntry, 100)
						for j := range 100 {
							inputs[i] <- metaCacheEntry{name: fmt.Sprintf("key-%03d-%d/", j, i)}
						}
						close(inputs[i])
					}
					out := make(chan metaCacheEntry, n*100)
					var resolveGroup func([]*metaCacheEntry) (*metaCacheEntry, error)
					if resolver {
						resolveGroup = func([]*metaCacheEntry) (*metaCacheEntry, error) { return nil, io.ErrUnexpectedEOF }
					}
					if err := mergeEntryChannelsWithResolver(context.Background(), inputs, out, 1, nil, resolveGroup); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
