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
	"fmt"
	"maps"
	"strings"
	"testing"

	xhttp "github.com/minio/minio/internal/http"
)

// The fixture places copies directly in real erasure pools. It models a
// duplicated version; it does not claim to exercise a rebalance workflow.
func TestPoolsMetadataUpdatePreservesTags(t *testing.T) {
	z, bucket := consistencyPools(t)
	const (
		old       = "2026-09-09T09:00:00Z"
		recent    = "2026-09-09T10:00:00Z"
		timestamp = ReservedMetadataPrefixLower + TaggingTimestamp
	)
	for _, test := range []struct {
		name         string
		tags, stamps [2]string
		winner       int
		single       bool
	}{
		{name: "newer-secondary", tags: [2]string{"key=old", "key=new"}, stamps: [2]string{old, recent}, winner: 1},
		{name: "newer-primary", tags: [2]string{"key=new", "key=old"}, stamps: [2]string{recent, old}},
		{name: "empty-secondary", tags: [2]string{"key=old", ""}, stamps: [2]string{old, recent}, winner: 1},
		{name: "empty-primary", tags: [2]string{"", "key=old"}, stamps: [2]string{recent, old}},
		{name: "single-copy-primary", tags: [2]string{"key=only", ""}, stamps: [2]string{recent, ""}, single: true},
		{name: "single-copy-secondary", tags: [2]string{"", "key=only"}, stamps: [2]string{"", recent}, winner: 1, single: true},
		{name: "legacy-single-copy", tags: [2]string{"key=legacy", ""}, single: true},
		{name: "legacy-duplicates", tags: [2]string{"key=legacy", "key=legacy"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			object := test.name
			var original ObjectInfo
			for pool := range 2 {
				if test.single && pool != test.winner {
					continue
				}
				metadata := map[string]string{
					xhttp.AmzObjectTagging: test.tags[pool],
					"copy-local":           fmt.Sprint(pool),
				}
				if test.stamps[pool] != "" {
					metadata[timestamp] = test.stamps[pool]
				}
				original = putConsistencyObject(t, z, bucket, object, pool, "data", ObjectOptions{
					Versioned: true, VersionID: original.VersionID, MTime: original.ModTime, UserDefined: metadata,
				})
				got, err := z.serverPools[pool].GetObjectInfo(t.Context(), bucket, object, ObjectOptions{VersionID: original.VersionID})
				if err != nil || got.UserTags != test.tags[pool] || got.UserDefined[timestamp] != test.stamps[pool] {
					t.Fatalf("pool %d fixture: tags=%q timestamp=%q err=%v", pool, got.UserTags, got.UserDefined[timestamp], err)
				}
				if _, exists := got.UserDefined[xhttp.AmzObjectTagging]; exists {
					t.Fatal("fixture must use the cleaned ObjectInfo representation")
				}
			}
			wantTags, wantStamp := test.tags[test.winner], test.stamps[test.winner]
			called := 0
			got, err := z.PutObjectMetadata(t.Context(), bucket, object, ObjectOptions{
				VersionID: original.VersionID, MTime: original.ModTime,
				EvalMetadataFn: func(current *ObjectInfo, _ error) (ReplicateDecision, error) {
					called++
					if current.UserTags != wantTags || current.UserDefined[timestamp] != wantStamp {
						t.Errorf("callback tags=%q timestamp=%q; want %q %q", current.UserTags, current.UserDefined[timestamp], wantTags, wantStamp)
					}
					current.UserDefined["unrelated-update"] = "preserved"
					return ReplicateDecision{}, nil
				},
			})
			if err != nil || called != 1 {
				t.Fatalf("metadata update: %v, callbacks=%d", err, called)
			}
			if got.UserTags != wantTags || got.UserDefined[timestamp] != wantStamp {
				t.Errorf("response tags=%q timestamp=%q; want %q %q", got.UserTags, got.UserDefined[timestamp], wantTags, wantStamp)
			}
			for pool := range 2 {
				got, err := z.serverPools[pool].GetObjectInfo(t.Context(), bucket, object, ObjectOptions{VersionID: original.VersionID})
				if test.single && pool != test.winner {
					if !isErrVersionNotFound(err) {
						t.Errorf("metadata update created another copy: %v", err)
					}
					continue
				}
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("pool %d persisted tags=%q timestamp=%q", pool, got.UserTags, got.UserDefined[timestamp])
				if got.UserTags != wantTags || got.UserDefined[timestamp] != wantStamp {
					t.Errorf("pool %d persisted tags=%q timestamp=%q; want %q %q", pool, got.UserTags, got.UserDefined[timestamp], wantTags, wantStamp)
				}
				if got.UserDefined["unrelated-update"] != "preserved" || got.UserDefined["copy-local"] != fmt.Sprint(pool) {
					t.Errorf("pool %d lost unrelated metadata: %v", pool, got.UserDefined)
				}
			}
		})
	}
}

func TestReplicaWritesPreserveTagOrdering(t *testing.T) {
	z, bucket := consistencyPools(t)
	// Pool allocation checks the host's used-space percentage. Present only
	// its free space as fixture capacity; all reads and writes still use the
	// real disks. This keeps unrelated host disk usage out of the tag test.
	for _, pool := range z.serverPools {
		for _, set := range pool.sets {
			getDisks := set.getDisks
			disks := append([]StorageAPI(nil), getDisks()...)
			for i := range disks {
				disks[i] = tagTestCapacityDisk{StorageAPI: disks[i]}
			}
			set.getDisks = func() []StorageAPI { return disks }
			t.Cleanup(func() { set.getDisks = getDisks })
		}
	}
	const (
		old       = "2026-09-09T09:00:00Z"
		recent    = "2026-09-09T10:00:00Z"
		timestamp = ReservedMetadataPrefixLower + TaggingTimestamp
	)
	for _, path := range []struct {
		name, operation string
		direct          bool
		winner          int
	}{
		{name: "set-put", operation: "put", direct: true},
		{name: "set-multipart", operation: "multipart", direct: true},
		{name: "set-copy", operation: "copy", direct: true},
		{name: "pools-put", operation: "put", winner: 1},
		{name: "pools-multipart", operation: "multipart", winner: 1},
		{name: "pools-copy-primary", operation: "copy"},
		{name: "pools-copy-secondary", operation: "copy", winner: 1},
	} {
		for _, test := range []struct {
			name, storedTags, incomingTags, storedStamp, incomingStamp, wantTags string
		}{
			{"stored-newer", "key=stored", "key=incoming", recent, old, "key=stored"},
			{"stored-deleted", "", "key=incoming", recent, old, ""},
			{"incoming-newer", "key=stored", "key=incoming", old, recent, "key=incoming"},
			{"incoming-deleted", "key=stored", "", old, recent, ""},
		} {
			t.Run(path.name+"/"+test.name, func(t *testing.T) {
				object := path.name + "-" + test.name
				var original ObjectInfo
				for pool := range 2 {
					if path.direct && pool != 0 {
						continue
					}
					metadata := map[string]string{
						xhttp.AmzObjectTagging: "key=older-copy",
						timestamp:              "2026-09-09T08:00:00Z",
					}
					if pool == path.winner {
						metadata[xhttp.AmzObjectTagging] = test.storedTags
						metadata[timestamp] = test.storedStamp
					}
					original = putConsistencyObject(t, z, bucket, object, pool, "data", ObjectOptions{
						Versioned: true, VersionID: original.VersionID, MTime: original.ModTime, UserDefined: metadata,
					})
				}
				opts := ObjectOptions{
					Versioned: true, VersionID: original.VersionID, MTime: original.ModTime, ReplicaLockReconcile: true,
					UserDefined: map[string]string{
						xhttp.AmzObjectTagging: test.incomingTags,
						timestamp:              test.incomingStamp,
					},
				}
				var got ObjectInfo
				var err error
				switch path.operation {
				case "put":
					put := z.PutObject
					if path.direct {
						put = z.serverPools[0].PutObject
					}
					got, err = put(t.Context(), bucket, object, mustGetPutObjReader(t, strings.NewReader("data"), 4, "", ""), opts)
				case "multipart":
					// Persist the incoming tags with the upload, before completion
					// reconciles the destination version through its real resolver.
					mp, err := z.serverPools[0].NewMultipartUpload(t.Context(), bucket, object, opts)
					if err != nil {
						t.Fatal(err)
					}
					part, err := z.serverPools[0].PutObjectPart(t.Context(), bucket, object, mp.UploadID, 1,
						mustGetPutObjReader(t, strings.NewReader("data"), 4, "", ""), ObjectOptions{})
					if err != nil {
						t.Fatal(err)
					}
					complete := z.CompleteMultipartUpload
					if path.direct {
						complete = z.serverPools[0].CompleteMultipartUpload
					}
					got, err = complete(t.Context(), bucket, object, mp.UploadID, []CompletePart{{PartNumber: 1, ETag: part.ETag}}, ObjectOptions{
						Versioned: true, MTime: original.ModTime, ReplicaLockReconcile: true,
					})
					if err != nil {
						t.Fatal(err)
					}
				case "copy":
					src := original
					src.metadataOnly = true
					src.UserDefined = maps.Clone(opts.UserDefined)
					copyObject := z.CopyObject
					if path.direct {
						copyObject = z.serverPools[0].CopyObject
					}
					got, err = copyObject(t.Context(), bucket, object, bucket, object, src, ObjectOptions{VersionID: original.VersionID}, opts)
				}
				if err != nil {
					t.Fatal(err)
				}
				if got.UserTags != test.wantTags || got.UserDefined[timestamp] != recent {
					t.Errorf("response tags=%q timestamp=%q; want %q %q", got.UserTags, got.UserDefined[timestamp], test.wantTags, recent)
				}
				copies := 0
				for pool := range 2 {
					got, err := z.serverPools[pool].GetObjectInfo(t.Context(), bucket, object, ObjectOptions{VersionID: original.VersionID})
					if isErrVersionNotFound(err) {
						continue
					}
					if err != nil {
						t.Fatal(err)
					}
					copies++
					if got.UserTags != test.wantTags || got.UserDefined[timestamp] != recent {
						t.Errorf("pool %d persisted tags=%q timestamp=%q; want %q %q", pool, got.UserTags, got.UserDefined[timestamp], test.wantTags, recent)
					}
				}
				if copies != 1 {
					t.Errorf("replacement left %d copies; want 1", copies)
				}
			})
		}
	}
}

type tagTestCapacityDisk struct{ StorageAPI }

func (d tagTestCapacityDisk) DiskInfo(ctx context.Context, opts DiskInfoOptions) (DiskInfo, error) {
	info, err := d.StorageAPI.DiskInfo(ctx, opts)
	info.Total, info.Used = info.Free, 0
	return info, err
}

func TestMergedPoolObjectInfoTagOrdering(t *testing.T) {
	const (
		old       = "2026-09-09T09:00:00Z"
		recent    = "2026-09-09T10:00:00Z"
		timestamp = ReservedMetadataPrefixLower + TaggingTimestamp
	)
	for _, test := range []struct {
		name, firstStamp, secondStamp, secondTags string
		winner                                    int
	}{
		{"newer", old, recent, "key=second", 1},
		{"newer-removal", old, recent, "", 1},
		{"equal", recent, recent, "key=second", 0},
		{"unordered", "", "", "key=second", 0},
		{"missing-first", "", recent, "key=second", 1},
		{"missing-second", recent, "", "key=second", 0},
		{"invalid-first", "invalid", recent, "key=second", 1},
		{"invalid-second", recent, "invalid", "key=second", 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			tagValues := []string{"key=first", test.secondTags}
			stamps := []string{test.firstStamp, test.secondStamp}
			copies := make([]PoolObjInfo, 2)
			before := make([]map[string]string, 2)
			for i := range copies {
				fi := FileInfo{Metadata: map[string]string{xhttp.AmzObjectTagging: tagValues[i]}}
				if stamps[i] != "" {
					fi.Metadata[timestamp] = stamps[i]
				}
				copies[i] = PoolObjInfo{Index: i, ObjInfo: fi.ToObjectInfo("bucket", "object", true)}
				before[i] = maps.Clone(copies[i].ObjInfo.UserDefined)
			}
			got := mergedPoolObjectInfo(copies)
			if got.UserTags != tagValues[test.winner] || got.UserDefined[timestamp] != stamps[test.winner] {
				t.Errorf("merged tags=%q timestamp=%q; want %q %q", got.UserTags, got.UserDefined[timestamp], tagValues[test.winner], stamps[test.winner])
			}
			if _, exists := got.UserDefined[xhttp.AmzObjectTagging]; exists {
				t.Error("merged ObjectInfo leaked the raw tagging key into UserDefined")
			}
			for i := range copies {
				if !maps.Equal(copies[i].ObjInfo.UserDefined, before[i]) || copies[i].ObjInfo.UserTags != tagValues[i] {
					t.Errorf("merge mutated input copy %d", i)
				}
			}
		})
	}
}

func TestPoolsMetadataCallbackReplacesTags(t *testing.T) {
	z, bucket := consistencyPools(t)
	const timestamp = ReservedMetadataPrefixLower + TaggingTimestamp
	for _, test := range []struct{ name, tags string }{
		{"replace", "key=callback"},
		{"remove", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			var original ObjectInfo
			for pool := range 2 {
				original = putConsistencyObject(t, z, bucket, test.name, pool, "data", ObjectOptions{
					Versioned: true, VersionID: original.VersionID, MTime: original.ModTime,
					UserDefined: map[string]string{
						xhttp.AmzObjectTagging: []string{"key=old", "key=new"}[pool],
						timestamp:              []string{"2026-09-09T09:00:00Z", "2026-09-09T10:00:00Z"}[pool],
					},
				})
			}
			const updatedStamp = "2026-09-09T11:00:00Z"
			got, err := z.PutObjectMetadata(t.Context(), bucket, test.name, ObjectOptions{
				VersionID: original.VersionID, MTime: original.ModTime,
				EvalMetadataFn: func(current *ObjectInfo, _ error) (ReplicateDecision, error) {
					if current.UserTags != "key=new" {
						t.Errorf("callback read tags=%q; want key=new", current.UserTags)
					}
					if _, exists := current.UserDefined[xhttp.AmzObjectTagging]; exists {
						t.Error("callback received the raw tagging key")
					}
					current.UserDefined[xhttp.AmzObjectTagging] = test.tags
					current.UserDefined[timestamp] = updatedStamp
					return ReplicateDecision{}, nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if got.UserTags != test.tags || got.UserDefined[timestamp] != updatedStamp {
				t.Errorf("callback update response tags=%q timestamp=%q", got.UserTags, got.UserDefined[timestamp])
			}
			for pool := range 2 {
				got, err := z.serverPools[pool].GetObjectInfo(t.Context(), bucket, test.name, ObjectOptions{VersionID: original.VersionID})
				if err != nil || got.UserTags != test.tags || got.UserDefined[timestamp] != updatedStamp {
					t.Errorf("pool %d did not persist callback tags: tags=%q timestamp=%q err=%v", pool, got.UserTags, got.UserDefined[timestamp], err)
				}
			}
		})
	}
}

func TestReconcileStoredObjectTagOrdering(t *testing.T) {
	const (
		old       = "2026-09-09T09:00:00Z"
		recent    = "2026-09-09T10:00:00Z"
		timestamp = ReservedMetadataPrefixLower + TaggingTimestamp
	)
	for _, test := range []struct {
		name, storedStamp, incomingStamp, storedTags string
		wantStored                                   bool
	}{
		{"stored-newer", recent, old, "key=stored", true},
		{"incoming-newer", old, recent, "key=stored", false},
		{"equal", recent, recent, "key=stored", true},
		{"equal-removal", recent, recent, "", true},
		{"missing-stored", "", recent, "key=stored", false},
		{"missing-incoming", recent, "", "key=stored", true},
		{"invalid-stored", "invalid", recent, "key=stored", false},
		{"invalid-incoming", recent, "invalid", "key=stored", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			metadata := map[string]string{
				xhttp.AmzObjectTagging: "key=incoming",
				timestamp:              test.incomingStamp,
				"unrelated":            "preserved",
			}
			reconcileStoredObjectTags(metadata, test.storedTags, test.storedStamp)
			wantTags, wantStamp := "key=incoming", test.incomingStamp
			if test.wantStored {
				wantTags, wantStamp = test.storedTags, test.storedStamp
			}
			if metadata[xhttp.AmzObjectTagging] != wantTags || metadata[timestamp] != wantStamp || metadata["unrelated"] != "preserved" {
				t.Errorf("reconciled metadata=%v; want tags=%q timestamp=%q and unrelated field preserved", metadata, wantTags, wantStamp)
			}
		})
	}
}

func TestPoolsMetadataUpdatePreservesAbsentTags(t *testing.T) {
	z, bucket := consistencyPools(t)
	const (
		old       = "2026-09-09T09:00:00Z"
		recent    = "2026-09-09T10:00:00Z"
		timestamp = ReservedMetadataPrefixLower + TaggingTimestamp
	)
	for _, removal := range []bool{false, true} {
		t.Run(fmt.Sprintf("timestamp-only-removal=%t", removal), func(t *testing.T) {
			object := fmt.Sprintf("absent-tags-%t", removal)
			var original ObjectInfo
			checkStored := func(pool int, wantKey bool, wantTags, wantStamp string) {
				t.Helper()
				infos, errs := readAllFileInfo(t.Context(), z.serverPools[pool].getHashedSet(object).getDisks(), "", bucket, object, original.VersionID, false, false)
				for disk, info := range infos {
					if errs[disk] != nil {
						t.Fatal(errs[disk])
					}
					tags, exists := info.Metadata[xhttp.AmzObjectTagging]
					if exists != wantKey || tags != wantTags || info.Metadata[timestamp] != wantStamp {
						t.Errorf("pool %d disk %d raw tagging key=%t value=%q stamp=%q; want %t %q %q", pool, disk, exists, tags, info.Metadata[timestamp], wantKey, wantTags, wantStamp)
					}
				}
			}
			for pool := range 2 {
				metadata := map[string]string{}
				if removal {
					metadata[timestamp] = recent
					if pool == 1 {
						metadata[xhttp.AmzObjectTagging] = "key=old"
						metadata[timestamp] = old
					}
				}
				original = putConsistencyObject(t, z, bucket, object, pool, "data", ObjectOptions{
					Versioned: true, VersionID: original.VersionID, MTime: original.ModTime, UserDefined: metadata,
				})
				checkStored(pool, removal && pool == 1, metadata[xhttp.AmzObjectTagging], metadata[timestamp])
			}
			_, err := z.PutObjectMetadata(t.Context(), bucket, object, ObjectOptions{
				VersionID: original.VersionID, MTime: original.ModTime,
				EvalMetadataFn: func(current *ObjectInfo, _ error) (ReplicateDecision, error) {
					current.UserDefined["unrelated-update"] = "preserved"
					return ReplicateDecision{}, nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			wantStamp := ""
			if removal {
				wantStamp = recent
			}
			for pool := range 2 {
				// A previously non-empty key needs an explicit empty value to
				// propagate deletion; an absent key should remain absent.
				checkStored(pool, removal && pool == 1, "", wantStamp)
			}
		})
	}
}
