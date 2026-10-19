// Copyright (c) 2026 Ruohang Feng
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
)

// Run explicitly with -benchtime=1x. Fixture creation is excluded from timing;
// each request still discovers real on-disk metadata. The 16-drive/10k case
// deliberately exceeds the existing aggregate directory-entry budget.
func BenchmarkMultipartDiagnosticScanCapacity(b *testing.B) {
	for _, tc := range []struct {
		drives, uploads int
		mixed, limited  bool
	}{
		{4, 1_000, false, false},
		{4, 10_000, false, false},
		{4, 10_000, true, false},
		{16, 1_000, false, false},
		{16, 10_000, false, true},
	} {
		b.Run(fmt.Sprintf("drives=%d/uploads=%d/mixed=%t", tc.drives, tc.uploads, tc.mixed), func(b *testing.B) {
			obj, dirs, err := prepareErasure(b.Context(), tc.drives)
			if err != nil {
				b.Fatal(err)
			}
			z := obj.(*erasureServerPools)
			b.Cleanup(func() { z.Shutdown(context.Background()); removeRoots(dirs) })
			const bucket, other = "multipart-capacity", "multipart-unrelated"
			for _, name := range []string{bucket, other} {
				if err := z.MakeBucket(b.Context(), name, MakeBucketOptions{}); err != nil {
					b.Fatal(err)
				}
			}
			for n := range tc.uploads {
				target := bucket
				if tc.mixed && n%2 != 0 {
					target = other
				}
				key := fmt.Sprintf("dir/%05d", n)
				mp, err := z.NewMultipartUpload(b.Context(), target, key, ObjectOptions{})
				if err != nil {
					b.Fatal(err)
				}
				if tc.mixed && n%10 == 0 {
					rewriteMultipartIdentity(b, z.serverPools[0].getHashedSet(key), target, key, mp.UploadID, removeMultipartIdentity)
				}
			}
			z.mpCache.Clear()
			var directoryCalls, metadataCalls atomic.Int64
			for _, set := range z.serverPools[0].sets {
				original := set.getDisks
				set.getDisks = func() []StorageAPI {
					disks := append([]StorageAPI(nil), original()...)
					for i, disk := range disks {
						disks[i] = multipartListingCountingDisk{disk, &directoryCalls, &metadataCalls}
					}
					return disks
				}
				b.Cleanup(func() { set.getDisks = original })
			}
			b.ResetTimer()
			for range b.N {
				got, err := z.scanMultipartUploadsForTest(b.Context(), bucket, "dir/", "", "", "", 1000)
				if tc.limited {
					var limited SlowDown
					if !errors.As(err, &limited) || len(got.Uploads) != 0 {
						b.Fatalf("overflow must return SlowDown without a partial page: %+v %v", got, err)
					}
				} else if err != nil || len(got.Uploads) != 1000 {
					b.Fatalf("capacity listing: count=%d err=%v", len(got.Uploads), err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(directoryCalls.Load())/float64(b.N), "ListDir/op")
			b.ReportMetric(float64(metadataCalls.Load())/float64(b.N), "ReadVersion/op")
		})
	}
}

// The public path is measured with a populated cache as well as after cache
// loss. An empty post-restart response alone is not capacity acceptance.
func BenchmarkMultipartPublicListingCapacity(b *testing.B) {
	for _, tc := range []struct {
		drives, uploads int
		sameKey         bool
	}{
		{4, 10_000, false},
		{16, 3_126, false},
		{16, 10_000, true},
	} {
		b.Run(fmt.Sprintf("drives=%d/uploads=%d/same-key=%t", tc.drives, tc.uploads, tc.sameKey), func(b *testing.B) {
			obj, dirs, err := prepareErasure(b.Context(), tc.drives)
			if err != nil {
				b.Fatal(err)
			}
			z := obj.(*erasureServerPools)
			b.Cleanup(func() { z.Shutdown(context.Background()); removeRoots(dirs) })
			const bucket, other = "multipart-public-capacity", "multipart-public-other"
			for _, name := range []string{bucket, other} {
				if err := z.MakeBucket(b.Context(), name, MakeBucketOptions{}); err != nil {
					b.Fatal(err)
				}
			}
			wantCached, wantExact := 0, 1
			for n := range tc.uploads {
				target, key := bucket, fmt.Sprintf("dir/%05d", n)
				if tc.sameKey {
					key = "dir/00000"
				} else if n%2 != 0 {
					target = other
				}
				if target == bucket {
					wantCached++
				}
				if _, err := z.NewMultipartUpload(b.Context(), target, key, ObjectOptions{}); err != nil {
					b.Fatal(err)
				}
			}
			if tc.sameKey {
				wantExact = 1000
			}
			if tc.drives == 16 && !tc.sameKey {
				_, err := z.scanMultipartUploadsForTest(b.Context(), bucket, "", "", "", "", 1000)
				var limited SlowDown
				if !errors.As(err, &limited) {
					b.Fatalf("fixture did not reproduce the previous global budget failure: %v", err)
				}
			}
			var directoryCalls, metadataCalls atomic.Int64
			for _, set := range z.serverPools[0].sets {
				original := set.getDisks
				set.getDisks = func() []StorageAPI {
					disks := append([]StorageAPI(nil), original()...)
					for i, disk := range disks {
						disks[i] = multipartListingCountingDisk{disk, &directoryCalls, &metadataCalls}
					}
					return disks
				}
				b.Cleanup(func() { set.getDisks = original })
			}
			measure := func(name, prefix string, want int) {
				b.Run(name, func(b *testing.B) {
					directoryCalls.Store(0)
					metadataCalls.Store(0)
					b.ResetTimer()
					for range b.N {
						got, err := z.ListMultipartUploads(b.Context(), bucket, prefix, "", "", "", 1000)
						if err != nil || len(got.Uploads) != want {
							b.Fatalf("public listing: count=%d want=%d err=%v", len(got.Uploads), want, err)
						}
					}
					b.StopTimer()
					b.ReportMetric(float64(directoryCalls.Load())/float64(b.N), "ListDir/op")
					b.ReportMetric(float64(metadataCalls.Load())/float64(b.N), "ReadVersion/op")
				})
			}
			measure("populated-cache", "", wantCached)
			z.mpCache.Clear()
			measure("lost-cache", "", 0)
			measure("known-key", "dir/00000", wantExact)
		})
	}
}
