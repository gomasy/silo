// Copyright (c) 2026 Ruohang Feng
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

type multipartRootScanGuard struct {
	StorageAPI
	roots *atomic.Int64
}

func (d multipartRootScanGuard) ListDir(ctx context.Context, original, volume, dir string, count int) ([]string, error) {
	if volume == minioMetaMultipartBucket && dir == "" {
		d.roots.Add(1)
		return nil, errFaultyDisk
	}
	return d.StorageAPI.ListDir(ctx, original, volume, dir, count)
}

// A busy or unreadable diagnostic namespace must not become a prerequisite for
// ordinary listing, including the lower-level exact-object entry points.
func TestMultipartPublicListingIndependentOfDiagnostics(t *testing.T) {
	for _, busy := range []bool{false, true} {
		t.Run(map[bool]string{false: "unreadable-root", true: "busy-diagnostics"}[busy], func(t *testing.T) {
			z, _ := consistencyPools(t)
			bucket, router, err := initAPIHandlerTest(t.Context(), z, []string{"ListMultipartUploads"}, MakeBucketOptions{})
			if err != nil {
				t.Fatal(err)
			}
			const key = "dir/known"
			mp, err := z.NewMultipartUpload(t.Context(), bucket, key, ObjectOptions{})
			if err != nil {
				t.Fatal(err)
			}
			var roots atomic.Int64
			for _, pool := range z.serverPools {
				for _, set := range pool.sets {
					original := set.getDisks
					set.getDisks = func() []StorageAPI {
						disks := append([]StorageAPI(nil), original()...)
						for i, disk := range disks {
							if disk != nil {
								disks[i] = multipartRootScanGuard{disk, &roots}
							}
						}
						return disks
					}
					t.Cleanup(func() { set.getDisks = original })
				}
			}
			if busy {
				for range cap(multipartScanSlots) {
					scan, err := startMultipartScan(t.Context(), true)
					if err != nil {
						t.Fatal(err)
					}
					defer scan.close()
				}
			}
			for _, prefix := range []string{"", key, "dir/"} {
				req, err := newTestSignedRequestV4(http.MethodGet,
					getListMultipartUploadsURLWithParams("", bucket, prefix, "", "", "", "1"),
					0, nil, globalActiveCred.AccessKey, globalActiveCred.SecretKey, nil)
				if err != nil {
					t.Fatal(err)
				}
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, req)
				if rec.Code != http.StatusOK {
					t.Fatalf("prefix %q: HTTP %d %s", prefix, rec.Code, rec.Body.String())
				}
				var got ListMultipartUploadsResponse
				if err := xml.Unmarshal(rec.Body.Bytes(), &got); err != nil {
					t.Fatal(err)
				}
				if prefix == "dir/" {
					if len(got.Uploads) != 0 {
						t.Fatalf("partial prefix must retain the documented exact-key limitation: %+v", got)
					}
				} else if len(got.Uploads) != 1 || got.Uploads[0].UploadID != mp.UploadID {
					t.Fatalf("known upload missing: %+v", got)
				}
			}
			for _, pool := range z.serverPools {
				for _, list := range []func(context.Context, string, string, string, string, string, int) (ListMultipartsInfo, error){
					pool.ListMultipartUploads, pool.getHashedSet(key).ListMultipartUploads,
				} {
					if _, err := list(t.Context(), bucket, key, "", "", "", 10); err != nil {
						t.Fatalf("lower-level exact listing: %v", err)
					}
				}
			}
			if roots.Load() != 0 {
				t.Fatalf("public listing attempted %d global multipart scans", roots.Load())
			}
		})
	}
}
