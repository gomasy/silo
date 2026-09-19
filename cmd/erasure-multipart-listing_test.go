// Copyright (c) 2026 Ruohang Feng
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio/internal/config/storageclass"
)

// Check the real handler and storage path, including a successful abort between
// pages. Choose IDs increasing in both initiation time and lexical order so the
// failure does not depend on differing interpretations of S3 marker ordering.
func TestMultipartListingAbortBetweenHTTPPages(t *testing.T) {
	z, _ := consistencyPools(t)
	bucket, router, err := initAPIHandlerTest(t.Context(), z, []string{"ListMultipartUploads", "AbortMultipart"}, MakeBucketOptions{})
	if err != nil {
		t.Fatal(err)
	}
	setMultipartListingTestMode(t, false)
	var firstID, secondID string
	for attempt := 0; attempt < 32; attempt++ {
		one, err := z.NewMultipartUpload(t.Context(), bucket, "a", ObjectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		two, err := z.NewMultipartUpload(t.Context(), bucket, "a", ObjectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if one.UploadID < two.UploadID {
			firstID, secondID = one.UploadID, two.UploadID
			break
		}
		for _, id := range []string{one.UploadID, two.UploadID} {
			if err := z.AbortMultipartUpload(t.Context(), bucket, "a", id, ObjectOptions{}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if firstID == "" {
		t.Fatal("could not construct increasing upload IDs")
	}
	if _, err := z.NewMultipartUpload(t.Context(), bucket, "b", ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	request := func(method, u string) *httptest.ResponseRecorder {
		t.Helper()
		req, err := newTestSignedRequestV4(method, u, 0, nil, globalActiveCred.AccessKey, globalActiveCred.SecretKey, nil)
		if err != nil {
			t.Fatal(err)
		}
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}
	list := func(keyMarker, uploadMarker string, limit int) ListMultipartUploadsResponse {
		t.Helper()
		rec := request(http.MethodGet, getListMultipartUploadsURLWithParams("", bucket, "", keyMarker, uploadMarker, "", strconv.Itoa(limit)))
		if rec.Code != http.StatusOK {
			t.Fatalf("list HTTP %d: %s", rec.Code, rec.Body.String())
		}
		var result ListMultipartUploadsResponse
		if err := xml.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	first := list("", "", 1)
	if len(first.Uploads) != 1 || first.Uploads[0].UploadID != firstID || !first.IsTruncated {
		t.Fatalf("unexpected first page: %+v", first)
	}
	rec := request(http.MethodDelete, getAbortMultipartUploadURL("", bucket, "a", firstID))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("abort HTTP %d: %s", rec.Code, rec.Body.String())
	}
	rest := list(first.NextKeyMarker, first.NextUploadIDMarker, 10)
	var keys []string
	for _, upload := range rest.Uploads {
		keys = append(keys, upload.Key)
	}
	t.Logf("GET first page=200; DELETE marker=204; GET next page=200 keys=%v truncated=%v", keys, rest.IsTruncated)
	if _, err := z.GetMultipartInfo(t.Context(), bucket, "a", secondID, ObjectOptions{}); err != nil {
		t.Fatalf("remaining upload is not valid: %v", err)
	}
	if len(rest.Uploads) != 2 || rest.Uploads[0].UploadID != secondID {
		t.Fatalf("valid remaining upload for key a is missing from continuation: %+v", rest.Uploads)
	}
}

type multipartListingFaultDisk struct {
	StorageAPI
	read   func(context.Context, string, string, string, string, ReadOptions) (FileInfo, error)
	delete func(context.Context, string, string, DeleteOptions) error
}

func (d multipartListingFaultDisk) ReadVersion(ctx context.Context, original, volume, object, version string, opts ReadOptions) (FileInfo, error) {
	if d.read != nil {
		return d.read(ctx, original, volume, object, version, opts)
	}
	return d.StorageAPI.ReadVersion(ctx, original, volume, object, version, opts)
}

func (d multipartListingFaultDisk) Delete(ctx context.Context, volume, object string, opts DeleteOptions) error {
	if d.delete != nil {
		return d.delete(ctx, volume, object, opts)
	}
	return d.StorageAPI.Delete(ctx, volume, object, opts)
}

func TestMultipartListingLegacyPreflight(t *testing.T) {
	z, set, bucket := multipartListingFixture(t)
	const other = "multipart-legacy-other"
	if err := z.MakeBucket(t.Context(), other, MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	old, err := z.NewMultipartUpload(t.Context(), other, "old", ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	fi, metadata, err := set.checkUploadIDExists(t.Context(), other, "old", old.UploadID, true)
	if err != nil {
		t.Fatal(err)
	}
	for i := range metadata {
		delete(metadata[i].Metadata, multipartMetaBucket)
		delete(metadata[i].Metadata, multipartMetaObject)
	}
	if _, err = writeAllMetadata(t.Context(), set.getDisks(), other, minioMetaMultipartBucket,
		set.getUploadIDDir(other, "old", old.UploadID), metadata, fi.WriteQuorum(set.defaultWQuorum())); err != nil {
		t.Fatal(err)
	}
	if _, err = z.NewMultipartUpload(t.Context(), bucket, "a", ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	z.mpCache.Clear()
	t.Run("legacy-upgrade", func(t *testing.T) {
		setMultipartListingTestMode(t, true)
		exact, err := z.ListMultipartUploads(t.Context(), other, "old", "", "", "", 10)
		if err != nil {
			t.Fatal(err)
		}
		requireMultipartUploadKeys(t, exact, "old")
		const empty = "multipart-legacy-empty"
		if err := z.MakeBucket(t.Context(), empty, MakeBucketOptions{}); err != nil {
			t.Fatal(err)
		}
		got, err := z.ListMultipartUploads(t.Context(), empty, "", "", "", "", 10)
		if err != nil || len(got.Uploads) != 0 {
			t.Fatalf("old upload in another bucket broke legacy listing: %+v %v", got, err)
		}
	})
	_, err = z.ListMultipartUploads(t.Context(), bucket, "", "", "", "", 10)
	if !errors.Is(err, errMultipartListingLegacy) {
		t.Fatalf("old upload in another bucket: %v", err)
	}
	report, err := z.multipartPreflight(t.Context())
	if err != nil || report.Ready || !report.Complete || report.LegacyUploads != 1 {
		t.Fatalf("legacy preflight: %+v %v", report, err)
	}
	if err = z.AbortMultipartUpload(t.Context(), other, "old", old.UploadID, ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	report, err = z.multipartPreflight(t.Context())
	if err != nil || !report.Ready || report.LegacyUploads != 0 {
		t.Fatalf("drained preflight: %+v %v", report, err)
	}
	got, err := z.ListMultipartUploads(t.Context(), bucket, "", "", "", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	requireMultipartUploadKeys(t, got, "a")
}

func TestMultipartListingIdentityFallback(t *testing.T) {
	for _, kind := range []string{"old", "corrupt", "read-failure", "wrong-bucket"} {
		t.Run(kind, func(t *testing.T) {
			z, set, bucket := multipartListingFixture(t)
			if _, err := z.NewMultipartUpload(t.Context(), bucket, "a", ObjectOptions{}); err != nil {
				t.Fatal(err)
			}
			original := set.getDisks
			t.Cleanup(func() { set.getDisks = original })
			var first atomic.Bool
			set.getDisks = func() []StorageAPI {
				disks := append([]StorageAPI(nil), original()...)
				for i, d := range disks {
					disks[i] = multipartListingFaultDisk{StorageAPI: d, read: func(ctx context.Context, b, v, p, version string, opts ReadOptions) (FileInfo, error) {
						fi, err := d.ReadVersion(ctx, b, v, p, version, opts)
						if v == minioMetaMultipartBucket && first.CompareAndSwap(false, true) {
							if kind == "read-failure" {
								return FileInfo{}, errDiskNotFound
							}
							if kind == "corrupt" {
								return FileInfo{}, errFileCorrupt
							}
							fi.Metadata = cloneMSS(fi.Metadata)
							if kind == "old" {
								delete(fi.Metadata, multipartMetaBucket)
							} else {
								fi.Metadata[multipartMetaBucket] = "another-bucket"
							}
						}
						return fi, err
					}}
				}
				return disks
			}
			got, err := z.ListMultipartUploads(t.Context(), bucket, "", "", "", "", 10)
			if err != nil {
				t.Fatal(err)
			}
			requireMultipartUploadKeys(t, got, "a")
		})
	}
}

func TestMultipartAbortPoolsAndRetry(t *testing.T) {
	z, bucket := consistencyPools(t)
	setMultipartListingTestMode(t, false)
	mp, err := z.serverPools[1].NewMultipartUpload(t.Context(), bucket, "a", ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	emptySet := z.serverPools[0].getHashedSet("a")
	original := emptySet.getDisks
	t.Cleanup(func() { emptySet.getDisks = original })
	emptySet.getDisks = func() []StorageAPI {
		disks := append([]StorageAPI(nil), original()...)
		for i, d := range disks {
			disks[i] = multipartListingFaultDisk{StorageAPI: d, read: func(context.Context, string, string, string, string, ReadOptions) (FileInfo, error) {
				return FileInfo{}, errDiskNotFound
			}}
		}
		return disks
	}
	err = z.AbortMultipartUpload(t.Context(), bucket, "a", mp.UploadID, ObjectOptions{})
	if err == nil || toAPIError(t.Context(), err).HTTPStatusCode != 503 {
		t.Fatalf("unknown pool must not acknowledge cancellation: %v", err)
	}
	emptySet.getDisks = original
	err = z.AbortMultipartUpload(t.Context(), bucket, "a", mp.UploadID, ObjectOptions{})
	if _, ok := err.(InvalidUploadID); !ok {
		t.Fatalf("retry after all pools confirm absence: %v", err)
	}
	mp, err = z.serverPools[1].NewMultipartUpload(t.Context(), bucket, "a", ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err = z.AbortMultipartUpload(t.Context(), bucket, "a", mp.UploadID, ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err = z.serverPools[1].GetMultipartInfo(t.Context(), bucket, "a", mp.UploadID, ObjectOptions{}); err == nil {
		t.Fatal("empty first pool hid the actual upload")
	}
}

func TestMultipartAbortRetryBelowReadQuorum(t *testing.T) {
	z, set, bucket := multipartListingFixture(t)
	mp, err := z.NewMultipartUpload(t.Context(), bucket, "a", ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	original := set.getDisks
	t.Cleanup(func() { set.getDisks = original })
	set.getDisks = func() []StorageAPI {
		disks := append([]StorageAPI(nil), original()...)
		disks[3] = nil
		d := disks[2]
		disks[2] = multipartListingFaultDisk{StorageAPI: d, delete: func(ctx context.Context, v, p string, opts DeleteOptions) error {
			if err := d.Delete(ctx, v, p, opts); err != nil {
				return err
			}
			return context.DeadlineExceeded // operation finished, acknowledgement lost
		}}
		return disks
	}
	if err = z.AbortMultipartUpload(t.Context(), bucket, "a", mp.UploadID, ObjectOptions{}); err == nil {
		t.Fatal("lost acknowledgement must fail")
	}
	set.getDisks = original
	if err = z.AbortMultipartUpload(t.Context(), bucket, "a", mp.UploadID, ObjectOptions{}); err != nil {
		t.Fatalf("one remaining metadata copy prevented retry: %v", err)
	}
}

func TestMultipartListingMarkerHTTP(t *testing.T) {
	z, _ := consistencyPools(t)
	bucket, router, err := initAPIHandlerTest(t.Context(), z, []string{"ListMultipartUploads"}, MakeBucketOptions{})
	if err != nil {
		t.Fatal(err)
	}
	setMultipartListingTestMode(t, false)
	for _, tc := range []struct {
		key, marker string
		status      int
	}{
		{"", "not-base64=", 200},
		{"a", "not-base64=", 404},
		{"a", base64.RawURLEncoding.EncodeToString([]byte("not-native")), 400},
		{"a", multipartListingTestID(time.Unix(100, 0), 1), 200},
	} {
		u := getListMultipartUploadsURLWithParams("", bucket, "", tc.key, tc.marker, "", "10")
		req, err := newTestSignedRequestV4(http.MethodGet, u, 0, nil, globalActiveCred.AccessKey, globalActiveCred.SecretKey, nil)
		if err != nil {
			t.Fatal(err)
		}
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != tc.status {
			t.Fatalf("key=%q marker=%q: %d %s", tc.key, tc.marker, rec.Code, rec.Body.String())
		}
	}
}

func TestMultipartPreflightAdminHTTP(t *testing.T) {
	bed, err := prepareAdminErasureTestBed(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { bed.done(); bed.objLayer.Shutdown(context.Background()); removeRoots(bed.erasureDirs) })
	const target = "/minio/admin/v3/multipart-preflight"
	rec := httptest.NewRecorder()
	bed.router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
	if rec.Code != 403 {
		t.Fatalf("anonymous preflight: %d", rec.Code)
	}
	req, err := newTestSignedRequestV4(http.MethodGet, target, 0, nil, globalActiveCred.AccessKey, globalActiveCred.SecretKey, nil)
	if err != nil {
		t.Fatal(err)
	}
	rec = httptest.NewRecorder()
	bed.router.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("admin preflight: %d %s", rec.Code, rec.Body.String())
	}
	var report multipartPreflightReport
	if err = json.Unmarshal(rec.Body.Bytes(), &report); err != nil || !report.Ready || !report.Complete {
		t.Fatalf("preflight: %+v %v", report, err)
	}
	for range cap(multipartScanSlots) {
		scan, err := startMultipartScan(t.Context(), false)
		if err != nil {
			t.Fatal(err)
		}
		defer scan.close()
	}
	rec = httptest.NewRecorder()
	bed.router.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("busy admin preflight: %d %s", rec.Code, rec.Body.String())
	}
}

type multipartLateCreateDisk struct {
	StorageAPI
	late chan<- func() error
}

func (d multipartLateCreateDisk) WriteMetadata(_ context.Context, original, volume, object string, fi FileInfo) error {
	if volume == minioMetaMultipartBucket {
		d.late <- func() error { return d.StorageAPI.WriteMetadata(context.Background(), original, volume, object, fi) }
		return context.DeadlineExceeded
	}
	return d.StorageAPI.WriteMetadata(context.Background(), original, volume, object, fi)
}

// This characterization is deliberately NOT an assertion of terminal abort
// correctness. It preserves an executable example of the separately scoped
// late-creation-write limitation. Change the expectation when fencing is added.
func TestMultipartAbortLateCreateBoundary(t *testing.T) {
	obj, dirs, err := prepareErasure(t.Context(), 16)
	if err != nil {
		t.Fatal(err)
	}
	z := obj.(*erasureServerPools)
	setMultipartListingTestMode(t, false)
	t.Cleanup(func() { z.Shutdown(context.Background()); removeRoots(dirs) })
	saved := globalStorageClass
	globalStorageClass.Update(storageclass.Config{Standard: storageclass.StorageClass{Parity: 8}})
	t.Cleanup(func() { globalStorageClass.Update(saved) })
	const bucket = "multipart-late-create"
	if err = z.MakeBucket(t.Context(), bucket, MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	set := z.serverPools[0].getHashedSet("a")
	original := set.getDisks
	t.Cleanup(func() { set.getDisks = original })
	late := make(chan func() error, 16)
	set.getDisks = func() []StorageAPI {
		disks := append([]StorageAPI(nil), original()...)
		for i := 9; i < 16; i++ {
			disks[i] = multipartLateCreateDisk{disks[i], late}
		}
		return disks
	}
	mp, err := z.NewMultipartUpload(t.Context(), bucket, "a", ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(late) != 7 {
		t.Fatalf("expected seven timed-out creation writes, got %d", len(late))
	}
	set.getDisks = func() []StorageAPI {
		disks := append([]StorageAPI(nil), original()...)
		for i := 2; i < 9; i++ {
			disks[i] = nil
		}
		return disks
	}
	if err = z.AbortMultipartUpload(t.Context(), bucket, "a", mp.UploadID, ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	for range 7 {
		if err = (<-late)(); err != nil {
			t.Fatal(err)
		}
	}
	set.getDisks = original
	_, metadata, err := set.checkUploadIDExists(t.Context(), bucket, "a", mp.UploadID, true)
	if err != nil {
		t.Fatalf("late creation boundary changed; review and remove the documented limitation: %v", err)
	}
	count := 0
	for _, fi := range metadata {
		if fi.IsValid() {
			count++
		}
	}
	if count != 14 {
		t.Fatalf("expected fourteen resurrected copies, got %d", count)
	}
	t.Log("KNOWN UNRESOLVED BOUNDARY: seven delayed creation writes plus seven offline copies restore a writable upload after acknowledged cancellation")
}

type multipartListingCountingDisk struct {
	StorageAPI
	directoryCalls *atomic.Int64
	metadataCalls  *atomic.Int64
}

func (d multipartListingCountingDisk) ListDir(ctx context.Context, original, volume, dir string, count int) ([]string, error) {
	if volume == minioMetaMultipartBucket {
		d.directoryCalls.Add(1)
	}
	return d.StorageAPI.ListDir(ctx, original, volume, dir, count)
}

func (d multipartListingCountingDisk) ReadVersion(ctx context.Context, original, volume, object, version string, opts ReadOptions) (FileInfo, error) {
	if volume == minioMetaMultipartBucket {
		d.metadataCalls.Add(1)
	}
	return d.StorageAPI.ReadVersion(ctx, original, volume, object, version, opts)
}

// Counts storage API calls; this is not a deployment throughput benchmark.
func TestMultipartListingScanCosts(t *testing.T) {
	obj, dirs, err := prepareErasure(t.Context(), 4)
	if err != nil {
		t.Fatal(err)
	}
	z := obj.(*erasureServerPools)
	setMultipartListingTestMode(t, false)
	t.Cleanup(func() { z.Shutdown(context.Background()); removeRoots(dirs) })
	const bucket, otherBucket = "r9-scan-target", "r9-scan-unrelated"
	for _, name := range []string{bucket, otherBucket} {
		if err := z.MakeBucket(t.Context(), name, MakeBucketOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := z.NewMultipartUpload(t.Context(), bucket, "dir/target", ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	var directoryCalls, metadataCalls atomic.Int64
	for _, pool := range z.serverPools {
		for _, set := range pool.sets {
			original := set.getDisks
			set.getDisks = func() []StorageAPI {
				disks := original()
				wrapped := make([]StorageAPI, len(disks))
				for i, disk := range disks {
					if disk != nil {
						wrapped[i] = multipartListingCountingDisk{disk, &directoryCalls, &metadataCalls}
					}
				}
				return wrapped
			}
			t.Cleanup(func() { set.getDisks = original })
		}
	}
	measure := func(label string) (int64, int64) {
		t.Helper()
		directoryCalls.Store(0)
		metadataCalls.Store(0)
		result, err := z.ListMultipartUploads(t.Context(), bucket, "dir/", "", "", "", 1)
		if err != nil {
			t.Fatal(err)
		}
		requireMultipartUploadKeys(t, result, "dir/target")
		d, m := directoryCalls.Load(), metadataCalls.Load()
		t.Logf("%s: max-uploads=1 returned=%d ListDir=%d ReadVersion=%d", label, len(result.Uploads), d, m)
		return d, m
	}
	_, baseline := measure("no unrelated uploads")
	for n := 0; n < 32; n++ {
		if _, err := z.NewMultipartUpload(t.Context(), otherBucket, fmt.Sprintf("other/%03d", n), ObjectOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	_, loaded := measure("32 uploads in another bucket")
	_, repeated := measure("same query repeated")
	if loaded != baseline+32 || repeated != loaded {
		t.Fatalf("unexpected scan accounting: baseline=%d loaded=%d repeated=%d", baseline, loaded, repeated)
	}
}

func multipartListingTestID(created time.Time, n int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("00000000-0000-4000-8000-000000000000.00000000-0000-4000-8000-%012xx%d", n, created.UnixNano())))
}

func multipartListingFixture(t *testing.T) (*erasureServerPools, *erasureObjects, string) {
	t.Helper()
	obj, dirs, err := prepareErasure(t.Context(), 4)
	if err != nil {
		t.Fatal(err)
	}
	z := obj.(*erasureServerPools)
	t.Cleanup(func() { z.Shutdown(context.Background()); removeRoots(dirs) })
	const bucket = "multipart-listing-test"
	if err := z.MakeBucket(t.Context(), bucket, MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	setMultipartListingTestMode(t, false)
	return z, z.serverPools[0].getHashedSet("a"), bucket
}

func TestMultipartListingMissingMarkers(t *testing.T) {
	base := time.Unix(100, 0)
	sameTime := []string{multipartListingTestID(base.Add(time.Second), 1), multipartListingTestID(base.Add(time.Second), 2), multipartListingTestID(base.Add(time.Second), 3)}
	slices.Sort(sameTime)
	uploads := []MultipartInfo{
		{Bucket: "bucket", Object: "a", UploadID: multipartListingTestID(base, 1), Initiated: base},
		{Bucket: "bucket", Object: "a", UploadID: sameTime[1], Initiated: base.Add(time.Second)},
		{Bucket: "bucket", Object: "b", UploadID: multipartListingTestID(base, 4), Initiated: base},
	}
	for _, tc := range []struct {
		name    string
		created time.Time
		id      int
		want    []string
	}{
		{"before", base.Add(-time.Second), 0, []string{"a", "a", "b"}},
		{"between", base.Add(time.Second / 2), 2, []string{"a", "b"}},
		{"same-time-before", base.Add(time.Second), 2, []string{"a", "b"}},
		{"same-time-after", base.Add(time.Second), 4, []string{"b"}},
		{"after", base.Add(2 * time.Second), 5, []string{"b"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			marker := multipartListingTestID(tc.created, tc.id)
			if tc.name == "same-time-before" {
				marker = sameTime[0]
			}
			if tc.name == "same-time-after" {
				marker = sameTime[2]
			}
			if err := checkListMultipartArgs(t.Context(), "bucket", "", "a", marker, ""); err != nil {
				t.Fatal(err)
			}
			got := paginateMultipartUploads(uploads, "", "a", marker, "", 10)
			if !slices.Equal(multipartUploadKeys(got.Uploads), tc.want) {
				t.Fatalf("got %v want %v", multipartUploadKeys(got.Uploads), tc.want)
			}
		})
	}
}

func TestMultipartListingAbortRecovery(t *testing.T) {
	for _, offline := range []int{1, 2} {
		t.Run(fmt.Sprint(offline), func(t *testing.T) {
			z, set, bucket := multipartListingFixture(t)
			mp, err := z.NewMultipartUpload(t.Context(), bucket, "a", ObjectOptions{})
			if err != nil {
				t.Fatal(err)
			}
			original := set.getDisks
			t.Cleanup(func() { set.getDisks = original })
			set.getDisks = func() []StorageAPI {
				disks := append([]StorageAPI(nil), original()...)
				for i := 0; i < offline; i++ {
					disks[i] = nil
				}
				return disks
			}
			err = z.AbortMultipartUpload(t.Context(), bucket, "a", mp.UploadID, ObjectOptions{})
			if offline == 1 && err != nil {
				t.Fatal(err)
			}
			if offline == 2 && (err == nil || toAPIError(t.Context(), err).HTTPStatusCode != 503) {
				t.Fatalf("two offline drives must not acknowledge cancellation: %v", err)
			}
			set.getDisks = original
			if offline == 2 {
				// Retry a partially completed deletion after the original disks return.
				if err = z.AbortMultipartUpload(t.Context(), bucket, "a", mp.UploadID, ObjectOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			got, err := z.ListMultipartUploads(t.Context(), bucket, "", "", "", "", 100)
			if err != nil || len(got.Uploads) != 0 {
				t.Fatalf("canceled upload reappeared: %+v %v", got, err)
			}
		})
	}
}

func TestMultipartListingCoverage(t *testing.T) {
	z, set, bucket := multipartListingFixture(t)
	mp, err := z.NewMultipartUpload(t.Context(), bucket, "a", ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	original := set.getDisks
	t.Cleanup(func() { set.getDisks = original })
	disks := original()
	// Leave a readable two-copy record, simulating a partially failed abort.
	for _, d := range disks[:2] {
		if err = d.Delete(t.Context(), minioMetaMultipartBucket, set.getUploadIDDir(bucket, "a", mp.UploadID), DeleteOptions{Recursive: true}); err != nil {
			t.Fatal(err)
		}
	}
	set.getDisks = func() []StorageAPI { return []StorageAPI{disks[0], disks[1], nil, nil} }
	_, err = z.ListMultipartUploads(t.Context(), bucket, "", "", "", "", 10)
	if err == nil || toAPIError(t.Context(), err).HTTPStatusCode != 503 {
		t.Fatalf("incomplete discovery returned success: %v", err)
	}
	set.getDisks = func() []StorageAPI { return []StorageAPI{nil, disks[1], disks[2], disks[3]} }
	got, err := z.ListMultipartUploads(t.Context(), bucket, "", "", "", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	requireMultipartUploadKeys(t, got, "a")
	report, err := z.multipartPreflight(t.Context())
	if err != nil || report.Ready || report.Complete || len(report.Sets[0].UncoveredDrives) != 1 {
		t.Fatalf("offline upgrade preflight: %+v %v", report, err)
	}
}

func TestMultipartListingBudgetAndAdmission(t *testing.T) {
	_, set, bucket := multipartListingFixture(t)
	if _, err := set.NewMultipartUpload(t.Context(), bucket, "a", ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	scan, err := startMultipartScan(t.Context(), false)
	if err != nil {
		t.Fatal(err)
	}
	defer scan.close()
	scan.remaining.Store(1)
	_, _, err = set.scanMultipartUploads(scan, bucket, 0, 0)
	var limited SlowDown
	if !errors.As(err, &limited) {
		t.Fatalf("budget: %v", err)
	}
	if apiErr := toAPIError(t.Context(), err); apiErr.HTTPStatusCode != http.StatusServiceUnavailable || apiErr.Code != "SlowDown" {
		t.Fatalf("budget error mapping: %+v", apiErr)
	}
	second, err := startMultipartScan(t.Context(), false)
	if err != nil {
		t.Fatal(err)
	}
	defer second.close()
	if third, err := startMultipartScan(t.Context(), false); err == nil {
		third.close()
		t.Fatal("third scan admitted")
	}
}

func TestMultipartListingAdmissionHTTP(t *testing.T) {
	z, _, _ := multipartListingFixture(t)
	bucket, router, err := initAPIHandlerTest(t.Context(), z, []string{"ListMultipartUploads"}, MakeBucketOptions{})
	if err != nil {
		t.Fatal(err)
	}
	setMultipartListingTestMode(t, false)
	for range cap(multipartScanSlots) {
		scan, err := startMultipartScan(t.Context(), false)
		if err != nil {
			t.Fatal(err)
		}
		defer scan.close()
	}
	req, err := newTestSignedRequestV4(http.MethodGet,
		getListMultipartUploadsURLWithParams("", bucket, "", "", "", "", "1"),
		0, nil, globalActiveCred.AccessKey, globalActiveCred.SecretKey, nil)
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	var response APIErrorResponse
	if err := xml.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusServiceUnavailable || response.Code != "SlowDown" {
		t.Fatalf("admission returned %d %s", rec.Code, rec.Body.String())
	}
}

func TestMultipartListingPreflightMinorityLegacy(t *testing.T) {
	z, set, bucket := multipartListingFixture(t)
	mp, err := z.NewMultipartUpload(t.Context(), bucket, "old", ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	path := set.getUploadIDDir(bucket, "old", mp.UploadID)
	disks := set.getDisks()
	fi, err := disks[0].ReadVersion(t.Context(), bucket, minioMetaMultipartBucket, path, "", ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	delete(fi.Metadata, multipartMetaBucket)
	delete(fi.Metadata, multipartMetaObject)
	if err = disks[0].WriteMetadata(t.Context(), bucket, minioMetaMultipartBucket, path, fi); err != nil {
		t.Fatal(err)
	}
	for _, d := range disks[1:] {
		if err = d.Delete(t.Context(), minioMetaMultipartBucket, path, DeleteOptions{Recursive: true}); err != nil {
			t.Fatal(err)
		}
	}
	report, err := z.multipartPreflight(t.Context())
	if err != nil || report.Ready || !report.Complete || report.LegacyUploads != 1 {
		t.Fatalf("minority legacy copy must prevent readiness: %+v %v", report, err)
	}
}

func TestMultipartListingCancellationRetainsAdmission(t *testing.T) {
	z, set, bucket := multipartListingFixture(t)
	for i := range 40 {
		if _, err := z.NewMultipartUpload(t.Context(), bucket, fmt.Sprintf("key-%02d", i), ObjectOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	original := set.getDisks
	t.Cleanup(func() { set.getDisks = original })
	entered := make(chan struct{}, 40)
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var reads atomic.Int32
	set.getDisks = func() []StorageAPI {
		disks := append([]StorageAPI(nil), original()...)
		for i, d := range disks {
			disks[i] = multipartListingFaultDisk{StorageAPI: d, read: func(ctx context.Context, b, v, p, version string, opts ReadOptions) (FileInfo, error) {
				if v != minioMetaMultipartBucket {
					return d.ReadVersion(ctx, b, v, p, version, opts)
				}
				reads.Add(1)
				entered <- struct{}{}
				// Model an RPC which does not return immediately on cancellation.
				<-release
				return FileInfo{}, ctx.Err()
			}}
		}
		return disks
	}
	second, err := startMultipartScan(t.Context(), false)
	if err != nil {
		t.Fatal(err)
	}
	defer second.close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := z.ListMultipartUploads(ctx, bucket, "", "", "", "", 10)
		done <- err
	}()
	for range 16 {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("identity workers did not start")
		}
	}
	cancel()
	if extra, err := startMultipartScan(t.Context(), false); err == nil {
		extra.close()
		t.Fatal("canceled request released admission while RPCs were still running")
	}
	start := time.Now()
	once.Do(func() { close(release) })
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled scan returned a successful partial list")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("scan did not stop after blocked RPCs returned")
	}
	if got := reads.Load(); got != 16 {
		t.Fatalf("scheduled more identity/metadata reads after cancellation: %d", got)
	}
	t.Logf("16 identity RPCs bounded; admission held until return; cancellation settled in %s", time.Since(start))
}
