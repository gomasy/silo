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
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	xhttp "github.com/minio/minio/internal/http"
)

func multipartConditionRequest(t *testing.T, router http.Handler, method, target, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req, err := newTestSignedRequestV4(method, target, int64(len(body)), strings.NewReader(body), globalActiveCred.AccessKey, globalActiveCred.SecretKey, headers)
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// The server writes the wire spelling ETag directly into Header; unlike a
// network response, httptest's map has not canonicalized it to Etag.
func multipartConditionResponseETag(rec *httptest.ResponseRecorder) string {
	for key, values := range rec.Header() {
		if strings.EqualFold(key, xhttp.ETag) && len(values) > 0 {
			return strings.Trim(values[0], "\"")
		}
	}
	return ""
}

func multipartConditionUpload(t *testing.T, z *erasureServerPools, bucket, object string, owner int, opts ObjectOptions) (string, []CompletePart) {
	t.Helper()
	mp, err := z.serverPools[owner].NewMultipartUpload(t.Context(), bucket, object, opts)
	if err != nil {
		t.Fatal(err)
	}
	part, err := z.serverPools[owner].PutObjectPart(t.Context(), bucket, object, mp.UploadID, 1, mustGetPutObjReader(t, bytes.NewBufferString("replacement"), 11, "", ""), ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return mp.UploadID, []CompletePart{{PartNumber: 1, ETag: part.ETag}}
}

func multipartConditionCompleteBody(parts []CompletePart) string {
	data, err := xml.Marshal(CompleteMultipartUpload{Parts: parts})
	if err != nil {
		panic(err)
	}
	return string(data)
}

func multipartConditionError(t *testing.T, rec *httptest.ResponseRecorder, code string) {
	t.Helper()
	decoder := xml.NewDecoder(strings.NewReader(rec.Body.String()))
	var response APIErrorResponse
	if err := decoder.Decode(&response); err != nil || response.Code != code {
		t.Fatalf("expected %s error, got %q: %v", code, rec.Body.String(), err)
	}
	if err := decoder.Decode(&response); err != io.EOF {
		t.Fatalf("expected exactly one error response, got %q: %v", rec.Body.String(), err)
	}
}

// State is deliberately placed per pool; the final request uses signed HTTP
// and the real handler, precondition callback, erasure metadata and rename.
func TestPoolsMultipartConditionalHTTPMatrix(t *testing.T) {
	z, _ := consistencyPools(t)
	bucket, router, err := initAPIHandlerTest(t.Context(), z, nil, MakeBucketOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for owner := range 2 {
		for _, localOld := range []bool{false, true} {
			for _, condition := range []string{"match-old", "match-current", "none-match"} {
				t.Run(fmt.Sprintf("owner=%d/old=%t/%s", owner, localOld, condition), func(t *testing.T) {
					object := fmt.Sprintf("http-%d-%t-%s", owner, localOld, condition)
					oldETag := "old-does-not-exist"
					if localOld {
						oldETag = putConsistencyObject(t, z, bucket, object, owner, "old", ObjectOptions{MTime: UTCNow().Add(-time.Hour)}).ETag
					}
					id, parts := multipartConditionUpload(t, z, bucket, object, owner, ObjectOptions{})
					current := putConsistencyObject(t, z, bucket, object, 1-owner, "current", ObjectOptions{MTime: UTCNow().Add(-time.Minute)})
					head := multipartConditionRequest(t, router, http.MethodHead, getGetObjectURL("", bucket, object), "", nil)
					if head.Code != 200 || multipartConditionResponseETag(head) != current.ETag {
						t.Fatalf("bad HEAD: %d %v", head.Code, head.Header())
					}
					h := map[string]string{xhttp.IfMatch: "\"" + oldETag + "\""}
					want := 412
					if condition == "match-current" {
						h[xhttp.IfMatch] = "\"" + current.ETag + "\""
						want = 200
					}
					if condition == "none-match" {
						h = map[string]string{xhttp.IfNoneMatch: "*"}
					}
					rec := multipartConditionRequest(t, router, http.MethodPost, getCompleteMultipartUploadURL("", bucket, object, id), multipartConditionCompleteBody(parts), h)
					t.Logf("HEAD current=%s, requested=%v, complete HTTP=%d", current.ETag, h, rec.Code)
					if rec.Code != want {
						t.Errorf("want HTTP %d, got %d: %s", want, rec.Code, rec.Body.String())
					}
					if want == 412 {
						multipartConditionError(t, rec, "PreconditionFailed")
						got := multipartConditionRequest(t, router, http.MethodGet, getGetObjectURL("", bucket, object), "", nil)
						if got.Code != 200 || got.Body.String() != "current" {
							t.Errorf("rejected request must preserve current data: %d %q", got.Code, got.Body.String())
						}
						if _, err := z.serverPools[owner].ListObjectParts(t.Context(), bucket, object, id, 0, 10, ObjectOptions{}); err != nil {
							t.Errorf("rejected request consumed upload: %v", err)
						}
					}
				})
			}
		}
	}
}

func TestPoolsMultipartConditionalHTTPAbsentObject(t *testing.T) {
	for _, deleted := range []bool{false, true} {
		for _, match := range []bool{false, true} {
			t.Run(fmt.Sprintf("delete-marker=%t/if-match=%t", deleted, match), func(t *testing.T) {
				z, _ := consistencyPools(t)
				bucket, router, err := initAPIHandlerTest(t.Context(), z, nil, MakeBucketOptions{VersioningEnabled: deleted})
				if err != nil {
					t.Fatal(err)
				}
				object := "http-absent"
				if deleted {
					putConsistencyObject(t, z, bucket, object, 0, "old", ObjectOptions{Versioned: true, MTime: UTCNow().Add(-time.Hour)})
					_, err = z.serverPools[1].DeleteObject(t.Context(), bucket, object, ObjectOptions{Versioned: true, VersionID: mustGetUUID(), DeleteMarker: true, MTime: UTCNow().Add(-time.Minute)})
					if err != nil {
						t.Fatal(err)
					}
				}
				id, parts := multipartConditionUpload(t, z, bucket, object, 0, ObjectOptions{Versioned: deleted})
				headers := map[string]string{xhttp.IfNoneMatch: "*"}
				want := http.StatusOK
				if match {
					headers = map[string]string{xhttp.IfMatch: "\"missing\""}
					want = http.StatusNotFound
				}
				rec := multipartConditionRequest(t, router, http.MethodPost, getCompleteMultipartUploadURL("", bucket, object, id), multipartConditionCompleteBody(parts), headers)
				if rec.Code != want {
					t.Fatalf("expected HTTP %d, got %d: %s", want, rec.Code, rec.Body.String())
				}
				if match {
					multipartConditionError(t, rec, "NoSuchKey")
					if _, err := z.serverPools[0].ListObjectParts(t.Context(), bucket, object, id, 0, 10, ObjectOptions{}); err != nil {
						t.Errorf("rejected request consumed upload: %v", err)
					}
				}
			})
		}
	}
}

// All writes below use ordinary signed S3 requests. The result must be correct
// for every placement; the matrix above deterministically covers split pools.
func TestPoolsMultipartConditionalHTTPNormalRouting(t *testing.T) {
	z, _ := consistencyPools(t)
	bucket, router, err := initAPIHandlerTest(t.Context(), z, nil, MakeBucketOptions{})
	if err != nil {
		t.Fatal(err)
	}
	object := "normal-routing"
	url := getPutObjectURL("", bucket, object)
	old := multipartConditionRequest(t, router, http.MethodPut, url, "old-data", nil)
	if old.Code != http.StatusOK {
		t.Fatalf("initial PUT %d: %s", old.Code, old.Body.String())
	}
	init := multipartConditionRequest(t, router, http.MethodPost, url+"?uploads", "", nil)
	if init.Code != http.StatusOK {
		t.Fatalf("init %d: %s", init.Code, init.Body.String())
	}
	var mp InitiateMultipartUploadResponse
	if err := xml.Unmarshal(init.Body.Bytes(), &mp); err != nil {
		t.Fatal(err)
	}
	part := multipartConditionRequest(t, router, http.MethodPut, getPutObjectPartURL("", bucket, object, mp.UploadID, "1"), "replacement", nil)
	if part.Code != http.StatusOK {
		t.Fatalf("part %d: %s", part.Code, part.Body.String())
	}
	parts := []CompletePart{{PartNumber: 1, ETag: multipartConditionResponseETag(part)}}
	newer := multipartConditionRequest(t, router, http.MethodPut, url, "newer-data", nil)
	if newer.Code != http.StatusOK {
		t.Fatalf("new PUT %d: %s", newer.Code, newer.Body.String())
	}
	head := multipartConditionRequest(t, router, http.MethodHead, url, "", nil)
	if head.Code != http.StatusOK || multipartConditionResponseETag(head) != multipartConditionResponseETag(newer) {
		t.Fatalf("HEAD did not pick new object: %d %v", head.Code, head.Header())
	}
	rec := multipartConditionRequest(t, router, http.MethodPost, getCompleteMultipartUploadURL("", bucket, object, mp.UploadID), multipartConditionCompleteBody(parts), map[string]string{xhttp.IfMatch: "\"" + multipartConditionResponseETag(old) + "\""})
	if rec.Code != http.StatusPreconditionFailed {
		t.Errorf("stale If-Match should be HTTP 412, got %d: %s", rec.Code, rec.Body.String())
	}
	get := multipartConditionRequest(t, router, http.MethodGet, url, "", nil)
	if get.Code != http.StatusOK || get.Body.String() != "newer-data" {
		t.Errorf("conditional completion changed newer data: %d %q", get.Code, get.Body.String())
	}
}

func TestPoolsMultipartConditionalUnreadablePool(t *testing.T) {
	z, bucket := consistencyPools(t)
	object := "quorum-with-readable-copy"
	old := putConsistencyObject(t, z, bucket, object, 0, "readable", ObjectOptions{MTime: UTCNow().Add(-time.Hour)})
	putConsistencyObject(t, z, bucket, object, 1, "hidden-newer", ObjectOptions{MTime: UTCNow().Add(-time.Minute)})
	id, parts := multipartConditionUpload(t, z, bucket, object, 0, ObjectOptions{})
	set := z.serverPools[1].getHashedSet(object)
	original := set.getDisks
	disks := append([]StorageAPI(nil), original()...)
	for i := range disks {
		disks[i] = consistencyReadFaultDisk{StorageAPI: disks[i], bucket: bucket, object: object}
	}
	set.getDisks = func() []StorageAPI { return disks }
	defer func() { set.getDisks = original }()
	read, readErr := z.GetObjectInfo(t.Context(), bucket, object, ObjectOptions{})
	t.Logf("ordinary GET lookup: etag=%s err=%v", read.ETag, readErr)
	called := 0
	_, err := z.CompleteMultipartUpload(t.Context(), bucket, object, id, parts, ObjectOptions{HasIfMatch: true, CheckPrecondFn: func(oi ObjectInfo) bool { called++; return oi.ETag != old.ETag }})
	if !isErrReadQuorum(err) {
		t.Errorf("conditional write must fail on unreadable pool, got %v", err)
	}
	if called != 0 {
		t.Errorf("callback evaluated without complete state: %d calls", called)
	}
}

func TestPoolsMultipartConditionalLatestVersionAndCallbackOnce(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%t", explicit), func(t *testing.T) {
			z, bucket := consistencyPools(t)
			object := "tie-version"
			opts := ObjectOptions{MTime: UTCNow().Add(-time.Hour), Versioned: explicit}
			if explicit {
				opts.VersionID = mustGetUUID()
			}
			current := putConsistencyObject(t, z, bucket, object, 0, "first", opts)
			putConsistencyObject(t, z, bucket, object, 1, "second", opts)
			if explicit {
				current = putConsistencyObject(t, z, bucket, object, 1, "latest-other-version", ObjectOptions{Versioned: true, MTime: UTCNow().Add(-time.Minute)})
			}
			id, parts := multipartConditionUpload(t, z, bucket, object, 1, opts)
			called := 0
			opts.CheckPrecondFn = func(oi ObjectInfo) bool { called++; return oi.ETag != current.ETag }
			opts.MTime = time.Time{}
			opts.HasIfMatch = true
			_, err := z.CompleteMultipartUpload(t.Context(), bucket, object, id, parts, opts)
			if err != nil {
				t.Errorf("logical current object should match: %v", err)
			}
			if called != 1 {
				t.Errorf("condition evaluated %d times; want exactly once", called)
			}
		})
	}
}

func TestPoolsMultipartConditionalConcurrentCompletes(t *testing.T) {
	z, bucket := consistencyPools(t)
	object := "concurrent-completes"
	old := putConsistencyObject(t, z, bucket, object, 0, "old", ObjectOptions{MTime: UTCNow().Add(-time.Hour)})
	putConsistencyObject(t, z, bucket, object, 1, "old", ObjectOptions{MTime: old.ModTime})
	ids := make([]string, 2)
	parts := make([][]CompletePart, 2)
	for i := range 2 {
		ids[i], parts[i] = multipartConditionUpload(t, z, bucket, object, i, ObjectOptions{})
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var gate, releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	errs := make([]error, 2)
	var wg sync.WaitGroup
	// Let pool 1's completion hold the object lock before pool 0's starts.
	// Once pool 1 commits, a set-local read in pool 0 would still see the
	// old ETag and incorrectly accept the second completion.
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, errs[1] = z.CompleteMultipartUpload(t.Context(), bucket, object, ids[1], parts[1], ObjectOptions{HasIfMatch: true, CheckPrecondFn: func(oi ObjectInfo) bool {
			gate.Do(func() { close(entered); <-release })
			return oi.ETag != old.ETag
		}})
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("first completion did not enter its condition callback")
	}
	started := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		close(started)
		_, errs[0] = z.CompleteMultipartUpload(t.Context(), bucket, object, ids[0], parts[0], ObjectOptions{HasIfMatch: true, CheckPrecondFn: func(oi ObjectInfo) bool { return oi.ETag != old.ETag }})
	}()
	<-started
	releaseOnce.Do(func() { close(release) })
	wg.Wait()
	success, failed := 0, 0
	for _, err := range errs {
		var p PreConditionFailed
		switch {
		case err == nil:
			success++
		case errors.As(err, &p):
			failed++
		default:
			t.Errorf("unexpected completion error %v", err)
		}
	}
	if success != 1 || failed != 1 {
		t.Errorf("CAS writers: success=%d conditional failures=%d errors=%v", success, failed, errs)
	}
}
