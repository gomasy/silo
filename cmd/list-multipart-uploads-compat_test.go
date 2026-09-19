// Copyright (c) 2026 mr javad seydi
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
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"
)

func multipartUploadKeys(uploads []MultipartInfo) []string {
	keys := make([]string, len(uploads))
	for i := range uploads {
		keys[i] = uploads[i].Object
	}
	return keys
}

func requireMultipartUploadKeys(t *testing.T, got ListMultipartsInfo, want ...string) {
	t.Helper()
	if keys := multipartUploadKeys(got.Uploads); !slices.Equal(keys, want) {
		t.Fatalf("uploads = %v, want %v", keys, want)
	}
}

func TestListMultipartUploadsS3Compatibility(t *testing.T) {
	obj, dirs, err := prepareErasureSets32(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	z := obj.(*erasureServerPools)
	setMultipartListingTestMode(t, false)
	t.Cleanup(func() {
		z.Shutdown(t.Context())
		removeRoots(dirs)
	})

	const bucket = "multipart-list-compat"
	if err = z.MakeBucket(t.Context(), bucket, MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}

	objects := []string{"t/a_b/p1", "t/a_b/p2", "t/c_d/p1", "u/x"}
	sets := z.serverPools[0]
	firstSet := sets.getHashedSetIndex(objects[0])
	if !slices.ContainsFunc(objects[1:], func(object string) bool {
		return sets.getHashedSetIndex(object) != firstSet
	}) {
		for n := 0; ; n++ {
			object := fmt.Sprintf("v/cross-set-%d", n)
			if sets.getHashedSetIndex(object) != firstSet {
				objects = append(objects, object)
				break
			}
		}
	}

	uploadIDs := make(map[string]string, len(objects))
	for _, object := range objects {
		mp, err := z.NewMultipartUpload(t.Context(), bucket, object, ObjectOptions{})
		if err != nil {
			t.Fatalf("NewMultipartUpload(%q): %v", object, err)
		}
		uploadIDs[object] = mp.UploadID
	}

	// Durable multipart metadata, rather than this node-local cache, must be
	// authoritative after a restart or when another node handles the request.
	z.mpCache.Range(func(uploadID string, _ MultipartInfo) bool {
		z.mpCache.Delete(uploadID)
		return true
	})

	all, err := z.ListMultipartUploads(t.Context(), bucket, "", "", "", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	requireMultipartUploadKeys(t, all, objects...)
	if all.IsTruncated || all.NextKeyMarker != "" || all.NextUploadIDMarker != "" {
		t.Fatalf("complete listing has truncation state: %+v", all)
	}

	first, err := z.ListMultipartUploads(t.Context(), bucket, "", "", "", "", 1)
	if err != nil {
		t.Fatal(err)
	}
	requireMultipartUploadKeys(t, first, objects[0])
	if !first.IsTruncated || first.NextKeyMarker != objects[0] || first.NextUploadIDMarker != uploadIDs[objects[0]] {
		t.Fatalf("first page markers = (%q, %q, %t), want (%q, %q, true)",
			first.NextKeyMarker, first.NextUploadIDMarker, first.IsTruncated,
			objects[0], uploadIDs[objects[0]])
	}

	rest, err := z.ListMultipartUploads(t.Context(), bucket, "", first.NextKeyMarker, first.NextUploadIDMarker, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	requireMultipartUploadKeys(t, rest, objects[1:]...)

	afterKey, err := z.ListMultipartUploads(t.Context(), bucket, "", objects[1], "", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	requireMultipartUploadKeys(t, afterKey, objects[2:]...)

	prefixed, err := z.ListMultipartUploads(t.Context(), bucket, "t/", "", "", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	requireMultipartUploadKeys(t, prefixed, objects[:3]...)

	nested, err := z.ListMultipartUploads(t.Context(), bucket, "t/a_b/", "", "", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	requireMultipartUploadKeys(t, nested, objects[:2]...)

	grouped, err := z.ListMultipartUploads(t.Context(), bucket, "t/", "", "", SlashSeparator, 100)
	if err != nil {
		t.Fatal(err)
	}
	requireMultipartUploadKeys(t, grouped)
	if want := []string{"t/a_b/", "t/c_d/"}; !slices.Equal(grouped.CommonPrefixes, want) {
		t.Fatalf("common prefixes = %v, want %v", grouped.CommonPrefixes, want)
	}

	groupPage, err := z.ListMultipartUploads(t.Context(), bucket, "t/", "", "", SlashSeparator, 1)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"t/a_b/"}; !slices.Equal(groupPage.CommonPrefixes, want) {
		t.Fatalf("first common-prefix page = %v, want %v", groupPage.CommonPrefixes, want)
	}
	if !groupPage.IsTruncated || groupPage.NextKeyMarker != "t/a_b/" || groupPage.NextUploadIDMarker != "" {
		t.Fatalf("common-prefix page markers = (%q, %q, %t)",
			groupPage.NextKeyMarker, groupPage.NextUploadIDMarker, groupPage.IsTruncated)
	}

	groupRest, err := z.ListMultipartUploads(t.Context(), bucket, "t/", groupPage.NextKeyMarker, "", SlashSeparator, 1)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"t/c_d/"}; !slices.Equal(groupRest.CommonPrefixes, want) {
		t.Fatalf("second common-prefix page = %v, want %v", groupRest.CommonPrefixes, want)
	}
	if groupRest.IsTruncated {
		t.Fatalf("last common-prefix page is truncated: %+v", groupRest)
	}

	part, err := z.PutObjectPart(t.Context(), bucket, objects[0], uploadIDs[objects[0]], 1,
		mustGetPutObjReader(t, bytes.NewBufferString("part"), 4, "", ""), ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	completed, err := z.CompleteMultipartUpload(t.Context(), bucket, objects[0], uploadIDs[objects[0]],
		[]CompletePart{{PartNumber: 1, ETag: part.ETag}}, ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{multipartMetaBucket, multipartMetaObject} {
		if _, ok := completed.UserDefined[key]; ok {
			t.Errorf("completed object retained upload-only metadata %q", key)
		}
	}

	// Simulate an upload written by a pre-upgrade server. Its key cannot be
	// recovered by scanning the hashed namespace. Strict listing must fail;
	// the old path is available only through an explicit migration setting.
	legacyObject := objects[1]
	er := sets.getHashedSet(legacyObject)
	fi, metadata, err := er.checkUploadIDExists(t.Context(), bucket, legacyObject, uploadIDs[legacyObject], true)
	if err != nil {
		t.Fatal(err)
	}
	for i := range metadata {
		delete(metadata[i].Metadata, multipartMetaBucket)
		delete(metadata[i].Metadata, multipartMetaObject)
	}
	if _, err = writeAllMetadata(t.Context(), er.getDisks(), bucket, minioMetaMultipartBucket,
		er.getUploadIDDir(bucket, legacyObject, uploadIDs[legacyObject]), metadata, fi.WriteQuorum(er.defaultWQuorum())); err != nil {
		t.Fatal(err)
	}
	_, err = z.ListMultipartUploads(t.Context(), bucket, legacyObject, "", "", "", 100)
	if !errors.Is(err, errMultipartListingLegacy) {
		t.Fatalf("legacy strict listing: %v", err)
	}
	setMultipartListingTestMode(t, true)
	legacy, err := z.ListMultipartUploads(t.Context(), bucket, legacyObject, "", "", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	requireMultipartUploadKeys(t, legacy, legacyObject)
}

func TestPaginateMultipartUploads(t *testing.T) {
	base := time.Unix(100, 0)
	id1, id2, id3 := multipartListingTestID(base, 1), multipartListingTestID(base.Add(time.Second), 2), multipartListingTestID(base, 3)
	uploads := []MultipartInfo{
		{Bucket: "bucket", Object: "b", UploadID: id3, Initiated: base},
		{Bucket: "bucket", Object: "a", UploadID: id2, Initiated: base.Add(time.Second)},
		{Bucket: "bucket", Object: "a", UploadID: id1, Initiated: base},
		{Bucket: "bucket", Object: "a", UploadID: id1, Initiated: base}, // duplicate discovery
	}

	first := paginateMultipartUploads(uploads, "", "", "", "", 1)
	requireMultipartUploadKeys(t, first, "a")
	if !first.IsTruncated || first.NextKeyMarker != "a" || first.NextUploadIDMarker != id1 {
		t.Fatalf("first page = %+v", first)
	}

	second := paginateMultipartUploads(uploads, "", first.NextKeyMarker, first.NextUploadIDMarker, "", 1)
	if len(second.Uploads) != 1 || second.Uploads[0].Object != "a" || second.Uploads[0].UploadID != id2 {
		t.Fatalf("second page uploads = %+v", second.Uploads)
	}
	if !second.IsTruncated || second.NextKeyMarker != "a" || second.NextUploadIDMarker != id2 {
		t.Fatalf("second page = %+v", second)
	}

	last := paginateMultipartUploads(uploads, "", second.NextKeyMarker, second.NextUploadIDMarker, "", 1)
	requireMultipartUploadKeys(t, last, "b")
	if last.IsTruncated || last.NextKeyMarker != "" || last.NextUploadIDMarker != "" {
		t.Fatalf("last page = %+v", last)
	}

	missingUploadMarker := paginateMultipartUploads(uploads, "", "a", multipartListingTestID(base.Add(2*time.Second), 4), "", 10)
	requireMultipartUploadKeys(t, missingUploadMarker, "b")

	if err := checkListMultipartArgs(t.Context(), "bucket", "", "", "not-base64=", ""); err != nil {
		t.Fatalf("upload-id-marker without key-marker must be ignored: %v", err)
	}

	overLimit := make([]MultipartInfo, maxUploadsList+1)
	for i := range overLimit {
		overLimit[i] = MultipartInfo{Bucket: "bucket", Object: fmt.Sprintf("%04d", i), UploadID: fmt.Sprint(i)}
	}
	capped := paginateMultipartUploads(overLimit, "", "", "", "", maxUploadsList+1)
	if capped.MaxUploads != maxUploadsList || len(capped.Uploads) != maxUploadsList || !capped.IsTruncated {
		t.Fatalf("over-limit page = MaxUploads %d, uploads %d, truncated %t",
			capped.MaxUploads, len(capped.Uploads), capped.IsTruncated)
	}
}

func TestListMultipartUploadsGlobalPageAcrossPools(t *testing.T) {
	z, bucket := consistencyPools(t)
	setMultipartListingTestMode(t, false)
	objects := []string{"a/one", "b/two", "c/three", "d/four"}
	for i, object := range objects {
		if _, err := z.serverPools[i%len(z.serverPools)].NewMultipartUpload(t.Context(), bucket, object, ObjectOptions{}); err != nil {
			t.Fatalf("NewMultipartUpload(%q): %v", object, err)
		}
	}
	z.mpCache.Range(func(uploadID string, _ MultipartInfo) bool {
		z.mpCache.Delete(uploadID)
		return true
	})

	page, err := z.ListMultipartUploads(t.Context(), bucket, "", "", "", "", 2)
	if err != nil {
		t.Fatal(err)
	}
	requireMultipartUploadKeys(t, page, objects[:2]...)
	if !page.IsTruncated || page.NextKeyMarker != objects[1] {
		t.Fatalf("first global page = %+v", page)
	}

	rest, err := z.ListMultipartUploads(t.Context(), bucket, "", page.NextKeyMarker, page.NextUploadIDMarker, "", 2)
	if err != nil {
		t.Fatal(err)
	}
	requireMultipartUploadKeys(t, rest, objects[2:]...)
	if rest.IsTruncated {
		t.Fatalf("last global page is truncated: %+v", rest)
	}
}
