// Copyright (c) 2026 Ruohang Feng
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"slices"
	"testing"
)

func rewriteMultipartIdentity(t testing.TB, set *erasureObjects, bucket, key, id string, change func(map[string]string)) {
	t.Helper()
	fi, metadata, err := set.checkUploadIDExists(t.Context(), bucket, key, id, true)
	if err != nil {
		t.Fatal(err)
	}
	for i := range metadata {
		change(metadata[i].Metadata)
	}
	if _, err := writeAllMetadata(t.Context(), set.getDisks(), bucket, minioMetaMultipartBucket,
		set.getUploadIDDir(bucket, key, id), metadata, fi.WriteQuorum(set.defaultWQuorum())); err != nil {
		t.Fatal(err)
	}
}

func removeMultipartIdentity(metadata map[string]string) {
	delete(metadata, multipartMetaBucket)
	delete(metadata, multipartMetaObject)
}

// The retained diagnostic scanner handles mixed records independently of a
// stale cache. Public HTTP behavior is tested separately.
func TestMultipartDiagnosticMixed(t *testing.T) {
	z, set, bucket := multipartListingFixture(t)
	for _, key := range []string{"dir/old", "dir/a", "dir/sub/b", "outside"} {
		mp, err := z.NewMultipartUpload(t.Context(), bucket, key, ObjectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if key == "dir/old" {
			rewriteMultipartIdentity(t, set, bucket, key, mp.UploadID, removeMultipartIdentity)
		}
	}
	z.mpCache.Clear()
	z.mpCache.Store("stale", MultipartInfo{Bucket: bucket, Object: "ghost", UploadID: "stale"})
	list := func(prefix, keyMarker, uploadMarker, delimiter string) ListMultipartUploadsResponse {
		t.Helper()
		got, err := z.scanMultipartUploadsForTest(t.Context(), bucket, prefix, keyMarker, uploadMarker, delimiter, 1)
		if err != nil {
			t.Fatal(err)
		}
		return generateListMultipartUploadsResponse(bucket, got, "")
	}
	var keys []string
	keyMarker, uploadMarker := "", ""
	for page := 0; ; page++ {
		if page >= 4 {
			t.Fatal("pagination did not terminate")
		}
		got := list("", keyMarker, uploadMarker, "")
		if len(got.Uploads) != 1 {
			t.Fatalf("page size: %+v", got)
		}
		keys = append(keys, got.Uploads[0].Key)
		if !got.IsTruncated {
			break
		}
		keyMarker, uploadMarker = got.NextKeyMarker, got.NextUploadIDMarker
	}
	if !slices.Equal(keys, []string{"dir/a", "dir/sub/b", "outside"}) {
		t.Fatalf("mixed full listing: %v", keys)
	}
	first := list("dir/", "", "", "/")
	if len(first.Uploads) != 1 || first.Uploads[0].Key != "dir/a" || !first.IsTruncated {
		t.Fatalf("prefix first page: %+v", first)
	}
	rest := list("dir/", first.NextKeyMarker, first.NextUploadIDMarker, "/")
	if len(rest.Uploads) != 0 || rest.IsTruncated || len(rest.CommonPrefixes) != 1 || rest.CommonPrefixes[0].Prefix != "dir/sub/" {
		t.Fatalf("prefix continuation: %+v", rest)
	}
	if got := list("dir/old", "", "", ""); len(got.Uploads) != 0 || got.IsTruncated {
		t.Fatalf("historical exact-looking prefix: %+v", got)
	}
}

func TestMultipartDiagnosticInvalidIdentity(t *testing.T) {
	for _, kind := range []string{"missing-bucket", "missing-object", "empty-bucket", "empty-object", "empty-both", "wrong-hash", "read-failure"} {
		t.Run(kind, func(t *testing.T) {
			z, set, bucket := multipartListingFixture(t)
			mp, err := z.NewMultipartUpload(t.Context(), bucket, "a", ObjectOptions{})
			if err != nil {
				t.Fatal(err)
			}
			rewriteMultipartIdentity(t, set, bucket, "a", mp.UploadID, func(metadata map[string]string) {
				switch kind {
				case "missing-bucket":
					delete(metadata, multipartMetaBucket)
				case "missing-object":
					delete(metadata, multipartMetaObject)
				case "empty-bucket":
					metadata[multipartMetaBucket] = ""
				case "empty-object":
					metadata[multipartMetaObject] = ""
				case "empty-both":
					metadata[multipartMetaBucket], metadata[multipartMetaObject] = "", ""
				case "wrong-hash":
					metadata[multipartMetaObject] = "wrong-key"
				case "read-failure":
					removeMultipartIdentity(metadata)
				}
			})
			if kind == "read-failure" {
				original := set.getDisks
				t.Cleanup(func() { set.getDisks = original })
				set.getDisks = func() []StorageAPI {
					disks := append([]StorageAPI(nil), original()...)
					for i, disk := range disks {
						disks[i] = multipartListingFaultDisk{StorageAPI: disk, read: func(context.Context, string, string, string, string, ReadOptions) (FileInfo, error) {
							return FileInfo{}, errFaultyDisk
						}}
					}
					return disks
				}
			}
			got, err := z.scanMultipartUploadsForTest(t.Context(), bucket, "", "", "", "", 10)
			if err == nil || len(got.Uploads) != 0 || toAPIError(t.Context(), err).HTTPStatusCode != http.StatusServiceUnavailable {
				t.Fatalf("invalid/unreadable record returned a successful page: %+v %v", got, err)
			}
			if kind != "read-failure" && !errors.Is(err, errMultipartListingIdentity) {
				t.Fatalf("identity error: %v", err)
			}
			report, err := z.multipartPreflight(t.Context())
			if err != nil || report.Complete || report.Ready || report.LegacyUploads != 0 {
				t.Fatalf("invalid/unreadable record counted as historical: %+v %v", report, err)
			}
		})
	}
}

func TestMultipartListingRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	obj, dirs, err := prepareErasure(ctx, 4)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); obj.Shutdown(context.Background()); removeRoots(dirs) })
	const bucket, key = "multipart-restart", "dir/new"
	if err := obj.MakeBucket(t.Context(), bucket, MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	mp, err := obj.NewMultipartUpload(t.Context(), bucket, key, ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := obj.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	reopened, _, err := initObjectLayer(t.Context(), mustGetPoolEndpoints(0, dirs...))
	if err != nil {
		t.Fatal(err)
	}
	obj = reopened
	if _, cached := obj.(*erasureServerPools).mpCache.Load(mp.UploadID); cached {
		t.Fatal("restart unexpectedly preserved the upload cache")
	}
	for _, prefix := range []string{"", "dir/"} {
		got, err := obj.ListMultipartUploads(t.Context(), bucket, prefix, "", "", "", 10)
		if err != nil {
			t.Fatal(err)
		}
		requireMultipartUploadKeys(t, got)
	}
	got, err := obj.ListMultipartUploads(t.Context(), bucket, key, "", "", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	requireMultipartUploadKeys(t, got, key)
	if got.Uploads[0].UploadID != mp.UploadID {
		t.Fatal("restart changed upload identity")
	}
	part, err := obj.PutObjectPart(t.Context(), bucket, key, mp.UploadID, 1,
		mustGetPutObjReader(t, bytes.NewBufferString("part"), 4, "", ""), ObjectOptions{})
	if err != nil {
		t.Fatalf("continue known upload after restart: %v", err)
	}
	if _, err := obj.CompleteMultipartUpload(t.Context(), bucket, key, mp.UploadID,
		[]CompletePart{{PartNumber: 1, ETag: part.ETag}}, ObjectOptions{}); err != nil {
		t.Fatalf("complete known upload after restart: %v", err)
	}
}

func TestMultipartDiagnosticConflictingIdentityCopies(t *testing.T) {
	for _, oldCopies := range []int{1, 3} {
		z, set, bucket := multipartListingFixture(t)
		mp, err := z.NewMultipartUpload(t.Context(), bucket, "a", ObjectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		path := set.getUploadIDDir(bucket, "a", mp.UploadID)
		for _, disk := range set.getDisks()[:oldCopies] {
			fi, err := disk.ReadVersion(t.Context(), bucket, minioMetaMultipartBucket, path, "", ReadOptions{})
			if err != nil {
				t.Fatal(err)
			}
			removeMultipartIdentity(fi.Metadata)
			if err := disk.WriteMetadata(t.Context(), bucket, minioMetaMultipartBucket, path, fi); err != nil {
				t.Fatal(err)
			}
		}
		got, err := z.scanMultipartUploadsForTest(t.Context(), bucket, "", "", "", "", 10)
		if !errors.Is(err, errMultipartListingIdentity) || len(got.Uploads) != 0 {
			t.Fatalf("%d contradictory copies returned a successful page: %+v %v", oldCopies, got, err)
		}
	}
}

func TestMultipartDiagnosticUnsupportedStoredID(t *testing.T) {
	for _, historical := range []bool{false, true} {
		z, set, bucket := multipartListingFixture(t)
		mp, err := z.NewMultipartUpload(t.Context(), bucket, "a", ObjectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		oldPath := set.getUploadIDDir(bucket, "a", mp.UploadID)
		unsupportedPath := pathJoin(set.getMultipartSHADir(bucket, "a"), "unsupported-id")
		for _, disk := range set.getDisks() {
			fi, err := disk.ReadVersion(t.Context(), bucket, minioMetaMultipartBucket, oldPath, "", ReadOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if historical {
				removeMultipartIdentity(fi.Metadata)
			}
			if err := disk.WriteMetadata(t.Context(), bucket, minioMetaMultipartBucket, unsupportedPath, fi); err != nil {
				t.Fatal(err)
			}
			if err := disk.Delete(t.Context(), minioMetaMultipartBucket, oldPath, DeleteOptions{Recursive: true}); err != nil {
				t.Fatal(err)
			}
		}
		got, err := z.scanMultipartUploadsForTest(t.Context(), bucket, "", "", "", "", 10)
		if historical {
			if err != nil || len(got.Uploads) != 0 {
				t.Fatalf("unidentified historical ID: %+v %v", got, err)
			}
		} else if !errors.Is(err, errMultipartListingIdentity) {
			t.Fatalf("invalid new ID silently treated as historical: %+v %v", got, err)
		}
	}
}
