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
	"testing"
	"time"
)

// A multipart completion's If-Match must be evaluated against the logical
// latest object across pools, not against the copy local to a pool.
//
// Multi-pool write placement is not sticky (getPoolIdx picks by available
// space even for existing objects), so an upload and a newer overwrite of
// the same name routinely end up in different pools. Uploads are pinned to
// their pools directly: routing through z.NewMultipartUpload would make the
// placement depend on the space-weighted random choice.
func TestPoolsMultipartConditionalUsesLogicalLatest(t *testing.T) {
	z, bucket := consistencyPools(t)
	ctx := t.Context()

	ifMatch := func(etag string) (opts ObjectOptions) {
		return ObjectOptions{
			HasIfMatch: true,
			CheckPrecondFn: func(oi ObjectInfo) bool {
				return oi.ETag != etag
			},
		}
	}

	uploadPart := func(t *testing.T, bucket, object, uploadID string) []CompletePart {
		t.Helper()
		pi, err := z.PutObjectPart(ctx, bucket, object, uploadID, 1,
			mustGetPutObjReader(t, bytes.NewBufferString("part"), 4, "", ""), ObjectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return []CompletePart{{PartNumber: 1, ETag: pi.ETag}}
	}

	// Scenario A: the uploaded If-Match carries the stale ETag of the pool-0
	// copy while the logical latest object lives in pool 1. The completion
	// must fail with 412 instead of shadowing the newer logical state.
	objectA := "cond-mp-stale-etag"
	base := time.Now()
	oldA := putConsistencyObject(t, z, bucket, objectA, 0, "old", ObjectOptions{MTime: base.Add(-2 * time.Minute)})
	mpA, err := z.serverPools[0].NewMultipartUpload(ctx, bucket, objectA, ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	newerA := putConsistencyObject(t, z, bucket, objectA, 1, "new", ObjectOptions{
		MTime: base.Add(-time.Minute),
	})

	latest, _, err := z.getLatestObjectInfoWithIdx(ctx, bucket, objectA, ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if latest.ETag != newerA.ETag {
		t.Fatalf("logical latest should be the pool-1 copy: got %s want %s", latest.ETag, newerA.ETag)
	}

	if _, err = z.CompleteMultipartUpload(ctx, bucket, objectA, mpA.UploadID,
		uploadPart(t, bucket, objectA, mpA.UploadID), ifMatch(oldA.ETag)); err == nil {
		t.Fatal("If-Match with the stale pool-0 ETag must not complete over the newer pool-1 object")
	} else if _, ok := err.(PreConditionFailed); !ok {
		t.Fatalf("expected PreconditionFailed, got %v", err)
	}

	if latest, _, err = z.getLatestObjectInfoWithIdx(ctx, bucket, objectA, ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	if latest.ETag != newerA.ETag {
		t.Fatalf("the newer pool-1 object must remain the logical latest, got %s", latest.ETag)
	}

	// Scenario B: the uploaded If-Match carries the logical latest ETag (the
	// pool-1 copy) while the upload sits next to the stale pool-0 copy. The
	// precondition is satisfied, so completion must succeed and its result
	// must become the logical latest.
	objectB := "cond-mp-latest-etag"
	putConsistencyObject(t, z, bucket, objectB, 0, "old", ObjectOptions{MTime: base.Add(-2 * time.Minute)})
	mpB, err := z.serverPools[0].NewMultipartUpload(ctx, bucket, objectB, ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	newerB := putConsistencyObject(t, z, bucket, objectB, 1, "new", ObjectOptions{
		MTime: base.Add(-time.Minute),
	})
	oiB, err := z.CompleteMultipartUpload(ctx, bucket, objectB, mpB.UploadID,
		uploadPart(t, bucket, objectB, mpB.UploadID), ifMatch(newerB.ETag))
	if err != nil {
		t.Fatalf("If-Match with the logical latest ETag must complete, got %v", err)
	}
	if oiB.ETag == "" {
		t.Fatal("completion returned an empty ETag")
	}
	if latest, _, err = z.getLatestObjectInfoWithIdx(ctx, bucket, objectB, ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	if latest.ETag != oiB.ETag {
		t.Fatalf("the completed object must be the logical latest: got %s want %s", latest.ETag, oiB.ETag)
	}

	// Scenario C: the upload lives in pool 1 with the newer copy while pool 0
	// holds the stale one. A set-local evaluation order would let pool 0's
	// stale copy fail the request before pool 1 is reached; the logical
	// latest ETag must complete.
	objectC := "cond-mp-upload-other-pool"
	putConsistencyObject(t, z, bucket, objectC, 0, "old", ObjectOptions{MTime: base.Add(-2 * time.Minute)})
	mpC, err := z.serverPools[1].NewMultipartUpload(ctx, bucket, objectC, ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	newerC := putConsistencyObject(t, z, bucket, objectC, 1, "new", ObjectOptions{
		MTime: base.Add(-time.Minute),
	})
	if _, err = z.CompleteMultipartUpload(ctx, bucket, objectC, mpC.UploadID,
		uploadPart(t, bucket, objectC, mpC.UploadID), ifMatch(newerC.ETag)); err != nil {
		t.Fatalf("If-Match with the logical latest ETag must complete regardless of upload pool, got %v", err)
	}

	// Scenario D: pool 1 holds the newer copy but cannot be read. An
	// unreadable pool may contain the newest state, so the unverifiable
	// condition must fail the request rather than pass it against pool 0's
	// stale ETag.
	objectD := "cond-mp-unreadable-pool"
	oldD := putConsistencyObject(t, z, bucket, objectD, 0, "old", ObjectOptions{MTime: base.Add(-2 * time.Minute)})
	mpD, err := z.serverPools[0].NewMultipartUpload(ctx, bucket, objectD, ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	newerD := putConsistencyObject(t, z, bucket, objectD, 1, "new", ObjectOptions{
		MTime: base.Add(-time.Minute),
	})
	if latest, _, err = z.getLatestObjectInfoWithIdx(ctx, bucket, objectD, ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	if latest.ETag != newerD.ETag {
		t.Fatalf("logical latest before faulting pool 1 should be its copy: got %s want %s", latest.ETag, newerD.ETag)
	}
	set := z.serverPools[1].getHashedSet(objectD)
	getDisks := set.getDisks
	faulty := append([]StorageAPI(nil), getDisks()...)
	for i := range faulty {
		faulty[i] = consistencyReadFaultDisk{StorageAPI: faulty[i], bucket: bucket, object: objectD}
	}
	set.getDisks = func() []StorageAPI { return faulty }
	defer func() { set.getDisks = getDisks }()

	_, err = z.CompleteMultipartUpload(ctx, bucket, objectD, mpD.UploadID,
		uploadPart(t, bucket, objectD, mpD.UploadID), ifMatch(oldD.ETag))
	if !isErrReadQuorum(err) {
		t.Fatalf("expected an insufficient read quorum error, got %v", err)
	}
}
