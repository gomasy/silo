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
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestPoolsMultipartConditionMatrix(t *testing.T) {
	for owner := range 2 {
		for _, withOld := range []bool{false, true} {
			for _, condition := range []string{"match-current", "match-old", "none-match-any"} {
				t.Run(fmt.Sprintf("upload-pool=%d/old-copy=%t/%s", owner, withOld, condition), func(t *testing.T) {
					z, bucket := consistencyPools(t)
					object := "conditional-multipart"
					oldETag := "arbitrary-old"
					if withOld {
						old := putConsistencyObject(t, z, bucket, object, owner, "old", ObjectOptions{MTime: UTCNow().Add(-time.Hour)})
						oldETag = old.ETag
					}
					mp, err := z.serverPools[owner].NewMultipartUpload(t.Context(), bucket, object, ObjectOptions{})
					if err != nil {
						t.Fatal(err)
					}
					part, err := z.serverPools[owner].PutObjectPart(t.Context(), bucket, object, mp.UploadID, 1, mustGetPutObjReader(t, bytes.NewBufferString("replacement"), 11, "", ""), ObjectOptions{})
					if err != nil {
						t.Fatal(err)
					}
					current := putConsistencyObject(t, z, bucket, object, 1-owner, "current", ObjectOptions{MTime: UTCNow().Add(-time.Minute)})
					visible, err := z.GetObjectInfo(t.Context(), bucket, object, ObjectOptions{})
					if err != nil || visible.ETag != current.ETag {
						t.Fatalf("invalid current state: %v", err)
					}
					opts := ObjectOptions{HasIfMatch: condition != "none-match-any", CheckPrecondFn: func(oi ObjectInfo) bool {
						switch condition {
						case "match-current":
							return oi.ETag != current.ETag
						case "match-old":
							return oi.ETag != oldETag
						default:
							return true
						}
					}}
					_, err = z.CompleteMultipartUpload(t.Context(), bucket, object, mp.UploadID, []CompletePart{{PartNumber: 1, ETag: part.ETag}}, opts)
					if condition == "match-current" {
						if err != nil {
							t.Errorf("correct logical If-Match rejected: %v", err)
						}
					} else {
						var expected PreConditionFailed
						if !errors.As(err, &expected) {
							t.Errorf("logical condition must fail, got %v", err)
						}
					}
				})
			}
		}
	}
}

func TestPoolsMultipartConditionBoundaries(t *testing.T) {
	for _, state := range []string{"missing", "latest-delete-marker", "unreadable-other-pool"} {
		for _, match := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/if-match=%t", state, match), func(t *testing.T) {
				z, bucket := consistencyPools(t)
				object := "conditional-boundary"
				versioned := state == "latest-delete-marker"
				if versioned {
					putConsistencyObject(t, z, bucket, object, 0, "old", ObjectOptions{Versioned: true, MTime: UTCNow().Add(-time.Hour)})
					if _, err := z.serverPools[1].DeleteObject(t.Context(), bucket, object, ObjectOptions{Versioned: true, VersionID: mustGetUUID(), DeleteMarker: true, MTime: UTCNow().Add(-time.Minute)}); err != nil {
						t.Fatal(err)
					}
				}
				mp, err := z.serverPools[0].NewMultipartUpload(t.Context(), bucket, object, ObjectOptions{Versioned: versioned})
				if err != nil {
					t.Fatal(err)
				}
				part, err := z.serverPools[0].PutObjectPart(t.Context(), bucket, object, mp.UploadID, 1, mustGetPutObjReader(t, bytes.NewBufferString("new"), 3, "", ""), ObjectOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if state == "unreadable-other-pool" {
					set := z.serverPools[1].getHashedSet(object)
					getDisks := set.getDisks
					faulty := append([]StorageAPI(nil), getDisks()...)
					for i := range faulty {
						faulty[i] = consistencyReadFaultDisk{StorageAPI: faulty[i], bucket: bucket, object: object}
					}
					set.getDisks = func() []StorageAPI { return faulty }
					defer func() { set.getDisks = getDisks }()
				}
				opts := ObjectOptions{Versioned: versioned, HasIfMatch: match, CheckPrecondFn: func(ObjectInfo) bool { return true }}
				_, err = z.CompleteMultipartUpload(t.Context(), bucket, object, mp.UploadID, []CompletePart{{PartNumber: 1, ETag: part.ETag}}, opts)
				switch {
				case state == "unreadable-other-pool":
					if !isErrReadQuorum(err) {
						t.Errorf("unreadable pool must not mean absence: %v", err)
					}
				case match:
					if !isErrObjectNotFound(err) {
						t.Errorf("If-Match against logical absence should report absence: %v", err)
					}
				default:
					if err != nil {
						t.Errorf("If-None-Match against logical absence must succeed: %v", err)
					}
				}
				if err != nil {
					if _, lerr := z.serverPools[0].ListObjectParts(t.Context(), bucket, object, mp.UploadID, 0, 10, ObjectOptions{}); lerr != nil {
						t.Errorf("failed condition consumed upload: %v", lerr)
					}
				}
			})
		}
	}
}
