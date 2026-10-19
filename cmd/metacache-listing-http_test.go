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
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"
)

// Exercise the actual LIST pipeline with the disputed entry in pool 1.
func TestListFallbackMultiPoolHTTPReadLock(t *testing.T) {
	for _, object := range []string{"object", "dir/"} {
		t.Run(object, func(t *testing.T) {
			z, bucket, router := listingPoolsBackend(t)
			ctx := t.Context()
			set := z.serverPools[1].sets[0]
			disks := set.getDisks()
			encoded := encodeDirObject(object)
			body := bytes.Repeat([]byte{'a'}, 8192)
			if object == "dir/" {
				body = nil
			}
			mtime := time.Now().UTC().Add(-time.Hour)
			put := func(tm time.Time) {
				_, err := set.PutObject(ctx, bucket, encoded, mustGetPutObjReader(t, bytes.NewReader(body), int64(len(body)), "", ""), ObjectOptions{MTime: tm})
				if err != nil {
					t.Fatal(err)
				}
			}
			put(mtime)
			old := make([][]byte, 3)
			for i := range old {
				old[i] = mustReadNullQuorumMeta(t, disks[i], bucket, encoded)
			}
			put(mtime.Add(time.Minute))
			for i := range old {
				if err := disks[i].WriteAll(ctx, bucket, encoded+"/"+xlStorageFormatFile, old[i]); err != nil {
					t.Fatal(err)
				}
			}
			walkers := slices.Clone(disks)
			for i := 8; i < len(walkers); i++ {
				walkers[i] = &failedMetadataWalkDisk{StorageAPI: disks[i]}
			}
			set.getDisks = func() []StorageAPI { return walkers }
			lock := z.NewNSLock(bucket, encoded)
			lkctx, err := lock.GetLock(ctx, globalOperationTimeout)
			if err != nil {
				t.Fatal(err)
			}
			locked := true
			defer func() {
				if locked {
					lock.Unlock(lkctx)
				}
			}()
			target := getListObjectsV2URL("", bucket, "", "1000", "", "", "")
			req, err := newTestSignedRequestV4(http.MethodGet, target, 0, nil, globalActiveCred.AccessKey, globalActiveCred.SecretKey, nil)
			if err != nil {
				t.Fatal(err)
			}
			rec := httptest.NewRecorder()
			done := make(chan struct{})
			go func() {
				defer close(done)
				router.ServeHTTP(rec, req.WithContext(ctx))
			}()
			waitForListingReadLock(t, z.serverPools[0].sets[0].nsMutex, bucket, encoded, done)
			lock.Unlock(lkctx)
			locked = false
			<-done
			var list ListObjectsV2Response
			if rec.Code != http.StatusOK || xml.Unmarshal(rec.Body.Bytes(), &list) != nil {
				t.Fatalf("LIST HTTP=%d body=%s", rec.Code, rec.Body.String())
			}
			if len(list.Contents) != 1 || list.Contents[0].Key != object {
				t.Fatalf("LIST lost key: %+v", list)
			}
		})
	}
}
