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
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio/internal/grid"
)

type listingReadFailDisk struct {
	StorageAPI
	object string
	reads  *atomic.Int64
}

func (d *listingReadFailDisk) ReadXL(ctx context.Context, bucket, object string, readData bool) (RawFileInfo, error) {
	if object == d.object {
		d.reads.Add(1)
		return RawFileInfo{}, errDiskNotFound
	}
	return d.StorageAPI.ReadXL(ctx, bucket, object, readData)
}

// Install before issuing requests: cache writers can outlive their HTTP
// response, so disk faults must be switched without replacing getDisks.
// Each published slice must remain immutable.
func installListingDiskSwitch(set *erasureObjects) ([]StorageAPI, func([]StorageAPI)) {
	disks := set.getDisks()
	var current atomic.Value
	current.Store(disks)
	set.getDisks = func() []StorageAPI { return current.Load().([]StorageAPI) }
	return disks, func(disks []StorageAPI) { current.Store(disks) }
}

func TestListingCachePreservesFallbackError(t *testing.T) {
	for _, continuation := range []bool{false, true} {
		name := "cold"
		if continuation {
			name = "continuation"
		}
		t.Run(name, func(t *testing.T) {
			testListingCacheFallbackError(t, continuation, false)
		})
	}
}

func TestListingCacheCleanupKeepsOriginalID(t *testing.T) {
	testListingCacheFallbackError(t, true, true)
}

// Use actual peer RPCs to verify the abandoned cache is updated after the
// caller clears its own listing ID. The local-only path cannot observe this.
func recordListingPeerCleanup(t *testing.T) func() {
	t.Helper()
	tg, err := grid.SetupTestGrid(2)
	if err != nil {
		t.Fatal(err)
	}
	previous := globalNotificationSys
	t.Cleanup(func() {
		tg.Cleanup()
		globalNotificationSys = previous
	})
	getIDs := make(chan string, 8)
	type cleanupResult struct {
		id  string
		err *grid.RemoteErr
	}
	updated := make(chan cleanupResult, 1)
	server := &peerRESTServer{}
	if err := getMetacacheListingRPC.Register(tg.Managers[1], func(o *listPathOptions) (*metacache, *grid.RemoteErr) {
		getIDs <- o.ID
		return server.GetMetacacheListingHandler(o)
	}); err != nil {
		t.Fatal(err)
	}
	if err := updateMetacacheListingRPC.Register(tg.Managers[1], func(m *metacache) (*metacache, *grid.RemoteErr) {
		result, err := server.UpdateMetacacheListingHandler(m)
		if m.error == "no longer used" {
			updated <- cleanupResult{id: m.id, err: err}
		}
		return result, err
	}); err != nil {
		t.Fatal(err)
	}
	client := &peerRESTClient{gridConn: func() *grid.Connection { return tg.Managers[0].Connection(tg.Hosts[1]) }}
	globalNotificationSys = &NotificationSys{peerClients: []*peerRESTClient{client}, allPeerClients: []*peerRESTClient{client}}
	return func() {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		var ids []string
		for range 2 {
			select {
			case id := <-getIDs:
				ids = append(ids, id)
			case <-ctx.Done():
				t.Fatal("peer did not receive the failed cache cleanup lookup")
			}
		}
		if ids[0] == "" || ids[1] != ids[0] {
			t.Fatalf("cleanup lookup lost the original cache ID: %q", ids)
		}
		select {
		case result := <-updated:
			if result.err != nil || result.id != ids[0] {
				t.Fatalf("cleanup update: id=%q err=%v, want id=%q", result.id, result.err, ids[0])
			}
			t.Logf("peer cleanup Get/Update retained cache ID %q", result.id)
		case <-ctx.Done():
			t.Fatal("peer did not finish the failed cache cleanup update")
		}
	}
}

func testListingCacheFallbackError(t *testing.T, continuation, peer bool) {
	t.Helper()
	z, bucket, router := nullQuorumBackend(t)
	var waitPeerCleanup func()
	if peer {
		waitPeerCleanup = recordListingPeerCleanup(t)
	}
	set := z.serverPools[0].sets[0]
	disks, switchDisks := installListingDiskSwitch(set)
	body := bytes.Repeat([]byte{'a'}, 8192)
	mtime := time.Now().UTC().Add(-time.Hour)
	for _, object := range []string{"a", "b", "z"} {
		nullQuorumPut(t, z, bucket, object, body, ObjectOptions{MTime: mtime})
	}
	old := make([][]byte, 3)
	for i := range old {
		old[i] = mustReadNullQuorumMeta(t, disks[i], bucket, "z")
	}
	nullQuorumPut(t, z, bucket, "z", body, ObjectOptions{MTime: mtime.Add(time.Minute)})
	var token string
	if continuation {
		rec := nullQuorumRequest(t, router, http.MethodGet, getListObjectsV2URL("", bucket, "", "1", "", "", ""))
		var list ListObjectsV2Response
		if rec.Code != http.StatusOK || xml.Unmarshal(rec.Body.Bytes(), &list) != nil || !list.IsTruncated || list.NextContinuationToken == "" {
			t.Fatalf("first page: %d %s", rec.Code, rec.Body.String())
		}
		token = list.NextContinuationToken
	}
	for i := range old {
		if err := disks[i].WriteAll(t.Context(), bucket, "z/"+xlStorageFormatFile, old[i]); err != nil {
			t.Fatal(err)
		}
	}
	var reads atomic.Int64
	walkers := slices.Clone(disks)
	for i := range walkers {
		walkers[i] = &listingReadFailDisk{StorageAPI: disks[i], object: "z", reads: &reads}
		if i >= 8 {
			walkers[i] = &failedMetadataWalkDisk{StorageAPI: walkers[i]}
		}
	}
	switchDisks(walkers)
	target := getListObjectsV2URL("", bucket, "", "1000", "", "", "")
	if token != "" {
		target += "&continuation-token=" + url.QueryEscape(token)
	}
	rec := nullQuorumRequest(t, router, http.MethodGet, target)
	t.Logf("LIST=%d failed metadata reads=%d body=%s", rec.Code, reads.Load(), rec.Body.String())
	if reads.Load() == 0 {
		t.Fatal("fallback not exercised")
	}
	var apiErr APIErrorResponse
	if rec.Code != http.StatusServiceUnavailable || xml.Unmarshal(rec.Body.Bytes(), &apiErr) != nil || apiErr.Code != "SlowDownRead" {
		t.Fatalf("fallback failure lost its read-quorum error: %d %s", rec.Code, rec.Body.String())
	}
	if waitPeerCleanup != nil {
		waitPeerCleanup()
	}
	if continuation {
		caches, _ := localMetacacheMgr.getBucket(t.Context(), bucket).cloneCaches()
		if len(caches) == 0 {
			t.Fatal("continuation did not create a cache")
		}
		for _, cache := range caches {
			if cache.status != scanStateError {
				t.Fatalf("incomplete cache was not marked failed: %+v", cache)
			}
		}
	}
	switchDisks(disks)
	retry := nullQuorumRequest(t, router, http.MethodGet, target)
	var recovered ListObjectsV2Response
	if retry.Code != http.StatusOK || xml.Unmarshal(retry.Body.Bytes(), &recovered) != nil {
		t.Fatalf("retry after metadata recovers: %d %s", retry.Code, retry.Body.String())
	}
	var keys []string
	for _, entry := range recovered.Contents {
		keys = append(keys, entry.Key)
	}
	want := []string{"a", "b", "z"}
	if continuation {
		want = []string{"b", "z"}
	}
	if !slices.Equal(keys, want) || recovered.IsTruncated {
		t.Fatalf("retry returned incomplete result: %+v", recovered)
	}
}

func TestListingCachePagination(t *testing.T) {
	z, bucket, router := nullQuorumBackend(t)
	for _, key := range []string{"a", "b", "c", "d"} {
		nullQuorumPut(t, z, bucket, key, []byte(key), ObjectOptions{})
	}
	var keys []string
	var token string
	var finished bool
	for page := 0; page < 8; page++ {
		target := getListObjectsV2URL("", bucket, "", "1", "", "", "")
		if token != "" {
			target += "&continuation-token=" + url.QueryEscape(token)
		}
		rec := nullQuorumRequest(t, router, http.MethodGet, target)
		var list ListObjectsV2Response
		if rec.Code != http.StatusOK || xml.Unmarshal(rec.Body.Bytes(), &list) != nil {
			t.Fatalf("page %d: %d %s", page, rec.Code, rec.Body.String())
		}
		for _, entry := range list.Contents {
			keys = append(keys, entry.Key)
		}
		if !list.IsTruncated {
			finished = true
			break
		}
		if list.NextContinuationToken == "" || list.NextContinuationToken == token {
			t.Fatal("pagination did not advance")
		}
		token = list.NextContinuationToken
	}
	if !finished || !slices.Equal(keys, []string{"a", "b", "c", "d"}) {
		t.Fatalf("pagination returned %v, finished=%v", keys, finished)
	}
}

// A cached full page must remain streamable even if a later key is slow. The
// completion barrier belongs at EOF, not before every page response.
func TestListingCacheFullPageDoesNotWaitForTail(t *testing.T) {
	z, bucket, router := nullQuorumBackend(t)
	disks, switchDisks := installListingDiskSwitch(z.serverPools[0].sets[0])
	for _, key := range []string{"a", "b", "c", "d", "e", "zz"} {
		nullQuorumPut(t, z, bucket, key, []byte(key), ObjectOptions{})
	}
	first := nullQuorumRequest(t, router, http.MethodGet, getListObjectsV2URL("", bucket, "", "1", "", "", ""))
	var page ListObjectsV2Response
	if first.Code != http.StatusOK || xml.Unmarshal(first.Body.Bytes(), &page) != nil || !page.IsTruncated {
		t.Fatalf("first page: %d %s", first.Code, first.Body.String())
	}
	resume := make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	defer release()
	var blocked, active atomic.Int64
	walkers := slices.Clone(disks)
	for i, disk := range walkers {
		walkers[i] = &listingPausedTailDisk{StorageAPI: disk, bucket: bucket, tail: "zz", resume: resume, blocked: &blocked, active: &active}
	}
	switchDisks(walkers)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	target := getListObjectsV2URL("", bucket, "", "1", "", "", "") + "&continuation-token=" + url.QueryEscape(page.NextContinuationToken)
	req, err := newTestSignedRequestV4(http.MethodGet, target, 0, nil, globalActiveCred.AccessKey, globalActiveCred.SecretKey, nil)
	if err != nil {
		t.Fatal(err)
	}
	response := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req.WithContext(ctx))
		response <- rec
	}()
	select {
	case rec := <-response:
		page = ListObjectsV2Response{}
		if rec.Code != http.StatusOK || xml.Unmarshal(rec.Body.Bytes(), &page) != nil || !page.IsTruncated || len(page.Contents) != 1 || page.Contents[0].Key != "b" || page.NextContinuationToken == "" {
			t.Fatalf("full page before tail: %d %s", rec.Code, rec.Body.String())
		}
		if blocked.Load() == 0 || active.Load() == 0 {
			t.Fatal("full page did not overlap the paused tail")
		}
	case <-ctx.Done():
		t.Fatal("full page waited for the paused tail")
	}
	release()
	waitForListingCondition(t, "disk walkers did not finish", func() bool { return active.Load() == 0 })
	target = getListObjectsV2URL("", bucket, "", "1000", "", "", "") + "&continuation-token=" + url.QueryEscape(page.NextContinuationToken)
	rec := nullQuorumRequest(t, router, http.MethodGet, target)
	page = ListObjectsV2Response{}
	if rec.Code != http.StatusOK || xml.Unmarshal(rec.Body.Bytes(), &page) != nil || page.IsTruncated {
		t.Fatalf("page after tail resumes: %d %s", rec.Code, rec.Body.String())
	}
	var keys []string
	for _, entry := range page.Contents {
		keys = append(keys, entry.Key)
	}
	if !slices.Equal(keys, []string{"c", "d", "e", "zz"}) {
		t.Fatalf("cached remainder lost keys: %v", keys)
	}
}

// Keep the error update pending at the actual peer RPC boundary. Other cache
// operations, including readers and abandoned-cache cleanup, remain live.
func listingCacheErrorPeer(t *testing.T, fail bool) (*peerRESTClient, <-chan struct{}, func()) {
	t.Helper()
	tg, err := grid.SetupTestGrid(2)
	if err != nil {
		t.Fatal(err)
	}
	ready, resume := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	t.Cleanup(func() {
		release()
		tg.Cleanup()
	})
	var entered sync.Once
	server := &peerRESTServer{}
	if err := getMetacacheListingRPC.Register(tg.Managers[1], server.GetMetacacheListingHandler); err != nil {
		t.Fatal(err)
	}
	if err := updateMetacacheListingRPC.Register(tg.Managers[1], func(m *metacache) (*metacache, *grid.RemoteErr) {
		if m.error != "" && m.error != "no longer used" {
			entered.Do(func() { close(ready) })
			select {
			case <-resume:
			case <-t.Context().Done():
				return nil, grid.NewRemoteErr(t.Context().Err())
			}
			if fail {
				return nil, grid.NewRemoteErr(errors.New("cache error update failed"))
			}
		}
		return server.UpdateMetacacheListingHandler(m)
	}); err != nil {
		t.Fatal(err)
	}
	return &peerRESTClient{gridConn: func() *grid.Connection { return tg.Managers[0].Connection(tg.Hosts[1]) }}, ready, release
}

func TestListingCacheRetainsErrorAfterPeerFailure(t *testing.T) {
	client, ready, release := listingCacheErrorPeer(t, true)
	o := listPathOptions{ID: mustGetUUID(), Bucket: "bucket"}
	m := o.newMetacache()
	mc := metaCacheRPC{meta: &m, rpc: client, o: o}
	done := make(chan struct{})
	go func() {
		defer close(done)
		mc.setErr("terminal listing failure")
	}()
	t.Cleanup(func() {
		release()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("setErr worker did not finish before grid cleanup")
		}
	})
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("error update did not reach peer")
	}
	release()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("failed peer update did not return")
	}
	if m.id != o.ID || m.bucket != o.Bucket || m.status != scanStateError || m.error != "terminal listing failure" || mc.getErr() == nil {
		t.Fatalf("peer failure erased local failure: %+v", m)
	}
	mc.setErr("later failure")
	if m.error != "terminal listing failure" {
		t.Fatalf("later failure replaced the terminal error: %+v", m)
	}
}
