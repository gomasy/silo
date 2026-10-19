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
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/tinylib/msgp/msgp"
)

// Replay real disk-walk metadata in its existing wire format, but pause before
// the final key. This keeps the healthy pool's listing alive after the other
// pool has already failed, without forging entries or listing errors.
type listingPausedTailDisk struct {
	StorageAPI
	bucket  string
	tail    string
	resume  <-chan struct{}
	blocked *atomic.Int64
	active  *atomic.Int64
}

func (d *listingPausedTailDisk) WalkDir(ctx context.Context, opts WalkDirOptions, out io.Writer) (result error) {
	if opts.Bucket != d.bucket {
		return d.StorageAPI.WalkDir(ctx, opts, out)
	}
	if d.active != nil {
		d.active.Add(1)
		defer d.active.Add(-1)
	}
	var source bytes.Buffer
	if err := d.StorageAPI.WalkDir(ctx, opts, &source); err != nil {
		return err
	}
	r := newMetacacheReader(bytes.NewReader(source.Bytes()))
	defer r.Close()
	compressed := s2.NewWriter(out)
	defer func() {
		if err := compressed.Close(); result == nil {
			result = err
		}
	}()
	w := msgp.NewWriter(compressed)
	if err := w.WriteByte(metacacheStreamVersion); err != nil {
		return err
	}
	for {
		entry, err := r.next()
		if err == io.EOF {
			if err = w.WriteBool(false); err != nil {
				return err
			}
			if err = w.Flush(); err != nil {
				return err
			}
			return nil
		}
		if err != nil {
			return err
		}
		if entry.name == d.tail {
			if err = w.Flush(); err != nil {
				return err
			}
			if err = compressed.Flush(); err != nil {
				return err
			}
			d.blocked.Add(1)
			select {
			case <-d.resume:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if err = w.WriteBool(true); err != nil {
			return err
		}
		if err = w.WriteString(entry.name); err != nil {
			return err
		}
		if err = w.WriteBytes(entry.metadata); err != nil {
			return err
		}
	}
}

func TestListingMergeDoesNotSkipFailedPool(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	z, bucket, router := listingPoolsBackend(t)
	broken, healthy := z.serverPools[0].sets[0], z.serverPools[1].sets[0]
	brokenDisks, switchBrokenDisks := installListingDiskSwitch(broken)
	healthyDisks, switchHealthyDisks := installListingDiskSwitch(healthy)
	body := bytes.Repeat([]byte{'a'}, 8192)
	mtime := time.Now().UTC().Add(-time.Hour)
	put := func(set *erasureObjects, name string, tm time.Time) {
		_, err := set.PutObject(ctx, bucket, name, mustGetPutObjReader(t, bytes.NewReader(body), int64(len(body)), "", ""), ObjectOptions{MTime: tm})
		if err != nil {
			t.Fatal(err)
		}
	}
	put(broken, "a", mtime)
	put(broken, "e", mtime)
	for _, name := range []string{"f", "g", "h", "i", "j", "k", "zz"} {
		put(healthy, name, mtime)
	}
	first := nullQuorumRequest(t, router, http.MethodGet, getListObjectsV2URL("", bucket, "", "1", "", "", ""))
	var page ListObjectsV2Response
	if first.Code != http.StatusOK || xml.Unmarshal(first.Body.Bytes(), &page) != nil || !page.IsTruncated || page.NextContinuationToken == "" || len(page.Contents) != 1 || page.Contents[0].Key != "a" {
		t.Fatalf("first page HTTP=%d body=%s", first.Code, first.Body.String())
	}
	firstToken := page.NextContinuationToken
	old := make([][]byte, 3)
	for i := range old {
		old[i] = mustReadNullQuorumMeta(t, brokenDisks[i], bucket, "e")
	}
	put(broken, "e", mtime.Add(time.Minute))
	for i := range old {
		if err := brokenDisks[i].WriteAll(ctx, bucket, "e/"+xlStorageFormatFile, old[i]); err != nil {
			t.Fatal(err)
		}
	}
	var reads, blocked, active atomic.Int64
	brokenWalkers := slices.Clone(brokenDisks)
	for i := range brokenWalkers {
		brokenWalkers[i] = &listingReadFailDisk{StorageAPI: brokenDisks[i], object: "e", reads: &reads}
		if i >= 8 {
			brokenWalkers[i] = &failedMetadataWalkDisk{StorageAPI: brokenWalkers[i]}
		}
	}
	switchBrokenDisks(brokenWalkers)
	resume := make(chan struct{})
	defer close(resume)
	healthyWalkers := slices.Clone(healthyDisks)
	for i, disk := range healthyWalkers {
		healthyWalkers[i] = &listingPausedTailDisk{StorageAPI: disk, bucket: bucket, tail: "zz", resume: resume, blocked: &blocked, active: &active}
	}
	switchHealthyDisks(healthyWalkers)

	target := getListObjectsV2URL("", bucket, "", "2", "", "", "") + "&continuation-token=" + url.QueryEscape(firstToken)
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
		t.Logf("continuation LIST=%d failed fallback ReadXL=%d paused healthy walkers=%d body=%s", rec.Code, reads.Load(), blocked.Load(), rec.Body.String())
		if reads.Load() == 0 || blocked.Load() == 0 {
			t.Fatal("schedule did not exercise fallback failure and unfinished healthy pool")
		}
		var apiErr APIErrorResponse
		if rec.Code != http.StatusServiceUnavailable || xml.Unmarshal(rec.Body.Bytes(), &apiErr) != nil || apiErr.Code != "SlowDownRead" {
			t.Fatalf("failed pool advanced the page instead of reporting read quorum: %d %s", rec.Code, rec.Body.String())
		}
		waitForListingCondition(t, "canceled disk walkers did not exit", func() bool { return active.Load() == 0 })
	case <-ctx.Done():
		t.Fatalf("no early response: %v reads=%d blocked=%d", ctx.Err(), reads.Load(), blocked.Load())
	}
}

func TestListingMergeCancellation(t *testing.T) {
	z, bucket, _ := nullQuorumBackend(t)
	for _, key := range []string{"a", "b", "zz"} {
		nullQuorumPut(t, z, bucket, key, []byte(key), ObjectOptions{})
	}
	set := z.serverPools[0].sets[0]
	var blocked, active atomic.Int64
	resume := make(chan struct{})
	defer close(resume)
	walkers := slices.Clone(set.getDisks())
	for i, disk := range walkers {
		walkers[i] = &listingPausedTailDisk{StorageAPI: disk, bucket: bucket, tail: "zz", resume: resume, blocked: &blocked, active: &active}
	}
	set.getDisks = func() []StorageAPI { return walkers }
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entries := make(chan metaCacheEntry, 10)
	done := make(chan error, 1)
	go func() {
		done <- z.listMerged(ctx, listPathOptions{Bucket: bucket, Recursive: true, Limit: 10}, entries)
	}()
	waitForListingCondition(t, "disk walks did not reach the paused tail", func() bool { return blocked.Load() > 0 })
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled merge returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled merge did not finish")
	}
	// listMerged must close its output and release every paused disk walk.
	for range entries {
	}
	waitForListingCondition(t, "canceled disk walkers did not exit", func() bool { return active.Load() == 0 })
}

func waitForListingCondition(t *testing.T, failure string, ready func() bool) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !ready() {
		select {
		case <-timer.C:
			t.Fatal(failure)
		case <-tick.C:
		}
	}
}
