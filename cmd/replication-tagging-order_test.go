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
	"context"
	"encoding/xml"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/bucket/replication"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/kms"
)

const r5TagStamp = ReservedMetadataPrefixLower + TaggingTimestamp

func r5Capacity(z *erasureServerPools) func() {
	var restores []func()
	for _, pool := range z.serverPools {
		pool.erasureDisksMu.Lock()
		for _, set := range pool.sets {
			old := pool.erasureDisks[set.setIndex]
			disks := append([]StorageAPI(nil), old...)
			for i := range disks {
				disks[i] = tagTestCapacityDisk{StorageAPI: disks[i]}
			}
			// GetDisks copies this list under the same mutex. Keep its function
			// stable while background IAM scans are using the fixture.
			pool.erasureDisks[set.setIndex] = disks
			restores = append(restores, func() {
				pool.erasureDisksMu.Lock()
				pool.erasureDisks[set.setIndex] = old
				pool.erasureDisksMu.Unlock()
			})
		}
		pool.erasureDisksMu.Unlock()
	}
	return func() {
		for _, restore := range restores {
			restore()
		}
	}
}

func TestAPITaggingCapacityConcurrentIAM(t *testing.T) {
	z, _ := consistencyPools(t)
	if _, _, err := initAPIHandlerTest(t.Context(), z, nil, MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	iam := globalIAMSys
	started, finished := make(chan struct{}), make(chan error, 1)
	go func() {
		close(started)
		for range 20 {
			if err := iam.Load(ctx, false); err != nil {
				finished <- err
				return
			}
		}
		finished <- nil
	}()
	<-started
	for range 5000 {
		r5Capacity(z)()
	}
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
}

func r5Request(t *testing.T, router http.Handler, cred auth.Credentials, method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	r, err := newTestSignedRequestV4(method, path, int64(len(body)), strings.NewReader(body), cred.AccessKey, cred.SecretKey, headers)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	return w
}

func r5Stored(t *testing.T, obj interface {
	GetObjectInfo(context.Context, string, string, ObjectOptions) (ObjectInfo, error)
}, bucket, name, vid, wantTags, wantStamp string,
) ObjectInfo {
	t.Helper()
	oi, err := obj.GetObjectInfo(t.Context(), bucket, name, ObjectOptions{VersionID: vid})
	if err != nil {
		t.Fatal(err)
	}
	if oi.UserTags != wantTags || oi.UserDefined[r5TagStamp] != wantStamp {
		t.Fatalf("%s(%s): tags=%q stamp=%q, want %q %q", name, vid, oi.UserTags, oi.UserDefined[r5TagStamp], wantTags, wantStamp)
	}
	return oi
}

// The request bytes are signed and enter the real API and storage implementation.
// Equal/stale retransmits may retain the existing 412 duplicate response.
func r5Receive(t *testing.T, obj ObjectLayer, router http.Handler, cred auth.Credentials, bucket, operation string, source ObjectInfo, stamp string, afterInit func()) {
	t.Helper()
	opts, _, err := putReplicationOpts(t.Context(), "", source)
	if err != nil {
		t.Fatal(err)
	}
	if operation == "multipart" {
		opts.Internal.SourceMTime = time.Time{}
	}
	headers := make(map[string]string)
	for k, vs := range opts.Header() {
		if len(vs) > 0 {
			headers[k] = vs[0]
		}
	}
	if stamp == "" {
		delete(headers, http.CanonicalHeaderKey(xhttp.MinIOSourceTaggingTimestamp))
		delete(headers, xhttp.MinIOSourceTaggingTimestamp)
	} else {
		headers[http.CanonicalHeaderKey(xhttp.MinIOSourceTaggingTimestamp)] = stamp
	}
	path := "/" + bucket + "/" + source.Name + "?versionId=" + source.VersionID
	var w *httptest.ResponseRecorder
	switch operation {
	case "copy", "copy-default":
		maps.Copy(headers, getCopyObjMetadata(source, ""))
		headers[xhttp.AmzCopySource] = "/" + bucket + "/" + source.Name + "?versionId=" + source.VersionID
		if operation == "copy" {
			headers[xhttp.AmzMetadataDirective] = "REPLACE"
		}
		w = r5Request(t, router, cred, http.MethodPut, path, "", headers)
	case "put":
		w = r5Request(t, router, cred, http.MethodPut, path, "data", headers)
	case "multipart":
		w = r5Request(t, router, cred, http.MethodPost, path+"&uploads", "", headers)
		if w.Code == http.StatusPreconditionFailed {
			return
		}
		if w.Code != http.StatusOK {
			t.Fatalf("init: %d %s", w.Code, w.Body.String())
		}
		var init struct {
			UploadID string `xml:"UploadId"`
		}
		if err := xml.Unmarshal(w.Body.Bytes(), &init); err != nil || init.UploadID == "" {
			t.Fatalf("init XML: %v %s", err, w.Body.String())
		}
		mi, err := obj.GetMultipartInfo(t.Context(), bucket, source.Name, init.UploadID, ObjectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if stamp != "" && mi.UserDefined[r5TagStamp] != stamp {
			t.Fatalf("upload persisted stamp=%q, want %q", mi.UserDefined[r5TagStamp], stamp)
		}
		partPath := "/" + bucket + "/" + source.Name + "?uploadId=" + url.QueryEscape(init.UploadID)
		ph := map[string]string{xhttp.MinIOSourceReplicationRequest: "true"}
		part := r5Request(t, router, cred, http.MethodPut, partPath+"&partNumber=1", "data", ph)
		if part.Code != http.StatusOK {
			t.Fatalf("part: %d %s", part.Code, part.Body.String())
		}
		if afterInit != nil {
			afterInit()
		}
		body := "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>" + canonicalizeETag(part.Header()[xhttp.ETag][0]) + "</ETag></Part></CompleteMultipartUpload>"
		ph[xhttp.MinIOSourceMTime] = source.ModTime.Format(time.RFC3339Nano)
		ph[xhttp.MinIOSourceETag] = source.ETag
		w = r5Request(t, router, cred, http.MethodPost, partPath, body, ph)
	}
	if w.Code != http.StatusOK && w.Code != http.StatusPreconditionFailed {
		t.Fatalf("%s: %d %s", operation, w.Code, w.Body.String())
	}
}

func TestAPITaggingReplicationOrdering(t *testing.T)    { r5APIOrdering(t, false) }
func TestAPITaggingReplicationOrderingKMS(t *testing.T) { r5APIOrdering(t, true) }
func r5APIOrdering(t *testing.T, encrypted bool) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: func(obj ObjectLayer, instance, bucket string, router http.Handler, cred auth.Credentials, t *testing.T) {
		defer r5Capacity(obj.(*erasureServerPools))()
		if _, err := globalBucketMetadataSys.Update(t.Context(), bucket, bucketVersioningConfig, enabledBucketVersioningConfig); err != nil {
			t.Fatal(err)
		}
		if encrypted {
			prev := GlobalKMS
			GlobalKMS = kms.NewStub("r5-tag-order")
			defer func() { GlobalKMS = prev }()
			sse := []byte(`<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID>r5-tag-order</KMSMasterKeyID></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>`)
			if _, err := globalBucketMetadataSys.Update(t.Context(), bucket, bucketSSEConfig, sse); err != nil {
				t.Fatal(err)
			}
		}
		base := time.Now().UTC().Add(-5 * time.Hour)
		for _, op := range []string{"copy", "copy-default", "put", "multipart"} {
			for _, version := range []string{"uuid", "null"} {
				t.Run(instance+"/"+op+"/"+version, func(t *testing.T) {
					name := op + "-" + version
					vid := mustGetUUID()
					if version == "null" {
						vid = nullVersionID
					}
					original, err := obj.PutObject(t.Context(), bucket, name, mustGetPutObjReader(t, strings.NewReader("data"), 4, "", ""), ObjectOptions{Versioned: true, VersionID: vid, UserDefined: map[string]string{xhttp.AmzObjectTagging: "key=original", r5TagStamp: base.Format(time.RFC3339Nano)}})
					if err != nil {
						t.Fatal(err)
					}
					if op == "multipart" {
						// The production sender uses multipart only for multipart
						// sources. Preserve a real multipart ETag and part layout.
						metadata := maps.Clone(original.UserDefined)
						metadata[xhttp.AmzObjectTagging] = original.UserTags
						mp, err := obj.NewMultipartUpload(t.Context(), bucket, name, ObjectOptions{Versioned: true, VersionID: vid, UserDefined: metadata})
						if err != nil {
							t.Fatal(err)
						}
						part, err := obj.PutObjectPart(t.Context(), bucket, name, mp.UploadID, 1, mustGetPutObjReader(t, strings.NewReader("data"), 4, "", ""), ObjectOptions{})
						if err != nil {
							t.Fatal(err)
						}
						original, err = obj.CompleteMultipartUpload(t.Context(), bucket, name, mp.UploadID, []CompletePart{{PartNumber: 1, ETag: part.ETag}}, ObjectOptions{Versioned: true})
						if err != nil {
							t.Fatal(err)
						}
					}
					// A later unrelated version must not contribute tags to an explicitly addressed UUID/null version.
					latest, err := obj.PutObject(t.Context(), bucket, name, mustGetPutObjReader(t, strings.NewReader("other"), 5, "", ""), ObjectOptions{Versioned: true, UserDefined: map[string]string{xhttp.AmzObjectTagging: "key=latest", r5TagStamp: base.Add(10 * time.Hour).Format(time.RFC3339Nano)}})
					if err != nil {
						t.Fatal(err)
					}
					for _, event := range []struct {
						name, tags string
						hours      int
						wantTags   string
						wantHours  int
					}{
						{"delete", "", 3, "", 3}, {"stale", "key=stale", 2, "", 3}, {"equal-conflict", "key=conflict", 3, "", 3}, {"newer", "key=new", 4, "key=new", 4},
					} {
						t.Run(event.name, func(t *testing.T) {
							source := original
							source.VersionID = vid
							source.UserDefined = maps.Clone(original.UserDefined)
							source.UserTags = event.tags
							stamp := base.Add(time.Duration(event.hours) * time.Hour).Format(time.RFC3339Nano)
							source.UserDefined[r5TagStamp] = stamp
							r5Receive(t, obj, router, cred, bucket, op, source, stamp, nil)
							r5Stored(t, obj, bucket, name, vid, event.wantTags, base.Add(time.Duration(event.wantHours)*time.Hour).Format(time.RFC3339Nano))
							r5Stored(t, obj, bucket, name, latest.VersionID, "key=latest", base.Add(10*time.Hour).Format(time.RFC3339Nano))
						})
					}
					source := original
					source.VersionID = vid
					source.UserTags = "key=unversioned-event"
					r5Receive(t, obj, router, cred, bucket, op, source, "", nil)
					r5Stored(t, obj, bucket, name, vid, "key=new", base.Add(4*time.Hour).Format(time.RFC3339Nano))
					get := r5Request(t, router, cred, http.MethodGet, "/"+bucket+"/"+name+"?versionId="+vid, "", nil)
					if get.Code != http.StatusOK || get.Body.String() != "data" {
						t.Fatalf("plaintext GET: %d %q", get.Code, get.Body.String())
					}
				})
			}
		}
	}})
}

func TestAPITaggingMultipartCommitRechecksRevision(t *testing.T) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: func(obj ObjectLayer, instance, bucket string, router http.Handler, cred auth.Credentials, t *testing.T) {
		defer r5Capacity(obj.(*erasureServerPools))()
		if _, err := globalBucketMetadataSys.Update(t.Context(), bucket, bucketVersioningConfig, enabledBucketVersioningConfig); err != nil {
			t.Fatal(err)
		}
		base := time.Now().UTC().Add(-time.Hour)
		source := ObjectInfo{Name: "commit-recheck", VersionID: mustGetUUID(), ModTime: base, UserTags: "key=incoming", UserDefined: map[string]string{r5TagStamp: base.Format(time.RFC3339Nano)}}
		later := base.Add(time.Minute).Format(time.RFC3339Nano)
		r5Receive(t, obj, router, cred, bucket, "multipart", source, base.Format(time.RFC3339Nano), func() {
			_, err := obj.PutObject(t.Context(), bucket, source.Name, mustGetPutObjReader(t, strings.NewReader("data"), 4, "", ""), ObjectOptions{Versioned: true, VersionID: source.VersionID, MTime: base, UserDefined: map[string]string{r5TagStamp: later}})
			if err != nil {
				t.Fatal(err)
			}
		})
		r5Stored(t, obj, bucket, source.Name, source.VersionID, "", later)
		t.Logf("%s: deletion committed between initiation and completion survived", instance)
	}})
}

func TestAPILocalTaggingAlwaysAdvancesRevision(t *testing.T) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: func(obj ObjectLayer, instance, bucket string, router http.Handler, cred auth.Credentials, t *testing.T) {
		defer r5Capacity(obj.(*erasureServerPools))()
		if _, err := globalBucketMetadataSys.Update(t.Context(), bucket, bucketVersioningConfig, enabledBucketVersioningConfig); err != nil {
			t.Fatal(err)
		}
		oi, err := obj.PutObject(t.Context(), bucket, "local-tags", mustGetPutObjReader(t, strings.NewReader("data"), 4, "", ""), ObjectOptions{Versioned: true})
		if err != nil {
			t.Fatal(err)
		}
		path := "/" + bucket + "/local-tags?tagging&versionId=" + oi.VersionID
		for n, method := range []string{http.MethodPut, http.MethodDelete, http.MethodDelete, http.MethodPut} {
			body := ""
			want := ""
			status := http.StatusNoContent
			if method == http.MethodPut {
				status = http.StatusOK
				body = "<Tagging><TagSet/></Tagging>"
				if n == 0 {
					want = "key=local"
					body = "<Tagging><TagSet><Tag><Key>key</Key><Value>local</Value></Tag></TagSet></Tagging>"
				}
			}
			before := time.Now().UTC()
			w := r5Request(t, router, cred, method, path, body, nil)
			if w.Code != status {
				t.Fatalf("%s: %d %s", method, w.Code, w.Body.String())
			}
			now, err := obj.GetObjectInfo(t.Context(), bucket, "local-tags", ObjectOptions{VersionID: oi.VersionID})
			if err != nil {
				t.Fatal(err)
			}
			stamp, err := time.Parse(time.RFC3339Nano, now.UserDefined[r5TagStamp])
			if err != nil || stamp.Before(before) || now.UserTags != want {
				t.Fatalf("local %s: tags=%q timestamp=%q err=%v", method, now.UserTags, now.UserDefined[r5TagStamp], err)
			}
			if !now.ModTime.Equal(oi.ModTime) {
				t.Fatal("tagging changed the object's data modification time")
			}
			t.Logf("%s mutation %d persisted tags=%q timestamp=%s", instance, n, now.UserTags, stamp)
		}
		// Ordinary COPY with empty REPLACE has the same local-deletion semantics.
		before := time.Now().UTC()
		headers := map[string]string{xhttp.AmzCopySource: "/" + bucket + "/local-tags?versionId=" + oi.VersionID, xhttp.AmzMetadataDirective: "REPLACE", xhttp.AmzTagDirective: "REPLACE"}
		w := r5Request(t, router, cred, http.MethodPut, "/"+bucket+"/copied-empty", "", headers)
		if w.Code != http.StatusOK {
			t.Fatalf("copy: %d %s", w.Code, w.Body.String())
		}
		copied, err := obj.GetObjectInfo(t.Context(), bucket, "copied-empty", ObjectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		stamp, err := time.Parse(time.RFC3339Nano, copied.UserDefined[r5TagStamp])
		if err != nil || stamp.Before(before) || copied.UserTags != "" {
			t.Fatalf("local COPY: %v %+v", err, copied)
		}
	}})
}

func TestTaggingTimestampWire(t *testing.T) {
	now := time.Now().UTC()
	for _, tag := range []string{"", "key=value"} {
		for _, stamp := range []string{"", now.Format(time.RFC3339Nano), "invalid"} {
			t.Run(fmt.Sprintf("%s/%s", tag, stamp), func(t *testing.T) {
				source := ObjectInfo{ModTime: now.Add(-time.Hour), UserTags: tag, UserDefined: map[string]string{}}
				if stamp != "" {
					source.UserDefined[r5TagStamp] = stamp
				}
				opts, _, err := putReplicationOpts(t.Context(), "", source)
				if stamp == "invalid" {
					if err == nil {
						t.Fatal("invalid timestamp accepted")
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				want := stamp
				if want == "" && tag != "" {
					want = source.ModTime.Format(time.RFC3339Nano)
				}
				if got := opts.Header().Get(xhttp.MinIOSourceTaggingTimestamp); got != want {
					t.Fatalf("wire timestamp=%q want %q", got, want)
				}
			})
		}
	}
}

func TestAPIPoolsTaggingReplicaDeletion(t *testing.T) {
	z, bucket := consistencyPools(t)
	defer r5Capacity(z)()
	if err := newTestConfig(globalMinioDefaultRegion, z); err != nil {
		t.Fatal(err)
	}
	if _, err := globalBucketMetadataSys.Update(t.Context(), bucket, bucketVersioningConfig, enabledBucketVersioningConfig); err != nil {
		t.Fatal(err)
	}
	router := initTestAPIEndPoints(z, nil)
	base := time.Now().UTC().Add(-5 * time.Hour)
	for _, op := range []string{"copy", "copy-default", "put", "multipart"} {
		for _, kind := range []string{"uuid", "null"} {
			t.Run(op+"/"+kind, func(t *testing.T) {
				vid := mustGetUUID()
				if kind == "null" {
					vid = nullVersionID
				}
				name := "pool-tags-" + op + "-" + kind
				var source ObjectInfo
				for pool := range 2 {
					tag := "key=stale-pool"
					ts := base
					if pool == 1 {
						tag = ""
						ts = base.Add(3 * time.Hour)
					}
					source = putConsistencyObject(t, z, bucket, name, pool, "data", ObjectOptions{Versioned: true, VersionID: vid, MTime: base, UserDefined: map[string]string{xhttp.AmzObjectTagging: tag, r5TagStamp: ts.Format(time.RFC3339Nano)}})
				}
				// Clear all copies through the signed local handler, then replay a stale incoming state.
				req := r5Request(t, router, globalActiveCred, http.MethodDelete, "/"+bucket+"/"+name+"?tagging&versionId="+vid, "", nil)
				if req.Code != http.StatusNoContent {
					t.Fatalf("DELETE: %d %s", req.Code, req.Body.String())
				}
				current, err := z.GetObjectInfo(t.Context(), bucket, name, ObjectOptions{VersionID: vid})
				if err != nil {
					t.Fatal(err)
				}
				deletedAt := current.UserDefined[r5TagStamp]
				for pool := range 2 {
					r5Stored(t, z.serverPools[pool], bucket, name, vid, "", deletedAt)
				}
				source.VersionID = vid
				source.UserTags = "key=delayed"
				source.UserDefined = map[string]string{r5TagStamp: base.Add(time.Hour).Format(time.RFC3339Nano)}
				r5Receive(t, z, router, globalActiveCred, bucket, op, source, source.UserDefined[r5TagStamp], nil)
				// The addressed version must remain readable through normal routing.
				r5Stored(t, z, bucket, name, vid, "", deletedAt)
				// Existing duplicate suppression may leave both identical tombstones; any retained copy must be correct.
				for pool := range 2 {
					got, err := z.serverPools[pool].GetObjectInfo(t.Context(), bucket, name, ObjectOptions{VersionID: vid})
					if isErrVersionNotFound(err) {
						continue
					}
					if err != nil {
						t.Fatal(err)
					}
					if got.UserTags != "" || got.UserDefined[r5TagStamp] != deletedAt {
						t.Fatalf("pool %d: tags=%q stamp=%q want deletion %q", pool, got.UserTags, got.UserDefined[r5TagStamp], deletedAt)
					}
				}
			})
		}
	}
}

func TestAPITaggingSSECRotationPreservesDeletionRevision(t *testing.T) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: func(obj ObjectLayer, instance, bucket string, router http.Handler, cred auth.Credentials, t *testing.T) {
		defer r5Capacity(obj.(*erasureServerPools))()
		oldTLS := globalIsTLS
		globalIsTLS = true
		defer func() { globalIsTLS = oldTLS }()
		if _, err := globalBucketMetadataSys.Update(t.Context(), bucket, bucketVersioningConfig, enabledBucketVersioningConfig); err != nil {
			t.Fatal(err)
		}
		keys := [][]byte{[]byte(strings.Repeat("a", 32)), []byte(strings.Repeat("b", 32)), []byte(strings.Repeat("c", 32)), []byte(strings.Repeat("d", 32))}
		const name = "tag-rotation"
		putCopyChecksumSource(t, router, cred, bucket, name, []byte("data"), ssecKeyHeaders(keys[0], false))
		oi, err := obj.GetObjectInfo(t.Context(), bucket, name, ObjectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		base := time.Now().UTC().Add(-time.Hour)
		for i, event := range []struct {
			tags      string
			delta     int
			wantTags  string
			wantDelta int
		}{
			{"key=live", 1, "key=live", 1}, {"", 3, "", 3}, {"key=stale", 2, "", 3},
		} {
			h := ssecKeyHeaders(keys[i], true)
			maps.Copy(h, ssecKeyHeaders(keys[i+1], false))
			h[xhttp.AmzObjectTagging] = event.tags
			h[xhttp.AmzTagDirective] = "REPLACE"
			h[xhttp.MinIOSourceTaggingTimestamp] = base.Add(time.Duration(event.delta) * time.Minute).Format(time.RFC3339Nano)
			sendReplicaLockCopy(t, router, cred, bucket, name, oi.VersionID, h)
			r5Stored(t, obj, bucket, name, oi.VersionID, event.wantTags, base.Add(time.Duration(event.wantDelta)*time.Minute).Format(time.RFC3339Nano))
		}
		get := r5Request(t, router, cred, http.MethodGet, "/"+bucket+"/"+name+"?versionId="+oi.VersionID, "", ssecKeyHeaders(keys[3], false))
		if get.Code != http.StatusOK || get.Body.String() != "data" {
			t.Fatalf("%s GET after rotations: %d %q", instance, get.Code, get.Body.String())
		}
	}})
}

func TestTaggingRepeatedValueNeedsRevisionDelivery(t *testing.T) {
	for _, tag := range []string{"", "key=same"} {
		now := time.Now().UTC()
		source := ObjectInfo{ModTime: now, UserTags: tag, UserDefined: map[string]string{r5TagStamp: now.Add(time.Hour).Format(time.RFC3339Nano)}}
		target := minio.ObjectInfo{LastModified: now}
		if tag != "" {
			target.UserTags = map[string]string{"key": "same"}
			target.UserTagCount = 1
		}
		if got := getReplicationAction(source, target, replication.MetadataReplicationType); got != replicateMetadata {
			t.Errorf("equal tags %q suppress a newer revision: got %s; an intervening delayed deletion can win", tag, got)
		}
	}
}

func TestTaggingProductionCopyWireShape(t *testing.T) {
	got := make(chan http.Header, 1)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r.Header.Clone()
		w.Header().Set(xhttp.ContentType, "application/xml")
		w.Write([]byte(`<CopyObjectResult><LastModified>2026-09-15T01:00:00Z</LastModified><ETag>"abc"</ETag></CopyObjectResult>`))
	}))
	defer peer.Close()
	c, err := minio.New(strings.TrimPrefix(peer.URL, "http://"), &minio.Options{Region: "us-east-1", MaxRetries: 1})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	oi := ObjectInfo{Name: "object", ModTime: now, ETag: "abc", VersionID: mustGetUUID()}
	core := minio.Core{Client: c}
	_, err = core.CopyObject(t.Context(), "bucket", "object", "bucket", "object", getCopyObjMetadata(oi, ""), minio.CopySrcOptions{VersionID: oi.VersionID}, minio.PutObjectOptions{Internal: minio.AdvancedPutOptions{SourceVersionID: oi.VersionID, ReplicationRequest: true, TaggingTimestamp: now}})
	if err != nil {
		t.Fatal(err)
	}
	h := <-got
	t.Logf("SDK wire: metadata-directive=%q tagging-directive=%q tagging=%q time=%q", h.Get(xhttp.AmzMetadataDirective), h.Get(xhttp.AmzTagDirective), h.Get(xhttp.AmzObjectTagging), h.Get(xhttp.MinIOSourceTaggingTimestamp))
	if h.Get(xhttp.AmzMetadataDirective) != "" || h.Get(xhttp.AmzTagDirective) != "REPLACE" || h.Get(xhttp.MinIOSourceTaggingTimestamp) != now.Format(time.RFC3339Nano) {
		t.Fatalf("unexpected SDK wire: %v", h)
	}
}

func TestLocalTaggingCommitCannotRegressRevision(t *testing.T) {
	z, bucket := consistencyPools(t)
	incoming := "2026-09-15T01:00:00Z"
	newer := "2026-09-15T02:00:00Z"
	newest := "2026-09-15T03:00:00Z"
	vid := mustGetUUID()
	name := "inverted-local-tags"
	for pool := range 2 {
		putConsistencyObject(t, z, bucket, name, pool, "data", ObjectOptions{Versioned: true, VersionID: vid, UserDefined: map[string]string{r5TagStamp: []string{newer, newest}[pool]}})
	}
	oi, err := z.PutObjectTags(t.Context(), bucket, name, "key=after-delete", ObjectOptions{VersionID: vid, UserDefined: map[string]string{r5TagStamp: incoming}})
	if err != nil {
		t.Fatal(err)
	}
	want := "2026-09-15T03:00:00.000000001Z"
	for pool := range 2 {
		r5Stored(t, z.serverPools[pool], bucket, name, vid, "key=after-delete", want)
	}
	if oi.UserDefined[r5TagStamp] != want {
		t.Fatalf("response stamp=%q want %q", oi.UserDefined[r5TagStamp], want)
	}
	// The single-set guard also applies when the pools dispatcher is bypassed.
	_, err = z.serverPools[0].PutObjectTags(t.Context(), bucket, name, "", ObjectOptions{VersionID: vid, UserDefined: map[string]string{r5TagStamp: incoming}})
	if err != nil {
		t.Fatal(err)
	}
	r5Stored(t, z.serverPools[0], bucket, name, vid, "", "2026-09-15T03:00:00.000000002Z")
}

func TestTaggingReplicaContentDuplicateGuard(t *testing.T) {
	stamp := time.Date(2026, 9, 15, 1, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name             string
		delta            int
		stored           string
		trusted, replica bool
		ifMatch, ifNone  string
		wantSkip         bool
	}{
		{name: "newer", delta: 1, trusted: true, replica: true},
		{name: "equal", trusted: true, replica: true, wantSkip: true},
		{name: "older", delta: -1, trusted: true, replica: true, wantSkip: true},
		{name: "invalid-stored", stored: "invalid", delta: 1, trusted: true, replica: true},
		{name: "untrusted", delta: 1, wantSkip: true},
		{name: "marker-only", delta: 1, trusted: true, wantSkip: true},
		{name: "if-match-fails", delta: 1, trusted: true, replica: true, ifMatch: "other", wantSkip: true},
		{name: "if-none-match-fails", delta: 1, trusted: true, replica: true, ifNone: "etag", wantSkip: true},
		{name: "if-match-passes", delta: 1, trusted: true, replica: true, ifMatch: "etag"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := withReplicationTrust(t.Context(), tc.trusted, tc.replica)
			r := httptest.NewRequest(http.MethodPut, "/bucket/object", nil).WithContext(ctx)
			if tc.ifMatch != "" {
				r.Header.Set(xhttp.IfMatch, tc.ifMatch)
			}
			if tc.ifNone != "" {
				r.Header.Set(xhttp.IfNoneMatch, tc.ifNone)
			}
			stored := tc.stored
			if stored == "" {
				stored = stamp.Format(time.RFC3339Nano)
			}
			oi := ObjectInfo{ModTime: stamp, VersionID: mustGetUUID(), ETag: "etag", UserDefined: map[string]string{r5TagStamp: stored}}
			opts := ObjectOptions{VersionID: oi.VersionID, PreserveETag: oi.ETag, ReplicationRequest: tc.trusted, ReplicationSourceTaggingTimestamp: stamp.Add(time.Duration(tc.delta) * time.Second)}
			if skip := checkPreconditionsPUT(ctx, httptest.NewRecorder(), r, oi, opts); skip != tc.wantSkip {
				t.Fatalf("skip=%v, want %v", skip, tc.wantSkip)
			}
		})
	}
}

func TestAPITaggingUnqualifiedCopyOrdering(t *testing.T) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: func(obj ObjectLayer, instance, bucket string, router http.Handler, cred auth.Credentials, t *testing.T) {
		defer r5Capacity(obj.(*erasureServerPools))()
		stamp := time.Now().UTC().Add(-time.Hour)
		oi, err := obj.PutObject(t.Context(), bucket, "unqualified-tags", mustGetPutObjReader(t, strings.NewReader("data"), 4, "", ""), ObjectOptions{UserDefined: map[string]string{xhttp.AmzObjectTagging: "key=stored", r5TagStamp: stamp.Format(time.RFC3339Nano)}})
		if err != nil {
			t.Fatal(err)
		}
		source := oi
		source.UserTags = ""
		source.UserDefined = maps.Clone(oi.UserDefined)
		r5Receive(t, obj, router, cred, bucket, "copy", source, stamp.Format(time.RFC3339Nano), nil)
		r5Stored(t, obj, bucket, oi.Name, "", "key=stored", stamp.Format(time.RFC3339Nano))
		later := stamp.Add(time.Minute).Format(time.RFC3339Nano)
		r5Receive(t, obj, router, cred, bucket, "copy-default", source, later, nil)
		r5Stored(t, obj, bucket, oi.Name, "", "", later)
		source.UserTags = "key=delayed"
		r5Receive(t, obj, router, cred, bucket, "copy-default", source, stamp.Format(time.RFC3339Nano), nil)
		r5Stored(t, obj, bucket, oi.Name, "", "", later)
		t.Logf("%s: unqualified COPY keeps stored ties and ordered deletion", instance)
	}})
}
