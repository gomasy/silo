// Copyright (c) 2026 PGSTY
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/xml"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/minio/minio/internal/auth"
	xhttp "github.com/minio/minio/internal/http"
)

// Exercise authenticated handlers and actual disk metadata, including the
// response headers consumers see after replication has completed.
func TestAPIReplicaContentEncoding(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: testAPIReplicaContentEncoding})
}

func testAPIReplicaContentEncoding(obj ObjectLayer, instance, bucket string, router http.Handler, owner auth.Credentials, t *testing.T) {
	ordinary := newObjectAttributesAuthzUser(t, instance, bucket, `"s3:PutObject","s3:GetObject"`)
	replicator := newObjectAttributesAuthzUser(t, instance, bucket, `"s3:PutObject","s3:GetObject","s3:ReplicateObject"`)
	for _, mode := range []string{"ordinary", "untrusted-marker", "replica"} {
		for _, tc := range []struct{ name, wire, want string }{
			{"bare", "aws-chunked", ""}, {"mixed", "aws-chunked,gzip", "gzip"}, {"gzip", "gzip", "gzip"},
		} {
			for _, operation := range []string{"put", "copy-replace", "multipart"} {
				t.Run(instance+"/"+mode+"/"+tc.name+"/"+operation, func(t *testing.T) {
					object := mode + "/" + tc.name + "/" + operation
					payload := replicaEncodingPayload(t, tc.want)
					creds := ordinary
					headers := map[string]string{xhttp.ContentEncoding: tc.wire, xhttp.ContentType: "application/octet-stream", "X-Amz-Meta-Source": "encoding-test"}
					if mode != "ordinary" {
						headers[xhttp.MinIOSourceReplicationRequest] = "true"
					}
					if mode == "replica" {
						creds = replicator
						headers[xhttp.AmzBucketReplicationStatus] = "REPLICA"
					}
					send := func(method, target string, data []byte, hdrs map[string]string) *httptest.ResponseRecorder {
						t.Helper()
						req, err := newTestSignedRequestV4(method, target, int64(len(data)), bytes.NewReader(data), creds.AccessKey, creds.SecretKey, hdrs)
						if err != nil {
							t.Fatal(err)
						}
						return replicaEncodingServe(t, router, req, http.StatusOK)
					}
					switch operation {
					case "put":
						if strings.Contains(tc.wire, "aws-chunked") {
							req := replicaEncodingStream(t, getPutObjectURL("", bucket, object), payload, creds, headers)
							replicaEncodingServe(t, router, req, http.StatusOK)
						} else {
							send(http.MethodPut, getPutObjectURL("", bucket, object), payload, headers)
						}
					case "copy-replace":
						source := object + "-source"
						if _, err := obj.PutObject(t.Context(), bucket, source, mustGetPutObjReader(t, bytes.NewReader(payload), int64(len(payload)), "", ""), ObjectOptions{}); err != nil {
							t.Fatal(err)
						}
						headers[xhttp.AmzCopySource] = url.QueryEscape("/" + bucket + "/" + source)
						headers[xhttp.AmzMetadataDirective] = replaceDirective
						send(http.MethodPut, getCopyObjectURL("", bucket, object), nil, headers)
					case "multipart":
						rec := send(http.MethodPost, getNewMultipartURL("", bucket, object), nil, headers)
						var init InitiateMultipartUploadResponse
						if err := xml.Unmarshal(rec.Body.Bytes(), &init); err != nil {
							t.Fatal(err)
						}
						// Part/completion metadata must not replace the encoding saved at initiation.
						partHeaders := map[string]string{xhttp.ContentEncoding: "br"}
						if mode == "replica" {
							partHeaders[xhttp.MinIOSourceReplicationRequest] = "true"
							partHeaders[xhttp.AmzBucketReplicationStatus] = "REPLICA"
						}
						part := send(http.MethodPut, getPutObjectPartURL("", bucket, object, init.UploadID, "1"), payload, partHeaders)
						partETags := part.Header()[xhttp.ETag]
						if len(partETags) != 1 {
							t.Fatalf("missing part ETag: %#v", part.Header())
						}
						complete, err := xml.Marshal(CompleteMultipartUpload{Parts: []CompletePart{{PartNumber: 1, ETag: canonicalizeETag(partETags[0])}}})
						if err != nil {
							t.Fatal(err)
						}
						send(http.MethodPost, getCompleteMultipartUploadURL("", bucket, object, init.UploadID), complete, partHeaders)
					}
					assertReplicaEncodingObject(t, obj, router, owner, bucket, object, tc.want, payload)
					info, err := obj.GetObjectInfo(t.Context(), bucket, object, ObjectOptions{})
					if err != nil {
						t.Fatal(err)
					}
					if got := info.UserDefined[xhttp.AmzBucketReplicationStatus]; (got == "REPLICA") != (mode == "replica") {
						t.Errorf("replica status %q for mode %s", got, mode)
					}
					if info.ContentType != "application/octet-stream" {
						t.Errorf("content-type=%q", info.ContentType)
					}
					if value, ok := caseInsensitiveMap(info.UserDefined).Lookup("x-amz-meta-source"); !ok || value != "encoding-test" {
						t.Errorf("user metadata lost: %#v", info.UserDefined)
					}
				})
			}
		}
	}
	t.Run(instance+"/unauthorized-replica", func(t *testing.T) {
		object := "denied-replica"
		req := replicaEncodingStream(t, getPutObjectURL("", bucket, object), []byte("denied"), ordinary, map[string]string{xhttp.ContentEncoding: "aws-chunked", xhttp.MinIOSourceReplicationRequest: "true", xhttp.AmzBucketReplicationStatus: "REPLICA"})
		rec := replicaEncodingServe(t, router, req, http.StatusForbidden)
		var response APIErrorResponse
		if err := xml.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if response.Code != "AccessDenied" {
			t.Fatalf("expected permission denial, got %s", response.Code)
		}
		if _, err := obj.GetObjectInfo(t.Context(), bucket, object, ObjectOptions{}); err == nil {
			t.Error("denied replica created an object")
		}
	})
}

func replicaEncodingPayload(t *testing.T, encoding string) []byte {
	t.Helper()
	data := bytes.Repeat([]byte("replica encoding payload\n"), 128)
	if encoding != "gzip" {
		return data
	}
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func replicaEncodingStream(t *testing.T, target string, data []byte, creds auth.Credentials, headers map[string]string) *http.Request {
	t.Helper()
	const chunkSize = 64
	body := bytes.NewReader(data)
	req, err := newTestStreamingRequest(http.MethodPut, target, int64(len(data)), chunkSize, body)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	now := UTCNow()
	signature, err := signStreamingRequest(req, creds.AccessKey, creds.SecretKey, now)
	if err != nil {
		t.Fatal(err)
	}
	req, err = assembleStreamingChunks(req, body, chunkSize, creds.SecretKey, signature, now)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func replicaEncodingServe(t *testing.T, router http.Handler, req *http.Request, want int) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != want {
		t.Fatalf("%s %s: status=%d want=%d body=%s", req.Method, req.URL, rec.Code, want, rec.Body.String())
	}
	return rec
}

func assertReplicaEncodingObject(t *testing.T, obj ObjectLayer, router http.Handler, creds auth.Credentials, bucket, object, encoding string, data []byte) {
	t.Helper()
	info, err := obj.GetObjectInfo(t.Context(), bucket, object, ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if info.ContentEncoding != encoding {
		t.Errorf("persisted content-encoding=%q want=%q", info.ContentEncoding, encoding)
	}
	if encoding == "" {
		if _, present := info.UserDefined["content-encoding"]; present {
			t.Error("transport-only content-encoding key persisted")
		}
	}
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		req, err := newTestSignedRequestV4(method, getPutObjectURL("", bucket, object), 0, nil, creds.AccessKey, creds.SecretKey, nil)
		if err != nil {
			t.Fatal(err)
		}
		rec := replicaEncodingServe(t, router, req, http.StatusOK)
		if got := rec.Header().Get(xhttp.ContentEncoding); got != encoding {
			t.Errorf("%s content-encoding=%q want=%q", method, got, encoding)
		}
		if encoding == "" {
			if _, present := rec.Header()[xhttp.ContentEncoding]; present {
				t.Errorf("%s sent an empty/transport encoding header", method)
			}
		}
		if method == http.MethodGet && !bytes.Equal(rec.Body.Bytes(), data) {
			t.Errorf("GET body differs: got %d bytes want %d", rec.Body.Len(), len(data))
		}
	}
}

func TestAPISnowballReplicaContentEncoding(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: func(obj ObjectLayer, instance, bucket string, router http.Handler, creds auth.Credentials, t *testing.T) {
		for _, tc := range []struct {
			name string
			pax  map[string]string
			want string
		}{
			{name: "no-pax"},
			{name: "pax-without-encoding", pax: map[string]string{"minio.metadata.Content-Type": "application/octet-stream"}},
			{name: "pax-bare", pax: map[string]string{"minio.metadata.Content-Encoding": "aws-chunked"}},
			{name: "pax-mixed", pax: map[string]string{"minio.metadata.Content-Encoding": "aws-chunked,gzip"}, want: "gzip"},
		} {
			t.Run(instance+"/"+tc.name, func(t *testing.T) {
				object := "snowball/" + tc.name
				data := replicaEncodingPayload(t, tc.want)
				var archive bytes.Buffer
				tw := tar.NewWriter(&archive)
				if err := tw.WriteHeader(&tar.Header{Name: object, Mode: 0o600, Size: int64(len(data)), PAXRecords: tc.pax}); err != nil {
					t.Fatal(err)
				}
				if _, err := tw.Write(data); err != nil {
					t.Fatal(err)
				}
				if err := tw.Close(); err != nil {
					t.Fatal(err)
				}
				var ordinaryMetadata map[string]string
				// An unauthorized entry in a REPLICA request is rejected. Compare the
				// same archive across ordinary and authorized replica requests instead.
				for _, replica := range []bool{false, true} {
					headers := map[string]string{
						xhttp.ContentEncoding: "aws-chunked", xhttp.AmzSnowballExtract: "true",
						xhttp.ContentType: "application/x-tar", xhttp.CacheControl: "max-age=123",
						"X-Amz-Meta-Archive": "outer-request",
					}
					if replica {
						headers[xhttp.MinIOSourceReplicationRequest] = "true"
						headers[xhttp.AmzBucketReplicationStatus] = "REPLICA"
					}
					req := replicaEncodingStream(t, getPutObjectURL("", bucket, "archive.tar"), archive.Bytes(), creds, headers)
					replicaEncodingServe(t, router, req, http.StatusOK)
					assertReplicaEncodingObject(t, obj, router, creds, bucket, object, tc.want, data)
					info, err := obj.GetObjectInfo(t.Context(), bucket, object, ObjectOptions{})
					if err != nil {
						t.Fatal(err)
					}
					metadata := maps.Clone(info.UserDefined)
					for _, key := range []string{xhttp.AmzBucketReplicationStatus, ReservedMetadataPrefixLower + ReplicaStatus, ReservedMetadataPrefixLower + ReplicaTimestamp, "etag"} {
						delete(metadata, key)
					}
					if !replica {
						ordinaryMetadata = metadata
					} else if !reflect.DeepEqual(metadata, ordinaryMetadata) {
						t.Errorf("replica inherited ordinary archive metadata: got %#v want %#v", metadata, ordinaryMetadata)
					}
				}
			})
		}
	}})
}
