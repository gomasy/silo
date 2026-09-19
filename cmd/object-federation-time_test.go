// Copyright (c) 2026 PGSTY
// SPDX-License-Identifier: AGPL-3.0-only

package cmd

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio/internal/amztime"
	"github.com/minio/minio/internal/auth"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/kms"
)

// Exercise the selected SDK's actual retry loop and concurrent calls sharing a
// transport. Timestamps must follow the final PUT response, never a location
// probe, another operation, a failed attempt, or the HTTP Date header.
func TestFederatedWriteTimeTransport(t *testing.T) {
	var mu sync.Mutex
	attempts := make(map[string]int)
	written := time.Date(2001, 2, 3, 4, 5, 6, 123456789, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set(federatedLastModified, written.Add(-time.Hour).Format(time.RFC3339Nano))
		if r.Method == http.MethodGet && r.URL.Query().Has("location") {
			_, _ = io.WriteString(w, `<LocationConstraint>us-east-1</LocationConstraint>`)
			return
		}
		if r.Method != http.MethodPut {
			t.Errorf("unexpected follow-up read: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		mu.Lock()
		attempts[r.URL.Path]++
		attempt := attempts[r.URL.Path]
		mu.Unlock()
		if strings.Contains(r.URL.Path, "retry-") && attempt == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `<Error><Code>SlowDown</Code></Error>`)
			return
		}
		stamp := written.Add(time.Duration(len(r.URL.Path)) * time.Nanosecond)
		if r.URL.Query().Has("partNumber") {
			stamp = stamp.Add(time.Second)
		}
		w.Header().Set(federatedLastModified, stamp.Format(time.RFC3339Nano))
		switch {
		case strings.HasSuffix(r.URL.Path, "missing"):
			w.Header().Del(federatedLastModified)
		case strings.HasSuffix(r.URL.Path, "malformed"):
			w.Header().Set(federatedLastModified, "invalid-time")
		}
		w.Header().Set("ETag", `"`+r.URL.Path+`"`)
	}))
	t.Cleanup(server.Close)
	core, err := miniogo.NewCore(server.Listener.Addr().String(), &miniogo.Options{
		Creds:        credentials.NewStaticV4("federation", "federation-secret", ""),
		Transport:    federatedWriteTransport{server.Client().Transport},
		BucketLookup: miniogo.BucketLookupPath,
		MaxRetries:   2,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range []bool{false, true} {
		for _, scenario := range []string{"present", "missing", "malformed", "retry-present", "retry-missing", "retry-malformed"} {
			t.Run(fmt.Sprintf("part=%v/%s", part, scenario), func(t *testing.T) {
				t.Parallel()
				ctx, modified := withFederatedWriteTime(t.Context())
				object := fmt.Sprintf("part-%v/%s", part, scenario)
				var etag string
				if part {
					info, err := core.PutObjectPart(ctx, "bucket", object, "upload-id", 1, bytes.NewReader([]byte("abc")), 3, miniogo.PutObjectPartOptions{})
					if err != nil {
						t.Fatal(err)
					}
					etag = info.ETag
				} else {
					info, err := core.PutObject(ctx, "bucket", object, bytes.NewReader([]byte("abc")), 3, "", "", miniogo.PutObjectOptions{})
					if err != nil {
						t.Fatal(err)
					}
					etag = info.ETag
				}
				want := time.Time{}
				if strings.HasSuffix(scenario, "present") {
					want = written.Add(time.Duration(len("/bucket/"+object)) * time.Nanosecond)
					if part {
						want = want.Add(time.Second)
					}
				}
				if !modified.Equal(want) || etag != "/bucket/"+object {
					t.Errorf("write result: time=%s ETag=%s, want time=%s ETag=/bucket/%s", modified, etag, want, object)
				}
				mu.Lock()
				count := attempts["/bucket/"+object]
				mu.Unlock()
				wantAttempts := 1
				if strings.HasPrefix(scenario, "retry-") {
					wantAttempts = 2
				}
				if count != wantAttempts {
					t.Errorf("PUT attempts = %d, want %d", count, wantAttempts)
				}
			})
		}
	}
}

func TestAPIFederatedCopyWriteTimeSSEAndCompatibility(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t:         t,
		endpoints: []string{"CopyObjectPart", "NewMultipart", "PutObjectPart", "ListObjectParts", "CompleteMultipart", "CopyObject", "PutObject", "GetObject"},
		objAPITest: func(obj ObjectLayer, instanceType, bucket string, router http.Handler, cred auth.Credentials, t *testing.T) {
			testKMS, err := kms.NewBuiltin(federationTestKMSKeyID, bytes.Repeat([]byte{0x58}, 32))
			if err != nil {
				t.Fatal(err)
			}
			previous := GlobalKMS
			GlobalKMS = testKMS
			defer func() { GlobalKMS = previous }()
			for _, mode := range []string{"current", "missing", "malformed"} {
				t.Run(mode, func(t *testing.T) {
					remote, _, cleanup := setupCopyObjectFederationRemote(t, obj, router, instanceType, bucket, true, func(h http.Header) {
						switch mode {
						case "missing":
							h.Del(federatedLastModified)
						case "malformed":
							h.Set(federatedLastModified, "invalid-time")
						}
					})
					defer cleanup()
					for _, kind := range []string{"plain", "s3", "c", "kms"} {
						if mode != "current" && kind != "plain" && kind != "c" {
							continue
						}
						t.Run(kind, func(t *testing.T) {
							data := []byte("federated write timestamp")
							putCopyChecksumSource(t, router, cred, bucket, "source", data, federationSSEHeaders(kind, 0x11, false))
							headers := federationSSEHeaders(kind, 0x22, false)
							maps.Copy(headers, federationSSEHeaders(kind, 0x11, true))
							rec := federatedCopyRequest(t, router, cred, bucket, "source", remote, kind+"-copy", headers)
							if rec.Code != http.StatusOK {
								t.Fatalf("CopyObject: %d %s", rec.Code, rec.Body.String())
							}
							info, err := obj.GetObjectInfo(t.Context(), remote, kind+"-copy", ObjectOptions{})
							if err != nil {
								t.Fatal(err)
							}
							var copied CopyObjectResponse
							if err := xml.Unmarshal(rec.Body.Bytes(), &copied); err != nil {
								t.Fatal(err)
							}
							assertTime := func(got string, stored time.Time) {
								t.Helper()
								if stored.IsZero() {
									t.Fatal("storage returned a zero timestamp")
								}
								if mode != "current" {
									stored = time.Time{}
								}
								if got != amztime.ISO8601Format(stored.UTC()) {
									t.Errorf("copy time = %q, want %q", got, amztime.ISO8601Format(stored.UTC()))
								}
							}
							assertTime(copied.LastModified, info.ModTime)

							object := kind + "-multipart"
							req, err := newTestSignedRequestV4(http.MethodPost, getNewMultipartURL("", remote, object), 0, nil,
								cred.AccessKey, cred.SecretKey, federationSSEHeaders(kind, 0x22, false))
							if err != nil {
								t.Fatal(err)
							}
							rec = httptest.NewRecorder()
							router.ServeHTTP(rec, req)
							if rec.Code != http.StatusOK {
								t.Fatalf("NewMultipartUpload: %d %s", rec.Code, rec.Body.String())
							}
							var upload InitiateMultipartUploadResponse
							if err := xml.Unmarshal(rec.Body.Bytes(), &upload); err != nil {
								t.Fatal(err)
							}
							// UploadPartCopy selects KMS/S3 encryption from the upload;
							// only SSE-C keys accompany the part request. The replica
							// markers must still take the ordinary decrypting copy path.
							headers = federationSSEHeaders(kind, 0x11, true)
							var keys map[string]string
							if kind == "c" {
								keys = federationSSEHeaders("c", 0x22, false)
								maps.Copy(headers, keys)
							}
							headers[xhttp.AmzCopySource] = SlashSeparator + pathJoin(bucket, "source")
							headers[xhttp.MinIOSourceReplicationRequest] = "true"
							headers[xhttp.AmzBucketReplicationStatus] = "REPLICA"
							req, err = newTestSignedRequestV4(http.MethodPut, getCopyObjectPartURL("", remote, object, upload.UploadID, "1"), 0, nil, cred.AccessKey, cred.SecretKey, headers)
							if err != nil {
								t.Fatal(err)
							}
							rec = httptest.NewRecorder()
							router.ServeHTTP(rec, req)
							if rec.Code != http.StatusOK {
								t.Fatalf("UploadPartCopy: %d %s", rec.Code, rec.Body.String())
							}
							var part CopyObjectPartResponse
							if err := xml.Unmarshal(rec.Body.Bytes(), &part); err != nil {
								t.Fatal(err)
							}
							parts, err := obj.ListObjectParts(t.Context(), remote, object, upload.UploadID, 0, 100, ObjectOptions{})
							if err != nil || len(parts.Parts) != 1 {
								t.Fatalf("stored part: %v, %v", parts, err)
							}
							assertTime(part.LastModified, parts.Parts[0].LastModified)
							rec = completePartsHTTP(t, router, cred, remote, object, upload.UploadID, []CompletePart{{PartNumber: 1, ETag: canonicalizeETag(part.ETag)}}, keys)
							if rec.Code != http.StatusOK {
								t.Fatalf("CompleteMultipartUpload: %d %s", rec.Code, rec.Body.String())
							}
							for _, name := range []string{kind + "-copy", object} {
								got := federationGetObject(t, router, cred, remote, name, keys)
								if got.Code != http.StatusOK || !bytes.Equal(got.Body.Bytes(), data) {
									t.Fatalf("readback %s: %d %s", name, got.Code, got.Body.String())
								}
							}
						})
					}
				})
			}
		},
	})
}
