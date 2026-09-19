// Copyright (c) 2026 PGSTY
// SPDX-License-Identifier: AGPL-3.0-only

package cmd

import (
	"bytes"
	"encoding/xml"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio/internal/amztime"
	"github.com/minio/minio/internal/auth"
	sse "github.com/minio/minio/internal/bucket/encryption"
	"github.com/minio/minio/internal/event"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/minio/internal/pubsub"
)

func TestAPIFederatedCopyObjectVersionAndEvent(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t:         t,
		endpoints: []string{"CopyObject", "PutObject", "GetObject", "HeadObject"},
		objAPITest: func(obj ObjectLayer, instanceType, bucket string, router http.Handler, cred auth.Credentials, t *testing.T) {
			testKMS, err := kms.NewBuiltin(federationTestKMSKeyID, bytes.Repeat([]byte{0x58}, 32))
			if err != nil {
				t.Fatal(err)
			}
			previousKMS := GlobalKMS
			GlobalKMS = testKMS
			defer func() { GlobalKMS = previousKMS }()
			restore := setCopyChecksumCompression(true)
			defer restore()
			remoteBucket, _, cleanup := setupCopyObjectFederation(t, obj, router, instanceType, bucket)
			defer cleanup()
			enableBucketObjectLock(t, remoteBucket)
			events := make(chan event.Event, 8)
			done := make(chan struct{})
			defer close(done)
			if err := globalHTTPListen.Subscribe(pubsub.MaskFromMaskable(event.ObjectCreatedCopy), events, done, nil); err != nil {
				t.Fatal(err)
			}
			for _, kind := range []string{"plain", "compressed", "encrypted"} {
				t.Run(instanceType+"/"+kind, func(t *testing.T) {
					data := []byte("logical object size")
					source := kind + "-source.bin"
					var headers map[string]string
					if kind == "compressed" {
						data = bytes.Repeat(data, 8192)
						source = kind + "-source.txt"
					}
					if kind == "encrypted" {
						headers = federationSSEHeaders("s3", 0, false)
					}
					putCopyChecksumSource(t, router, cred, bucket, source, data, headers)
					destination := "result/" + kind + " with space.bin"
					rec := federatedCopyRequest(t, router, cred, bucket, source, remoteBucket, destination, nil)
					if rec.Code != http.StatusOK {
						t.Fatalf("copy: %d %s", rec.Code, rec.Body.String())
					}
					versionID := strings.Join(rec.Header()[xhttp.AmzVersionID], "")
					if versionID == "" {
						t.Error("copy response omitted destination version ID")
					} else {
						info, err := obj.GetObjectInfo(t.Context(), remoteBucket, destination, ObjectOptions{VersionID: versionID})
						if err != nil || info.VersionID != versionID {
							t.Fatalf("response does not name the written version: %v, %q", err, info.VersionID)
						}
						var response CopyObjectResponse
						if err := xml.Unmarshal(rec.Body.Bytes(), &response); err != nil {
							t.Fatal(err)
						}
						if want := amztime.ISO8601Format(info.ModTime.UTC()); response.LastModified != want {
							t.Errorf("copy time = %q, want written version time %q", response.LastModified, want)
						}
					}
					select {
					case evt := <-events:
						key, err := url.QueryUnescape(evt.S3.Object.Key)
						if err != nil || key != destination || evt.S3.Bucket.Name != remoteBucket || evt.S3.Object.Size != int64(len(data)) || evt.S3.Object.VersionID == "" || evt.S3.Object.VersionID != versionID {
							t.Errorf("copy event does not describe the written object: %+v", evt.S3)
						}
					case <-time.After(5 * time.Second):
						t.Fatal("copy event was not emitted")
					}
				})
			}
		},
	})
}

func TestAPIFederatedCopyObjectDestinationSSEDefaults(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t:         t,
		endpoints: []string{"CopyObject", "PutObject", "GetObject", "HeadObject"},
		objAPITest: func(obj ObjectLayer, instanceType, bucket string, router http.Handler, cred auth.Credentials, t *testing.T) {
			data := []byte("destination chooses default encryption")
			putCopyChecksumSource(t, router, cred, bucket, "source", data, nil)
			testKMS, err := kms.NewBuiltin(federationTestKMSKeyID, bytes.Repeat([]byte{0x58}, 32))
			if err != nil {
				t.Fatal(err)
			}
			previousKMS, previousAuto := GlobalKMS, globalAutoEncryption
			GlobalKMS, globalAutoEncryption = testKMS, true
			defer func() { GlobalKMS, globalAutoEncryption = previousKMS, previousAuto }()
			remoteBucket, capture, cleanup := setupCopyObjectFederationRemote(t, obj, router, instanceType, bucket, true)
			defer cleanup()
			destinationConfig, err := sse.ParseBucketSSEConfig(strings.NewReader(`<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID>destination-bucket-key</KMSMasterKeyID></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>`))
			if err != nil {
				t.Fatal(err)
			}
			for _, kind := range []string{"plain", "s3", "kms", "kms-default", "c"} {
				t.Run(instanceType+"/"+kind, func(t *testing.T) {
					var headers map[string]string
					if kind == "kms-default" {
						headers = map[string]string{xhttp.AmzServerSideEncryption: xhttp.AmzEncryptionKMS}
					} else {
						headers = federationSSEHeaders(kind, 0x22, false)
					}
					rec := federatedCopyRequest(t, router, cred, bucket, "source", remoteBucket, kind, headers)
					if rec.Code != http.StatusOK {
						t.Fatalf("copy: %d %s", rec.Code, rec.Body.String())
					}
					capture.mu.Lock()
					forwarded := capture.headers[len(capture.headers)-1].Clone()
					capture.mu.Unlock()
					if kind == "plain" {
						if forwarded.Get(xhttp.AmzServerSideEncryption) != "" {
							t.Errorf("proxy injected SSE %q into a request with no client SSE", forwarded.Get(xhttp.AmzServerSideEncryption))
						}
						// With nothing injected, the KMS encryption the destination
						// stores can only be the remote applying its own defaults.
						info, err := obj.GetObjectInfo(t.Context(), remoteBucket, kind, ObjectOptions{})
						if err != nil || federationStoredSSE(info.UserDefined) != "kms" {
							t.Errorf("remote destination did not apply its own auto-encryption: %v, %v", err, info.UserDefined)
						}
					}
					// Replay the actual inbound headers against an independent bucket
					// configuration. No handler flips shared globals while serving.
					destinationConfig.Apply(forwarded, sse.ApplyOptions{})
					wantKey := headers[xhttp.AmzServerSideEncryptionKmsID]
					if kind == "plain" {
						wantKey = "destination-bucket-key"
					}
					if got := forwarded.Get(xhttp.AmzServerSideEncryptionKmsID); got != wantKey {
						t.Errorf("destination selected key %q, want %q", got, wantKey)
					}
				})
			}
			// A local destination still inherits the server's auto-encryption.
			rec := federatedCopyRequest(t, router, cred, bucket, "source", bucket, "local-copy", nil)
			if rec.Code != http.StatusOK {
				t.Fatalf("local copy: %d %s", rec.Code, rec.Body.String())
			}
			info, err := obj.GetObjectInfo(t.Context(), bucket, "local-copy", ObjectOptions{})
			if err != nil || federationStoredSSE(info.UserDefined) != "kms" {
				t.Errorf("local copy lost auto-encryption: %v, %v", err, info.UserDefined)
			}
		},
	})
}
