// Copyright (c) 2015-2025 MinIO, Inc.
// Copyright (c) 2025-2026 PGSTY
//
// This file is part of MinIO Object Storage stack
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
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	miniogo "github.com/minio/minio-go/v7"
	miniocredentials "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/amztime"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/config/dns"
	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/hash"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/kms"
)

// federationRemoteCapture records the exact request headers the remote
// deployment receives on each forwarded request, so a test can assert what
// actually crossed the wire rather than what the destination ends up storing.
type federationRemoteCapture struct {
	mu      sync.Mutex
	headers []http.Header
}

type federationResponseFilter struct {
	http.ResponseWriter
	filter func(http.Header)
}

func (w federationResponseFilter) WriteHeader(status int) {
	w.filter(w.Header())
	w.ResponseWriter.WriteHeader(status)
}

func (c *federationRemoteCapture) record(h http.Header) {
	c.mu.Lock()
	c.headers = append(c.headers, h.Clone())
	c.mu.Unlock()
}

// reservedKeys returns every reserved-prefix header key seen across all
// forwarded requests.
func (c *federationRemoteCapture) reservedKeys() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var keys []string
	for _, h := range c.headers {
		for k := range h {
			if stringsHasPrefixFold(k, ReservedMetadataPrefix) {
				keys = append(keys, k)
			}
		}
	}
	return keys
}

// setupCopyObjectFederation makes the current process play both federation
// roles for a whole-object CopyObject. The destination bucket really exists in
// the shared backend, but the proxy's own bucket lookup is told it lives on a
// remote deployment, so CopyObjectHandler takes the legacy etcd federation
// branch and forwards the write through getRemoteInstanceClient and minio-go
// into a second HTTP endpoint that serves the real PutObjectHandler. That
// endpoint records the inbound headers first so a caller can inspect the wire.
//
// GetBucketLocation is deliberately left unregistered on that endpoint: the
// remoteBucketObjectLayer reports remoteBucket as missing (so the proxy takes
// the federation branch), which would also fail a real GetBucketLocation the
// minio-go client probes for. Leaving it unregistered makes the probe fall back
// to the default region, exactly as TestAPIFederatedCopyObjectPartChecksum does.
func setupCopyObjectFederation(t *testing.T, objectAPI ObjectLayer, apiRouter http.Handler,
	instanceType, srcBucket string, responseFilters ...func(http.Header),
) (remoteBucket string, capture *federationRemoteCapture, cleanup func()) {
	t.Helper()
	return setupCopyObjectFederationRemote(t, objectAPI, apiRouter, instanceType, srcBucket, false, responseFilters...)
}

// setupCopyObjectFederationTLS is setupCopyObjectFederation with a TLS remote
// endpoint and globalIsTLS set, so an SSE-C copy is accepted on both hops: the
// proxy's own SSE-C transport gate and the minio-go client's SSE-C policy. The
// proxy client is built exactly as in production, trusting the test
// certificate for the duration.
func setupCopyObjectFederationTLS(t *testing.T, objectAPI ObjectLayer, apiRouter http.Handler,
	instanceType, srcBucket string,
) (remoteBucket string, cleanup func()) {
	t.Helper()
	remoteBucket, _, cleanup = setupCopyObjectFederationRemote(t, objectAPI, apiRouter, instanceType, srcBucket, true)
	return remoteBucket, cleanup
}

func setupCopyObjectFederationRemote(t *testing.T, objectAPI ObjectLayer, apiRouter http.Handler,
	instanceType, srcBucket string, secure bool, responseFilters ...func(http.Header),
) (remoteBucket string, capture *federationRemoteCapture, cleanup func()) {
	t.Helper()
	remoteBucket = getRandomBucketName()
	if err := objectAPI.MakeBucket(t.Context(), remoteBucket, MakeBucketOptions{}); err != nil {
		t.Fatalf("%s: unable to create the remote bucket: %v", instanceType, err)
	}

	capture = &federationRemoteCapture{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capture.record(r.Header)
		if len(responseFilters) != 0 && r.Method == http.MethodPut {
			w = federationResponseFilter{ResponseWriter: w, filter: responseFilters[0]}
		}
		apiRouter.ServeHTTP(w, r)
	})
	var remote *httptest.Server
	if secure {
		remote = httptest.NewTLSServer(handler)
	} else {
		remote = httptest.NewServer(handler)
	}
	host, port, _ := strings.Cut(remote.Listener.Addr().String(), ":")

	globalObjLayerMutex.Lock()
	previousLayer := globalObjectAPI
	globalObjectAPI = remoteBucketObjectLayer{ObjectLayer: previousLayer, remoteBucket: remoteBucket}
	globalObjLayerMutex.Unlock()
	previousDNS, previousFederation, previousIPs := globalDNSConfig, globalBucketFederation, globalDomainIPs
	previousTLS, previousClient := globalIsTLS, getRemoteInstanceClient
	globalDNSConfig = federationTestDNS{records: map[string][]dns.SrvRecord{
		srcBucket:    {{Host: host, Port: json.Number(port)}},
		remoteBucket: {{Host: host, Port: json.Number(port)}},
	}}
	// Every DNS record resolves to this process, so the bucket forwarding
	// middleware always serves locally and only the handler proxies.
	globalDomainIPs = set.CreateStringSet(remote.Listener.Addr().String())
	globalBucketFederation = true
	if secure {
		globalIsTLS = true
		transport := remote.Client().Transport
		getRemoteInstanceClient = func(r *http.Request, host string) (*miniogo.Core, error) {
			cred := getReqAccessCred(r, globalSite.Region())
			core, err := miniogo.NewCore(host, &miniogo.Options{
				Creds:     miniocredentials.NewStaticV4(cred.AccessKey, cred.SecretKey, ""),
				Secure:    true,
				Transport: federatedWriteTransport{transport},
			})
			if err != nil {
				return nil, err
			}
			core.SetAppInfo(federatedInternalAppName, ReleaseTag)
			return core, nil
		}
	}

	cleanup = func() {
		remote.Close()
		globalObjLayerMutex.Lock()
		globalObjectAPI = previousLayer
		globalObjLayerMutex.Unlock()
		globalDNSConfig, globalBucketFederation, globalDomainIPs = previousDNS, previousFederation, previousIPs
		globalIsTLS, getRemoteInstanceClient = previousTLS, previousClient
	}
	return remoteBucket, capture, cleanup
}

// federatedCopyRequest drives a whole-object CopyObject at the proxy that
// forwards across deployments, and returns the recorded response.
func federatedCopyRequest(t *testing.T, apiRouter http.Handler, credentials auth.Credentials,
	srcBucket, srcObject, dstBucket, dstObject string, headers map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()
	req, err := newTestSignedRequestV4(http.MethodPut, getCopyObjectURL("", dstBucket, dstObject),
		0, nil, credentials.AccessKey, credentials.SecretKey, headers)
	if err != nil {
		t.Fatalf("failed to build federated CopyObject request: %v", err)
	}
	req.Header.Set(xhttp.AmzCopySource, SlashSeparator+pathJoin(srcBucket, srcObject))
	// Re-sign so x-amz-copy-source is covered by the signature, as real S3
	// clients send it; the verifier rejects unsigned x-amz-* headers.
	if credentials.AccessKey != "" && credentials.SecretKey != "" {
		if err := signRequestV4(req, credentials.AccessKey, credentials.SecretKey); err != nil {
			t.Fatalf("failed to re-sign federated CopyObject request: %v", err)
		}
	}
	rec := httptest.NewRecorder()
	apiRouter.ServeHTTP(rec, req)
	return rec
}

func TestAPIFederatedCopyObjectRejectsRawSSECReplica(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t:         t,
		endpoints: []string{"CopyObject", "PutObject"},
		objAPITest: func(obj ObjectLayer, instanceType, bucket string, router http.Handler, cred auth.Credentials, t *testing.T) {
			remoteBucket, capture, cleanup := setupCopyObjectFederationRemote(t, obj, router, instanceType, bucket, true)
			defer cleanup()
			for _, body := range []string{"", "raw SSE-C replica"} {
				for _, withKey := range []bool{false, true} {
					t.Run("size="+strconv.Itoa(len(body))+"/key="+strconv.FormatBool(withKey), func(t *testing.T) {
						putCopyChecksumSource(t, router, cred, bucket, "source", []byte(body), federationSSEHeaders("c", 0x11, false))
						headers := map[string]string{
							xhttp.MinIOSourceReplicationRequest: "true",
							xhttp.AmzBucketReplicationStatus:    "REPLICA",
						}
						if withKey {
							maps.Copy(headers, federationSSEHeaders("c", 0x11, true))
						}
						destination := "destination-" + strconv.Itoa(len(body)) + "-" + strconv.FormatBool(withKey)
						rec := federatedCopyRequest(t, router, cred, bucket, "source", remoteBucket, destination, headers)
						var response APIErrorResponse
						if err := xml.Unmarshal(rec.Body.Bytes(), &response); err != nil {
							t.Fatal(err)
						}
						if rec.Code != http.StatusNotImplemented || response.Code != "NotImplemented" ||
							!strings.Contains(response.Message, "federated raw SSE-C replica CopyObject") {
							t.Errorf("copy = %d %s, want explicit 501 rejection", rec.Code, rec.Body.String())
						}
						if _, err := obj.GetObjectInfo(t.Context(), remoteBucket, destination, ObjectOptions{}); !isErrObjectNotFound(err) {
							t.Errorf("rejected copy created a destination: %v", err)
						}
					})
				}
			}
			capture.mu.Lock()
			defer capture.mu.Unlock()
			if len(capture.headers) != 0 {
				t.Errorf("rejected copies made %d remote requests", len(capture.headers))
			}
		},
	})
}

// TestAPIFederatedCopyObjectInlineSource drives the legacy etcd federation
// branch of CopyObjectHandler end to end for a source object stored inline.
// Such a source carries x-minio-internal-inline-data in its stored metadata,
// which the remote deployment rejects as a reserved-prefix header. Before the
// fix the forwarded write failed with 400 InvalidArgument; the copy must now
// succeed and forward no reserved-prefix metadata at all.
func TestAPIFederatedCopyObjectInlineSource(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t:          t,
		objAPITest: testAPIFederatedCopyObjectInlineSource,
		endpoints:  []string{"CopyObject", "PutObject", "HeadObject", "GetObject"},
	})
}

func testAPIFederatedCopyObjectInlineSource(objectAPI ObjectLayer, instanceType, bucketName string,
	apiRouter http.Handler, credentials auth.Credentials, t *testing.T,
) {
	// A small object is stored inline, so its stored metadata carries
	// x-minio-internal-inline-data. A user metadata key rides along to prove
	// the fix strips only the reserved class, never ordinary metadata.
	data := []byte("federated inline copy 26b!")
	srcObject := "federation/inline-source"
	putCopyChecksumSource(t, apiRouter, credentials, bucketName, srcObject, data,
		map[string]string{"X-Amz-Meta-Origin": "inline-source"})

	remoteBucket, capture, cleanup := setupCopyObjectFederation(t, objectAPI, apiRouter, instanceType, bucketName)
	defer cleanup()

	dstObject := "federation/inline-destination"
	rec := federatedCopyRequest(t, apiRouter, credentials, bucketName, srcObject, remoteBucket, dstObject, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: federated CopyObject of an inline source failed: %d %s",
			instanceType, rec.Code, rec.Body.String())
	}

	// No reserved-prefix header may reach the remote deployment on any of the
	// forwarded requests.
	if leaked := capture.reservedKeys(); len(leaked) != 0 {
		t.Fatalf("%s: forwarded reserved metadata to the remote: %v", instanceType, leaked)
	}

	// The destination object must be readable and byte-identical, and it must
	// still carry the copied user metadata.
	gr, err := objectAPI.GetObjectNInfo(t.Context(), remoteBucket, dstObject, nil, nil, ObjectOptions{})
	if err != nil {
		t.Fatalf("%s: unable to read the federated copy destination: %v", instanceType, err)
	}
	got, err := io.ReadAll(gr)
	closeErr := gr.Close()
	if err != nil {
		t.Fatalf("%s: reading the federated copy destination failed: %v", instanceType, err)
	}
	if closeErr != nil {
		t.Fatalf("%s: closing the federated copy destination failed: %v", instanceType, closeErr)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("%s: federated copy destination body = %q, want %q", instanceType, got, data)
	}
	if origin, ok := gr.ObjInfo.UserDefined["X-Amz-Meta-Origin"]; !ok || origin != "inline-source" {
		t.Fatalf("%s: destination lost copied user metadata: %v", instanceType, gr.ObjInfo.UserDefined)
	}
}

// TestAPIFederatedCopyObjectRequestedChecksum drives the legacy etcd federation
// branch of CopyObjectHandler and verifies that a server-side checksum is both
// returned and persisted, matching the local CopyObject path. Before the fix
// the federated copy forwarded the write without asking for a checksum and
// discarded whatever the remote returned, so the response carried an empty
// checksum even when the client requested one (#99).
//
// The no-algorithm case is included deliberately: a checksum-less source gains
// the S3 default CRC-64NVME full-object checksum on the local path, so the
// federated path must return the same. "No requested algorithm" does not mean
// "no checksum".
func TestAPIFederatedCopyObjectRequestedChecksum(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t:          t,
		objAPITest: testAPIFederatedCopyObjectRequestedChecksum,
		endpoints:  []string{"CopyObject", "PutObject", "HeadObject", "GetObject"},
	})
}

func testAPIFederatedCopyObjectRequestedChecksum(objectAPI ObjectLayer, instanceType, bucketName string,
	apiRouter http.Handler, credentials auth.Credentials, t *testing.T,
) {
	data := []byte("federated copy checksum body")
	srcObject := "federation/checksum-source"
	putCopyChecksumSource(t, apiRouter, credentials, bucketName, srcObject, data, nil)

	remoteBucket, _, cleanup := setupCopyObjectFederation(t, objectAPI, apiRouter, instanceType, bucketName)
	defer cleanup()

	cases := []struct {
		name     string
		typ      hash.ChecksumType
		explicit bool
	}{
		{name: "CRC32", typ: hash.ChecksumCRC32, explicit: true},
		{name: "CRC32C", typ: hash.ChecksumCRC32C, explicit: true},
		{name: "SHA1", typ: hash.ChecksumSHA1, explicit: true},
		{name: "SHA256", typ: hash.ChecksumSHA256, explicit: true},
		{name: "CRC64NVME", typ: hash.ChecksumCRC64NVME, explicit: true},
		// Default: no requested algorithm still yields the S3 CRC-64NVME.
		{name: "default", typ: hash.ChecksumCRC64NVME},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var headers map[string]string
			if tc.explicit {
				headers = map[string]string{xhttp.AmzChecksumAlgo: tc.typ.String()}
			}
			dstObject := "federation/checksum-destination-" + tc.name
			rec := federatedCopyRequest(t, apiRouter, credentials, bucketName, srcObject, remoteBucket, dstObject, headers)
			if rec.Code != http.StatusOK {
				t.Fatalf("%s: federated CopyObject failed: %d %s", instanceType, rec.Code, rec.Body.String())
			}
			// The CopyObjectResult must carry the checksum of the copied bytes.
			assertCopyChecksumResponse(t, rec, tc.typ, data)
			// The remote must have persisted that same checksum.
			assertCopyChecksum(t, objectAPI, remoteBucket, dstObject, tc.typ, data, false, nil)
		})
	}
}

func TestAPIFederatedCopyObjectRejectsInvalidRemoteChecksum(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t: t,
		objAPITest: func(obj ObjectLayer, instanceType, bucket string, router http.Handler, credentials auth.Credentials, t *testing.T) {
			data := []byte("remote checksum response fixture")
			putCopyChecksumSource(t, router, credentials, bucket, "source", data, nil)
			// "" missing, "invalid-base64" unparseable, "YQ==" wrong digest
			// length, and "<valid>-0" a multipart-marked value that a single
			// forwarded PutObject must never yield (it would mislabel the
			// destination as composite).
			for _, value := range []string{"", "invalid-base64", "YQ==", mustChecksum(t, hash.ChecksumCRC32, data) + "-0"} {
				t.Run("checksum="+value, func(t *testing.T) {
					remoteBucket, _, cleanup := setupCopyObjectFederation(t, obj, router, instanceType, bucket, func(header http.Header) {
						header.Set(xhttp.AmzChecksumCRC32, value)
					})
					defer cleanup()
					rec := federatedCopyRequest(t, router, credentials, bucket, "source", remoteBucket, "destination",
						map[string]string{xhttp.AmzChecksumAlgo: "CRC32"})
					if rec.Code < 500 {
						t.Fatalf("invalid remote checksum must fail the copy, got %d: %s", rec.Code, rec.Body.String())
					}
				})
			}
		},
		endpoints: []string{"CopyObject", "PutObject", "HeadObject", "GetObject"},
	})
}

// TestAPIFederatedCopyObjectChecksumIsBoundToWrite guards the checksum
// representation: a federated copy that requests one algorithm must return only
// that algorithm, and a copy of a checksum-less source without a requested
// algorithm must never fabricate one other than the S3 default.
func TestAPIFederatedCopyObjectChecksumIsBoundToWrite(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t:          t,
		objAPITest: testAPIFederatedCopyObjectChecksumIsBoundToWrite,
		endpoints:  []string{"CopyObject", "PutObject", "HeadObject", "GetObject"},
	})
}

func testAPIFederatedCopyObjectChecksumIsBoundToWrite(objectAPI ObjectLayer, instanceType, bucketName string,
	apiRouter http.Handler, credentials auth.Credentials, t *testing.T,
) {
	data := []byte("federated copy single checksum body")
	srcObject := "federation/single-checksum-source"
	putCopyChecksumSource(t, apiRouter, credentials, bucketName, srcObject, data, nil)

	remoteBucket, _, cleanup := setupCopyObjectFederation(t, objectAPI, apiRouter, instanceType, bucketName)
	defer cleanup()

	dstObject := "federation/single-checksum-destination"
	rec := federatedCopyRequest(t, apiRouter, credentials, bucketName, srcObject, remoteBucket, dstObject,
		map[string]string{xhttp.AmzChecksumAlgo: hash.ChecksumCRC32.String()})
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: federated CopyObject failed: %d %s", instanceType, rec.Code, rec.Body.String())
	}

	var response CopyObjectResponse
	if err := xml.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("%s: unable to decode CopyObjectResult: %v", instanceType, err)
	}
	if response.ChecksumCRC32 == "" {
		t.Fatalf("%s: requested CRC32 checksum missing from response: %s", instanceType, rec.Body.String())
	}
	// Only the requested algorithm may be present.
	if response.ChecksumCRC32C != "" || response.ChecksumSHA1 != "" ||
		response.ChecksumSHA256 != "" || response.ChecksumCRC64NVME != "" {
		t.Fatalf("%s: response carried checksums beyond the requested CRC32: %s", instanceType, rec.Body.String())
	}
}

// TestAPIFederatedCopyObjectEmptySource guards the empty-body regression: a
// checksum-less object gains the S3 default CRC-64NVME, but minio-go streams no
// trailing checksum for a 0-byte body, so the remote returned none, the bind
// found nothing, and every empty-object federated copy 500'd. An empty source
// must now copy with 200 and carry the empty-content checksum, both when a
// checksum is requested explicitly and via the default.
func TestAPIFederatedCopyObjectEmptySource(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t:          t,
		objAPITest: testAPIFederatedCopyObjectEmptySource,
		endpoints:  []string{"CopyObject", "PutObject", "HeadObject", "GetObject"},
	})
}

func testAPIFederatedCopyObjectEmptySource(objectAPI ObjectLayer, instanceType, bucketName string,
	apiRouter http.Handler, credentials auth.Credentials, t *testing.T,
) {
	srcObject := "federation/empty-source"
	putCopyChecksumSource(t, apiRouter, credentials, bucketName, srcObject, nil, nil)

	remoteBucket, _, cleanup := setupCopyObjectFederation(t, objectAPI, apiRouter, instanceType, bucketName)
	defer cleanup()

	cases := []struct {
		name     string
		typ      hash.ChecksumType
		explicit bool
	}{
		{name: "explicit-CRC32", typ: hash.ChecksumCRC32, explicit: true},
		{name: "explicit-SHA256", typ: hash.ChecksumSHA256, explicit: true},
		// No requested algorithm: the S3 default CRC-64NVME still applies.
		{name: "default-CRC64NVME", typ: hash.ChecksumCRC64NVME},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var headers map[string]string
			if tc.explicit {
				headers = map[string]string{xhttp.AmzChecksumAlgo: tc.typ.String()}
			}
			dstObject := "federation/empty-destination-" + tc.name
			rec := federatedCopyRequest(t, apiRouter, credentials, bucketName, srcObject, remoteBucket, dstObject, headers)
			if rec.Code != http.StatusOK {
				t.Fatalf("%s: federated CopyObject of an empty source failed: %d %s",
					instanceType, rec.Code, rec.Body.String())
			}
			// The empty-content digest must be returned and persisted.
			assertCopyChecksumResponse(t, rec, tc.typ, nil)
			assertCopyChecksum(t, objectAPI, remoteBucket, dstObject, tc.typ, nil, false, nil)
		})
	}
}

// TestAPIFederatedCopyObjectInheritedChecksum guards that a full-object checksum
// already stored on the source is preserved across a federated copy that
// requests no algorithm. That checksum sets dstOpts.WantChecksum (not
// WantServerSideChecksumType), which the federated branch previously ignored,
// silently dropping the checksum the local path keeps.
func TestAPIFederatedCopyObjectInheritedChecksum(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t:          t,
		objAPITest: testAPIFederatedCopyObjectInheritedChecksum,
		endpoints:  []string{"CopyObject", "PutObject", "HeadObject", "GetObject"},
	})
}

func testAPIFederatedCopyObjectInheritedChecksum(objectAPI ObjectLayer, instanceType, bucketName string,
	apiRouter http.Handler, credentials auth.Credentials, t *testing.T,
) {
	data := []byte("abc")
	want := mustChecksum(t, hash.ChecksumCRC32, data) // "NSRBwg=="
	srcObject := "federation/inherited-checksum-source"
	putCopyChecksumSource(t, apiRouter, credentials, bucketName, srcObject, data,
		map[string]string{xhttp.AmzChecksumCRC32: want})

	remoteBucket, _, cleanup := setupCopyObjectFederation(t, objectAPI, apiRouter, instanceType, bucketName)
	defer cleanup()

	// No algorithm header: the source's stored CRC32 must survive the copy.
	dstObject := "federation/inherited-checksum-destination"
	rec := federatedCopyRequest(t, apiRouter, credentials, bucketName, srcObject, remoteBucket, dstObject, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: federated CopyObject failed: %d %s", instanceType, rec.Code, rec.Body.String())
	}
	assertCopyChecksumResponse(t, rec, hash.ChecksumCRC32, data)
	assertCopyChecksum(t, objectAPI, remoteBucket, dstObject, hash.ChecksumCRC32, data, false, nil)
}

// federationTestKMSKeyID names the single key of the builtin KMS the SSE test
// installs; SSE-S3 seals with it implicitly, SSE-KMS names it by ID.
const federationTestKMSKeyID = "federation-test-key"

// federationSSEHeaders returns the request headers that select one server-side
// encryption kind: "plain", "s3", "kms", "kms-context" (SSE-KMS with an
// explicit encryption context), "kms-empty-context" or "c". SSE-C keys derive from keyByte so a
// case can name two distinct customer keys. With copySource the SSE-C key is
// returned in its x-amz-copy-source-* form, the only kind a copy has to name
// for its source; the other kinds then return nothing.
func federationSSEHeaders(kind string, keyByte byte, copySource bool) map[string]string {
	h := map[string]string{}
	if copySource && kind != "c" {
		return h
	}
	switch kind {
	case "plain":
	case "s3":
		h[xhttp.AmzServerSideEncryption] = xhttp.AmzEncryptionAES
	case "kms", "kms-context", "kms-empty-context":
		h[xhttp.AmzServerSideEncryption] = xhttp.AmzEncryptionKMS
		h[xhttp.AmzServerSideEncryptionKmsID] = federationTestKMSKeyID
		switch kind {
		case "kms-context":
			h[xhttp.AmzServerSideEncryptionKmsContext] = base64.StdEncoding.EncodeToString([]byte(`{"tenant":"federation"}`))
		case "kms-empty-context":
			h[xhttp.AmzServerSideEncryptionKmsContext] = base64.StdEncoding.EncodeToString([]byte(`{}`))
		}
	case "c":
		key := bytes.Repeat([]byte{keyByte}, 32)
		sum := md5.Sum(key)
		algorithm, customerKey, keyMD5 := xhttp.AmzServerSideEncryptionCustomerAlgorithm,
			xhttp.AmzServerSideEncryptionCustomerKey, xhttp.AmzServerSideEncryptionCustomerKeyMD5
		if copySource {
			algorithm, customerKey, keyMD5 = xhttp.AmzServerSideEncryptionCopyCustomerAlgorithm,
				xhttp.AmzServerSideEncryptionCopyCustomerKey, xhttp.AmzServerSideEncryptionCopyCustomerKeyMD5
		}
		h[algorithm] = xhttp.AmzEncryptionAES
		h[customerKey] = base64.StdEncoding.EncodeToString(key)
		h[keyMD5] = base64.StdEncoding.EncodeToString(sum[:])
	default:
		panic("unknown SSE kind " + kind)
	}
	return h
}

// federationStoredSSE reports which SSE kind stored object metadata declares.
func federationStoredSSE(metadata map[string]string) string {
	switch {
	case crypto.SSEC.IsEncrypted(metadata):
		return "c"
	case crypto.S3KMS.IsEncrypted(metadata):
		return "kms"
	case crypto.S3.IsEncrypted(metadata):
		return "s3"
	}
	return "plain"
}

// federationGetObject reads an object through GetObjectHandler.
func federationGetObject(t *testing.T, apiRouter http.Handler, credentials auth.Credentials,
	bucket, object string, headers map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()
	req, err := newTestSignedRequestV4(http.MethodGet, getGetObjectURL("", bucket, object),
		0, nil, credentials.AccessKey, credentials.SecretKey, headers)
	if err != nil {
		t.Fatalf("failed to build GetObject request: %v", err)
	}
	rec := httptest.NewRecorder()
	apiRouter.ServeHTTP(rec, req)
	return rec
}

// assertFederationCopyReadback checks storage and the public GET/HEAD checksum
// surface with the destination key, including the committed write's time.
func assertFederationCopyReadback(t *testing.T, obj ObjectLayer, router http.Handler, cred auth.Credentials,
	bucket, object string, rec *httptest.ResponseRecorder, typ hash.ChecksumType, data []byte, compressed bool, keyHeaders map[string]string,
) ObjectInfo {
	t.Helper()
	assertCopyChecksumResponse(t, rec, typ, data)
	headers := make(http.Header)
	for k, v := range keyHeaders {
		headers.Set(k, v)
	}
	info := assertCopyChecksum(t, obj, bucket, object, typ, data, compressed, headers)
	var response CopyObjectResponse
	if err := xml.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if want := amztime.ISO8601Format(info.ModTime.UTC()); info.ModTime.IsZero() || response.LastModified != want {
		t.Errorf("copy time = %q, want stored time %q", response.LastModified, want)
	}
	readHeaders := map[string]string{xhttp.AmzChecksumMode: "ENABLED"}
	maps.Copy(readHeaders, keyHeaders)
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		req, err := newTestSignedRequestV4(method, getGetObjectURL("", bucket, object), 0, nil, cred.AccessKey, cred.SecretKey, readHeaders)
		if err != nil {
			t.Fatal(err)
		}
		got := httptest.NewRecorder()
		router.ServeHTTP(got, req)
		if got.Code != http.StatusOK {
			t.Fatalf("destination %s = %d %s", method, got.Code, got.Body.String())
		}
		if method == http.MethodGet && !bytes.Equal(got.Body.Bytes(), data) {
			t.Fatalf("destination GET differs from the %d-byte plaintext", len(data))
		}
		if got.Header().Get(xhttp.ContentLength) != strconv.Itoa(len(data)) ||
			got.Header().Get(typ.Key()) != mustChecksum(t, typ, data) ||
			got.Header().Get(xhttp.AmzChecksumType) != xhttp.AmzChecksumTypeFullObject {
			t.Fatalf("destination %s length/checksum headers = %v", method, got.Header())
		}
	}
	return info
}

func assertFederationKMSContext(t *testing.T, info ObjectInfo, requested string) {
	t.Helper()
	decode := func(encoded string) map[string]string {
		t.Helper()
		value, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			t.Fatal(err)
		}
		var context map[string]string
		if err := json.Unmarshal(value, &context); err != nil {
			t.Fatal(err)
		}
		return context
	}
	want := map[string]string{}
	if requested != "" {
		want = decode(requested)
	}
	stored, present := info.UserDefined[crypto.MetaContext]
	if len(want) == 0 {
		if present {
			t.Errorf("absent/empty destination context stored client metadata %q", stored)
		}
	} else if !present || !maps.Equal(decode(stored), want) {
		t.Errorf("stored KMS context = %q, want %v", stored, want)
	}
}

// TestAPIFederatedCopyObjectSSE guards the legacy etcd federation branch of
// CopyObjectHandler for encrypted sources and destinations (#158). The proxy
// reads its source through getObjectNInfo, which yields the decrypted and
// decompressed bytes, but it used to run the destination encryption locally
// as well and then forward that stream with the source's stored size under
// the destination SSE option. SSE to plain and plain to SSE failed on the
// length mismatch; SSE to SSE matched by coincidence, so the remote encrypted
// the ciphertext a second time and a destination GET returned the inner
// ciphertext with HTTP 200. The proxy must forward the logical bytes with
// their logical size and let the remote encrypt exactly once.
func TestAPIFederatedCopyObjectSSE(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t:          t,
		objAPITest: testAPIFederatedCopyObjectSSE,
		// Register PutObjectPart and CopyObject before the query-less
		// PutObject PUT route; otherwise PutObject shadows them.
		endpoints: []string{
			"NewMultipart", "PutObjectPart", "CompleteMultipart",
			"CopyObject", "PutObject", "HeadObject", "GetObject",
		},
	})
}

// putFederationMultipartSource uploads parts as one multipart object through
// the API router. Headers select the upload's encryption/checksum; SSE-C keys
// and checksums are also supplied on the parts and completion as required.
func putFederationMultipartSource(t *testing.T, apiRouter http.Handler, credentials auth.Credentials,
	bucket, object string, parts [][]byte, headers map[string]string,
) {
	t.Helper()
	do := func(method, url string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
		req, err := newTestSignedRequestV4(method, url, int64(len(body)), bytes.NewReader(body),
			credentials.AccessKey, credentials.SecretKey, headers)
		if err != nil {
			t.Fatalf("failed to build %s %s request: %v", method, url, err)
		}
		rec := httptest.NewRecorder()
		apiRouter.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s %s failed: %d %s", method, url, rec.Code, rec.Body.String())
		}
		return rec
	}
	var upload InitiateMultipartUploadResponse
	rec := do(http.MethodPost, getNewMultipartURL("", bucket, object), nil, headers)
	if err := xml.Unmarshal(rec.Body.Bytes(), &upload); err != nil {
		t.Fatalf("failed to decode NewMultipartUpload response: %v", err)
	}
	completion := CompleteMultipartUpload{}
	partHeaders := map[string]string{}
	for _, key := range []string{xhttp.AmzServerSideEncryptionCustomerAlgorithm, xhttp.AmzServerSideEncryptionCustomerKey, xhttp.AmzServerSideEncryptionCustomerKeyMD5} {
		if value := headers[key]; value != "" {
			partHeaders[key] = value
		}
	}
	checksumType := hash.NewChecksumType(headers[xhttp.AmzChecksumAlgo], headers[xhttp.AmzChecksumType])
	for i, part := range parts {
		if checksumType.IsSet() {
			partHeaders[checksumType.Key()] = mustChecksum(t, checksumType, part)
		}
		rec := do(http.MethodPut, getPutObjectPartURL("", bucket, object, upload.UploadID, strconv.Itoa(i+1)), part, partHeaders)
		completion.Parts = append(completion.Parts, completePartWithChecksum(checksumType, i+1,
			canonicalizeETag(rec.Header()[xhttp.ETag][0]), partHeaders[checksumType.Key()]))
	}
	body, err := xml.Marshal(completion)
	if err != nil {
		t.Fatalf("failed to encode CompleteMultipartUpload: %v", err)
	}
	delete(partHeaders, checksumType.Key())
	do(http.MethodPost, getCompleteMultipartUploadURL("", bucket, object, upload.UploadID), body, partHeaders)
}

func testAPIFederatedCopyObjectSSE(objectAPI ObjectLayer, instanceType, bucketName string,
	apiRouter http.Handler, credentials auth.Credentials, t *testing.T,
) {
	testKMS, err := kms.NewBuiltin(federationTestKMSKeyID, bytes.Repeat([]byte{0x58}, 32))
	if err != nil {
		t.Fatalf("unable to create the test KMS: %v", err)
	}
	previousKMS := GlobalKMS
	GlobalKMS = testKMS
	defer func() { GlobalKMS = previousKMS }()
	// .txt sources are stored compressed, SSE-S3 and SSE-KMS ones included;
	// SSE-C data is never compressed.
	restoreCompression := setCopyChecksumCompression(true)
	defer restoreCompression()

	remoteBucket, cleanup := setupCopyObjectFederationTLS(t, objectAPI, apiRouter, instanceType, bucketName)
	defer cleanup()

	// A DARE package holds 64 KiB of plaintext, so the larger bodies span two
	// packages and catch a size that accounts for only one.
	large := bytes.Repeat([]byte("federated sse copy body "), 64*1024/24+1)[:64*1024+1]
	bodies := []struct {
		name, ext            string
		data                 []byte
		requested, inherited hash.ChecksumType
	}{
		{name: "empty", ext: ".bin"},
		{name: "empty-explicit", ext: ".bin", requested: hash.ChecksumSHA256},
		{name: "inherited", ext: ".bin", data: []byte("stored encrypted checksum"), inherited: hash.ChecksumCRC32},
		{name: "small", ext: ".bin", data: []byte("abc")},
		{name: "two-packages", ext: ".bin", data: large},
		{name: "compressed", ext: ".txt", data: large},
	}
	pairs := []struct{ src, dst string }{
		{"plain", "s3"},
		{"s3", "plain"},
		{"s3", "s3"},
		{"plain", "c"},
		{"c", "plain"},
		{"c", "c"},
		{"s3", "c"},
		{"plain", "kms"},
		{"kms", "plain"},
		{"kms", "kms"},
		{"plain", "kms-context"},
		{"kms-context", "kms-context"},
		{"kms-context", "kms"},
		{"kms-context", "kms-empty-context"},
	}
	const srcKeyByte, dstKeyByte = 0x11, 0x22

	for _, body := range bodies {
		for _, pair := range pairs {
			// Focus empty and inherited-checksum cases on each encrypted kind.
			if body.name == "empty" || body.name == "empty-explicit" || body.name == "inherited" {
				if pair.src != pair.dst || pair.src == "kms-context" {
					continue
				}
			}
			t.Run(body.name+"/"+pair.src+"-to-"+pair.dst, func(t *testing.T) {
				prefix := "federation/sse-" + body.name + "-" + pair.src + "-to-" + pair.dst
				srcObject, dstObject := prefix+"-source"+body.ext, prefix+"-destination"+body.ext

				sourceHeaders := federationSSEHeaders(pair.src, srcKeyByte, false)
				if pair.src == "kms-context" {
					sourceHeaders[xhttp.AmzServerSideEncryptionKmsContext] = base64.StdEncoding.EncodeToString([]byte(`{"tenant":"source"}`))
				}
				if body.inherited.IsSet() {
					sourceHeaders[body.inherited.Key()] = mustChecksum(t, body.inherited, body.data)
				}
				putCopyChecksumSource(t, apiRouter, credentials, bucketName, srcObject, body.data, sourceHeaders)
				before, err := objectAPI.GetObjectInfo(t.Context(), bucketName, srcObject, ObjectOptions{})
				if err != nil {
					t.Fatalf("%s: GetObjectInfo(source) failed: %v", instanceType, err)
				}
				if got, want := federationStoredSSE(before.UserDefined), strings.Split(pair.src, "-")[0]; got != want {
					t.Fatalf("%s: source stored as %s, want %s", instanceType, got, want)
				}
				if compressed := body.ext == ".txt" && pair.src != "c"; before.IsCompressed() != compressed {
					t.Fatalf("%s: source compressed=%v, want %v", instanceType, before.IsCompressed(), compressed)
				}
				assertFederationKMSContext(t, before, sourceHeaders[xhttp.AmzServerSideEncryptionKmsContext])

				if body.inherited.IsSet() {
					sourceKeys := make(http.Header)
					for k, v := range federationSSEHeaders(pair.src, srcKeyByte, true) {
						sourceKeys.Set(k, v)
					}
					assertCopyChecksum(t, objectAPI, bucketName, srcObject, body.inherited, body.data, false, sourceKeys)
				}
				headers := federationSSEHeaders(pair.dst, dstKeyByte, false)
				if body.requested.IsSet() {
					headers[xhttp.AmzChecksumAlgo] = body.requested.String()
				}
				maps.Copy(headers, federationSSEHeaders(pair.src, srcKeyByte, true))
				rec := federatedCopyRequest(t, apiRouter, credentials, bucketName, srcObject, remoteBucket, dstObject, headers)
				if rec.Code != http.StatusOK {
					t.Fatalf("%s: federated CopyObject failed: %d %s", instanceType, rec.Code, rec.Body.String())
				}
				wantChecksum := hash.ChecksumCRC64NVME
				if body.requested.IsSet() {
					wantChecksum = body.requested
				} else if body.inherited.IsSet() {
					wantChecksum = body.inherited
				}
				var getHeaders map[string]string
				if pair.dst == "c" {
					getHeaders = federationSSEHeaders("c", dstKeyByte, false)
				}
				after := assertFederationCopyReadback(t, objectAPI, apiRouter, credentials, remoteBucket, dstObject,
					rec, wantChecksum, body.data, body.ext == ".txt" && pair.dst != "c", getHeaders)
				assertFederationKMSContext(t, after, headers[xhttp.AmzServerSideEncryptionKmsContext])
				// A second encryption layer would add its own DARE overhead.
				if got, want := federationStoredSSE(after.UserDefined), strings.Split(pair.dst, "-")[0]; got != want {
					t.Fatalf("%s: destination stored as %s, want %s", instanceType, got, want)
				}
				if pair.dst != "plain" && !after.IsCompressed() {
					once := ObjectInfo{Size: int64(len(body.data))}
					if want := once.EncryptedSize(); after.Size != want {
						t.Fatalf("%s: destination stored %d bytes, want %d for %d bytes encrypted once",
							instanceType, after.Size, want, len(body.data))
					}
				}

				// The source must be untouched.
				source, err := objectAPI.GetObjectInfo(t.Context(), bucketName, srcObject, ObjectOptions{})
				if err != nil {
					t.Fatalf("%s: GetObjectInfo(source) after the copy failed: %v", instanceType, err)
				}
				if source.Size != before.Size || source.ETag != before.ETag || !source.ModTime.Equal(before.ModTime) {
					t.Fatalf("%s: federated copy modified its source: size %d->%d etag %s->%s",
						instanceType, before.Size, source.Size, before.ETag, source.ETag)
				}
			})
		}
	}

	// Multipart sources cross an encryption boundary per part. SHA256 also
	// exercises promotion from a stored composite to a full-object checksum.
	parts := [][]byte{bytes.Repeat([]byte("p"), globalMinPartSize+1), []byte("q!")}
	data := bytes.Join(parts, nil)
	for _, pair := range []struct {
		src, dst  string
		composite bool
	}{
		{src: "s3", dst: "plain"},
		{src: "s3", dst: "s3"},
		{src: "c", dst: "c"},
		{src: "kms", dst: "kms", composite: true},
	} {
		t.Run("multipart/"+pair.src+"-to-"+pair.dst, func(t *testing.T) {
			srcObject, dstObject := "federation/multipart-source-"+pair.src+"-"+pair.dst+".bin", "federation/multipart-destination-"+pair.src+"-"+pair.dst+".bin"
			sourceHeaders := federationSSEHeaders(pair.src, srcKeyByte, false)
			wantChecksum := hash.ChecksumCRC64NVME
			if pair.composite {
				wantChecksum = hash.ChecksumSHA256
				sourceHeaders[xhttp.AmzChecksumAlgo] = wantChecksum.String()
				sourceHeaders[xhttp.AmzChecksumType] = xhttp.AmzChecksumTypeComposite
			}
			putFederationMultipartSource(t, apiRouter, credentials, bucketName, srcObject, parts, sourceHeaders)
			before, err := objectAPI.GetObjectInfo(t.Context(), bucketName, srcObject, ObjectOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if len(before.Parts) != len(parts) || federationStoredSSE(before.UserDefined) != pair.src {
				t.Fatalf("source has %d parts stored as %s, want %d parts as %s", len(before.Parts), federationStoredSSE(before.UserDefined), len(parts), pair.src)
			}
			if pair.composite {
				checksums, _ := before.decryptChecksums(0, nil)
				if checksums[xhttp.AmzChecksumType] != xhttp.AmzChecksumTypeComposite || !strings.HasSuffix(checksums[wantChecksum.String()], "-2") {
					t.Fatalf("source did not persist a two-part composite checksum: %v", checksums)
				}
			}
			headers := federationSSEHeaders(pair.dst, dstKeyByte, false)
			maps.Copy(headers, federationSSEHeaders(pair.src, srcKeyByte, true))
			rec := federatedCopyRequest(t, apiRouter, credentials, bucketName, srcObject, remoteBucket, dstObject, headers)
			if rec.Code != http.StatusOK {
				t.Fatalf("%s: federated CopyObject failed: %d %s", instanceType, rec.Code, rec.Body.String())
			}
			var getHeaders map[string]string
			if pair.dst == "c" {
				getHeaders = federationSSEHeaders("c", dstKeyByte, false)
			}
			after := assertFederationCopyReadback(t, objectAPI, apiRouter, credentials, remoteBucket, dstObject, rec, wantChecksum, data, false, getHeaders)
			if got := federationStoredSSE(after.UserDefined); got != pair.dst {
				t.Fatalf("destination stored as %s, want %s", got, pair.dst)
			}
			if once := (ObjectInfo{Size: int64(len(data))}); pair.dst != "plain" && after.Size != once.EncryptedSize() {
				t.Fatalf("destination stored %d bytes, want %d for %d bytes encrypted once", after.Size, once.EncryptedSize(), len(data))
			}
		})
	}
}
