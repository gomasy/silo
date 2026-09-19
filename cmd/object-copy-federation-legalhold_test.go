// Copyright (c) 2015-2026 MinIO, Inc.
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
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio/internal/auth"
	objectlock "github.com/minio/minio/internal/bucket/object/lock"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/kms"
)

// enableBucketObjectLock puts a lock-enabled configuration on an existing
// bucket so checkPutObjectLockAllowed accepts a legal-hold header instead of
// rejecting the request with ErrInvalidBucketObjectLockConfiguration.
func enableBucketObjectLock(t *testing.T, bucket string) {
	t.Helper()
	meta, err := globalBucketMetadataSys.Get(bucket)
	if err != nil {
		t.Fatalf("unable to read bucket metadata for %s: %v", bucket, err)
	}
	updated := meta
	updated.ObjectLockConfigXML = enabledBucketObjectLockConfig
	updated.VersioningConfigXML = enabledBucketVersioningConfig
	// The XML alone is not enough: BucketMetadata keeps a parsed copy that the
	// lookups actually read, and it is only populated by parseAllConfigs.
	if err := updated.parseAllConfigs(t.Context(), newObjectLayerFn()); err != nil {
		t.Fatalf("unable to parse bucket metadata for %s: %v", bucket, err)
	}
	globalBucketMetadataSys.Set(bucket, updated)
}

// Exercise #165 and #166 together, including hold OFF, default retention and
// explicit subsecond retention. Ordinary copies must not inherit source locks.
func TestAPIFederatedCopyObjectLockParity(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t:         t,
		endpoints: []string{"CopyObject", "PutObject", "HeadObject", "GetObject"},
		objAPITest: func(obj ObjectLayer, instanceType, bucket string, router http.Handler, cred auth.Credentials, t *testing.T) {
			testKMS, err := kms.NewBuiltin(federationTestKMSKeyID, bytes.Repeat([]byte{0x58}, 32))
			if err != nil {
				t.Fatal(err)
			}
			previousKMS := GlobalKMS
			GlobalKMS = testKMS
			defer func() { GlobalKMS = previousKMS }()
			remoteBucket, capture, cleanup := setupCopyObjectFederation(t, obj, router, instanceType, bucket)
			defer cleanup()
			enableBucketObjectLock(t, bucket)
			until := UTCNow().Add(7 * 24 * time.Hour).Truncate(time.Second).Add(789 * time.Millisecond).Format(time.RFC3339Nano)
			for _, sourceType := range []string{"plain", "s3"} {
				source := sourceType + "-held-source"
				headers := federationSSEHeaders(sourceType, 0, false)
				headers[xhttp.AmzObjectLockLegalHold] = "ON"
				putCopyChecksumSource(t, router, cred, bucket, source, []byte("held source"), headers)
				for _, tc := range []struct {
					name, hold, defaultMode, explicitMode string
					replace                               bool
				}{
					{name: "on", hold: "ON"},
					{name: "off", hold: "OFF"},
					{name: "no source inheritance"},
					{name: "on with default", hold: "ON", defaultMode: "GOVERNANCE"},
					{name: "off with default", hold: "OFF", defaultMode: "COMPLIANCE"},
					{name: "explicit retention", explicitMode: "GOVERNANCE"},
					{name: "hold and explicit retention", hold: "ON", explicitMode: "COMPLIANCE"},
					{name: "replace metadata", hold: "ON", explicitMode: "GOVERNANCE", replace: true},
				} {
					t.Run(instanceType+"/"+sourceType+"/"+tc.name, func(t *testing.T) {
						setTestBucketDefaultRetention(t, bucket, tc.defaultMode)
						setTestBucketDefaultRetention(t, remoteBucket, tc.defaultMode)
						headers := map[string]string{}
						if tc.hold != "" {
							headers[xhttp.AmzObjectLockLegalHold] = tc.hold
						}
						if tc.explicitMode != "" {
							headers[xhttp.AmzObjectLockMode] = tc.explicitMode
							headers[xhttp.AmzObjectLockRetainUntilDate] = until
						}
						if tc.replace {
							headers[xhttp.AmzMetadataDirective] = "REPLACE"
							headers["X-Amz-Meta-Origin"] = "replacement"
						}
						capture.mu.Lock()
						capture.headers = nil
						capture.mu.Unlock()
						for _, destinationBucket := range []string{bucket, remoteBucket} {
							destination := sourceType + "-copy-" + strings.ReplaceAll(tc.name, " ", "-")
							rec := federatedCopyRequest(t, router, cred, bucket, source, destinationBucket, destination, headers)
							if rec.Code != http.StatusOK {
								t.Fatalf("copy to %s: %d %s", destinationBucket, rec.Code, rec.Body.String())
							}
							info, err := obj.GetObjectInfo(t.Context(), destinationBucket, destination, ObjectOptions{})
							if err != nil {
								t.Fatal(err)
							}
							if got := objectlock.GetObjectLegalHoldMeta(info.UserDefined).Status; string(got) != tc.hold {
								t.Errorf("stored hold = %q, want %q", got, tc.hold)
							}
							retention := objectlock.GetObjectRetentionMeta(info.UserDefined)
							mode := tc.explicitMode
							if mode == "" {
								mode = tc.defaultMode
							}
							if string(retention.Mode) != mode {
								t.Errorf("stored retention = %q, want %q", retention.Mode, mode)
							}
							if tc.explicitMode != "" && retention.RetainUntilDate.Format(time.RFC3339Nano) != until {
								t.Errorf("retention date lost precision: %s, want %s", retention.RetainUntilDate, until)
							}
							for key := range info.UserDefined {
								if stringsHasPrefixFold(key, "X-Amz-Meta-X-Amz-Object-Lock-") {
									t.Errorf("lock state became user metadata: %s", key)
								}
							}
							if tc.replace && info.UserDefined["X-Amz-Meta-Origin"] != "replacement" {
								t.Errorf("replacement metadata was lost: %v", info.UserDefined)
							}
						}
						typed, asMetadata := capture.legalHoldHeaders()
						if len(asMetadata) != 0 || (tc.hold != "" && strings.Join(typed, "") != tc.hold) || (tc.hold == "" && len(typed) != 0) {
							t.Errorf("forwarded hold headers = %v, metadata = %v; want %q", typed, asMetadata, tc.hold)
						}
					})
				}
			}
		},
	})
}

// legalHoldHeaders returns the forwarded legal-hold headers the remote saw,
// separated into the real Object Lock header and the user-metadata spelling
// minio-go produces for an unrecognized UserMetadata key.
func (c *federationRemoteCapture) legalHoldHeaders() (typed, asMetadata []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	metaKey := "X-Amz-Meta-" + xhttp.AmzObjectLockLegalHold
	for _, h := range c.headers {
		for k, v := range h {
			switch {
			case strings.EqualFold(k, xhttp.AmzObjectLockLegalHold):
				typed = append(typed, strings.Join(v, ","))
			case strings.EqualFold(k, metaKey):
				asMetadata = append(asMetadata, strings.Join(v, ","))
			}
		}
	}
	return typed, asMetadata
}

// TestAPIFederatedCopyObjectLegalHold drives the legacy etcd federation branch
// of CopyObjectHandler with an explicit legal hold on the copy.
//
// Before the fix the resolved hold was forwarded inside
// PutObjectOptions.UserMetadata. minio-go's Header() prefixes every
// UserMetadata key it does not recognize with "x-amz-meta-", and
// x-amz-object-lock-legal-hold is in neither supportedHeaders nor isAmzHeader,
// so the hold reached the remote as X-Amz-Meta-X-Amz-Object-Lock-Legal-Hold.
// The destination stored no hold and the copy still answered 200 -- a silent
// loss of a WORM control (#166). Retention requested on the same copy survived,
// which is what made it easy to miss.
func TestAPIFederatedCopyObjectLegalHold(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t:          t,
		objAPITest: testAPIFederatedCopyObjectLegalHold,
		endpoints:  []string{"CopyObject", "PutObject", "HeadObject", "GetObject"},
	})
}

func testAPIFederatedCopyObjectLegalHold(objectAPI ObjectLayer, instanceType, bucketName string,
	apiRouter http.Handler, credentials auth.Credentials, t *testing.T,
) {
	data := []byte("federated copy with a legal hold")
	srcObject := "federation/legal-hold-source"
	putCopyChecksumSource(t, apiRouter, credentials, bucketName, srcObject, data, nil)

	remoteBucket, capture, cleanup := setupCopyObjectFederation(t, objectAPI, apiRouter, instanceType, bucketName)
	defer cleanup()

	// Both roles read bucket metadata from the shared backend in this fixture,
	// so one configuration covers the proxy's own check and the remote's.
	enableBucketObjectLock(t, bucketName)
	enableBucketObjectLock(t, remoteBucket)

	dstObject := "federation/legal-hold-destination"
	rec := federatedCopyRequest(t, apiRouter, credentials, bucketName, srcObject, remoteBucket, dstObject,
		map[string]string{xhttp.AmzObjectLockLegalHold: string(objectlock.LegalHoldOn)})
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: federated CopyObject with a legal hold failed: %d %s",
			instanceType, rec.Code, rec.Body.String())
	}

	// The wire is the point: the hold must arrive as the Object Lock header,
	// never as user metadata. A 200 with the metadata spelling is exactly the
	// silent loss this test exists for.
	typed, asMetadata := capture.legalHoldHeaders()
	if len(asMetadata) != 0 {
		t.Fatalf("%s: legal hold forwarded as user metadata %v; the destination stores no hold",
			instanceType, asMetadata)
	}
	if len(typed) == 0 {
		t.Fatalf("%s: no %s header reached the remote deployment", instanceType, xhttp.AmzObjectLockLegalHold)
	}
	for _, got := range typed {
		if !strings.EqualFold(got, string(objectlock.LegalHoldOn)) {
			t.Fatalf("%s: forwarded legal hold = %q, want %q", instanceType, got, objectlock.LegalHoldOn)
		}
	}

	// And it must actually be stored on the destination version.
	oi, err := objectAPI.GetObjectInfo(t.Context(), remoteBucket, dstObject, ObjectOptions{})
	if err != nil {
		t.Fatalf("%s: unable to stat the federated copy destination: %v", instanceType, err)
	}
	if hold := objectlock.GetObjectLegalHoldMeta(oi.UserDefined); hold.Status != objectlock.LegalHoldOn {
		t.Fatalf("%s: destination legal hold = %q, want %q (metadata: %v)",
			instanceType, hold.Status, objectlock.LegalHoldOn, oi.UserDefined)
	}
}
