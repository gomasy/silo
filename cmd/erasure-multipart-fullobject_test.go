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
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/hash"
	xhttp "github.com/minio/minio/internal/http"
)

// multipartChecksumTestData is the payload used by the full object checksum
// tests: one 5 MiB part (the minimum allowed non-final part size) and a small
// trailing part, so part merging is actually exercised.
func multipartChecksumTestData() (parts [][]byte, full []byte) {
	parts = [][]byte{
		bytes.Repeat([]byte("a"), 5*humanize.MiByte),
		bytes.Repeat([]byte("b"), 1*humanize.KiByte),
	}
	for _, p := range parts {
		full = append(full, p...)
	}
	return parts, full
}

func mustChecksum(t *testing.T, typ hash.ChecksumType, data []byte) string {
	t.Helper()
	cs := hash.NewChecksumFromData(typ, data)
	if cs == nil {
		t.Fatalf("unable to compute %s checksum", typ.String())
	}
	return cs.Encoded
}

// newMultipartUploadHTTP starts a multipart upload over the API router and
// returns the upload ID.
func newMultipartUploadHTTP(t *testing.T, apiRouter http.Handler, creds auth.Credentials,
	bucket, object, algo, checksumType string,
) string {
	t.Helper()
	hdrs := map[string]string{xhttp.AmzChecksumAlgo: algo}
	if checksumType != "" {
		hdrs[xhttp.AmzChecksumType] = checksumType
	}
	req, err := newTestSignedRequestV4(http.MethodPost, getNewMultipartURL("", bucket, object),
		0, nil, creds.AccessKey, creds.SecretKey, hdrs)
	if err != nil {
		t.Fatalf("failed to build NewMultipartUpload request: %v", err)
	}
	rec := httptest.NewRecorder()
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("NewMultipartUpload failed: %d %s", rec.Code, rec.Body.String())
	}
	var res InitiateMultipartUploadResponse
	if err := xml.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to decode NewMultipartUpload response: %v", err)
	}
	return res.UploadID
}

// uploadPartsHTTP uploads every part carrying its own checksum header, the way
// modern AWS SDKs do by default, and returns the part ETags.
func uploadPartsHTTP(t *testing.T, apiRouter http.Handler, creds auth.Credentials,
	bucket, object, uploadID string, typ hash.ChecksumType, parts [][]byte,
) []string {
	t.Helper()
	etags := make([]string, len(parts))
	for i, p := range parts {
		req, err := newTestSignedRequestV4(http.MethodPut,
			getPutObjectPartURL("", bucket, object, uploadID, strconv.Itoa(i+1)),
			int64(len(p)), bytes.NewReader(p), creds.AccessKey, creds.SecretKey,
			map[string]string{typ.Key(): mustChecksum(t, typ, p)})
		if err != nil {
			t.Fatalf("failed to build UploadPart %d request: %v", i+1, err)
		}
		rec := httptest.NewRecorder()
		apiRouter.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("UploadPart %d failed: %d %s", i+1, rec.Code, rec.Body.String())
		}
		// MinIO writes the header under a literal "ETag" map key, which
		// http.Header.Get would canonicalize to "Etag" and miss.
		etags[i] = rec.Header()[xhttp.ETag][0]
	}
	return etags
}

// completeMultipartUploadHTTP completes the upload. partCS supplies an optional
// per-part checksum for each part; an empty string omits it, which is exactly
// what boto3, aws-sdk-js and the Java SDK send when the caller does not track
// part checksums.
func completeMultipartUploadHTTP(t *testing.T, apiRouter http.Handler, creds auth.Credentials,
	bucket, object, uploadID string, etags []string, partCS []string, hdrs map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()
	var body bytes.Buffer
	body.WriteString("<CompleteMultipartUpload>")
	for i, etag := range etags {
		fmt.Fprintf(&body, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag>", i+1, etag)
		if i < len(partCS) && partCS[i] != "" {
			fmt.Fprintf(&body, "<ChecksumCRC32>%s</ChecksumCRC32>", partCS[i])
		}
		body.WriteString("</Part>")
	}
	body.WriteString("</CompleteMultipartUpload>")

	req, err := newTestSignedRequestV4(http.MethodPost,
		getCompleteMultipartUploadURL("", bucket, object, uploadID),
		int64(body.Len()), bytes.NewReader(body.Bytes()), creds.AccessKey, creds.SecretKey, hdrs)
	if err != nil {
		t.Fatalf("failed to build CompleteMultipartUpload request: %v", err)
	}
	rec := httptest.NewRecorder()
	apiRouter.ServeHTTP(rec, req)
	return rec
}

func apiError(t *testing.T, rec *httptest.ResponseRecorder) APIErrorResponse {
	t.Helper()
	var e APIErrorResponse
	if err := xml.Unmarshal(rec.Body.Bytes(), &e); err != nil {
		t.Fatalf("unable to decode error response %q: %v", rec.Body.String(), err)
	}
	return e
}

func apiErrorCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	return apiError(t, rec).Code
}

// TestAPICompleteMultipartFullObjectChecksum covers pgsty/silo#31.
//
// A multipart upload created with a full object checksum type must be
// completable by sending only PartNumber and ETag per part, plus the object
// level checksum in the request headers. That is what AWS S3 accepts, and it is
// the point of FULL_OBJECT: the client no longer has to retain per-part
// checksums, only the part numbers and ETags it already tracks.
func TestAPICompleteMultipartFullObjectChecksum(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t:          t,
		objAPITest: testAPICompleteMultipartFullObjectChecksum,
		endpoints:  []string{"NewMultipart", "PutObjectPart", "CompleteMultipart"},
	})
}

func testAPICompleteMultipartFullObjectChecksum(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	partData, full := multipartChecksumTestData()

	// Only CRC based algorithms can linearize into a full object checksum.
	// Which one a client picks by default is SDK specific - the AWS CLI v2
	// defaults to CRC64NVME while the Go and JavaScript SDKs default to CRC32 -
	// so cover all three.
	for _, typ := range []hash.ChecksumType{hash.ChecksumCRC32, hash.ChecksumCRC32C, hash.ChecksumCRC64NVME} {
		objectName := "uploads/full-object-" + typ.String()

		uploadID := newMultipartUploadHTTP(t, apiRouter, credentials, bucketName, objectName,
			typ.String(), xhttp.AmzChecksumTypeFullObject)
		etags := uploadPartsHTTP(t, apiRouter, credentials, bucketName, objectName, uploadID, typ, partData)

		rec := completeMultipartUploadHTTP(t, apiRouter, credentials, bucketName, objectName, uploadID, etags, nil,
			map[string]string{
				typ.Key():             mustChecksum(t, typ, full),
				xhttp.AmzChecksumType: xhttp.AmzChecksumTypeFullObject,
			})
		if rec.Code != http.StatusOK {
			t.Fatalf("%s/%s: CompleteMultipartUpload failed: %d %s",
				instanceType, typ.String(), rec.Code, rec.Body.String())
		}

		oi, err := obj.GetObjectInfo(t.Context(), bucketName, objectName, ObjectOptions{})
		if err != nil {
			t.Fatalf("%s/%s: GetObjectInfo failed: %v", instanceType, typ.String(), err)
		}
		if oi.Size != int64(len(full)) {
			t.Fatalf("%s/%s: expected object size %d, got %d", instanceType, typ.String(), len(full), oi.Size)
		}

		// The persisted checksum must be the merged full object value - not a
		// composite "<checksum>-<parts>" value - and must report FULL_OBJECT.
		cs, _ := oi.decryptChecksums(0, nil)
		if got, want := cs[typ.String()], mustChecksum(t, typ, full); got != want {
			t.Fatalf("%s/%s: expected stored checksum %q, got %q", instanceType, typ.String(), want, got)
		}
		if got := cs[xhttp.AmzChecksumType]; got != xhttp.AmzChecksumTypeFullObject {
			t.Fatalf("%s/%s: expected stored checksum type %q, got %q",
				instanceType, typ.String(), xhttp.AmzChecksumTypeFullObject, got)
		}
	}
}

// TestAPICompleteMultipartFullObjectChecksumMismatch asserts that accepting
// completions without part checksums does not weaken integrity: a wrong object
// level checksum is still rejected.
func TestAPICompleteMultipartFullObjectChecksumMismatch(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t:          t,
		objAPITest: testAPICompleteMultipartFullObjectChecksumMismatch,
		endpoints:  []string{"NewMultipart", "PutObjectPart", "CompleteMultipart"},
	})
}

func testAPICompleteMultipartFullObjectChecksumMismatch(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	partData, _ := multipartChecksumTestData()
	objectName := "uploads/full-object-mismatch"
	typ := hash.ChecksumCRC32

	uploadID := newMultipartUploadHTTP(t, apiRouter, credentials, bucketName, objectName,
		typ.String(), xhttp.AmzChecksumTypeFullObject)
	etags := uploadPartsHTTP(t, apiRouter, credentials, bucketName, objectName, uploadID, typ, partData)

	rec := completeMultipartUploadHTTP(t, apiRouter, credentials, bucketName, objectName, uploadID, etags, nil,
		map[string]string{
			typ.Key():             mustChecksum(t, typ, []byte("not the object content")),
			xhttp.AmzChecksumType: xhttp.AmzChecksumTypeFullObject,
		})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("%s: CompleteMultipartUpload with a bad full object checksum returned %d, want 400",
			instanceType, rec.Code)
	}
	apiErr := apiError(t, rec)
	if apiErr.Code != "BadDigest" {
		t.Fatalf("%s: expected BadDigest, got %q", instanceType, apiErr.Code)
	}
	if want := "The CRC32 you specified did not match the calculated checksum."; apiErr.Message != want {
		t.Fatalf("%s: expected message %q, got %q", instanceType, want, apiErr.Message)
	}

	if _, err := obj.GetObjectInfo(t.Context(), bucketName, objectName, ObjectOptions{}); err == nil {
		t.Fatalf("%s: object was created despite a failed checksum validation", instanceType)
	}
}

// TestAPICompleteMultipartCompositeStillRequiresPartChecksums locks in that the
// relaxation is scoped to full object checksums. For the algorithm/type pairs
// AWS actually supports as composite, AWS requires a checksum for every part in
// the CompleteMultipartUpload body, and so do we. (CRC64NVME is deliberately not
// covered: AWS does not support it as composite and MinIO canonicalises it to a
// full object checksum at initiation.)
func TestAPICompleteMultipartCompositeStillRequiresPartChecksums(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t:          t,
		objAPITest: testAPICompleteMultipartCompositeStillRequiresPartChecksums,
		endpoints:  []string{"NewMultipart", "PutObjectPart", "CompleteMultipart"},
	})
}

func testAPICompleteMultipartCompositeStillRequiresPartChecksums(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	partData, _ := multipartChecksumTestData()

	for _, typ := range []hash.ChecksumType{hash.ChecksumCRC32, hash.ChecksumSHA256} {
		objectName := "uploads/composite-" + typ.String()

		uploadID := newMultipartUploadHTTP(t, apiRouter, credentials, bucketName, objectName,
			typ.String(), xhttp.AmzChecksumTypeComposite)
		etags := uploadPartsHTTP(t, apiRouter, credentials, bucketName, objectName, uploadID, typ, partData)

		rec := completeMultipartUploadHTTP(t, apiRouter, credentials, bucketName, objectName, uploadID, etags, nil, nil)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s/%s: composite CompleteMultipartUpload without part checksums returned %d, want 400",
				instanceType, typ.String(), rec.Code)
		}
		apiErr := apiError(t, rec)
		if apiErr.Code != "InvalidRequest" {
			t.Fatalf("%s/%s: expected InvalidRequest, got %q", instanceType, typ.String(), apiErr.Code)
		}
		wantMessage := fmt.Sprintf("The upload was created using a %s checksum. The complete request must include the checksum for each part. It was missing for part 1 in the request.", strings.ToLower(typ.String()))
		if apiErr.Message != wantMessage {
			t.Fatalf("%s/%s: expected message %q, got %q", instanceType, typ.String(), wantMessage, apiErr.Message)
		}

		// A retry that supplies part 1 but omits part 2 must name the actual
		// missing part, not merely the first part in the upload.
		completedParts := []CompletePart{
			completePartWithChecksum(typ, 1, etags[0], mustChecksum(t, typ, partData[0])),
			{PartNumber: 2, ETag: etags[1]},
		}
		rec = completePartsHTTP(t, apiRouter, credentials, bucketName, objectName, uploadID, completedParts, nil)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s/%s: composite completion missing part 2 checksum returned %d, want 400",
				instanceType, typ.String(), rec.Code)
		}
		apiErr = apiError(t, rec)
		if apiErr.Code != "InvalidRequest" {
			t.Fatalf("%s/%s: expected InvalidRequest, got %q", instanceType, typ.String(), apiErr.Code)
		}
		wantMessage = fmt.Sprintf("The upload was created using a %s checksum. The complete request must include the checksum for each part. It was missing for part 2 in the request.", strings.ToLower(typ.String()))
		if apiErr.Message != wantMessage {
			t.Fatalf("%s/%s: expected message %q, got %q", instanceType, typ.String(), wantMessage, apiErr.Message)
		}
		if _, err := obj.GetObjectInfo(t.Context(), bucketName, objectName, ObjectOptions{}); err == nil {
			t.Fatalf("%s/%s: object was created despite a rejected completion", instanceType, typ.String())
		}
	}
}

// TestAPICompleteMultipartCompositeChecksumMismatch covers the composite
// object-checksum path independently from full object checksum merging.
func TestAPICompleteMultipartCompositeChecksumMismatch(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t:          t,
		objAPITest: testAPICompleteMultipartCompositeChecksumMismatch,
		endpoints:  []string{"NewMultipart", "PutObjectPart", "CompleteMultipart"},
	})
}

func testAPICompleteMultipartCompositeChecksumMismatch(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	typ := hash.ChecksumCRC32
	partData, _ := multipartChecksumTestData()
	objectName := "uploads/composite-object-mismatch"
	uploadID := newMultipartUploadHTTP(t, apiRouter, credentials, bucketName, objectName,
		typ.String(), xhttp.AmzChecksumTypeComposite)
	etags := uploadPartsHTTP(t, apiRouter, credentials, bucketName, objectName, uploadID, typ, partData)
	partCS := []string{mustChecksum(t, typ, partData[0]), mustChecksum(t, typ, partData[1])}
	rec := completeMultipartUploadHTTP(t, apiRouter, credentials, bucketName, objectName, uploadID, etags, partCS,
		map[string]string{
			typ.Key():             mustChecksum(t, typ, []byte("wrong composite checksum")) + "-2",
			xhttp.AmzChecksumType: xhttp.AmzChecksumTypeComposite,
		})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("%s: composite checksum mismatch returned %d, want 400", instanceType, rec.Code)
	}
	apiErr := apiError(t, rec)
	if apiErr.Code != "BadDigest" {
		t.Fatalf("%s: expected BadDigest, got %q", instanceType, apiErr.Code)
	}
	if want := "The CRC32 you specified did not match the calculated checksum."; apiErr.Message != want {
		t.Fatalf("%s: expected message %q, got %q", instanceType, want, apiErr.Message)
	}
	if _, err := obj.GetObjectInfo(t.Context(), bucketName, objectName, ObjectOptions{}); err == nil {
		t.Fatalf("%s: object was created despite a failed composite checksum validation", instanceType)
	}
}

// TestAPICompleteMultipartChecksumTypeMismatch verifies the type comparison in
// both directions. ChecksumType is a bitmask, so a containment check alone
// incorrectly accepts COMPOSITE uploads completed as FULL_OBJECT.
func TestAPICompleteMultipartChecksumTypeMismatch(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t:          t,
		objAPITest: testAPICompleteMultipartChecksumTypeMismatch,
		endpoints:  []string{"NewMultipart", "PutObjectPart", "CompleteMultipart"},
	})
}

func testAPICompleteMultipartChecksumTypeMismatch(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	typ := hash.ChecksumCRC32
	partData, full := multipartChecksumTestData()
	for _, test := range []struct {
		name         string
		createdType  string
		providedType string
	}{
		{name: "full-to-composite", createdType: xhttp.AmzChecksumTypeFullObject, providedType: xhttp.AmzChecksumTypeComposite},
		{name: "composite-to-full", createdType: xhttp.AmzChecksumTypeComposite, providedType: xhttp.AmzChecksumTypeFullObject},
	} {
		t.Run(test.name, func(t *testing.T) {
			objectName := "type-mismatch/" + test.name
			uploadID := newMultipartUploadHTTP(t, apiRouter, credentials, bucketName, objectName,
				typ.String(), test.createdType)
			etags := uploadPartsHTTP(t, apiRouter, credentials, bucketName, objectName, uploadID, typ, partData)
			partCS := []string{mustChecksum(t, typ, partData[0]), mustChecksum(t, typ, partData[1])}
			rec := completeMultipartUploadHTTP(t, apiRouter, credentials, bucketName, objectName, uploadID, etags, partCS,
				map[string]string{
					typ.Key():             mustChecksum(t, typ, full),
					xhttp.AmzChecksumType: test.providedType,
				})
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("%s: checksum type mismatch returned %d, want 400", instanceType, rec.Code)
			}
			apiErr := apiError(t, rec)
			if apiErr.Code != "BadDigest" {
				t.Fatalf("%s: expected BadDigest, got %q", instanceType, apiErr.Code)
			}
			wantMessage := fmt.Sprintf("The checksum type %s does not match the multipart upload checksum type %s.", test.providedType, test.createdType)
			if apiErr.Message != wantMessage {
				t.Fatalf("%s: expected message %q, got %q", instanceType, wantMessage, apiErr.Message)
			}
			if _, err := obj.GetObjectInfo(t.Context(), bucketName, objectName, ObjectOptions{}); err == nil {
				t.Fatalf("%s: object was created despite a rejected checksum type", instanceType)
			}
		})

		t.Run(test.name+"-type-only", func(t *testing.T) {
			objectName := "type-mismatch/type-only-" + test.name
			uploadID := newMultipartUploadHTTP(t, apiRouter, credentials, bucketName, objectName,
				typ.String(), test.createdType)
			etags := uploadPartsHTTP(t, apiRouter, credentials, bucketName, objectName, uploadID, typ, partData)
			partCS := []string{mustChecksum(t, typ, partData[0]), mustChecksum(t, typ, partData[1])}
			rec := completeMultipartUploadHTTP(t, apiRouter, credentials, bucketName, objectName, uploadID, etags, partCS,
				map[string]string{xhttp.AmzChecksumType: test.providedType})
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("%s: checksum type-only mismatch returned %d, want 400", instanceType, rec.Code)
			}
			apiErr := apiError(t, rec)
			if apiErr.Code != "BadDigest" {
				t.Fatalf("%s: expected BadDigest, got %q", instanceType, apiErr.Code)
			}
			wantMessage := fmt.Sprintf("The checksum type %s does not match the multipart upload checksum type %s.", test.providedType, test.createdType)
			if apiErr.Message != wantMessage {
				t.Fatalf("%s: expected message %q, got %q", instanceType, wantMessage, apiErr.Message)
			}
			if _, err := obj.GetObjectInfo(t.Context(), bucketName, objectName, ObjectOptions{}); err == nil {
				t.Fatalf("%s: object was created despite a rejected checksum type-only assertion", instanceType)
			}
		})
	}

	for _, test := range []struct {
		name         string
		providedType string
		withChecksum bool
	}{
		{name: "unknown-type-only", providedType: "NOT_A_TYPE"},
		{name: "lowercase-type-only", providedType: "full_object"},
		{name: "unknown-with-checksum", providedType: "NOT_A_TYPE", withChecksum: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			objectName := "type-mismatch/invalid-" + test.name
			uploadID := newMultipartUploadHTTP(t, apiRouter, credentials, bucketName, objectName,
				typ.String(), xhttp.AmzChecksumTypeComposite)
			etags := uploadPartsHTTP(t, apiRouter, credentials, bucketName, objectName, uploadID, typ, partData)
			partCS := []string{mustChecksum(t, typ, partData[0]), mustChecksum(t, typ, partData[1])}
			headers := map[string]string{xhttp.AmzChecksumType: test.providedType}
			if test.withChecksum {
				headers[typ.Key()] = mustChecksum(t, typ, full)
			}
			rec := completeMultipartUploadHTTP(t, apiRouter, credentials, bucketName, objectName, uploadID, etags, partCS, headers)
			if rec.Code != http.StatusBadRequest || apiErrorCode(t, rec) != "InvalidArgument" {
				t.Fatalf("%s: invalid checksum type returned %d %s", instanceType, rec.Code, rec.Body.String())
			}
			if _, err := obj.GetObjectInfo(t.Context(), bucketName, objectName, ObjectOptions{}); err == nil {
				t.Fatalf("%s: object was created despite an invalid checksum type", instanceType)
			}
		})
	}

	t.Run("matching-type-only", func(t *testing.T) {
		objectName := "type-mismatch/matching-type-only"
		uploadID := newMultipartUploadHTTP(t, apiRouter, credentials, bucketName, objectName,
			typ.String(), xhttp.AmzChecksumTypeComposite)
		etags := uploadPartsHTTP(t, apiRouter, credentials, bucketName, objectName, uploadID, typ, partData)
		partCS := []string{mustChecksum(t, typ, partData[0]), mustChecksum(t, typ, partData[1])}
		rec := completeMultipartUploadHTTP(t, apiRouter, credentials, bucketName, objectName, uploadID, etags, partCS,
			map[string]string{xhttp.AmzChecksumType: xhttp.AmzChecksumTypeComposite})
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: matching checksum type-only assertion returned %d %s", instanceType, rec.Code, rec.Body.String())
		}
	})

	t.Run("omitted-type-is-not-composite", func(t *testing.T) {
		objectName := "type-mismatch/omitted-type"
		uploadID := newMultipartUploadHTTP(t, apiRouter, credentials, bucketName, objectName,
			typ.String(), xhttp.AmzChecksumTypeFullObject)
		etags := uploadPartsHTTP(t, apiRouter, credentials, bucketName, objectName, uploadID, typ, partData)
		rec := completeMultipartUploadHTTP(t, apiRouter, credentials, bucketName, objectName, uploadID, etags, nil,
			map[string]string{typ.Key(): mustChecksum(t, typ, full)})
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: completion without an explicit checksum type returned %d %s",
				instanceType, rec.Code, rec.Body.String())
		}
	})

	t.Run("algorithm-mismatch-remains-invalid-argument", func(t *testing.T) {
		objectName := "type-mismatch/algorithm"
		uploadID := newMultipartUploadHTTP(t, apiRouter, credentials, bucketName, objectName,
			typ.String(), xhttp.AmzChecksumTypeFullObject)
		etags := uploadPartsHTTP(t, apiRouter, credentials, bucketName, objectName, uploadID, typ, partData)
		rec := completeMultipartUploadHTTP(t, apiRouter, credentials, bucketName, objectName, uploadID, etags, nil,
			map[string]string{
				hash.ChecksumCRC32C.Key(): mustChecksum(t, hash.ChecksumCRC32C, full),
				xhttp.AmzChecksumType:     xhttp.AmzChecksumTypeFullObject,
			})
		if rec.Code != http.StatusBadRequest || apiErrorCode(t, rec) != "InvalidArgument" {
			t.Fatalf("%s: algorithm mismatch returned %d %s", instanceType, rec.Code, rec.Body.String())
		}
	})

	t.Run("crc64nvme-composite-is-rejected", func(t *testing.T) {
		crc64Type := hash.ChecksumCRC64NVME
		objectName := "type-mismatch/crc64nvme-composite"
		req, err := newTestSignedRequestV4(http.MethodPost, getNewMultipartURL("", bucketName, objectName),
			0, nil, credentials.AccessKey, credentials.SecretKey, map[string]string{
				xhttp.AmzChecksumAlgo: crc64Type.String(),
				xhttp.AmzChecksumType: xhttp.AmzChecksumTypeComposite,
			})
		if err != nil {
			t.Fatal(err)
		}
		rec := httptest.NewRecorder()
		apiRouter.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest || apiErrorCode(t, rec) != "InvalidArgument" {
			t.Fatalf("%s: CRC64NVME/COMPOSITE returned %d %s", instanceType, rec.Code, rec.Body.String())
		}
	})

	t.Run("crc64nvme-composite-completion-is-rejected", func(t *testing.T) {
		crc64Type := hash.ChecksumCRC64NVME
		objectName := "type-mismatch/crc64nvme-composite-completion"
		uploadID := newMultipartUploadHTTP(t, apiRouter, credentials, bucketName, objectName,
			crc64Type.String(), xhttp.AmzChecksumTypeFullObject)
		etags := uploadPartsHTTP(t, apiRouter, credentials, bucketName, objectName, uploadID, crc64Type, partData)
		partCS := []string{mustChecksum(t, crc64Type, partData[0]), mustChecksum(t, crc64Type, partData[1])}
		rec := completeMultipartUploadHTTP(t, apiRouter, credentials, bucketName, objectName, uploadID, etags, partCS,
			map[string]string{xhttp.AmzChecksumType: xhttp.AmzChecksumTypeComposite})
		if rec.Code != http.StatusBadRequest || apiErrorCode(t, rec) != "BadDigest" {
			t.Fatalf("%s: CRC64NVME composite completion returned %d %s", instanceType, rec.Code, rec.Body.String())
		}
		if _, err := obj.GetObjectInfo(t.Context(), bucketName, objectName, ObjectOptions{}); err == nil {
			t.Fatalf("%s: object was created despite a rejected CRC64NVME checksum type", instanceType)
		}
	})
}

// TestAPICompleteMultipartFullObjectVariants pins down the surrounding
// behavior of the relaxation: what may be omitted, what must still match, and
// that a zero length object is handled like any other.
func TestAPICompleteMultipartFullObjectVariants(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t:          t,
		objAPITest: testAPICompleteMultipartFullObjectVariants,
		endpoints:  []string{"NewMultipart", "PutObjectPart", "CompleteMultipart"},
	})
}

func testAPICompleteMultipartFullObjectVariants(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	typ := hash.ChecksumCRC32
	partData, full := multipartChecksumTestData()
	goodCS := []string{mustChecksum(t, typ, partData[0]), mustChecksum(t, typ, partData[1])}

	setup := func(name string) (string, []string) {
		uploadID := newMultipartUploadHTTP(t, apiRouter, credentials, bucketName, name,
			typ.String(), xhttp.AmzChecksumTypeFullObject)
		return uploadID, uploadPartsHTTP(t, apiRouter, credentials, bucketName, name, uploadID, typ, partData)
	}
	objCSHdr := map[string]string{
		typ.Key():             mustChecksum(t, typ, full),
		xhttp.AmzChecksumType: xhttp.AmzChecksumTypeFullObject,
	}

	t.Run("mixed-present-and-omitted", func(t *testing.T) {
		name := "variants/mixed"
		uploadID, etags := setup(name)
		rec := completeMultipartUploadHTTP(t, apiRouter, credentials, bucketName, name, uploadID, etags,
			[]string{goodCS[0], ""}, objCSHdr)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: want 200, got %d %s", instanceType, rec.Code, rec.Body.String())
		}
	})

	t.Run("supplied-part-checksum-must-match", func(t *testing.T) {
		name := "variants/wrong-part-cs"
		uploadID, etags := setup(name)
		rec := completeMultipartUploadHTTP(t, apiRouter, credentials, bucketName, name, uploadID, etags,
			[]string{mustChecksum(t, typ, []byte("wrong")), ""}, objCSHdr)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: a non-empty but wrong part checksum must be rejected, got %d", instanceType, rec.Code)
		}
		if got := apiErrorCode(t, rec); got != "InvalidPart" {
			t.Fatalf("%s: expected InvalidPart, got %q", instanceType, got)
		}
	})

	t.Run("wrong-algorithm-part-checksum-is-rejected", func(t *testing.T) {
		// A part carrying a checksum under an algorithm other than the upload's
		// is malformed, not "omitted", and must not slip through the relaxation.
		name := "variants/wrong-algo-part-cs"
		uploadID, etags := setup(name)
		var body bytes.Buffer
		body.WriteString("<CompleteMultipartUpload>")
		for i, etag := range etags {
			fmt.Fprintf(&body, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag>"+
				"<ChecksumCRC32C>AAAAAA==</ChecksumCRC32C></Part>", i+1, etag)
		}
		body.WriteString("</CompleteMultipartUpload>")

		req, err := newTestSignedRequestV4(http.MethodPost,
			getCompleteMultipartUploadURL("", bucketName, name, uploadID),
			int64(body.Len()), bytes.NewReader(body.Bytes()), credentials.AccessKey, credentials.SecretKey, objCSHdr)
		if err != nil {
			t.Fatalf("failed to build CompleteMultipartUpload request: %v", err)
		}
		rec := httptest.NewRecorder()
		apiRouter.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: a part checksum under the wrong algorithm must be rejected, got %d",
				instanceType, rec.Code)
		}
		if got := apiErrorCode(t, rec); got != "InvalidPart" {
			t.Fatalf("%s: expected InvalidPart, got %q", instanceType, got)
		}
	})

	t.Run("no-object-checksum-supplied", func(t *testing.T) {
		// AWS treats the object level checksum on completion as optional; the
		// server stores the checksum it computed from the parts.
		name := "variants/no-object-cs"
		uploadID, etags := setup(name)
		rec := completeMultipartUploadHTTP(t, apiRouter, credentials, bucketName, name, uploadID, etags, nil, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: want 200, got %d %s", instanceType, rec.Code, rec.Body.String())
		}
		oi, err := obj.GetObjectInfo(t.Context(), bucketName, name, ObjectOptions{})
		if err != nil {
			t.Fatalf("%s: GetObjectInfo failed: %v", instanceType, err)
		}
		cs, _ := oi.decryptChecksums(0, nil)
		if got, want := cs[typ.String()], mustChecksum(t, typ, full); got != want {
			t.Fatalf("%s: expected server computed checksum %q, got %q", instanceType, want, got)
		}
	})

	t.Run("zero-length-object", func(t *testing.T) {
		// A single empty part is a legal multipart upload: only non-final parts
		// have a minimum size. The merged checksum must be the checksum of no
		// bytes, not the empty string.
		name := "variants/zero-length"
		empty := []byte{}
		uploadID := newMultipartUploadHTTP(t, apiRouter, credentials, bucketName, name,
			typ.String(), xhttp.AmzChecksumTypeFullObject)
		etags := uploadPartsHTTP(t, apiRouter, credentials, bucketName, name, uploadID, typ, [][]byte{empty})

		rec := completeMultipartUploadHTTP(t, apiRouter, credentials, bucketName, name, uploadID, etags, nil,
			map[string]string{
				typ.Key():             mustChecksum(t, typ, empty),
				xhttp.AmzChecksumType: xhttp.AmzChecksumTypeFullObject,
			})
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: want 200, got %d %s", instanceType, rec.Code, rec.Body.String())
		}
		oi, err := obj.GetObjectInfo(t.Context(), bucketName, name, ObjectOptions{})
		if err != nil {
			t.Fatalf("%s: GetObjectInfo failed: %v", instanceType, err)
		}
		if oi.Size != 0 {
			t.Fatalf("%s: expected zero length object, got %d", instanceType, oi.Size)
		}
		cs, _ := oi.decryptChecksums(0, nil)
		if got, want := cs[typ.String()], mustChecksum(t, typ, empty); got != want {
			t.Fatalf("%s: expected stored checksum %q, got %q", instanceType, want, got)
		}
	})
}
