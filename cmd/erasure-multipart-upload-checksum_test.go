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
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/hash"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/kms"
)

func uploadPartHTTP(t *testing.T, apiRouter http.Handler, creds auth.Credentials,
	bucket, object, uploadID string, partNumber int, data []byte, headers map[string]string,
) (string, *httptest.ResponseRecorder) {
	t.Helper()
	req, err := newTestSignedRequestV4(http.MethodPut,
		getPutObjectPartURL("", bucket, object, uploadID, strconv.Itoa(partNumber)),
		int64(len(data)), bytes.NewReader(data), creds.AccessKey, creds.SecretKey, headers)
	if err != nil {
		t.Fatalf("failed to build UploadPart request: %v", err)
	}
	rec := httptest.NewRecorder()
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("UploadPart failed: %d %s", rec.Code, rec.Body.String())
	}
	return canonicalizeETag(rec.Header()[xhttp.ETag][0]), rec
}

func listPartsHTTP(t *testing.T, apiRouter http.Handler, creds auth.Credentials,
	bucket, object, uploadID string, headers map[string]string,
) ListPartsResponse {
	t.Helper()
	req, err := newTestSignedRequestV4(http.MethodGet,
		getListMultipartURLWithParams("", bucket, object, uploadID, "1000", "", ""),
		0, nil, creds.AccessKey, creds.SecretKey, headers)
	if err != nil {
		t.Fatalf("failed to build ListParts request: %v", err)
	}
	rec := httptest.NewRecorder()
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("ListParts failed: %d %s", rec.Code, rec.Body.String())
	}
	var response ListPartsResponse
	if err := xml.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to decode ListParts response: %v", err)
	}
	return response
}

func partChecksum(typ hash.ChecksumType, part Part) string {
	switch typ.Base() {
	case hash.ChecksumCRC32:
		return part.ChecksumCRC32
	case hash.ChecksumCRC32C:
		return part.ChecksumCRC32C
	case hash.ChecksumSHA1:
		return part.ChecksumSHA1
	case hash.ChecksumSHA256:
		return part.ChecksumSHA256
	case hash.ChecksumCRC64NVME:
		return part.ChecksumCRC64NVME
	default:
		return ""
	}
}

func copyPartChecksum(typ hash.ChecksumType, response CopyObjectPartResponse) string {
	switch typ.Base() {
	case hash.ChecksumCRC32:
		return response.ChecksumCRC32
	case hash.ChecksumCRC32C:
		return response.ChecksumCRC32C
	case hash.ChecksumSHA1:
		return response.ChecksumSHA1
	case hash.ChecksumSHA256:
		return response.ChecksumSHA256
	case hash.ChecksumCRC64NVME:
		return response.ChecksumCRC64NVME
	default:
		return ""
	}
}

func completePartWithChecksum(typ hash.ChecksumType, partNumber int, etag, checksum string) CompletePart {
	part := CompletePart{PartNumber: partNumber, ETag: etag}
	switch typ.Base() {
	case hash.ChecksumCRC32:
		part.ChecksumCRC32 = checksum
	case hash.ChecksumCRC32C:
		part.ChecksumCRC32C = checksum
	case hash.ChecksumSHA1:
		part.ChecksumSHA1 = checksum
	case hash.ChecksumSHA256:
		part.ChecksumSHA256 = checksum
	case hash.ChecksumCRC64NVME:
		part.ChecksumCRC64NVME = checksum
	}
	return part
}

func completePartsHTTP(t *testing.T, apiRouter http.Handler, creds auth.Credentials,
	bucket, object, uploadID string, parts []CompletePart, headers map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()
	body, err := xml.Marshal(CompleteMultipartUpload{Parts: parts})
	if err != nil {
		t.Fatalf("failed to encode CompleteMultipartUpload request: %v", err)
	}
	req, err := newTestSignedRequestV4(http.MethodPost,
		getCompleteMultipartUploadURL("", bucket, object, uploadID),
		int64(len(body)), bytes.NewReader(body), creds.AccessKey, creds.SecretKey, headers)
	if err != nil {
		t.Fatalf("failed to build CompleteMultipartUpload request: %v", err)
	}
	rec := httptest.NewRecorder()
	apiRouter.ServeHTTP(rec, req)
	return rec
}

func copyPartWithoutChecksumHTTP(t *testing.T, apiRouter http.Handler, creds auth.Credentials,
	bucket, source, object, uploadID, sourceRange string, headers map[string]string,
) CopyObjectPartResponse {
	t.Helper()
	req, err := newTestSignedRequestV4(http.MethodPut,
		getCopyObjectPartURL("", bucket, object, uploadID, "1"),
		0, nil, creds.AccessKey, creds.SecretKey, headers)
	if err != nil {
		t.Fatalf("failed to build UploadPartCopy request: %v", err)
	}
	req.Header.Set(xhttp.AmzCopySource, SlashSeparator+pathJoin(bucket, source))
	if sourceRange != "" {
		req.Header.Set(xhttp.AmzCopySourceRange, sourceRange)
	}
	// Re-sign so the copy-source x-amz-* headers are covered by the signature,
	// as real S3 clients send them; the verifier rejects unsigned x-amz-*.
	if creds.AccessKey != "" && creds.SecretKey != "" {
		if err := signRequestV4(req, creds.AccessKey, creds.SecretKey); err != nil {
			t.Fatalf("failed to re-sign UploadPartCopy request: %v", err)
		}
	}
	rec := httptest.NewRecorder()
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("UploadPartCopy failed: %d %s", rec.Code, rec.Body.String())
	}
	var response CopyObjectPartResponse
	if err := xml.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to decode UploadPartCopy response: %v", err)
	}
	return response
}

// TestAPIUploadPartServerSideChecksum exercises the data transformations that
// made installing a checksum hasher in the object layer unsafe. The checksum
// must always cover logical plaintext, regardless of compression or encryption.
func TestAPIUploadPartServerSideChecksum(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecExtendedObjectLayerAPITest(t, testAPIUploadPartServerSideChecksum,
		[]string{"CopyObjectPart", "PutObjectPart", "NewMultipart", "ListObjectParts", "CompleteMultipart"})
}

func testAPIUploadPartServerSideChecksum(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	typ := hash.ChecksumCRC32
	data := bytes.Repeat([]byte("multipart-checksum-plaintext-"), 48*1024)
	want := mustChecksum(t, typ, data)

	t.Run("upload", func(t *testing.T) {
		object := "checksums/upload"
		uploadID := newMultipartUploadHTTP(t, apiRouter, credentials, bucketName, object,
			typ.String(), xhttp.AmzChecksumTypeFullObject)
		etag, rec := uploadPartHTTP(t, apiRouter, credentials,
			bucketName, object, uploadID, 1, data, nil)
		if got := rec.Header().Get(typ.Key()); got != "" {
			t.Fatalf("%s: UploadPart returned server-computed checksum %q", instanceType, got)
		}

		listed := listPartsHTTP(t, apiRouter, credentials, bucketName, object, uploadID, nil)
		if len(listed.Parts) != 1 || partChecksum(typ, listed.Parts[0]) != want {
			t.Fatalf("%s: ListParts checksum mismatch: %+v, want %q", instanceType, listed.Parts, want)
		}

		rec = completePartsHTTP(t, apiRouter, credentials, bucketName, object, uploadID,
			[]CompletePart{{PartNumber: 1, ETag: etag}}, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: CompleteMultipartUpload failed: %d %s", instanceType, rec.Code, rec.Body.String())
		}

		oi, err := obj.GetObjectInfo(t.Context(), bucketName, object, ObjectOptions{})
		if err != nil {
			t.Fatalf("%s: GetObjectInfo failed: %v", instanceType, err)
		}
		checksums, _ := oi.decryptChecksums(0, nil)
		if got := checksums[typ.String()]; got != want {
			t.Fatalf("%s: stored checksum %q, want plaintext checksum %q", instanceType, got, want)
		}
	})

	t.Run("copy", func(t *testing.T) {
		source := "checksums/source"
		if _, err := obj.PutObject(t.Context(), bucketName, source,
			mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{}); err != nil {
			t.Fatalf("%s: source PutObject failed: %v", instanceType, err)
		}
		object := "checksums/copy"
		uploadID := newMultipartUploadHTTP(t, apiRouter, credentials, bucketName, object,
			typ.String(), xhttp.AmzChecksumTypeFullObject)
		response := copyPartWithoutChecksumHTTP(t, apiRouter, credentials,
			bucketName, source, object, uploadID, "", nil)
		if got := copyPartChecksum(typ, response); got != want {
			t.Fatalf("%s: UploadPartCopy checksum %q, want %q", instanceType, got, want)
		}

		listed := listPartsHTTP(t, apiRouter, credentials, bucketName, object, uploadID, nil)
		if len(listed.Parts) != 1 || partChecksum(typ, listed.Parts[0]) != want {
			t.Fatalf("%s: copied ListParts checksum mismatch: %+v, want %q", instanceType, listed.Parts, want)
		}

		rec := completePartsHTTP(t, apiRouter, credentials, bucketName, object, uploadID,
			[]CompletePart{{PartNumber: 1, ETag: canonicalizeETag(response.ETag)}}, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: copied CompleteMultipartUpload failed: %d %s", instanceType, rec.Code, rec.Body.String())
		}
	})
}

func TestAPIUploadPartServerSideChecksumAlgorithms(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t:          t,
		objAPITest: testAPIUploadPartServerSideChecksumAlgorithms,
		endpoints:  []string{"CopyObjectPart", "PutObjectPart", "NewMultipart", "ListObjectParts", "CompleteMultipart"},
	})
}

func testAPIUploadPartServerSideChecksumAlgorithms(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	tests := []struct {
		typ       hash.ChecksumType
		objType   string
		composite bool
	}{
		{hash.ChecksumCRC32, xhttp.AmzChecksumTypeFullObject, false},
		{hash.ChecksumCRC32C, xhttp.AmzChecksumTypeFullObject, false},
		{hash.ChecksumCRC64NVME, xhttp.AmzChecksumTypeFullObject, false},
		{hash.ChecksumCRC32, xhttp.AmzChecksumTypeComposite, true},
		{hash.ChecksumSHA1, xhttp.AmzChecksumTypeComposite, true},
		{hash.ChecksumSHA256, xhttp.AmzChecksumTypeComposite, true},
	}
	data := bytes.Repeat([]byte("server-side-part-checksum"), 1024)

	for _, test := range tests {
		t.Run(test.typ.String()+"/"+test.objType, func(t *testing.T) {
			object := "algorithms/" + test.typ.String() + "/" + test.objType
			uploadID := newMultipartUploadHTTP(t, apiRouter, credentials, bucketName, object,
				test.typ.String(), test.objType)
			etag, rec := uploadPartHTTP(t, apiRouter, credentials,
				bucketName, object, uploadID, 1, data, nil)
			if got := rec.Header().Get(test.typ.Key()); got != "" {
				t.Fatalf("%s: UploadPart returned server-computed checksum %q", instanceType, got)
			}

			want := mustChecksum(t, test.typ, data)
			listed := listPartsHTTP(t, apiRouter, credentials, bucketName, object, uploadID, nil)
			if len(listed.Parts) != 1 || partChecksum(test.typ, listed.Parts[0]) != want {
				t.Fatalf("%s: ListParts checksum mismatch: %+v, want %q", instanceType, listed.Parts, want)
			}

			part := CompletePart{PartNumber: 1, ETag: etag}
			if test.composite {
				part = completePartWithChecksum(test.typ, 1, etag, want)
			}
			rec = completePartsHTTP(t, apiRouter, credentials, bucketName, object, uploadID,
				[]CompletePart{part}, nil)
			if rec.Code != http.StatusOK {
				t.Fatalf("%s: CompleteMultipartUpload failed: %d %s", instanceType, rec.Code, rec.Body.String())
			}
		})
	}

	t.Run("multi-part/FULL_OBJECT", func(t *testing.T) {
		typ := hash.ChecksumCRC32
		parts, full := multipartChecksumTestData()
		object := "algorithms/multi-part-full-object"
		uploadID := newMultipartUploadHTTP(t, apiRouter, credentials, bucketName, object,
			typ.String(), xhttp.AmzChecksumTypeFullObject)
		etags := make([]string, len(parts))
		for i, data := range parts {
			etag, rec := uploadPartHTTP(t, apiRouter, credentials,
				bucketName, object, uploadID, i+1, data, nil)
			if got := rec.Header().Get(typ.Key()); got != "" {
				t.Fatalf("%s: UploadPart returned server-computed checksum %q", instanceType, got)
			}
			etags[i] = etag
		}

		listed := listPartsHTTP(t, apiRouter, credentials, bucketName, object, uploadID, nil)
		if len(listed.Parts) != len(parts) {
			t.Fatalf("%s: ListParts returned %d parts, want %d", instanceType, len(listed.Parts), len(parts))
		}
		for i, part := range listed.Parts {
			if got, want := partChecksum(typ, part), mustChecksum(t, typ, parts[i]); got != want {
				t.Fatalf("%s: part %d checksum %q, want %q", instanceType, i+1, got, want)
			}
		}

		complete := make([]CompletePart, len(etags))
		for i, etag := range etags {
			complete[i] = CompletePart{PartNumber: i + 1, ETag: etag}
		}
		rec := completePartsHTTP(t, apiRouter, credentials, bucketName, object, uploadID, complete,
			map[string]string{
				typ.Key():             mustChecksum(t, typ, full),
				xhttp.AmzChecksumType: xhttp.AmzChecksumTypeFullObject,
			})
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: multi-part CompleteMultipartUpload failed: %d %s", instanceType, rec.Code, rec.Body.String())
		}
	})

	t.Run("zero-length-part", func(t *testing.T) {
		typ := hash.ChecksumCRC32
		object := "algorithms/zero-length"
		uploadID := newMultipartUploadHTTP(t, apiRouter, credentials, bucketName, object,
			typ.String(), xhttp.AmzChecksumTypeFullObject)
		etag, _ := uploadPartHTTP(t, apiRouter, credentials,
			bucketName, object, uploadID, 1, nil, nil)
		listed := listPartsHTTP(t, apiRouter, credentials, bucketName, object, uploadID, nil)
		if len(listed.Parts) != 1 || partChecksum(typ, listed.Parts[0]) != mustChecksum(t, typ, nil) {
			t.Fatalf("%s: zero-length ListParts checksum mismatch: %+v", instanceType, listed.Parts)
		}
		rec := completePartsHTTP(t, apiRouter, credentials, bucketName, object, uploadID,
			[]CompletePart{{PartNumber: 1, ETag: etag}}, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: zero-length CompleteMultipartUpload failed: %d %s", instanceType, rec.Code, rec.Body.String())
		}
	})

	t.Run("overwrite-part-checksum", func(t *testing.T) {
		typ := hash.ChecksumCRC32
		object := "algorithms/overwrite"
		uploadID := newMultipartUploadHTTP(t, apiRouter, credentials, bucketName, object,
			typ.String(), xhttp.AmzChecksumTypeFullObject)
		first := []byte("first part contents")
		second := []byte("replacement part contents")
		uploadPartHTTP(t, apiRouter, credentials, bucketName, object, uploadID, 1, first, nil)
		etag, _ := uploadPartHTTP(t, apiRouter, credentials, bucketName, object, uploadID, 1, second, nil)
		listed := listPartsHTTP(t, apiRouter, credentials, bucketName, object, uploadID, nil)
		if len(listed.Parts) != 1 || partChecksum(typ, listed.Parts[0]) != mustChecksum(t, typ, second) {
			t.Fatalf("%s: overwritten ListParts checksum mismatch: %+v", instanceType, listed.Parts)
		}
		rec := completePartsHTTP(t, apiRouter, credentials, bucketName, object, uploadID,
			[]CompletePart{{PartNumber: 1, ETag: etag}}, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: overwritten CompleteMultipartUpload failed: %d %s", instanceType, rec.Code, rec.Body.String())
		}
	})

	t.Run("copy/SHA256/COMPOSITE", func(t *testing.T) {
		typ := hash.ChecksumSHA256
		source := "algorithms/copy-source"
		if _, err := obj.PutObject(t.Context(), bucketName, source,
			mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{}); err != nil {
			t.Fatalf("%s: source PutObject failed: %v", instanceType, err)
		}
		object := "algorithms/copy-SHA256"
		uploadID := newMultipartUploadHTTP(t, apiRouter, credentials, bucketName, object,
			typ.String(), xhttp.AmzChecksumTypeComposite)
		start, end := 7, len(data)-9
		response := copyPartWithoutChecksumHTTP(t, apiRouter, credentials,
			bucketName, source, object, uploadID, "bytes="+strconv.Itoa(start)+"-"+strconv.Itoa(end-1), nil)
		want := mustChecksum(t, typ, data[start:end])
		if got := copyPartChecksum(typ, response); got != want {
			t.Fatalf("%s: UploadPartCopy checksum %q, want %q", instanceType, got, want)
		}

		part := completePartWithChecksum(typ, 1, canonicalizeETag(response.ETag), want)
		rec := completePartsHTTP(t, apiRouter, credentials, bucketName, object, uploadID,
			[]CompletePart{part}, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: copied CompleteMultipartUpload failed: %d %s", instanceType, rec.Code, rec.Body.String())
		}
	})
}

func TestAPIUploadPartServerSideChecksumDoesNotMaskClientErrors(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t:          t,
		objAPITest: testAPIUploadPartServerSideChecksumDoesNotMaskClientErrors,
		endpoints:  []string{"PutObjectPart", "NewMultipart", "ListObjectParts"},
	})
}

func testAPIUploadPartServerSideChecksumDoesNotMaskClientErrors(_ ObjectLayer, instanceType, bucketName string,
	apiRouter http.Handler, credentials auth.Credentials, t *testing.T,
) {
	data := []byte("client checksum must remain authoritative")

	t.Run("correct-value", func(t *testing.T) {
		typ := hash.ChecksumCRC32
		object := "errors/correct-value"
		uploadID := newMultipartUploadHTTP(t, apiRouter, credentials, bucketName, object,
			typ.String(), xhttp.AmzChecksumTypeFullObject)
		want := mustChecksum(t, typ, data)
		_, rec := uploadPartHTTP(t, apiRouter, credentials,
			bucketName, object, uploadID, 1, data, map[string]string{typ.Key(): want})
		if got := rec.Header().Get(typ.Key()); got != want {
			t.Fatalf("%s: client checksum response %q, want %q", instanceType, got, want)
		}
		listed := listPartsHTTP(t, apiRouter, credentials, bucketName, object, uploadID, nil)
		if len(listed.Parts) != 1 || partChecksum(typ, listed.Parts[0]) != want {
			t.Fatalf("%s: client checksum ListParts mismatch: %+v", instanceType, listed.Parts)
		}
	})

	t.Run("wrong-algorithm", func(t *testing.T) {
		object := "errors/wrong-algorithm"
		uploadID := newMultipartUploadHTTP(t, apiRouter, credentials, bucketName, object,
			hash.ChecksumCRC32.String(), xhttp.AmzChecksumTypeFullObject)
		req, err := newTestSignedRequestV4(http.MethodPut,
			getPutObjectPartURL("", bucketName, object, uploadID, "1"),
			int64(len(data)), bytes.NewReader(data), credentials.AccessKey, credentials.SecretKey,
			map[string]string{hash.ChecksumSHA256.Key(): mustChecksum(t, hash.ChecksumSHA256, data)})
		if err != nil {
			t.Fatalf("failed to build UploadPart request: %v", err)
		}
		rec := httptest.NewRecorder()
		apiRouter.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest || apiErrorCode(t, rec) != "InvalidArgument" {
			t.Fatalf("%s: wrong algorithm returned %d %s", instanceType, rec.Code, rec.Body.String())
		}
	})

	t.Run("wrong-value", func(t *testing.T) {
		object := "errors/wrong-value"
		uploadID := newMultipartUploadHTTP(t, apiRouter, credentials, bucketName, object,
			hash.ChecksumCRC32.String(), xhttp.AmzChecksumTypeFullObject)
		req, err := newTestSignedRequestV4(http.MethodPut,
			getPutObjectPartURL("", bucketName, object, uploadID, "1"),
			int64(len(data)), bytes.NewReader(data), credentials.AccessKey, credentials.SecretKey,
			map[string]string{hash.ChecksumCRC32.Key(): mustChecksum(t, hash.ChecksumCRC32, []byte("wrong"))})
		if err != nil {
			t.Fatalf("failed to build UploadPart request: %v", err)
		}
		rec := httptest.NewRecorder()
		apiRouter.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest || apiErrorCode(t, rec) != "XAmzContentChecksumMismatch" {
			t.Fatalf("%s: wrong value returned %d %s", instanceType, rec.Code, rec.Body.String())
		}
	})
}

func TestAPIUploadPartServerSideChecksumSSEC(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t:          t,
		objAPITest: testAPIUploadPartServerSideChecksumSSEC,
		endpoints:  []string{"PutObjectPart", "NewMultipart", "CompleteMultipart"},
	})
}

func testAPIUploadPartServerSideChecksumSSEC(_ ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	globalIsTLS = true
	defer func() { globalIsTLS = false }()

	key := bytes.Repeat([]byte{0x2a}, 32)
	keyMD5 := md5.Sum(key)
	ssecHeaders := map[string]string{
		xhttp.AmzServerSideEncryptionCustomerAlgorithm: xhttp.AmzEncryptionAES,
		xhttp.AmzServerSideEncryptionCustomerKey:       base64.StdEncoding.EncodeToString(key),
		xhttp.AmzServerSideEncryptionCustomerKeyMD5:    base64.StdEncoding.EncodeToString(keyMD5[:]),
	}
	initHeaders := map[string]string{
		xhttp.AmzChecksumAlgo:                          hash.ChecksumCRC32.String(),
		xhttp.AmzChecksumType:                          xhttp.AmzChecksumTypeFullObject,
		xhttp.AmzServerSideEncryptionCustomerAlgorithm: ssecHeaders[xhttp.AmzServerSideEncryptionCustomerAlgorithm],
		xhttp.AmzServerSideEncryptionCustomerKey:       ssecHeaders[xhttp.AmzServerSideEncryptionCustomerKey],
		xhttp.AmzServerSideEncryptionCustomerKeyMD5:    ssecHeaders[xhttp.AmzServerSideEncryptionCustomerKeyMD5],
	}
	object := "checksums/ssec"
	req, err := newTestSignedRequestV4(http.MethodPost, getNewMultipartURL("", bucketName, object),
		0, nil, credentials.AccessKey, credentials.SecretKey, initHeaders)
	if err != nil {
		t.Fatalf("failed to build NewMultipartUpload request: %v", err)
	}
	rec := httptest.NewRecorder()
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: NewMultipartUpload failed: %d %s", instanceType, rec.Code, rec.Body.String())
	}
	var initiated InitiateMultipartUploadResponse
	if err := xml.Unmarshal(rec.Body.Bytes(), &initiated); err != nil {
		t.Fatalf("failed to decode NewMultipartUpload response: %v", err)
	}

	data := bytes.Repeat([]byte("ssec-checksum-plaintext"), 4096)
	etag, uploadRec := uploadPartHTTP(t, apiRouter, credentials,
		bucketName, object, initiated.UploadID, 1, data, ssecHeaders)
	if got := uploadRec.Header().Get(hash.ChecksumCRC32.Key()); got != "" {
		t.Fatalf("%s: UploadPart returned server-computed checksum %q", instanceType, got)
	}

	completeHeaders := map[string]string{
		xhttp.AmzChecksumCRC32:                         mustChecksum(t, hash.ChecksumCRC32, data),
		xhttp.AmzChecksumType:                          xhttp.AmzChecksumTypeFullObject,
		xhttp.AmzServerSideEncryptionCustomerAlgorithm: ssecHeaders[xhttp.AmzServerSideEncryptionCustomerAlgorithm],
		xhttp.AmzServerSideEncryptionCustomerKey:       ssecHeaders[xhttp.AmzServerSideEncryptionCustomerKey],
		xhttp.AmzServerSideEncryptionCustomerKeyMD5:    ssecHeaders[xhttp.AmzServerSideEncryptionCustomerKeyMD5],
	}
	rec = completePartsHTTP(t, apiRouter, credentials, bucketName, object, initiated.UploadID,
		[]CompletePart{{PartNumber: 1, ETag: etag}}, completeHeaders)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: CompleteMultipartUpload failed: %d %s", instanceType, rec.Code, rec.Body.String())
	}
}

func TestAPIUploadPartServerSideChecksumSSES3(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t:          t,
		objAPITest: testAPIUploadPartServerSideChecksumSSES3,
		endpoints:  []string{"PutObjectPart", "NewMultipart", "CompleteMultipart"},
	})
}

func testAPIUploadPartServerSideChecksumSSES3(_ ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	KMS, err := kms.ParseSecretKey("my-minio-key:5lF+0pJM0OWwlQrvK2S/I7W9mO4a6rJJI7wzj7v09cw=")
	if err != nil {
		t.Fatal(err)
	}
	GlobalKMS = KMS
	defer func() { GlobalKMS = nil }()

	object := "checksums/sse-s3"
	initHeaders := map[string]string{
		xhttp.AmzChecksumAlgo:         hash.ChecksumCRC32.String(),
		xhttp.AmzChecksumType:         xhttp.AmzChecksumTypeFullObject,
		xhttp.AmzServerSideEncryption: xhttp.AmzEncryptionAES,
	}
	req, err := newTestSignedRequestV4(http.MethodPost, getNewMultipartURL("", bucketName, object),
		0, nil, credentials.AccessKey, credentials.SecretKey, initHeaders)
	if err != nil {
		t.Fatalf("failed to build NewMultipartUpload request: %v", err)
	}
	rec := httptest.NewRecorder()
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: NewMultipartUpload failed: %d %s", instanceType, rec.Code, rec.Body.String())
	}
	var initiated InitiateMultipartUploadResponse
	if err := xml.Unmarshal(rec.Body.Bytes(), &initiated); err != nil {
		t.Fatalf("failed to decode NewMultipartUpload response: %v", err)
	}

	data := bytes.Repeat([]byte("sse-s3-checksum-plaintext"), 4096)
	etag, uploadRec := uploadPartHTTP(t, apiRouter, credentials,
		bucketName, object, initiated.UploadID, 1, data, nil)
	if got := uploadRec.Header().Get(hash.ChecksumCRC32.Key()); got != "" {
		t.Fatalf("%s: UploadPart returned server-computed checksum %q", instanceType, got)
	}

	rec = completePartsHTTP(t, apiRouter, credentials, bucketName, object, initiated.UploadID,
		[]CompletePart{{PartNumber: 1, ETag: etag}}, map[string]string{
			xhttp.AmzChecksumCRC32: mustChecksum(t, hash.ChecksumCRC32, data),
			xhttp.AmzChecksumType:  xhttp.AmzChecksumTypeFullObject,
		})
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: CompleteMultipartUpload failed: %d %s", instanceType, rec.Code, rec.Body.String())
	}
}
