// Copyright (c) 2015-2026 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
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
	"testing"

	"github.com/minio/minio/internal/auth"
	xhttp "github.com/minio/minio/internal/http"
)

func TestAPIZeroByteSSECAuthenticatesKey(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t:          t,
		objAPITest: testAPIZeroByteSSECAuthenticatesKey,
		endpoints:  []string{"CopyObject", "CopyObjectPart", "PutObject", "GetObject", "HeadObject", "NewMultipart"},
	})
}

func testAPIZeroByteSSECAuthenticatesKey(obj ObjectLayer, instanceType, bucketName string,
	apiRouter http.Handler, credentials auth.Credentials, t *testing.T,
) {
	previousTLS := globalIsTLS
	globalIsTLS = true
	defer func() { globalIsTLS = previousTLS }()

	object := "ssec/zero-byte"
	oldKey := bytes.Repeat([]byte{0x11}, 32)
	oldMD5 := md5.Sum(oldKey)
	wrongKey := bytes.Repeat([]byte{0x22}, 32)
	wrongMD5 := md5.Sum(wrongKey)
	putCopyChecksumSource(t, apiRouter, credentials, bucketName, object, nil, map[string]string{
		xhttp.AmzServerSideEncryptionCustomerAlgorithm: xhttp.AmzEncryptionAES,
		xhttp.AmzServerSideEncryptionCustomerKey:       base64.StdEncoding.EncodeToString(oldKey),
		xhttp.AmzServerSideEncryptionCustomerKeyMD5:    base64.StdEncoding.EncodeToString(oldMD5[:]),
	})

	correctHeaders := map[string]string{
		xhttp.AmzServerSideEncryptionCustomerAlgorithm: xhttp.AmzEncryptionAES,
		xhttp.AmzServerSideEncryptionCustomerKey:       base64.StdEncoding.EncodeToString(oldKey),
		xhttp.AmzServerSideEncryptionCustomerKeyMD5:    base64.StdEncoding.EncodeToString(oldMD5[:]),
	}
	wrongHeaders := map[string]string{
		xhttp.AmzServerSideEncryptionCustomerAlgorithm: xhttp.AmzEncryptionAES,
		xhttp.AmzServerSideEncryptionCustomerKey:       base64.StdEncoding.EncodeToString(wrongKey),
		xhttp.AmzServerSideEncryptionCustomerKeyMD5:    base64.StdEncoding.EncodeToString(wrongMD5[:]),
	}

	if rec := ssecZeroByteRequest(t, apiRouter, credentials, http.MethodGet, bucketName, object, correctHeaders); rec.Code != http.StatusOK || rec.Body.Len() != 0 {
		t.Fatalf("%s: correct-key GET returned %d with %d bytes: %s", instanceType, rec.Code, rec.Body.Len(), rec.Body.String())
	}
	headRec := ssecZeroByteRequest(t, apiRouter, credentials, http.MethodHead, bucketName, object, correctHeaders)
	if headRec.Code != http.StatusOK {
		t.Fatalf("%s: correct-key HEAD returned %d", instanceType, headRec.Code)
	}
	if rec := ssecZeroByteRequest(t, apiRouter, credentials, http.MethodGet, bucketName, object, wrongHeaders); rec.Code != http.StatusForbidden {
		t.Fatalf("%s: wrong-key GET returned %d, want %d: %s", instanceType, rec.Code, http.StatusForbidden, rec.Body.String())
	}
	if rec := ssecZeroByteRequest(t, apiRouter, credentials, http.MethodHead, bucketName, object, wrongHeaders); rec.Code != http.StatusForbidden {
		t.Fatalf("%s: wrong-key HEAD returned %d, want %d", instanceType, rec.Code, http.StatusForbidden)
	}
	conditionalHeaders := make(map[string]string, len(wrongHeaders)+1)
	for key, value := range wrongHeaders {
		conditionalHeaders[key] = value
	}
	conditionalInfo, err := obj.GetObjectInfo(t.Context(), bucketName, object, ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	conditionalRequest := httptest.NewRequest(http.MethodGet, getGetObjectURL("", bucketName, object), nil)
	for key, value := range wrongHeaders {
		conditionalRequest.Header.Set(key, value)
	}
	if _, err := DecryptObjectInfo(&conditionalInfo, conditionalRequest); err != nil {
		t.Fatal(err)
	}
	conditionalHeaders[xhttp.IfNoneMatch] = conditionalInfo.ETag
	if rec := ssecZeroByteRequest(t, apiRouter, credentials, http.MethodGet, bucketName, object, conditionalHeaders); rec.Code != http.StatusNotModified {
		t.Fatalf("%s: conditional wrong-key GET returned %d, want %d: %s", instanceType, rec.Code, http.StatusNotModified, rec.Body.String())
	}
	if rec := ssecZeroByteRequest(t, apiRouter, credentials, http.MethodGet, bucketName, object, nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("%s: missing-key GET returned %d, want %d: %s", instanceType, rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	nonEmptyObject := "ssec/one-byte"
	putCopyChecksumSource(t, apiRouter, credentials, bucketName, nonEmptyObject, []byte{1}, correctHeaders)
	if rec := ssecZeroByteRequest(t, apiRouter, credentials, http.MethodGet, bucketName, nonEmptyObject, wrongHeaders); rec.Code != http.StatusForbidden {
		t.Fatalf("%s: one-byte wrong-key GET returned %d, want %d: %s", instanceType, rec.Code, http.StatusForbidden, rec.Body.String())
	}

	plainObject := "ssec/plain-zero-byte"
	putCopyChecksumSource(t, apiRouter, credentials, bucketName, plainObject, nil, nil)
	if rec := ssecZeroByteRequest(t, apiRouter, credentials, http.MethodGet, bucketName, plainObject, wrongHeaders); rec.Code != http.StatusBadRequest {
		t.Fatalf("%s: unencrypted wrong-key GET returned %d, want %d: %s", instanceType, rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	destination := "ssec/zero-byte-copy"
	rec := copyChecksumRequest(t, apiRouter, credentials, bucketName, object, destination, map[string]string{
		xhttp.AmzServerSideEncryptionCopyCustomerAlgorithm: xhttp.AmzEncryptionAES,
		xhttp.AmzServerSideEncryptionCopyCustomerKey:       base64.StdEncoding.EncodeToString(wrongKey),
		xhttp.AmzServerSideEncryptionCopyCustomerKeyMD5:    base64.StdEncoding.EncodeToString(wrongMD5[:]),
	})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("%s: wrong-key CopyObject returned %d, want %d: %s", instanceType, rec.Code, http.StatusForbidden, rec.Body.String())
	}
	if _, err := obj.GetObjectInfo(t.Context(), bucketName, destination, ObjectOptions{}); !isErrObjectNotFound(err) {
		t.Fatalf("%s: rejected CopyObject created the destination: %v", instanceType, err)
	}

	rec = copyChecksumRequest(t, apiRouter, credentials, bucketName, object, object, map[string]string{
		xhttp.AmzStorageClass:                              "REDUCED_REDUNDANCY",
		xhttp.AmzServerSideEncryptionCopyCustomerAlgorithm: xhttp.AmzEncryptionAES,
		xhttp.AmzServerSideEncryptionCopyCustomerKey:       base64.StdEncoding.EncodeToString(wrongKey),
		xhttp.AmzServerSideEncryptionCopyCustomerKeyMD5:    base64.StdEncoding.EncodeToString(wrongMD5[:]),
	})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("%s: wrong-key storage-class CopyObject returned %d, want %d: %s", instanceType, rec.Code, http.StatusForbidden, rec.Body.String())
	}

	multipartObject := "ssec/zero-byte-multipart-copy"
	req, err := newTestSignedRequestV4(http.MethodPost, getNewMultipartURL("", bucketName, multipartObject),
		0, nil, credentials.AccessKey, credentials.SecretKey, nil)
	if err != nil {
		t.Fatal(err)
	}
	rec = httptest.NewRecorder()
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: NewMultipartUpload returned %d: %s", instanceType, rec.Code, rec.Body.String())
	}
	var initiated InitiateMultipartUploadResponse
	if err := xml.Unmarshal(rec.Body.Bytes(), &initiated); err != nil {
		t.Fatal(err)
	}
	req, err = newTestSignedRequestV4(http.MethodPut,
		getCopyObjectPartURL("", bucketName, multipartObject, initiated.UploadID, "1"),
		0, nil, credentials.AccessKey, credentials.SecretKey, map[string]string{
			xhttp.AmzServerSideEncryptionCopyCustomerAlgorithm: xhttp.AmzEncryptionAES,
			xhttp.AmzServerSideEncryptionCopyCustomerKey:       base64.StdEncoding.EncodeToString(wrongKey),
			xhttp.AmzServerSideEncryptionCopyCustomerKeyMD5:    base64.StdEncoding.EncodeToString(wrongMD5[:]),
		})
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(xhttp.AmzCopySource, SlashSeparator+pathJoin(bucketName, object))
	// Re-sign so x-amz-copy-source is covered by the signature, as real S3
	// clients send it; the verifier rejects unsigned x-amz-* headers.
	if err = signRequestV4(req, credentials.AccessKey, credentials.SecretKey); err != nil {
		t.Fatalf("%s: failed to re-sign UploadPartCopy request: %v", instanceType, err)
	}
	rec = httptest.NewRecorder()
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("%s: wrong-key UploadPartCopy returned %d, want %d: %s", instanceType, rec.Code, http.StatusForbidden, rec.Body.String())
	}
	parts, err := obj.ListObjectParts(t.Context(), bucketName, multipartObject, initiated.UploadID, 0, 1000, ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(parts.Parts) != 0 {
		t.Fatalf("%s: rejected UploadPartCopy stored %d parts", instanceType, len(parts.Parts))
	}
	if err := obj.AbortMultipartUpload(t.Context(), bucketName, multipartObject, initiated.UploadID, ObjectOptions{}); err != nil {
		t.Fatal(err)
	}

	wrongHeader := http.Header{}
	for key, value := range wrongHeaders {
		wrongHeader.Set(key, value)
	}
	for _, test := range []struct {
		header http.Header
		opts   ObjectOptions
	}{
		{header: nil, opts: ObjectOptions{}},
		{header: wrongHeader, opts: ObjectOptions{NoDecryption: true}},
		{header: wrongHeader, opts: ObjectOptions{ReplicationRequest: true}},
		{header: wrongHeader, opts: ObjectOptions{Transition: TransitionOptions{RestoreRequest: &RestoreObjectRequest{}}}},
	} {
		gr, err := obj.GetObjectNInfo(t.Context(), bucketName, object, nil, test.header, test.opts)
		if err != nil {
			t.Fatalf("%s: internal zero-byte read with opts %+v failed: %v", instanceType, test.opts, err)
		}
		gr.Close()
	}

	rangeHeaders := make(map[string]string, len(wrongHeaders)+1)
	for key, value := range wrongHeaders {
		rangeHeaders[key] = value
	}
	rangeHeaders[xhttp.Range] = "bytes=0-0"
	if rec := ssecZeroByteRequest(t, apiRouter, credentials, http.MethodGet, bucketName, object, rangeHeaders); rec.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("%s: ranged wrong-key GET returned %d, want %d: %s", instanceType, rec.Code, http.StatusRequestedRangeNotSatisfiable, rec.Body.String())
	}
}

func ssecZeroByteRequest(t *testing.T, apiRouter http.Handler, credentials auth.Credentials,
	method, bucket, object string, headers map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()
	req, err := newTestSignedRequestV4(method, getGetObjectURL("", bucket, object),
		0, nil, credentials.AccessKey, credentials.SecretKey, headers)
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	apiRouter.ServeHTTP(rec, req)
	return rec
}
