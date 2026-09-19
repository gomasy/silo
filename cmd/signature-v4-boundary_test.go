// Copyright (c) 2026 PGSTY
// SPDX-License-Identifier: AGPL-3.0-only

package cmd

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/auth"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/pgsty/silo-pkg/v3/policy"
)

// forgedSignatureAgeHeader is the name of the scratch header the presigned
// verifier once wrote s3:signatureAge through. Production code no longer knows
// it; a client that sends it is sending an ordinary unsigned x-amz-* header.
const forgedSignatureAgeHeader = "X-Amz-Signature-Age"

// Presign at a chosen time, including exactly the supplied operation headers.
func presignBoundaryRequest(t *testing.T, r *http.Request, date time.Time, signedHeaders []string, cred auth.Credentials) {
	t.Helper()
	query := r.URL.Query()
	query.Del(xhttp.AmzSignature)
	query.Set(xhttp.AmzAlgorithm, signV4Algorithm)
	query.Set(xhttp.AmzDate, date.Format(iso8601Format))
	query.Set(xhttp.AmzExpires, "3600")
	query.Set(xhttp.AmzSignedHeaders, strings.Join(signedHeaders, ";"))
	query.Set(xhttp.AmzCredential, cred.AccessKey+"/"+getScope(date, globalSite.Region()))
	r.Form = query
	headers, code := extractSignedHeaders(signedHeaders, r)
	if code != ErrNone {
		t.Fatal(niceError(code))
	}
	canonical := getCanonicalRequest(headers, getContentSha256Cksum(r, serviceS3), query.Encode(), r.URL.Path, r.Method)
	key := getSigningKey(cred.SecretKey, date, globalSite.Region(), serviceS3)
	query.Set(xhttp.AmzSignature, getSignature(key, getStringToSign(canonical, date, getScope(date, globalSite.Region()))))
	r.URL.RawQuery = query.Encode()
	r.Form = query
}

func setupSignatureBoundaryTest(t *testing.T) {
	t.Helper()
	obj, fsDir, err := prepareFS(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(fsDir) })
	if err = newTestConfig(globalMinioDefaultRegion, obj); err != nil {
		t.Fatal(err)
	}
}

// Canonicalization comma-joins repeated header fields, so a signature over one
// x-amz-copy-source value containing a comma also covers the same text split
// into two fields, while the copy handlers act on Header.Get alone. A single
// value may contain a literal or percent-encoded comma; a repeated header is
// rejected at the shared SigV4 boundary for both signed and presigned requests.
func TestV4CopySourceMultiplicity(t *testing.T) {
	setupSignatureBoundaryTest(t)
	for _, presigned := range []bool{false, true} {
		for _, source := range []string{"/source/allowed,tail", "/source/allowed%2Ctail"} {
			t.Run(fmt.Sprintf("presigned=%v/%s", presigned, source), func(t *testing.T) {
				r := httptest.NewRequest(http.MethodPut, "http://minio.local/destination/object", nil)
				r.Header.Set(xhttp.AmzContentSha256, emptySHA256)
				r.Header.Set(xhttp.AmzCopySource, source)
				if presigned {
					presignBoundaryRequest(t, r, UTCNow(), []string{"host", "x-amz-copy-source"}, globalActiveCred)
				} else {
					if err := signRequestV4(r, globalActiveCred.AccessKey, globalActiveCred.SecretKey); err != nil {
						t.Fatal(err)
					}
					r.Form = r.URL.Query()
				}
				if code := reqSignatureV4Verify(r, globalSite.Region(), serviceS3); code != ErrNone {
					t.Fatalf("a single source key containing a comma must remain valid: %s", niceError(code))
				}
				if source != "/source/allowed,tail" {
					return
				}
				r.Header[xhttp.AmzCopySource] = []string{"/source/allowed", "tail"}
				if code := reqSignatureV4Verify(r, globalSite.Region(), serviceS3); code != ErrInvalidCopySource {
					t.Fatalf("split copy source: got %s, want %s; handler would copy %q",
						niceError(code), niceError(ErrInvalidCopySource), r.Header.Get(xhttp.AmzCopySource))
				}
			})
		}
	}
}

// PutObject and UploadPart authorize before they verify the signature, so
// s3:signatureAge must come from the signed X-Amz-Date on the first policy
// evaluation. A client header under the former scratch name must neither
// supply the value nor survive verification, and verifying the same request
// twice must give the same answer.
func TestGetConditionValuesPresignedAgeFromDate(t *testing.T) {
	setupSignatureBoundaryTest(t)
	for _, tc := range []struct {
		name, header string
		age          time.Duration
		wantVerify   APIErrorCode
	}{
		{name: "old", age: 10 * time.Minute},
		{name: "old forged 0", age: 10 * time.Minute, header: "0", wantVerify: ErrUnsignedHeaders},
		{name: "old forged 1", age: 10 * time.Minute, header: "1", wantVerify: ErrUnsignedHeaders},
		// A signer slightly ahead of the server stays within globalMaxSkewTime.
		{name: "future within skew", age: -2 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPut, "http://minio.local/bucket/object", nil)
			presignBoundaryRequest(t, r, UTCNow().Add(-tc.age), []string{"host"}, globalActiveCred)
			if tc.header != "" {
				r.Header.Set(forgedSignatureAgeHeader, tc.header)
			}
			values := getConditionValues(r, "", globalActiveCred)
			age, err := strconv.ParseInt(strings.Join(values["signatureAge"], ""), 10, 64)
			lo, hi := (tc.age - time.Minute).Milliseconds(), (tc.age + time.Minute).Milliseconds()
			if err != nil || age < lo || age > hi {
				t.Errorf("pre-verification policy got age %v; want the age of the signed date (about %d ms)", values["signatureAge"], tc.age.Milliseconds())
			}
			code := reqSignatureV4Verify(r, globalSite.Region(), serviceS3)
			if code != tc.wantVerify {
				t.Fatalf("verification: got %s, want %s", niceError(code), niceError(tc.wantVerify))
			}
			if again := reqSignatureV4Verify(r, globalSite.Region(), serviceS3); again != code {
				t.Fatalf("second verification changed the outcome: %s -> %s", niceError(code), niceError(again))
			}
		})
	}
}

// The s3:x-amz-content-sha256 policy value must be the single payload hash the
// request is verified and enforced against. Header presence decides whether the
// key exists at all; the value is the one getContentSha256Cksum selects, so a
// presigned query value wins over the header and a repeated header contributes
// only its first value. None of these requests is rejected at the protocol
// level; the policy simply sees what verification bound.
func TestGetConditionValuesPayloadHashMatchesVerifiedValue(t *testing.T) {
	setupSignatureBoundaryTest(t)
	hashA, hashB := getSHA256Hash([]byte("a")), getSHA256Hash([]byte("b"))
	for _, tc := range []struct {
		name      string
		presigned bool
		query     string
		header    []string
		want      []string
	}{
		{name: "signed header", header: []string{hashA}, want: []string{hashA}},
		{name: "signed absent"},
		{name: "signed present empty", header: []string{""}, want: []string{""}},
		{name: "signed duplicate header", header: []string{hashA, hashB}, want: []string{hashA}},
		{name: "signed streaming with second value", header: []string{streamingContentSHA256, hashA}, want: []string{streamingContentSHA256}},
		{name: "presigned query only", presigned: true, query: hashA},
		{name: "presigned header only", presigned: true, header: []string{hashA}, want: []string{hashA}},
		{name: "presigned matching query and header", presigned: true, query: hashA, header: []string{hashA}, want: []string{hashA}},
		{name: "presigned unsigned query with forged header", presigned: true, query: unsignedPayload, header: []string{hashA}, want: []string{unsignedPayload}},
		{name: "presigned duplicate header", presigned: true, header: []string{hashA, hashB}, want: []string{hashA}},
		{name: "presigned query with empty header", presigned: true, query: hashA, header: []string{""}, want: []string{hashA}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := "http://minio.local/bucket/object"
			if tc.query != "" {
				target += "?" + xhttp.AmzContentSha256 + "=" + tc.query
			}
			r := httptest.NewRequest(http.MethodPut, target, nil)
			if tc.header != nil {
				r.Header[xhttp.AmzContentSha256] = tc.header
			}
			if tc.presigned {
				presignBoundaryRequest(t, r, UTCNow(), []string{"host"}, globalActiveCred)
			} else {
				r.Header.Set(xhttp.Authorization, signV4Algorithm+" Credential=x/20260910/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=x")
				r.Form = r.URL.Query()
			}
			got, ok := getConditionValues(r, "", globalActiveCred)[xhttp.AmzContentSha256]
			if ok != (tc.want != nil) || !slices.Equal(got, tc.want) {
				t.Fatalf("policy value = %v (present=%v), want %v (present=%v)", got, ok, tc.want, tc.want != nil)
			}
			if tc.presigned {
				if code := reqSignatureV4Verify(r, globalSite.Region(), serviceS3); code != ErrNone {
					t.Fatalf("the presigned request must still verify: %s", niceError(code))
				}
			}
		})
	}
}

func newSignatureBoundaryUser(t *testing.T, bucket, statements string) auth.Credentials {
	t.Helper()
	cred, err := auth.GetNewCredentials()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := globalIAMSys.CreateUser(t.Context(), cred.AccessKey, madmin.AddOrUpdateUserReq{SecretKey: cred.SecretKey, Status: madmin.AccountEnabled}); err != nil {
		t.Fatal(err)
	}
	p, err := policy.ParseConfig(strings.NewReader(fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::%s/*"},%s]}`, bucket, statements)))
	if err != nil {
		t.Fatal(err)
	}
	name := "signature-boundary-" + mustGetUUID()
	if _, err := globalIAMSys.SetPolicy(t.Context(), name, *p); err != nil {
		t.Fatal(err)
	}
	if _, err := globalIAMSys.PolicyDBSet(t.Context(), cred.AccessKey, name, regUser, false); err != nil {
		t.Fatal(err)
	}
	return cred
}

func TestAPIPresignedSignatureAgeBeforeAuthorization(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t:         t,
		endpoints: []string{"NewMultipart", "PutObjectPart", "PutObject"},
		objAPITest: func(obj ObjectLayer, instanceType, bucket string, router http.Handler, root auth.Credentials, t *testing.T) {
			user := newSignatureBoundaryUser(t, bucket, fmt.Sprintf(`{"Effect":"Deny","Action":"s3:PutObject","Resource":"arn:aws:s3:::%s/*","Condition":{"NumericGreaterThan":{"s3:signatureAge":"60000"}}}`, bucket))
			initReq, err := newTestSignedRequestV4(http.MethodPost, getNewMultipartURL("", bucket, "multipart"), 0, nil, root.AccessKey, root.SecretKey, nil)
			if err != nil {
				t.Fatal(err)
			}
			initRec := httptest.NewRecorder()
			router.ServeHTTP(initRec, initReq)
			var upload InitiateMultipartUploadResponse
			if initRec.Code != http.StatusOK || xml.Unmarshal(initRec.Body.Bytes(), &upload) != nil {
				t.Fatalf("multipart initiation: %d %s", initRec.Code, initRec.Body.String())
			}
			for _, operation := range []string{"put", "part"} {
				for _, tc := range []struct {
					name, header string
					age          time.Duration
					want         int
				}{
					{name: "fresh", want: http.StatusOK},
					{name: "old without header", age: 10 * time.Minute, want: http.StatusForbidden},
					{name: "old forged header", age: 10 * time.Minute, header: "0", want: http.StatusForbidden},
					// Policy allows a fresh signature; the unsigned header then fails
					// verification with the existing ErrUnsignedHeaders (HTTP 400).
					{name: "fresh with unsigned header", header: "0", want: http.StatusBadRequest},
				} {
					t.Run(instanceType+"/"+operation+"/"+tc.name, func(t *testing.T) {
						target := getPutObjectURL("", bucket, "put-"+strings.ReplaceAll(tc.name, " ", "-"))
						if operation == "part" {
							target = getPutObjectPartURL("", bucket, "multipart", upload.UploadID, "1")
						}
						r, err := newTestRequest(http.MethodPut, target, 4, bytes.NewReader([]byte("body")))
						if err != nil {
							t.Fatal(err)
						}
						r.Header.Del(xhttp.AmzContentSha256)
						presignBoundaryRequest(t, r, UTCNow().Add(-tc.age), []string{"host"}, user)
						if tc.header != "" {
							r.Header.Set(forgedSignatureAgeHeader, tc.header)
						}
						rec := httptest.NewRecorder()
						router.ServeHTTP(rec, r)
						if rec.Code != tc.want {
							t.Errorf("got %d %s, want %d", rec.Code, rec.Body.String(), tc.want)
						}
					})
				}
			}
		},
	})
}

func TestAPIPayloadHashPolicyMatchesVerifiedValue(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t:         t,
		endpoints: []string{"PutObject"},
		objAPITest: func(_ ObjectLayer, instanceType, bucket string, router http.Handler, _ auth.Credentials, t *testing.T) {
			allowedHash := getSHA256Hash([]byte("expected"))
			user := newSignatureBoundaryUser(t, bucket, fmt.Sprintf(`{"Effect":"Deny","Action":"s3:PutObject","Resource":"arn:aws:s3:::%s/*","Condition":{"StringNotEquals":{"s3:x-amz-content-sha256":"%s"}}}`, bucket, allowedHash))
			for _, kind := range []string{"signed control", "presigned control", "unsigned query forged header", "signed duplicate header", "presigned duplicate header"} {
				t.Run(instanceType+"/"+kind, func(t *testing.T) {
					control := strings.HasSuffix(kind, "control")
					body := "modified"
					if control {
						body = "expected"
					}
					r, err := newTestRequest(http.MethodPut, getPutObjectURL("", bucket, strings.ReplaceAll(kind, " ", "-")), int64(len(body)), strings.NewReader(body))
					if err != nil {
						t.Fatal(err)
					}
					if strings.Contains(kind, "duplicate") {
						r.Header.Add(xhttp.AmzContentSha256, allowedHash)
					}
					if kind == "unsigned query forged header" {
						q := r.URL.Query()
						q.Set(xhttp.AmzContentSha256, unsignedPayload)
						r.URL.RawQuery = q.Encode()
						r.Header.Set(xhttp.AmzContentSha256, allowedHash)
					}
					if strings.HasPrefix(kind, "signed") {
						if err := signRequestV4(r, user.AccessKey, user.SecretKey); err != nil {
							t.Fatal(err)
						}
					} else {
						presignBoundaryRequest(t, r, UTCNow(), []string{"host"}, user)
					}
					rec := httptest.NewRecorder()
					router.ServeHTTP(rec, r)
					if control && rec.Code != http.StatusOK {
						t.Errorf("control rejected: %d %s", rec.Code, rec.Body.String())
					}
					if !control && rec.Code < http.StatusBadRequest {
						t.Errorf("payload-hash policy bypass returned %d %s", rec.Code, rec.Body.String())
					}
				})
			}
		},
	})
}
