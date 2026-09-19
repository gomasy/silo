// Copyright (c) 2026 PGSTY
// SPDX-License-Identifier: AGPL-3.0-only

package cmd

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio/internal/auth"
	xhttp "github.com/minio/minio/internal/http"
)

func TestPresignedHeaderPayloadHashIsEnforced(t *testing.T) {
	setupSignatureBoundaryTest(t)
	digest := getSHA256Hash([]byte("expected"))
	for _, tc := range []struct {
		name, query, header string
		unsigned            bool
	}{
		{name: "header only", header: digest},
		{name: "query only", query: digest},
		{name: "query wins", query: digest, header: getSHA256Hash([]byte("different"))},
		{name: "unsigned query wins", query: unsignedPayload, header: digest, unsigned: true},
		{name: "unsigned header", header: unsignedPayload, unsigned: true},
		{name: "absent", unsigned: true},
	} {
		for _, body := range []string{"expected", "modified"} {
			t.Run(tc.name+"/"+body, func(t *testing.T) {
				r := httptest.NewRequest(http.MethodPut, "http://minio.local/bucket/object", strings.NewReader(body))
				if tc.header != "" {
					r.Header.Set(xhttp.AmzContentSha256, tc.header)
				}
				if tc.query != "" {
					query := r.URL.Query()
					query.Set(xhttp.AmzContentSha256, tc.query)
					r.URL.RawQuery = query.Encode()
				}
				presignBoundaryRequest(t, r, UTCNow(), []string{"host"}, globalActiveCred)
				if code := isReqAuthenticated(t.Context(), r, globalSite.Region(), serviceS3); code != ErrNone {
					t.Fatal(niceError(code))
				}
				_, err := io.ReadAll(r.Body)
				wantMismatch := body == "modified" && !tc.unsigned
				if wantMismatch {
					if toAPIErrorCode(t.Context(), err) != ErrContentSHA256Mismatch {
						t.Errorf("modified signed payload: got %v, want SHA-256 mismatch", err)
					}
				} else if err != nil {
					t.Errorf("valid payload: %v", err)
				}
			})
		}
	}
}

func TestAPIPresignedBucketPolicyPayloadHash(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t: t,
		objAPITest: func(objectAPI ObjectLayer, instanceType, bucket string, apiRouter http.Handler, credentials auth.Credentials, t *testing.T) {
			server := httptest.NewServer(apiRouter)
			defer server.Close()
			client := server.Client()
			client.Timeout = 10 * time.Second
			expected := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::%s/*"]}]}`, bucket)
			modified := strings.Replace(expected, "s3:GetObject", "s3:PutObject", 1)
			for _, tc := range []struct {
				name, body string
				status     int
			}{
				{"original", expected, http.StatusNoContent},
				{"modified", modified, http.StatusBadRequest},
			} {
				t.Run(instanceType+"/"+tc.name, func(t *testing.T) {
					r, err := http.NewRequestWithContext(t.Context(), http.MethodPut, server.URL+"/"+bucket+"?policy=", strings.NewReader(tc.body))
					if err != nil {
						t.Fatal(err)
					}
					r.Header.Set(xhttp.AmzContentSha256, getSHA256Hash([]byte(expected)))
					presignBoundaryRequest(t, r, UTCNow(), []string{"host"}, credentials)
					response, err := client.Do(r)
					if err != nil {
						t.Fatal(err)
					}
					defer response.Body.Close()
					body, err := io.ReadAll(response.Body)
					if err != nil {
						t.Fatal(err)
					}
					if response.StatusCode != tc.status {
						t.Fatalf("PutBucketPolicy: got %d %s, want %d", response.StatusCode, body, tc.status)
					}
					if tc.status == http.StatusBadRequest && !strings.Contains(string(body), "XAmzContentSHA256Mismatch") {
						t.Fatalf("expected payload checksum rejection, got %s", body)
					}
				})
			}
			meta, err := globalBucketMetadataSys.Get(bucket)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(meta.PolicyConfigJSON), "s3:PutObject") {
				t.Fatal("tampered presigned request replaced the bucket policy")
			}
		},
		endpoints: []string{"PutBucketPolicy"},
	})
}
