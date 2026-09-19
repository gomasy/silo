// Copyright (c) 2026 Feng Ruohang
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"bytes"
	"crypto/sha256"
	"hash"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7/pkg/signer"
	"github.com/minio/minio/internal/auth"
)

type sdkStreamingHasher struct{ hash.Hash }

func (sdkStreamingHasher) Close() {}

func TestAPIUpstreamSDKStreamingContentType(t *testing.T) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t: t,
		objAPITest: func(obj ObjectLayer, instanceType, bucketName string, router http.Handler, cred auth.Credentials, t *testing.T) {
			payload := bytes.Repeat([]byte("sdk-streaming-"), 8192)
			for _, tamper := range []bool{false, true} {
				req, err := http.NewRequest(http.MethodPut, getPutObjectURL("http://localhost", bucketName, "sdk-streaming"), bytes.NewReader(payload))
				if err != nil {
					t.Fatal(err)
				}
				req.Header.Set("Content-Type", "application/x-silo-test")
				hasher := sdkStreamingHasher{sha256.New()}
				req = signer.StreamingSignV4(req, cred.AccessKey, cred.SecretKey, "", globalSite.Region(), int64(len(payload)), UTCNow(), hasher)
				_, signedHeaders, _ := strings.Cut(req.Header.Get("Authorization"), "SignedHeaders=")
				signedHeaders, _, _ = strings.Cut(signedHeaders, ",")
				if !slices.Contains(strings.Split(signedHeaders, ";"), "content-type") {
					t.Fatal("upstream SDK did not sign Content-Type")
				}
				if tamper {
					req.Header.Set("Content-Type", "application/x-tampered")
				}
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, req)
				want := http.StatusOK
				if tamper {
					want = http.StatusForbidden
				}
				if rec.Code != want {
					t.Fatalf("%s tamper=%v: status %d, want %d: %s", instanceType, tamper, rec.Code, want, rec.Body.String())
				}
			}
			info, err := obj.GetObjectInfo(t.Context(), bucketName, "sdk-streaming", ObjectOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if info.Size != int64(len(payload)) || info.ContentType != "application/x-silo-test" {
				t.Fatalf("stored size/type = %d/%q", info.Size, info.ContentType)
			}
		},
	})
}
