// Copyright (c) 2026 PGSTY
// SPDX-License-Identifier: AGPL-3.0-only

package cmd

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio/internal/auth"
	objectlock "github.com/minio/minio/internal/bucket/object/lock"
	xhttp "github.com/minio/minio/internal/http"
)

func setTestBucketDefaultRetention(t *testing.T, bucket, mode string) {
	t.Helper()
	enableBucketObjectLock(t, bucket)
	if mode == "" {
		return
	}
	meta, err := globalBucketMetadataSys.Get(bucket)
	if err != nil {
		t.Fatal(err)
	}
	meta.ObjectLockConfigXML = fmt.Appendf(nil, `<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>%s</Mode><Days>2</Days></DefaultRetention></Rule></ObjectLockConfiguration>`, mode)
	if err := meta.parseAllConfigs(t.Context(), newObjectLayerFn()); err != nil {
		t.Fatal(err)
	}
	globalBucketMetadataSys.Set(bucket, meta)
}

func TestAPIObjectLockDefaultRetentionWithLegalHold(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t:         t,
		endpoints: []string{"NewMultipart", "PutObjectPart", "CompleteMultipart", "CopyObject", "PutObject", "DeleteObject"},
		objAPITest: func(obj ObjectLayer, instanceType, bucket string, router http.Handler, cred auth.Credentials, t *testing.T) {
			data := []byte("default retention and legal hold are independent")
			putCopyChecksumSource(t, router, cred, bucket, "source", data, nil)
			for _, mode := range []string{"GOVERNANCE", "COMPLIANCE", ""} {
				setTestBucketDefaultRetention(t, bucket, mode)
				for _, hold := range []string{"ON", "OFF"} {
					for _, operation := range []string{"put", "copy", "multipart"} {
						t.Run(instanceType+"/"+mode+"/"+hold+"/"+operation, func(t *testing.T) {
							object := mode + "-" + hold + "-" + operation
							headers := map[string]string{xhttp.AmzObjectLockLegalHold: hold}
							// The stored retention date carries millisecond precision.
							before := UTCNow().Truncate(time.Millisecond)
							switch operation {
							case "put":
								putCopyChecksumSource(t, router, cred, bucket, object, data, headers)
							case "copy":
								rec := federatedCopyRequest(t, router, cred, bucket, "source", bucket, object, headers)
								if rec.Code != http.StatusOK {
									t.Fatalf("copy: %d %s", rec.Code, rec.Body.String())
								}
							case "multipart":
								putFederationMultipartSource(t, router, cred, bucket, object, [][]byte{data}, headers)
							}
							info, err := obj.GetObjectInfo(t.Context(), bucket, object, ObjectOptions{})
							if err != nil {
								t.Fatal(err)
							}
							ret := objectlock.GetObjectRetentionMeta(info.UserDefined)
							if string(ret.Mode) != mode {
								t.Errorf("stored retention mode = %q, want %q", ret.Mode, mode)
							}
							if mode != "" && (ret.RetainUntilDate.Before(before.Add(48*time.Hour)) || ret.RetainUntilDate.After(UTCNow().Add(48*time.Hour))) {
								t.Errorf("default retention date = %s, want write time + 2 days", ret.RetainUntilDate)
							}
							if got := objectlock.GetObjectLegalHoldMeta(info.UserDefined).Status; string(got) != hold {
								t.Errorf("stored legal hold = %q, want %q", got, hold)
							}
							if mode == "COMPLIANCE" && hold == "OFF" {
								req, err := newTestSignedRequestV4(http.MethodDelete, getPutObjectURL("", bucket, object)+"?versionId="+info.VersionID, 0, nil, cred.AccessKey, cred.SecretKey, nil)
								if err != nil {
									t.Fatal(err)
								}
								rec := httptest.NewRecorder()
								router.ServeHTTP(rec, req)
								if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "InvalidRequest") {
									t.Errorf("version DELETE must enforce retention: %d %s", rec.Code, rec.Body.String())
								}
							}
						})
					}
				}
			}
		},
	})
}

func TestObjectLockDefaultRetentionBoundaries(t *testing.T) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t: t,
		objAPITest: func(obj ObjectLayer, instanceType, bucket string, _ http.Handler, _ auth.Credentials, t *testing.T) {
			setTestBucketDefaultRetention(t, bucket, "COMPLIANCE")
			for _, tc := range []struct {
				name                      string
				replica, marker, explicit bool
				retentionErr, holdErr     APIErrorCode
				wantMode                  objectlock.RetMode
				wantErr                   APIErrorCode
			}{
				{name: "trusted replica", replica: true},
				{name: "marker-only ordinary write", marker: true, wantMode: objectlock.RetCompliance},
				{name: "explicit retention", explicit: true, wantMode: objectlock.RetGovernance},
				{name: "retention permission denied", retentionErr: ErrAccessDenied, wantErr: ErrAccessDenied},
				{name: "legal hold permission denied", holdErr: ErrAccessDenied, wantErr: ErrAccessDenied},
			} {
				t.Run(instanceType+"/"+tc.name, func(t *testing.T) {
					r := httptest.NewRequest(http.MethodPut, "http://minio.local/"+bucket+"/object", nil)
					r.Header.Set(xhttp.AmzObjectLockLegalHold, "OFF")
					if tc.marker {
						r.Header.Set(xhttp.MinIOSourceReplicationRequest, "true")
					}
					until := UTCNow().Add(24 * time.Hour).Truncate(time.Second).Add(789 * time.Millisecond)
					if tc.explicit {
						r.Header.Set(xhttp.AmzObjectLockMode, "GOVERNANCE")
						r.Header.Set(xhttp.AmzObjectLockRetainUntilDate, until.Format(time.RFC3339Nano))
					}
					mode, date, _, code := checkPutObjectLockAllowed(t.Context(), r, bucket, "object", obj.GetObjectInfo, tc.retentionErr, tc.holdErr, tc.replica)
					if code != tc.wantErr || mode != tc.wantMode {
						t.Fatalf("got mode %s error %s, want mode %s error %s", mode, niceError(code), tc.wantMode, niceError(tc.wantErr))
					}
					if tc.explicit && !date.Equal(until) {
						t.Errorf("explicit retention lost precision: %s != %s", date, until)
					}
					if tc.replica && !date.IsZero() {
						t.Errorf("replica acquired a destination default: %s", date)
					}
				})
			}
		},
	})
}
