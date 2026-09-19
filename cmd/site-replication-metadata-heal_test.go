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
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/logger/target/testlogger"
)

func bucketConfigTestData(bucket string) map[string][]byte {
	return map[string][]byte{
		bucketPolicyConfig:     []byte(fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::%s/*"}]}`, bucket)),
		bucketTaggingConfig:    []byte(`<Tagging><TagSet><Tag><Key>key</Key><Value>value</Value></Tag></TagSet></Tagging>`),
		bucketSSEConfig:        []byte(`<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>`),
		bucketQuotaConfigFile:  []byte(`{"quota":1024,"quotatype":"hard"}`),
		bucketVersioningConfig: enabledBucketVersioningConfig,
		objectLockConfig:       enabledBucketObjectLockConfig,
	}
}

type bucketConfigLogCapture struct {
	testing.TB
	mu    sync.Mutex
	lines []string
}

func (c *bucketConfigLogCapture) Logf(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, fmt.Sprintf(format, args...))
}

func TestHealBucketConfigDiagnostics(t *testing.T) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: func(obj ObjectLayer, backend, bucket string, _ http.Handler, cred auth.Credentials, t *testing.T) {
		t.Run(backend, func(t *testing.T) {
			recordBucketConfigPeer(t, cred)
			capture := &bucketConfigLogCapture{TB: t}
			defer testlogger.T.SetLogTB(capture)()
			disabled := logger.DisableLog
			logger.DisableLog = false
			defer func() { logger.DisableLog = disabled }()
			created := UTCNow().Add(-time.Hour)
			local := globalDeploymentID()
			bs := map[string]srBucketStatsSummary{
				local:            bucketConfigTestInfo(bucket, bucketTaggingConfig, nil, created, created),
				"missing-bucket": {},
				"broken-rpc":     bucketConfigTestInfo(bucket, bucketTaggingConfig, nil, created, created),
			}
			info := srStatusInfo{Sites: map[string]madmin.PeerInfo{local: {}, "missing-bucket": {}, "unreachable": {}, "broken-rpc": {}}, BucketStats: map[string]map[string]srBucketStatsSummary{bucket: bs}}
			if err := globalSiteReplicationSys.healBucketConfig(t.Context(), bucket, bucketTaggingConfig, info); err != nil {
				t.Fatal(err)
			}
			if len(capture.lines) != 0 {
				t.Fatal("empty baselines produced diagnostics")
			}
			bs[local] = bucketConfigTestInfo(bucket, bucketTaggingConfig, bucketConfigTestData(bucket)[bucketTaggingConfig], created.Add(time.Minute), created)
			for range 2 {
				if err := globalSiteReplicationSys.healBucketConfig(t.Context(), bucket, bucketTaggingConfig, info); err != nil {
					t.Fatal(err)
				}
			}
			capture.mu.Lock()
			defer capture.mu.Unlock()
			var unreachable, peerError int
			for _, line := range capture.lines {
				switch {
				case strings.Contains(line, "bucket metadata replication: unreachable"):
					unreachable++
				case strings.Contains(line, "bucket metadata replication: peer-error"):
					peerError++
				default:
					t.Fatalf("unexpected diagnostic: %s", line)
				}
				if !strings.HasPrefix(line, "WARNING:") {
					t.Fatalf("diagnostic is not a warning: %s", line)
				}
			}
			if unreachable != 1 || peerError != 1 {
				t.Fatalf("distinct reasons were lost or not deduplicated: unreachable=%d peer-error=%d", unreachable, peerError)
			}
		})
	}})
}

func TestHealBucketConfigWithoutSourceDiagnostics(t *testing.T) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: func(obj ObjectLayer, backend, bucket string, _ http.Handler, cred auth.Credentials, t *testing.T) {
		t.Run(backend, func(t *testing.T) {
			events := recordBucketConfigPeer(t, cred)
			disabled := logger.DisableLog
			logger.DisableLog = false
			defer func() { logger.DisableLog = disabled }()
			created := UTCNow().Add(-time.Hour)
			data := bucketConfigTestData(bucket)[bucketTaggingConfig]
			for _, tc := range []struct {
				name    string
				data    []byte
				at      time.Time
				created time.Time
				wantLog bool
			}{
				{"unknown-created", data, created.Add(time.Minute), time.Time{}, true},
				{"malformed", []byte("<Tagging>"), created.Add(time.Minute), created, true},
				{"before-created", data, created.Add(-time.Minute), created, true},
				{"empty-baseline", nil, created, created, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					capture := &bucketConfigLogCapture{TB: t}
					defer testlogger.T.SetLogTB(capture)()
					// Each case has its own diagnostic key; no source can be selected
					// and neither local storage nor the recording peer may be written.
					name := bucket + "-" + tc.name
					local := globalDeploymentID()
					info := srStatusInfo{
						Sites: map[string]madmin.PeerInfo{local: {}, "metadata-peer": {}, "unreachable": {}},
						BucketStats: map[string]map[string]srBucketStatsSummary{name: {
							local:           bucketConfigTestInfo(name, bucketTaggingConfig, tc.data, tc.at, tc.created),
							"metadata-peer": {},
						}},
					}
					for range 2 {
						if err := globalSiteReplicationSys.healBucketConfig(t.Context(), name, bucketTaggingConfig, info); err != nil {
							t.Fatal(err)
						}
					}
					capture.mu.Lock()
					defer capture.mu.Unlock()
					want := 0
					if tc.wantLog {
						want = 1
					}
					if len(capture.lines) != want {
						t.Fatalf("got %d diagnostics, want %d: %v", len(capture.lines), want, capture.lines)
					}
					for _, line := range capture.lines {
						if !strings.HasPrefix(line, "WARNING:") || !strings.Contains(line, "bucket metadata replication: indeterminate") {
							t.Fatalf("unexpected diagnostic: %s", line)
						}
					}
					if len(events()) != 0 {
						t.Fatal("healing without a source sent a metadata RPC")
					}
				})
			}
		})
	}})
}

// Construct independent wire fixtures, including timestamps hidden by legacy
// exporters, without going through the production event or state helpers.
func bucketConfigTestInfo(bucket, file string, data []byte, at, created time.Time) srBucketStatsSummary {
	info := madmin.SRBucketInfo{Bucket: bucket, CreatedAt: created}
	var payload *string
	if len(data) != 0 {
		encoded := base64.StdEncoding.EncodeToString(data)
		payload = &encoded
	}
	switch file {
	case bucketPolicyConfig:
		info.Policy, info.PolicyUpdatedAt = data, at
	case bucketTaggingConfig:
		info.Tags, info.TagConfigUpdatedAt = payload, at
	case bucketSSEConfig:
		info.SSEConfig, info.SSEConfigUpdatedAt = payload, at
	case bucketQuotaConfigFile:
		info.QuotaConfig, info.QuotaConfigUpdatedAt = payload, at
	case bucketVersioningConfig:
		info.Versioning, info.VersioningConfigUpdatedAt = payload, at
	case objectLockConfig:
		info.ObjectLockConfig, info.ObjectLockConfigUpdatedAt = payload, at
	}
	return srBucketStatsSummary{meta: srBucketMetaInfo{SRBucketInfo: info}}
}

func TestLatestBucketConfigCandidates(t *testing.T) {
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for file, data := range bucketConfigTestData("bucket") {
		t.Run(file, func(t *testing.T) {
			info := srStatusInfo{Sites: map[string]madmin.PeerInfo{"a": {}, "b": {}, "c": {}}, BucketStats: map[string]map[string]srBucketStatsSummary{"bucket": {}}}
			bs := info.BucketStats["bucket"]
			baseline := bucketConfigTestInfo("bucket", file, nil, created, created)
			bs["a"], bs["b"], bs["c"] = baseline, baseline, baseline
			if _, found := latestBucketConfig("bucket", file, info); found {
				t.Fatal("empty baseline chosen as source")
			}
			// Unknown and unavailable IDs cannot contribute, even with future clocks.
			bs[""], bs["unknown"] = bucketConfigTestInfo("bucket", file, data, created.Add(10*time.Hour), created), bucketConfigTestInfo("bucket", file, data, created.Add(20*time.Hour), created)
			if _, found := latestBucketConfig("bucket", file, info); found {
				t.Fatal("unknown source chosen")
			}
			liveBaseline := bucketConfigTestInfo("bucket", file, data, created, created)
			modified := bucketConfigTestInfo("bucket", file, data, created.Add(time.Hour), created)
			oldGeneration := bucketConfigTestInfo("bucket", file, data, created, created.Add(time.Second))
			states := []srBucketStatsSummary{liveBaseline, modified, oldGeneration}
			for _, order := range [][3]int{{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}} {
				bs["a"], bs["b"], bs["c"] = states[order[0]], states[order[1]], states[order[2]]
				winner, found := latestBucketConfig("bucket", file, info)
				if !found || !winner.at.Equal(created.Add(time.Hour)) || !bytes.Equal(winner.data, data) {
					t.Fatalf("permutation %v chose %+v found=%v", order, winner, found)
				}
			}
			bs["a"], bs["b"], bs["c"] = baseline, liveBaseline, baseline
			if winner, found := latestBucketConfig("bucket", file, info); !found || !bytes.Equal(winner.data, data) || winner.real {
				t.Fatal("historical baseline-live cannot initialize")
			}
			if !bucketConfigUpdateOnly(file) {
				// A late-created empty baseline cannot beat a real earlier write.
				bs["a"], bs["b"], bs["c"] = modified, bucketConfigTestInfo("bucket", file, nil, created.Add(2*time.Hour), created.Add(2*time.Hour)), baseline
				if winner, found := latestBucketConfig("bucket", file, info); !found || len(winner.data) == 0 {
					t.Fatal("default became a deletion")
				}
				bs["c"] = bucketConfigTestInfo("bucket", file, nil, created.Add(time.Hour), created)
				if winner, found := latestBucketConfig("bucket", file, info); !found || len(winner.data) != 0 || !winner.real {
					t.Fatal("real tombstone lost equal-time tie")
				}
			}
		})
	}
}

func TestHealBucketConfigSourceAndQuiescence(t *testing.T) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: func(obj ObjectLayer, backend, bucket string, _ http.Handler, cred auth.Credentials, t *testing.T) {
		t.Run(backend, func(t *testing.T) {
			ctx := t.Context()
			events := recordBucketConfigPeer(t, cred)
			local := globalDeploymentID()
			created := UTCNow().Add(-time.Hour)
			oldAt, newAt := created.Add(time.Minute), created.Add(2*time.Minute)
			counter := &bucketConfigWriteCounter{ObjectLayer: obj}
			setObjectLayer(counter)
			defer setObjectLayer(obj)
			for file, data := range bucketConfigTestData(bucket) {
				t.Run(file, func(t *testing.T) {
					meta := newBucketMetadata(bucket)
					meta.Created = created
					meta.defaultTimestamps()
					value, at := replicatedBucketConfig(&meta, file)
					*value, *at = data, newAt
					if err := globalBucketMetadataSys.save(ctx, meta); err != nil {
						t.Fatal(err)
					}
					bs := map[string]srBucketStatsSummary{
						local:           bucketConfigTestInfo(bucket, file, data, newAt, created),
						"metadata-peer": bucketConfigTestInfo(bucket, file, data, oldAt, created),
						"":              {}, "unknown": bucketConfigTestInfo(bucket, file, nil, newAt.Add(time.Hour), created),
						// Known but absent from c.state.Peers, forcing getAdminClient failure.
						"broken": bucketConfigTestInfo(bucket, file, nil, created, created),
					}
					info := srStatusInfo{Sites: map[string]madmin.PeerInfo{local: {}, "metadata-peer": {}, "broken": {}}, BucketStats: map[string]map[string]srBucketStatsSummary{bucket: bs}}
					before := len(events())
					writes := counter.writes.Load()
					if err := globalSiteReplicationSys.healBucketConfig(ctx, bucket, file, info); err != nil {
						t.Fatal(err)
					}
					got := events()[before:]
					if len(got) != 1 || !got[0].UpdatedAt.Equal(newAt) {
						t.Fatalf("same-payload timestamp heal did not reach healthy peer: %+v", got)
					}
					if file == bucketTaggingConfig && got[0].Tags == nil {
						t.Fatal("tag event missing payload")
					}
					if counter.writes.Load() != writes {
						t.Fatal("matching local state was rewritten")
					}
					bs["metadata-peer"], bs["broken"] = bs[local], bs[local]
					if err := globalSiteReplicationSys.healBucketConfig(ctx, bucket, file, info); err != nil {
						t.Fatal(err)
					}
					if len(events()) != before+1 || counter.writes.Load() != writes {
						t.Fatal("stable second heal wrote or broadcast")
					}
					if bucketConfigUpdateOnly(file) {
						return
					}
					bs["metadata-peer"] = bucketConfigTestInfo(bucket, file, nil, newAt.Add(time.Minute), created)
					delete(bs, "broken")
					if err := globalSiteReplicationSys.healBucketConfig(ctx, bucket, file, info); err != nil {
						t.Fatal(err)
					}
					after, err := loadBucketMetadata(ctx, obj, bucket)
					if err != nil {
						t.Fatal(err)
					}
					value, at = replicatedBucketConfig(&after, file)
					if len(*value) != 0 || !at.Equal(newAt.Add(time.Minute)) {
						t.Fatalf("local heal lost deletion/source time: %s %v", *value, *at)
					}
					if file == bucketQuotaConfigFile {
						q, _, err := globalBucketMetadataSys.GetQuotaConfig(ctx, bucket)
						if err != nil || q.Quota != 0 {
							t.Fatalf("quota cache not cleared: %+v %v", q, err)
						}
					}
				})
			}
		})
	}})
}
