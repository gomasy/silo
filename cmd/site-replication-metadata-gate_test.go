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
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/auth"
)

func TestBucketMetadataTombstoneExportAndInitialSync(t *testing.T) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: func(obj ObjectLayer, backend, bucket string, _ http.Handler, cred auth.Credentials, t *testing.T) {
		t.Run(backend, func(t *testing.T) {
			recordBucketConfigPeer(t, cred)
			old := globalSiteReplicationMetadataTombstones
			defer func() { globalSiteReplicationMetadataTombstones = old }()
			created := UTCNow().Add(-time.Hour)
			deletedAt := created.Add(time.Minute)
			for _, enabled := range []bool{false, true} {
				globalSiteReplicationMetadataTombstones = enabled
				meta := newBucketMetadata(bucket)
				meta.Created = created
				meta.defaultTimestamps()
				for _, file := range []string{bucketPolicyConfig, bucketTaggingConfig, bucketSSEConfig, bucketQuotaConfigFile} {
					_, at := replicatedBucketConfig(&meta, file)
					*at = deletedAt
				}
				if err := globalBucketMetadataSys.save(t.Context(), meta); err != nil {
					t.Fatal(err)
				}
				// Force a disk reload instead of accepting the just-published cache.
				globalBucketMetadataSys.Remove(bucket)
				info, err := globalSiteReplicationSys.SiteReplicationMetaInfo(t.Context(), obj, madmin.SRStatusOptions{Buckets: true})
				if err != nil {
					t.Fatal(err)
				}
				exported := info.Buckets[bucket]
				if !exported.PolicyUpdatedAt.Equal(deletedAt) {
					t.Fatal("Policy tombstone hidden by gate")
				}
				for _, at := range []time.Time{exported.TagConfigUpdatedAt, exported.SSEConfigUpdatedAt, exported.QuotaConfigUpdatedAt} {
					if enabled && !at.Equal(deletedAt) || !enabled && !at.IsZero() {
						t.Fatalf("gate=%v timestamp=%v", enabled, at)
					}
				}
				for file, data := range bucketConfigTestData(bucket) {
					event, send, err := initialBucketConfigReplicationEvent(meta, file)
					wantSend := enabled && !bucketConfigUpdateOnly(file)
					if err != nil || send != wantSend || send && !event.UpdatedAt.Equal(deletedAt) {
						t.Fatalf("%s initial gate=%v: %+v %v %v", file, enabled, event, send, err)
					}
					baseline := newBucketMetadata(bucket)
					baseline.Created = created
					baseline.defaultTimestamps()
					if _, send, err := initialBucketConfigReplicationEvent(baseline, file); err != nil || send {
						t.Fatalf("empty baseline sent: %s", file)
					}
					value, _ := replicatedBucketConfig(&baseline, file)
					*value = data
					event, send, err = initialBucketConfigReplicationEvent(baseline, file)
					if err != nil || !send || !event.UpdatedAt.Equal(created) {
						t.Fatalf("historical baseline-live omitted: %s %v", file, err)
					}
				}
			}
		})
	}})
}

func TestPeerBucketMetadataLegacyAndGeneration(t *testing.T) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: func(obj ObjectLayer, backend, bucket string, _ http.Handler, cred auth.Credentials, t *testing.T) {
		t.Run(backend, func(t *testing.T) {
			old := globalSiteReplicationMetadataTombstones
			defer func() { globalSiteReplicationMetadataTombstones = old }()
			created := UTCNow().Add(-time.Hour)
			for _, enabled := range []bool{false, true} {
				globalSiteReplicationMetadataTombstones = enabled
				for file, data := range bucketConfigTestData(bucket) {
					meta := newBucketMetadata(bucket)
					meta.Created = created
					meta.defaultTimestamps()
					if err := globalBucketMetadataSys.save(t.Context(), meta); err != nil {
						t.Fatal(err)
					}
					counter := &bucketConfigWriteCounter{ObjectLayer: obj}
					setObjectLayer(counter)
					event := newBucketConfigReplicationEvent(bucket, file, bucketConfigState{data: data, at: created.Add(-time.Second)})
					rec := applySRBucketMetaViaAdmin(t, cred, event)
					if rec.Code != http.StatusOK || counter.writes.Load() != 0 {
						t.Fatalf("pre-creation apply: %s %d writes=%d", file, rec.Code, counter.writes.Load())
					}
					// A live baseline may initialize. The same-time nil cannot delete it.
					event.UpdatedAt = created
					rec = applySRBucketMetaViaAdmin(t, cred, event)
					if rec.Code != http.StatusOK {
						t.Fatalf("baseline apply: %s %s", file, rec.Body.String())
					}
					before := counter.writes.Load()
					rec = applySRBucketMetaViaAdmin(t, cred, madmin.SRBucketMeta{Type: event.Type, Bucket: bucket, UpdatedAt: created})
					if rec.Code != http.StatusOK || counter.writes.Load() != before {
						t.Fatalf("nil baseline cleared configuration: %s", file)
					}
					event.UpdatedAt = time.Time{}
					for range 2 {
						rec = applySRBucketMetaViaAdmin(t, cred, event)
						if rec.Code != http.StatusOK {
							t.Fatalf("legacy-zero rejected: %s %s", file, rec.Body.String())
						}
					}
					meta, err := loadBucketMetadata(t.Context(), obj, bucket)
					if err != nil {
						t.Fatal(err)
					}
					value, at := replicatedBucketConfig(&meta, file)
					if len(*value) == 0 || !at.After(created) {
						t.Fatalf("legacy-zero not reclocked: %s %v", file, *at)
					}
					setObjectLayer(obj)
				}
			}
		})
	}})
}

type bucketMetadataCreatedObjectLayer struct {
	ObjectLayer
	missing bool
}

func (o bucketMetadataCreatedObjectLayer) GetBucketInfo(ctx context.Context, bucket string, opts BucketOptions) (BucketInfo, error) {
	if opts.NoMetadata {
		if o.missing {
			return BucketInfo{}, BucketNotFound{Bucket: bucket}
		}
		// A physical bucket that reports no creation time either.
		return BucketInfo{Name: bucket}, nil
	}
	return o.ObjectLayer.GetBucketInfo(ctx, bucket, opts)
}

func TestPeerBucketMetadataUnknownCreated(t *testing.T) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: func(obj ObjectLayer, backend, bucket string, _ http.Handler, _ auth.Credentials, t *testing.T) {
		t.Run(backend, func(t *testing.T) {
			defer setObjectLayer(obj)
			data := bucketConfigTestData(bucket)[bucketTaggingConfig]
			for _, mode := range []string{"unknown", "missing"} {
				t.Run(mode, func(t *testing.T) {
					setObjectLayer(obj)
					if err := globalBucketMetadataSys.save(t.Context(), newBucketMetadata(bucket)); err != nil {
						t.Fatal(err)
					}
					counter := &bucketConfigWriteCounter{ObjectLayer: bucketMetadataCreatedObjectLayer{ObjectLayer: obj, missing: mode == "missing"}}
					setObjectLayer(counter)
					stamp := UTCNow()
					_, err := globalBucketMetadataSys.updateAndParseMetadata(t.Context(), bucket, bucketTaggingConfig, data, false, false, &stamp)
					if err == nil || counter.writes.Load() != 0 {
						t.Fatalf("unknown generation was invented: %v writes=%d", err, counter.writes.Load())
					}
				})
			}
		})
	}})
}

// setPhysicalBucketCreated stamps the bucket directory on every local drive,
// which is what StatVol reports as the physical creation time.
func setPhysicalBucketCreated(t *testing.T, bucket string, at time.Time) {
	t.Helper()
	globalLocalDrivesMu.RLock()
	drives := cloneDrives(globalLocalDrivesMap)
	globalLocalDrivesMu.RUnlock()
	if len(drives) == 0 {
		t.Fatal("no local drives registered")
	}
	for _, drive := range drives {
		if err := os.Chtimes(pathJoin(drive.Endpoint().Path, bucket), at, at); err != nil {
			t.Fatal(err)
		}
	}
}

// Recovery has to run against the real ObjectLayer: a stub GetBucketInfo
// returning the expected time would hide the cached zero creation time
// overwriting it, which is what a bucket that never held a configuration has.
func TestBucketMetadataPhysicalCreatedRecovery(t *testing.T) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: func(obj ObjectLayer, backend, bucket string, _ http.Handler, _ auth.Credentials, t *testing.T) {
		// One known time on every drive, so the recovered value can be neither
		// confused with UTCNow() nor dependent on which drive answers first.
		physical := UTCNow().Add(-3 * time.Hour).Truncate(time.Second)
		for _, missing := range []bool{false, true} {
			for _, file := range replicatedBucketConfigs {
				t.Run(backend+"/"+file+"/missing="+strconv.FormatBool(missing), func(t *testing.T) {
					ctx := t.Context()
					setPhysicalBucketCreated(t, bucket, physical)
					if err := globalBucketMetadataSys.save(ctx, newBucketMetadata(bucket)); err != nil {
						t.Fatal(err)
					}
					if missing {
						if err := deleteConfig(ctx, obj, pathJoin(bucketMetaPrefix, bucket, bucketMetadataFile)); err != nil {
							t.Fatal(err)
						}
					}
					data := bucketConfigTestData(bucket)[file]
					at, err := globalBucketMetadataSys.Update(ctx, bucket, file, data)
					if err != nil {
						t.Fatalf("bucket without a recorded creation time cannot update %s: %v", file, err)
					}
					got, err := readBucketMetadata(ctx, obj, bucket)
					if err != nil || !got.Created.Equal(physical) || !at.After(physical) {
						t.Fatalf("physical creation not persisted: created=%v physical=%v updated=%v err=%v", got.Created, physical, at, err)
					}
				})
			}
		}
	}})
}

func TestBucketMetadataInitialSyncPhysicalCreated(t *testing.T) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: func(obj ObjectLayer, backend, bucket string, _ http.Handler, cred auth.Credentials, t *testing.T) {
		t.Run(backend, func(t *testing.T) {
			ctx := t.Context()
			serviceCred, _, err := globalIAMSys.NewServiceAccount(ctx, cred.AccessKey, nil, newServiceAccountOpts{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { globalIAMSys.DeleteServiceAccount(context.Background(), serviceCred.AccessKey, false) })
			physical := UTCNow().Add(-3 * time.Hour).Truncate(time.Second)
			setPhysicalBucketCreated(t, bucket, physical)
			meta := newBucketMetadata(bucket)
			meta.TaggingConfigXML = bucketConfigTestData(bucket)[bucketTaggingConfig]
			if err := globalBucketMetadataSys.save(ctx, meta); err != nil {
				t.Fatal(err)
			}
			var mu sync.Mutex
			var createdAt string
			var events []madmin.SRBucketMeta
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				if r.URL.Query().Get("operation") == string(madmin.MakeWithVersioningBktOp) {
					createdAt = r.URL.Query().Get("createdAt")
				}
				if strings.HasSuffix(r.URL.Path, "/bucket-meta") {
					var event madmin.SRBucketMeta
					if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
						t.Error(err)
					}
					events = append(events, event)
				}
				if r.URL.Path == "/minio/admin/v3/site-replication/peer/iam-revisions" {
					_ = json.NewEncoder(w).Encode(iamRevisionResponse{iamRevisionStatus: iamRevisionStatus{Version: iamRevisionProtocol, Node: "initial-peer", Instance: "initial-boot", Digest: "ack"}})
					return
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer peer.Close()
			// Exercise the complete outgoing sync sequence with real source
			// storage. This peer acknowledges RPCs; it is not a second ObjectLayer.
			c := &SiteReplicationSys{enabled: true, state: srState{
				ServiceAccountAccessKey: serviceCred.AccessKey,
				Peers:                   map[string]madmin.PeerInfo{"initial-peer": {DeploymentID: "initial-peer", Endpoint: peer.URL}},
			}}
			if err := c.syncToAllPeers(ctx, madmin.SRAddOptions{}); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			if createdAt != physical.Format(time.RFC3339Nano) {
				t.Fatalf("peer creation time %q, want physical time %s", createdAt, physical)
			}
			for _, event := range events {
				if event.Bucket == bucket && event.Type == madmin.SRBucketMetaTypeTags {
					if event.Tags == nil || *event.Tags != base64.StdEncoding.EncodeToString(meta.TaggingConfigXML) || !event.UpdatedAt.Equal(physical) {
						t.Fatalf("historical tags lost baseline: %+v", event)
					}
					return
				}
			}
			t.Fatal("initial sync silently skipped historical tags")
		})
	}})
}

func TestPeerBucketMetadataPhysicalCreatedBoundary(t *testing.T) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: func(obj ObjectLayer, backend, bucket string, _ http.Handler, _ auth.Credentials, t *testing.T) {
		t.Run(backend, func(t *testing.T) {
			ctx := t.Context()
			physical := UTCNow().Truncate(time.Second)
			setPhysicalBucketCreated(t, bucket, physical)
			if err := globalBucketMetadataSys.save(ctx, newBucketMetadata(bucket)); err != nil {
				t.Fatal(err)
			}
			counter := &bucketConfigWriteCounter{ObjectLayer: obj}
			setObjectLayer(counter)
			defer setObjectLayer(obj)
			data := bucketConfigTestData(bucket)[bucketTaggingConfig]
			at := physical.Add(-time.Hour)
			_, err := globalBucketMetadataSys.updateAndParseMetadata(ctx, bucket, bucketTaggingConfig, data, false, false, &at)
			if err != nil || counter.writes.Load() != 0 {
				t.Fatalf("event before recovered creation must be skipped: err=%v writes=%d", err, counter.writes.Load())
			}
			// Physical mtime is only an approximation. A local correction can
			// establish it; an earlier peer event cannot lower the bucket identity.
			at, err = globalBucketMetadataSys.Update(ctx, bucket, bucketTaggingConfig, data)
			if err != nil {
				t.Fatal(err)
			}
			got, err := readBucketMetadata(ctx, obj, bucket)
			if err != nil || !got.Created.Equal(physical) || !at.After(physical) || !bytes.Equal(got.TaggingConfigXML, data) {
				t.Fatalf("local correction did not establish physical creation: %+v %v", got, err)
			}
		})
	}})
}
