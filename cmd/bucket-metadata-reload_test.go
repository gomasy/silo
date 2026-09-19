// Copyright 2026 PGSTY contributors.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"net/http"
	"testing"
	"time"

	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/grid"
)

func TestPeerMetadataReloadWithEqualMaximumTimestamp(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: testPeerMetadataReloadWithEqualMaximumTimestamp})
}

func testPeerMetadataReloadWithEqualMaximumTimestamp(obj ObjectLayer, instanceType, bucket string, _ http.Handler, _ auth.Credentials, t *testing.T) {
	disk, err := loadBucketMetadata(t.Context(), obj, bucket)
	if err != nil {
		t.Fatal(err)
	}
	disk.TaggingConfigXML = []byte(`<Tagging><TagSet><Tag><Key>revision</Key><Value>new</Value></Tag></TagSet></Tagging>`)
	disk.TaggingConfigUpdatedAt = UTCNow()
	// An unrelated configuration has the greatest timestamp in both records.
	disk.PolicyConfigUpdatedAt = disk.TaggingConfigUpdatedAt.Add(time.Hour)
	if err := disk.Save(t.Context(), obj); err != nil {
		t.Fatal(err)
	}
	resident := disk
	resident.TaggingConfigXML = []byte(`<Tagging><TagSet><Tag><Key>revision</Key><Value>old</Value></Tag></TagSet></Tagging>`)
	resident.TaggingConfigUpdatedAt = disk.TaggingConfigUpdatedAt.Add(-time.Minute)
	if err := resident.parseAllConfigs(t.Context(), obj); err != nil {
		t.Fatal(err)
	}
	if !resident.lastUpdate().Equal(disk.lastUpdate()) {
		t.Fatal("fixture must have equal maximum timestamps")
	}
	globalBucketMetadataSys.Set(bucket, resident)

	args := grid.MSS{peerRESTBucket: bucket}
	if _, err := (&peerRESTServer{}).LoadBucketMetadataHandler(&args); err != nil {
		t.Fatal(err)
	}
	tagging, _, err := globalBucketMetadataSys.GetTaggingConfig(bucket)
	if err != nil {
		t.Fatal(err)
	}
	if got := tagging.ToMap()["revision"]; got != "new" {
		t.Errorf("%s: peer reload retained tag %q despite a newer tagging configuration", instanceType, got)
	}
}
