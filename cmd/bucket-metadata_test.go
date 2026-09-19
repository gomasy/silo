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

import "testing"

func TestBucketMetadataCorsRoundTrip(t *testing.T) {
	meta := newBucketMetadata("test-cors")
	meta.CorsConfigXML = []byte(`<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>`)
	meta.CorsConfigUpdatedAt = UTCNow()

	buf, err := meta.MarshalMsg(nil)
	if err != nil {
		t.Fatal(err)
	}
	var got BucketMetadata
	if _, err := got.UnmarshalMsg(buf); err != nil {
		t.Fatal(err)
	}
	if string(got.CorsConfigXML) != string(meta.CorsConfigXML) {
		t.Fatalf("CorsConfigXML not preserved: %q", string(got.CorsConfigXML))
	}
	if !got.CorsConfigUpdatedAt.Equal(meta.CorsConfigUpdatedAt) {
		t.Fatalf("CorsConfigUpdatedAt not preserved")
	}
}

// A persisted retired extension must not prevent the whole bucket's metadata
// from loading, including unrelated versioning and ordinary lifecycle rules.
func TestBucketMetadataRetiredAccessTiering(t *testing.T) {
	meta := newBucketMetadata("retired-access")
	meta.LifecycleConfigXML = []byte(`<LifecycleConfiguration><AccessTierQuota>500GiB</AccessTierQuota><Rule><ID>access</ID><Status>Enabled</Status><Filter><Prefix>logs/</Prefix></Filter><AccessTransition><Window>10m</Window><PromoteAfterAccesses>10</PromoteAfterAccesses></AccessTransition></Rule><Rule><ID>ordinary</ID><Status>Enabled</Status><Filter><Prefix>expired/</Prefix></Filter><Expiration><Days>30</Days></Expiration></Rule></LifecycleConfiguration>`)
	meta.VersioningConfigXML = []byte(`<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>`)
	data, err := meta.MarshalMsg(nil)
	if err != nil {
		t.Fatal(err)
	}
	got := newBucketMetadata(meta.Name)
	if _, err := got.UnmarshalMsg(data); err != nil {
		t.Fatal(err)
	}
	if err := got.parseAllConfigs(t.Context(), nil); err != nil {
		t.Fatalf("bucket metadata failed to load: %v", err)
	}
	if got.lifecycleConfig == nil || got.lifecycleConfig.HasActiveRules("logs/") || !got.lifecycleConfig.HasActiveRules("expired/") {
		t.Fatal("unexpected lifecycle behavior")
	}
	if got.versioningConfig == nil || !got.versioningConfig.Enabled() {
		t.Fatal("unrelated versioning lost")
	}
}
