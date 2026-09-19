// Copyright (c) 2026 Feng Ruohang
//
// This file is part of Silo Object Storage stack
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
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

// Read a real pre-removal scanner cache with nonzero hot-tier bytes. A v8
// payload with a relabeled header would not exercise the ignored hts field.
func TestDataUsageCacheReadV9(t *testing.T) {
	raw, err := os.ReadFile("testdata/data-usage-v9/data-usage-v9.bin")
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) == 0 || raw[0] != 9 {
		t.Fatal("fixture is not v9")
	}
	expected, err := os.ReadFile("testdata/data-usage-v9/data-usage-v9.json")
	if err != nil {
		t.Fatal(err)
	}
	var want, got dataUsageCache
	if err := json.Unmarshal(expected, &want); err != nil {
		t.Fatal(err)
	}
	if err := got.deserialize(bytes.NewReader(raw)); err != nil {
		t.Fatal(err)
	}
	if !got.Info.LastUpdate.Equal(want.Info.LastUpdate) {
		t.Fatal("cache timestamp changed")
	}
	// msgp restores local time while JSON preserves the UTC representation.
	want.Info.LastUpdate = got.Info.LastUpdate
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("v9 ordinary cache fields changed:\ngot: %#v\nwant: %#v", got, want)
	}
	flat := got.flatten(*got.root())
	if flat.Size != 74962 || flat.Objects != 3 || flat.Versions != 5 || flat.DeleteMarkers != 2 {
		t.Fatalf("statistics changed: %+v", flat)
	}
	if flat.AllTierStats == nil || flat.AllTierStats.Tiers["COLD"] != (tierStats{TotalSize: 65536, NumVersions: 1, NumObjects: 1}) {
		t.Fatalf("remote tier statistics changed: %+v", flat.AllTierStats)
	}
	buckets := []BucketInfo{{Name: "v9-bucket"}}
	if gotInfo, wantInfo := got.dui(dataUsageRoot, buckets), want.dui(dataUsageRoot, buckets); !reflect.DeepEqual(gotInfo, wantInfo) {
		t.Fatalf("bucket usage aggregation changed: %+v != %+v", gotInfo, wantInfo)
	}
	var buf bytes.Buffer
	if err := got.serializeTo(&buf); err != nil {
		t.Fatal(err)
	}
	if buf.Bytes()[0] != 8 {
		t.Fatalf("wrote cache version %d, want 8", buf.Bytes()[0])
	}
	var roundtrip dataUsageCache
	if err := roundtrip.deserialize(&buf); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, roundtrip) {
		t.Fatal("v8 round trip lost ordinary fields")
	}
	if err := roundtrip.deserialize(bytes.NewReader(raw[:len(raw)/2])); err == nil {
		t.Fatal("truncated v9 cache accepted")
	}
	raw[0] = 10
	if err := roundtrip.deserialize(bytes.NewReader(raw)); err == nil {
		t.Fatal("unknown cache version accepted")
	}
}
