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
	"io"
	"maps"
	"net/http"
	"testing"
	"time"

	"github.com/minio/minio/internal/bucket/replication"
)

func TestDeleteMarkerReadDataControls(t *testing.T) {
	obj, er, disks, bucket := markerPurgeFixture(t, 4)
	for _, body := range []string{"", "inline-data-control"} {
		name := "data-" + mustGetUUID()
		oi, err := er.PutObject(t.Context(), bucket, name, mustGetPutObjReader(t, bytes.NewBufferString(body), int64(len(body)), "", ""), ObjectOptions{Versioned: true})
		if err != nil {
			t.Fatal(err)
		}
		for _, disk := range disks {
			fi, err := disk.ReadVersion(t.Context(), "", bucket, name, oi.VersionID, ReadOptions{ReadData: true})
			if err != nil || fi.Deleted || fi.Size != int64(len(body)) || !fi.InlineData() || (body != "" && len(fi.Data) == 0) {
				t.Fatalf("data read: size=%d inline=%v bytes=%d error=%v", fi.Size, fi.InlineData(), len(fi.Data), err)
			}
			// Legacy inline data without its annotation still gets the original
			// ReadData behavior; the new marker guard must not affect it.
			delete(fi.Metadata, ReservedMetadataPrefixLower+"inline-data")
			if err := disk.WriteMetadata(t.Context(), "", bucket, name, fi); err != nil {
				t.Fatal(err)
			}
			fi, err = disk.ReadVersion(t.Context(), "", bucket, name, oi.VersionID, ReadOptions{ReadData: true})
			if err != nil || !fi.InlineData() {
				t.Fatalf("legacy inline read: %+v error=%v", fi, err)
			}
		}
		reader, err := obj.GetObjectNInfo(t.Context(), bucket, name, nil, http.Header{}, ObjectOptions{VersionID: oi.VersionID})
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(reader)
		reader.Close()
		if err != nil || string(got) != body {
			t.Fatalf("payload changed: %q error=%v", got, err)
		}
	}
	_, opts := seedPurgeMarker(t, er, bucket, "stored-marker-key", false)
	for _, disk := range disks {
		fi, err := disk.ReadVersion(t.Context(), "", bucket, "stored-marker-key", opts.VersionID, ReadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		// Preserve even an existing unusual marker key; suppress only the
		// manufacture of a new inline annotation by the data-read branch.
		fi.Metadata[ReservedMetadataPrefixLower+"inline-data"] = "original"
		if err := disk.WriteMetadata(t.Context(), "", bucket, "stored-marker-key", fi); err != nil {
			t.Fatal(err)
		}
		fi, err = disk.ReadVersion(t.Context(), "", bucket, "stored-marker-key", opts.VersionID, ReadOptions{ReadData: true})
		if err != nil || fi.Metadata[ReservedMetadataPrefixLower+"inline-data"] != "original" {
			t.Errorf("existing marker key changed: metadata=%v error=%v", fi.Metadata, err)
		}
	}
}

func TestDeleteMarkerMetadataRoundTrip(t *testing.T) {
	stamp := time.Date(2026, 9, 1, 12, 1, 2, 345, time.UTC)
	for _, free := range []bool{false, true} {
		t.Run(map[bool]string{false: "replication", true: "free-version"}[free], func(t *testing.T) {
			metadata := map[string]string{
				ReservedMetadataPrefixLower + ReplicaStatus:        "REPLICA",
				ReservedMetadataPrefixLower + ReplicaTimestamp:     stamp.Format(time.RFC3339Nano),
				ReservedMetadataPrefixLower + ReplicationStatus:    "arn1=COMPLETED;arn2=FAILED;",
				ReservedMetadataPrefixLower + ReplicationTimestamp: stamp.Add(-time.Hour).Format(time.RFC3339Nano),
				VersionPurgeStatusKey:                              "arn1=PENDING;arn2=FAILED;",
				targetResetHeader("arn1"):                          "original;reset1",
				targetResetHeader("arn2"):                          "original;reset2",
				ReservedMetadataPrefixLower + "unknown":            "",
			}
			if free {
				metadata[ReservedMetadataPrefixLower+freeVersion] = ""
				metadata[metaTierName] = "tier"
				metadata[metaTierObjName] = "remote-object"
				metadata[metaTierVersionID] = "remote-version"
			}
			fi := FileInfo{VersionID: mustGetUUID(), Deleted: true, ModTime: stamp, Metadata: maps.Clone(metadata)}
			// Deliberately conflicting parsed state must never replace stored
			// values or add a key missing from the raw metadata.
			fi.ReplicationState = ReplicationState{ReplicaStatus: replication.Failed, ReplicaTimeStamp: stamp.Add(time.Hour), ResetStatusesMap: map[string]string{"new-arn": "invented"}}
			fi.SetHealing()
			fi.SetDataMov()
			fi.SetTierFreeVersionID(mustGetUUID())
			fi.SetTierFreeVersion()
			fi.SetSkipTierFreeVersion()
			xl := xlMetaV2{}
			if err := xl.AddVersion(fi); err != nil {
				t.Fatal(err)
			}
			stored, err := xl.getIdx(0)
			if err != nil {
				t.Fatal(err)
			}
			got := make(map[string]string)
			for k, v := range stored.DeleteMarker.MetaSys {
				got[k] = string(v)
			}
			if !maps.Equal(got, metadata) {
				t.Errorf("stored metadata=%v want=%v", got, metadata)
			}
			decoded, err := xl.ToFileInfo("bucket", "marker", fi.VersionID, true, true)
			if err != nil || !decoded.ModTime.Equal(fi.ModTime) || decoded.TierFreeVersion() != free {
				t.Errorf("round trip identity: %+v error=%v", decoded, err)
			}
			if decoded.ReplicationState.ReplicaStatus != replication.Replica || decoded.ReplicationState.PurgeTargets["arn2"] != replication.VersionPurgeFailed || decoded.ReplicationState.Targets["arn1"] != replication.Completed {
				t.Errorf("lost parsed replication state: %+v", decoded.ReplicationState)
			}
		})
	}
}

func TestDeleteMarkerCreationMetadata(t *testing.T) {
	_, er, disks, bucket := markerPurgeFixture(t, 4)
	_, opts := seedPurgeMarker(t, er, bucket, "empty-key", false)
	for i, disk := range disks {
		fi, err := disk.ReadVersion(t.Context(), "", bucket, "empty-key", opts.VersionID, ReadOptions{})
		if err != nil || fi.Metadata[ReservedMetadataPrefixLower+ReplicaStatus] != "REPLICA" || fi.Metadata[ReservedMetadataPrefixLower+ReplicaTimestamp] != opts.DeleteReplication.ReplicaTimeStamp.Format(time.RFC3339Nano) {
			t.Errorf("disk %d lost new marker replica identity: metadata=%v error=%v", i, fi.Metadata, err)
		}
	}
}
