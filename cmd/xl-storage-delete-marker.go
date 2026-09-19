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
	"strings"
	"time"
)

// deleteMarkerMetadata preserves the complete stored marker state during
// healing. The parsed ReplicationState is lossy; even absent raw keys must not
// be reconstructed from it. Only creation writers without stored metadata
// use typed state, and never invent timestamps.
func deleteMarkerMetadata(fi FileInfo) map[string][]byte {
	meta := make(map[string][]byte, len(fi.Metadata))
	for k, v := range fi.Metadata {
		switch k {
		case xMinIOHealing, xMinIODataMov,
			ReservedMetadataPrefixLower + tierFVID,
			ReservedMetadataPrefixLower + tierFVMarker,
			ReservedMetadataPrefixLower + tierSkipFVID:
			continue
		}
		meta[k] = []byte(v)
	}
	if len(meta) != 0 || fi.Healing() || fi.DataMov() {
		return meta
	}
	rs := fi.ReplicationState
	if !rs.ReplicaStatus.Empty() {
		meta[ReservedMetadataPrefixLower+ReplicaStatus] = []byte(rs.ReplicaStatus)
		if !rs.ReplicaTimeStamp.IsZero() {
			meta[ReservedMetadataPrefixLower+ReplicaTimestamp] = []byte(rs.ReplicaTimeStamp.UTC().Format(time.RFC3339Nano))
		}
	}
	if rs.ReplicationStatusInternal != "" {
		meta[ReservedMetadataPrefixLower+ReplicationStatus] = []byte(rs.ReplicationStatusInternal)
		if !rs.ReplicationTimeStamp.IsZero() {
			meta[ReservedMetadataPrefixLower+ReplicationTimestamp] = []byte(rs.ReplicationTimeStamp.UTC().Format(time.RFC3339Nano))
		}
	}
	if rs.VersionPurgeStatusInternal != "" {
		meta[VersionPurgeStatusKey] = []byte(rs.VersionPurgeStatusInternal)
	}
	for k, v := range rs.ResetStatusesMap {
		if !strings.HasPrefix(k, ReservedMetadataPrefixLower+ReplicationReset) {
			k = targetResetHeader(k)
		}
		meta[k] = []byte(v)
	}
	return meta
}
