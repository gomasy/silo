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
	"context"

	"github.com/google/uuid"
	"github.com/minio/minio/internal/bucket/replication"
)

// isVersionPurge distinguishes physical removal from marker creation and
// replication-state updates. Call it again after metadata callbacks, which
// can change an ordinary deletion into a pending purge update.
func (o ObjectOptions) isVersionPurge() bool {
	if o.VersionID == "" || o.VersionID == nullVersionID || o.DeleteMarker ||
		o.DeletePrefix || o.DataMovement || o.InclFreeVersions || o.Expiration.Expire ||
		o.Transition != (TransitionOptions{}) {
		return false
	}
	id, err := uuid.Parse(o.VersionID)
	if err != nil || id == uuid.Nil {
		return false
	}
	purge := o.VersionPurgeStatus()
	if purge == replication.VersionPurgeComplete {
		return true
	}
	if !purge.Empty() || o.DeleteReplication.VersionPurgeStatusInternal != "" {
		return false
	}
	status := o.DeleteMarkerReplicationStatus()
	return (status.Empty() && o.DeleteReplication.ReplicationStatusInternal == "") ||
		(status == replication.Replica && o.ReplicationRequest)
}

// confirmVersionAbsent is a read-only proof for an already-missing purge.
// Read quorum (or a synthesized NotFound) cannot acknowledge a write. Do not
// delete here: unreadable minority copies may carry retention we cannot check.
// The caller holds the object lock. This function never heals or enqueues work.
func (er erasureObjects) confirmVersionAbsent(ctx context.Context, bucket, object, versionID string) (allAbsent bool, err error) {
	disks := er.getDisks()
	_, errs := readAllFileInfo(ctx, disks, "", bucket, object, versionID, false, false)
	absent := 0
	for _, err := range errs {
		if err == errFileNotFound || err == errFileVersionNotFound {
			absent++
		}
	}
	if absent < len(disks)/2+1 {
		return false, InsufficientWriteQuorum{}
	}
	return absent == len(disks), nil
}

// checkPurgeAbsent schedules recovery explicitly, outside the read-only proof.
func (er erasureObjects) checkPurgeAbsent(ctx context.Context, bucket, object, versionID string) error {
	allAbsent, err := er.confirmVersionAbsent(ctx, bucket, object, versionID)
	if !allAbsent {
		er.addPartial(bucket, object, versionID)
	}
	return err
}

func (z *erasureServerPools) checkPurgeAbsent(ctx context.Context, bucket, object, versionID string) error {
	for _, pool := range z.serverPools {
		if err := pool.getHashedSet(object).checkPurgeAbsent(ctx, bucket, object, versionID); err != nil {
			return err
		}
	}
	return nil
}
