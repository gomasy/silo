// Copyright (c) 2026 Ruohang Feng
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/minio/minio/internal/config/storageclass"
	"github.com/minio/minio/internal/dsync"
	"github.com/minio/minio/internal/grid"
	xnet "github.com/pgsty/silo-pkg/v3/net"
)

func TestMultipartAbortDefaultAvailability(t *testing.T) {
	for _, drives := range []int{4, 16} {
		for _, offline := range []int{0, 1, drives / 2} {
			t.Run(fmt.Sprintf("drives=%d/offline=%d", drives, offline), func(t *testing.T) {
				obj, dirs, err := prepareErasure(t.Context(), drives)
				if err != nil {
					t.Fatal(err)
				}
				z := obj.(*erasureServerPools)
				t.Cleanup(func() { z.Shutdown(context.Background()); removeRoots(dirs) })

				previous := globalStorageClass
				globalStorageClass.Update(storageclass.Config{Standard: storageclass.StorageClass{Parity: drives / 2}})
				t.Cleanup(func() { globalStorageClass.Update(previous) })
				const bucket, key = "multipart-legacy-availability", "keep-available"
				if err := z.MakeBucket(t.Context(), bucket, MakeBucketOptions{}); err != nil {
					t.Fatal(err)
				}
				mp, err := z.NewMultipartUpload(t.Context(), bucket, key, ObjectOptions{})
				if err != nil {
					t.Fatal(err)
				}
				set := z.serverPools[0].getHashedSet(key)
				original := set.getDisks
				disks := original()
				set.getDisks = func() []StorageAPI {
					visible := append([]StorageAPI(nil), disks...)
					clear(visible[:offline])
					return visible
				}
				t.Cleanup(func() { set.getDisks = original })
				if err := z.AbortMultipartUpload(t.Context(), bucket, key, mp.UploadID, ObjectOptions{}); err != nil {
					t.Fatalf("released read-quorum availability regressed: %v", err)
				}
				for _, disk := range disks[offline:] {
					_, err := disk.ReadVersion(t.Context(), bucket, minioMetaMultipartBucket, set.getUploadIDDir(bucket, key, mp.UploadID), "", ReadOptions{})
					if !errors.Is(err, errFileNotFound) {
						t.Fatalf("online replica not cleaned: %v", err)
					}
				}
			})
		}
	}
}

func TestMultipartAbortDefaultPoolOrder(t *testing.T) {
	for _, owner := range []int{0, 1} {
		t.Run(fmt.Sprintf("owner=%d", owner), func(t *testing.T) {
			z, bucket := consistencyPools(t)

			mp, err := z.serverPools[owner].NewMultipartUpload(t.Context(), bucket, "a", ObjectOptions{})
			if err != nil {
				t.Fatal(err)
			}
			emptySet := z.serverPools[1-owner].getHashedSet("a")
			original := emptySet.getDisks
			t.Cleanup(func() { emptySet.getDisks = original })
			emptySet.getDisks = func() []StorageAPI { return make([]StorageAPI, emptySet.setDriveCount) }
			err = z.AbortMultipartUpload(t.Context(), bucket, "a", mp.UploadID, ObjectOptions{})
			if owner == 0 && err != nil {
				t.Fatalf("unrelated later pool blocked cancellation: %v", err)
			}
			if owner == 1 {
				if err == nil || toAPIError(t.Context(), err).HTTPStatusCode != 503 {
					t.Fatalf("unknown earlier pool must retain released error behavior: %v", err)
				}
				if _, err := z.serverPools[owner].GetMultipartInfo(t.Context(), bucket, "a", mp.UploadID, ObjectOptions{}); err != nil {
					t.Fatalf("later pool visited despite earlier error: %v", err)
				}
				emptySet.getDisks = original
				if err := z.AbortMultipartUpload(t.Context(), bucket, "a", mp.UploadID, ObjectOptions{}); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestMultipartAbortWrongTargetKeepsCache(t *testing.T) {
	z, _, bucket := multipartListingFixture(t)

	const other = "multipart-other-bucket"
	if err := z.MakeBucket(t.Context(), other, MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	mp, err := z.NewMultipartUpload(t.Context(), bucket, "valid-key", ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range [][2]string{{bucket, "wrong-key"}, {other, "valid-key"}} {
		err := z.AbortMultipartUpload(t.Context(), target[0], target[1], mp.UploadID, ObjectOptions{})
		var invalid InvalidUploadID
		if !errors.As(err, &invalid) {
			t.Fatalf("wrong target result: %v", err)
		}
		if _, present := z.mpCache.Load(mp.UploadID); !present {
			t.Fatal("wrong-target abort evicted another upload")
		}
		if _, err := z.GetMultipartInfo(t.Context(), bucket, "valid-key", mp.UploadID, ObjectOptions{}); err != nil {
			t.Fatalf("wrong-target abort harmed valid upload: %v", err)
		}
	}
	if err := z.AbortMultipartUpload(t.Context(), bucket, "valid-key", mp.UploadID, ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	var invalid InvalidUploadID
	if err := z.AbortMultipartUpload(t.Context(), bucket, "valid-key", mp.UploadID, ObjectOptions{}); !errors.As(err, &invalid) {
		t.Fatalf("repeat abort: %v", err)
	}
}

func TestMultipartAbortRejectsUnsafeID(t *testing.T) {
	z, _, bucket := multipartListingFixture(t)

	mp, err := z.NewMultipartUpload(t.Context(), bucket, "keep", ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{".", "..", "../other", "a/b", "a\\b", ""} {
		id := base64.RawURLEncoding.EncodeToString([]byte("deployment." + suffix))
		err := z.AbortMultipartUpload(t.Context(), bucket, "keep", id, ObjectOptions{})
		var invalid InvalidUploadID
		if !errors.As(err, &invalid) {
			t.Fatalf("unsafe ID accepted: suffix=%q err=%v", suffix, err)
		}
		if _, err := z.GetMultipartInfo(t.Context(), bucket, "keep", mp.UploadID, ObjectOptions{}); err != nil {
			t.Fatalf("valid upload harmed by rejected ID: %v", err)
		}
	}
}

func TestMultipartAbortPeerNotification(t *testing.T) {
	z, set, bucket := multipartListingFixture(t)
	tg, err := grid.SetupTestGrid(2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tg.Cleanup)
	var notifications atomic.Int32
	if err := cleanupUploadIDCacheMetaRPC.Register(tg.Managers[1], func(*grid.MSS) (grid.NoPayload, *grid.RemoteErr) {
		notifications.Add(1)
		return grid.NoPayload{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	host, err := xnet.ParseHost(strings.TrimPrefix(tg.Hosts[1], "http://"))
	if err != nil {
		t.Fatal(err)
	}
	previous := globalNotificationSys
	globalNotificationSys = &NotificationSys{peerClients: []*peerRESTClient{{
		host:     host,
		gridConn: func() *grid.Connection { return tg.Managers[0].Connection(tg.Hosts[1]) },
	}}}
	t.Cleanup(func() { globalNotificationSys = previous })
	// Use the real distributed lock implementation: Unlock cancels its derived
	// context. Local locks do not, and would hide a broken notification context.
	oldMutex, oldLockers := set.nsMutex, set.getLockers
	lockers := []dsync.NetLocker{newLocker(), newLocker()}
	set.nsMutex = newNSLock(true)
	set.getLockers = func() ([]dsync.NetLocker, string) { return lockers, "multipart-test" }
	t.Cleanup(func() { set.nsMutex, set.getLockers = oldMutex, oldLockers })

	for range 4 {
		mp, err := z.NewMultipartUpload(t.Context(), bucket, "valid", ObjectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		before := notifications.Load()
		var invalid InvalidUploadID
		if err := z.AbortMultipartUpload(t.Context(), bucket, "wrong", mp.UploadID, ObjectOptions{}); !errors.As(err, &invalid) {
			t.Fatalf("wrong target: %v", err)
		}
		if notifications.Load() != before {
			t.Fatal("wrong-target abort sent a destructive peer notification")
		}
		if err := z.AbortMultipartUpload(t.Context(), bucket, "valid", mp.UploadID, ObjectOptions{}); err != nil {
			t.Fatal(err)
		}
		if notifications.Load() != before+1 {
			t.Fatal("successful abort lost peer notification after unlocking")
		}
	}
}

func TestMultipartAbortBestEffortCleanup(t *testing.T) {
	z, set, bucket := multipartListingFixture(t)
	mp, err := z.NewMultipartUpload(t.Context(), bucket, "a", ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	original := set.getDisks
	disks := original()
	set.getDisks = func() []StorageAPI {
		wrapped := append([]StorageAPI(nil), disks...)
		wrapped[0] = multipartListingFaultDisk{StorageAPI: disks[0], delete: func(context.Context, string, string, DeleteOptions) error { return errFaultyDisk }}
		return wrapped
	}
	t.Cleanup(func() { set.getDisks = original })
	if err := z.AbortMultipartUpload(t.Context(), bucket, "a", mp.UploadID, ObjectOptions{}); err != nil {
		t.Fatalf("read-quorum cancellation was tightened: %v", err)
	}
	// Best-effort deletion may leave a remnant. Retrying after recovery cleans
	// it, but the read quorum no longer recognizes it as an active upload.
	if _, err := disks[0].ReadVersion(t.Context(), bucket, minioMetaMultipartBucket, set.getUploadIDDir(bucket, "a", mp.UploadID), "", ReadOptions{}); err != nil {
		t.Fatalf("fault fixture did not leave a remnant: %v", err)
	}
	set.getDisks = original
	var invalid InvalidUploadID
	if err := z.AbortMultipartUpload(t.Context(), bucket, "a", mp.UploadID, ObjectOptions{}); !errors.As(err, &invalid) {
		t.Fatalf("minority retry must preserve default absent-upload result: %v", err)
	}
	if _, err := disks[0].ReadVersion(t.Context(), bucket, minioMetaMultipartBucket, set.getUploadIDDir(bucket, "a", mp.UploadID), "", ReadOptions{}); !errors.Is(err, errFileNotFound) {
		t.Fatalf("best-effort retry did not clean remnant: %v", err)
	}
}
