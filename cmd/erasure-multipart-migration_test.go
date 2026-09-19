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

	"github.com/minio/minio/internal/config/api"
	"github.com/minio/minio/internal/config/storageclass"
	"github.com/minio/minio/internal/dsync"
	"github.com/minio/minio/internal/grid"
	xnet "github.com/pgsty/silo-pkg/v3/net"
)

func setMultipartListingTestMode(t *testing.T, legacy bool) {
	t.Helper()
	globalAPIConfig.mu.Lock()
	previous := globalAPIConfig.multipartListingStrict
	globalAPIConfig.multipartListingStrict = !legacy
	globalAPIConfig.mu.Unlock()
	t.Cleanup(func() {
		globalAPIConfig.mu.Lock()
		globalAPIConfig.multipartListingStrict = previous
		globalAPIConfig.mu.Unlock()
	})
}

func TestMultipartListingDefaultMode(t *testing.T) {
	var uninitialized apiConfig
	if !uninitialized.getMultipartListingLegacy() {
		t.Fatal("runtime zero value enabled strict mode before config initialization")
	}
	for _, mode := range []string{"", "legacy", "strict"} {
		var local apiConfig
		local.init(api.Config{RequestsMax: 1, MultipartListing: mode}, []int{4}, false)
		if local.getMultipartListingLegacy() != (mode != "strict") {
			t.Fatalf("unexpected mode for %q", mode)
		}
	}
}

func TestMultipartAbortLegacyAvailability(t *testing.T) {
	for _, drives := range []int{4, 16} {
		for _, offline := range []int{0, 1, drives / 2} {
			t.Run(fmt.Sprintf("drives=%d/offline=%d", drives, offline), func(t *testing.T) {
				obj, dirs, err := prepareErasure(t.Context(), drives)
				if err != nil {
					t.Fatal(err)
				}
				z := obj.(*erasureServerPools)
				t.Cleanup(func() { z.Shutdown(context.Background()); removeRoots(dirs) })
				setMultipartListingTestMode(t, true)
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

func TestMultipartAbortLegacyPoolOrder(t *testing.T) {
	for _, owner := range []int{0, 1} {
		t.Run(fmt.Sprintf("owner=%d", owner), func(t *testing.T) {
			z, bucket := consistencyPools(t)
			setMultipartListingTestMode(t, true)
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
				t.Fatalf("unrelated later pool blocked legacy cancellation: %v", err)
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

func TestMultipartAbortMinorityLegacyCleanup(t *testing.T) {
	for _, failDelete := range []bool{false, true} {
		t.Run(fmt.Sprintf("delete-fails=%v", failDelete), func(t *testing.T) {
			z, set, bucket := multipartListingFixture(t)
			mp, err := z.NewMultipartUpload(t.Context(), bucket, "old", ObjectOptions{})
			if err != nil {
				t.Fatal(err)
			}
			path := set.getUploadIDDir(bucket, "old", mp.UploadID)
			original := set.getDisks
			disks := original()
			fi, err := disks[0].ReadVersion(t.Context(), bucket, minioMetaMultipartBucket, path, "", ReadOptions{})
			if err != nil {
				t.Fatal(err)
			}
			delete(fi.Metadata, multipartMetaBucket)
			delete(fi.Metadata, multipartMetaObject)
			if err := disks[0].WriteMetadata(t.Context(), bucket, minioMetaMultipartBucket, path, fi); err != nil {
				t.Fatal(err)
			}
			for _, d := range disks[1:] {
				if err := d.Delete(t.Context(), minioMetaMultipartBucket, path, DeleteOptions{Recursive: true}); err != nil {
					t.Fatal(err)
				}
			}
			report, err := z.multipartPreflight(t.Context())
			if err != nil || report.Ready || report.LegacyUploads != 1 {
				t.Fatalf("invalid legacy fixture: %+v %v", report, err)
			}
			if failDelete {
				set.getDisks = func() []StorageAPI {
					wrapped := append([]StorageAPI(nil), disks...)
					wrapped[0] = multipartListingFaultDisk{StorageAPI: disks[0], delete: func(context.Context, string, string, DeleteOptions) error { return errFaultyDisk }}
					return wrapped
				}
				t.Cleanup(func() { set.getDisks = original })
				err := z.AbortMultipartUpload(t.Context(), bucket, "old", mp.UploadID, ObjectOptions{})
				if err == nil || toAPIError(t.Context(), err).HTTPStatusCode != 503 {
					t.Fatalf("empty drives masked failed deletion of the observed replica: %v", err)
				}
				if _, present := z.mpCache.Load(mp.UploadID); !present {
					t.Fatal("failed cancellation evicted upload from cache")
				}
				set.getDisks = original
			}
			if err := z.AbortMultipartUpload(t.Context(), bucket, "old", mp.UploadID, ObjectOptions{}); err != nil {
				t.Fatal(err)
			}
			report, err = z.multipartPreflight(t.Context())
			if err != nil || !report.Ready || report.LegacyUploads != 0 {
				t.Fatalf("acknowledged cleanup left online legacy remnants: %+v %v", report, err)
			}
			if _, present := z.mpCache.Load(mp.UploadID); present {
				t.Fatal("successful cancellation retained cache entry")
			}
		})
	}
}

func TestMultipartAbortWrongTargetKeepsCache(t *testing.T) {
	for _, legacy := range []bool{true, false} {
		t.Run(fmt.Sprintf("legacy=%v", legacy), func(t *testing.T) {
			z, _, bucket := multipartListingFixture(t)
			setMultipartListingTestMode(t, legacy)
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
		})
	}
}

func TestMultipartAbortRejectsUnsafeID(t *testing.T) {
	for _, legacy := range []bool{true, false} {
		t.Run(fmt.Sprintf("legacy=%v", legacy), func(t *testing.T) {
			z, _, bucket := multipartListingFixture(t)
			setMultipartListingTestMode(t, legacy)
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
		})
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
	for _, legacy := range []bool{true, false} {
		t.Run(fmt.Sprintf("legacy=%v", legacy), func(t *testing.T) {
			setMultipartListingTestMode(t, legacy)
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
		})
	}
}

func TestMultipartAbortStrictMajority(t *testing.T) {
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
		t.Fatalf("ordinary majority cancellation was tightened: %v", err)
	}
	// One failed deletion is tolerated in the ordinary majority path. Retrying
	// after recovery must also clean the now-minority remnant.
	if _, err := disks[0].ReadVersion(t.Context(), bucket, minioMetaMultipartBucket, set.getUploadIDDir(bucket, "a", mp.UploadID), "", ReadOptions{}); err != nil {
		t.Fatalf("fault fixture did not leave a remnant: %v", err)
	}
	set.getDisks = original
	if err := z.AbortMultipartUpload(t.Context(), bucket, "a", mp.UploadID, ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
}
