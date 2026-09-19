// Copyright (c) 2026 mr javad seydi and Ruohang Feng
//
// This file is part of Silo Object Storage stack.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pgsty/silo-pkg/v3/policy"
)

const multipartScanEntryLimit = 100_000

var (
	multipartScanSlots          = make(chan struct{}, 2)
	errMultipartListingLegacy   = errors.New("legacy multipart uploads require a coordinated upgrade and drain")
	errMultipartListingIdentity = errors.New("multipart upload identity is invalid")
)

// One budget and admission slot cover the entire request, including all pools.
// A slot is released only after the scan workers have actually stopped.
type multipartScan struct {
	ctx           context.Context
	cancel        context.CancelFunc
	remaining     atomic.Int64
	metadataSlots chan struct{}
	preflight     bool
	sets          []multipartScanSet
}

type multipartScanSet struct {
	Pool            int       `json:"pool"`
	Set             int       `json:"set"`
	Drives          int       `json:"drives"`
	ScannedDrives   int       `json:"scannedDrives"`
	UncoveredDrives []int     `json:"uncoveredDrives,omitempty"`
	Candidates      int       `json:"candidates"`
	LegacyUploads   int       `json:"legacyUploads"`
	OldestLegacy    time.Time `json:"oldestLegacy,omitempty"`
	Error           string    `json:"error,omitempty"`
}

type multipartPreflightReport struct {
	Ready          bool               `json:"ready"`
	Complete       bool               `json:"complete"`
	Mode           string             `json:"mode"`
	ScannedEntries int64              `json:"scannedEntries"`
	LegacyUploads  int                `json:"legacyUploads"`
	Sets           []multipartScanSet `json:"sets"`
}

func startMultipartScan(ctx context.Context, preflight bool) (*multipartScan, error) {
	select {
	case multipartScanSlots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return nil, SlowDown{}
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	s := &multipartScan{ctx: ctx, cancel: cancel, preflight: preflight, metadataSlots: make(chan struct{}, multipartMetadataScanConcurrency)}
	s.remaining.Store(multipartScanEntryLimit)
	return s, nil
}

func (s *multipartScan) close() {
	s.cancel()
	<-multipartScanSlots
}

func (s *multipartScan) listDir(disk StorageAPI, bucket, dir string) ([]string, error) {
	if err := s.ctx.Err(); err != nil {
		return nil, SlowDown{}
	}
	remaining := s.remaining.Load()
	if remaining <= 0 {
		return nil, SlowDown{}
	}
	// Request one extra entry to detect overflow, never a silently partial page.
	entries, err := disk.ListDir(s.ctx, bucket, minioMetaMultipartBucket, dir, int(remaining)+1)
	if errors.Is(err, errFileNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if s.remaining.Add(-int64(len(entries))) < 0 {
		return nil, SlowDown{}
	}
	return entries, nil
}

func (s *multipartScan) listUploadDirs(disk StorageAPI, bucket string) ([]string, error) {
	hashDirs, err := s.listDir(disk, bucket, "")
	if err != nil {
		return nil, err
	}
	var candidates []string
	for _, hashDir := range hashDirs {
		if !strings.HasSuffix(hashDir, SlashSeparator) {
			continue
		}
		hashDir = strings.TrimSuffix(hashDir, SlashSeparator)
		uploadDirs, err := s.listDir(disk, bucket, hashDir)
		if err != nil {
			return nil, err
		}
		for _, uploadDir := range uploadDirs {
			if strings.HasSuffix(uploadDir, SlashSeparator) {
				candidates = append(candidates, pathJoin(hashDir, strings.TrimSuffix(uploadDir, SlashSeparator)))
			}
		}
	}
	return candidates, nil
}

// Identity is immutable at hash/uploadUUID. Check the hash before using even
// a single source drive to exclude another bucket. Missing/bad identities must
// fall back to the full metadata read; they cannot prove absence.
func (er erasureObjects) multipartIdentity(fi FileInfo, shaDir string) (string, string, bool) {
	bucket, object := fi.Metadata[multipartMetaBucket], fi.Metadata[multipartMetaObject]
	return bucket, object, bucket != "" && object != "" && IsValidBucketName(bucket) &&
		IsValidObjectPrefix(object) && er.getMultipartSHADir(bucket, object) == shaDir
}

func (er erasureObjects) readMultipartUploadCandidate(s *multipartScan, bucket, candidate string, source StorageAPI) (MultipartInfo, bool, bool, error) {
	if s.ctx.Err() != nil {
		return MultipartInfo{}, false, false, SlowDown{}
	}
	shaDir, uploadUUID, ok := strings.Cut(candidate, SlashSeparator)
	if !ok || shaDir == "" || uploadUUID == "" || strings.Contains(uploadUUID, SlashSeparator) {
		return MultipartInfo{}, false, false, errMultipartListingIdentity
	}
	if !s.preflight && bucket != "" && source != nil {
		fi, err := source.ReadVersion(s.ctx, bucket, minioMetaMultipartBucket, candidate, "", ReadOptions{})
		if err == nil {
			storedBucket, _, valid := er.multipartIdentity(fi, shaDir)
			if valid && storedBucket != bucket {
				return MultipartInfo{}, false, false, nil
			}
		}
	}
	if s.ctx.Err() != nil {
		return MultipartInfo{}, false, false, SlowDown{}
	}
	select {
	case s.metadataSlots <- struct{}{}:
		defer func() { <-s.metadataSlots }()
	case <-s.ctx.Done():
		return MultipartInfo{}, false, false, SlowDown{}
	}
	disks := er.getDisks()
	metadata, errs := readAllFileInfo(s.ctx, disks, bucket, minioMetaMultipartBucket, candidate, "", false, false)
	if s.preflight {
		// Readiness is stronger than listing liveness: even one readable legacy
		// copy must be drained, and an unreadable copy cannot certify readiness.
		var oldest MultipartInfo
		legacy := false
		_, nativeID := multipartUploadTime(uploadUUID)
		for i, err := range errs {
			if errors.Is(err, errFileNotFound) || errors.Is(err, errFileVersionNotFound) {
				continue
			}
			if err != nil {
				return MultipartInfo{}, false, false, err
			}
			fi := metadata[i]
			storedBucket, storedObject, valid := er.multipartIdentity(fi, shaDir)
			if storedBucket != "" && storedObject != "" && !valid {
				return MultipartInfo{}, false, false, errMultipartListingIdentity
			}
			if !valid || !nativeID {
				info := multipartUploadInfo(storedBucket, storedObject, uploadUUID, fi.ModTime)
				if !legacy || info.Initiated.Before(oldest.Initiated) {
					oldest = info
				}
				legacy = true
			}
		}
		if legacy {
			return oldest, false, true, nil
		}
	}
	readQuorum, _, err := objectQuorumFromMeta(s.ctx, metadata, errs, er.defaultParityCount)
	if err != nil {
		return MultipartInfo{}, false, false, err
	}
	_, modTime, etag := listOnlineDisks(disks, metadata, errs, readQuorum)
	if err := reduceReadQuorumErrs(s.ctx, errs, objectOpIgnoredErrs, readQuorum); err != nil {
		return MultipartInfo{}, false, false, err
	}
	fi, err := pickValidFileInfo(s.ctx, metadata, modTime, etag, readQuorum)
	if err != nil {
		return MultipartInfo{}, false, false, err
	}
	storedBucket, storedObject, valid := er.multipartIdentity(fi, shaDir)
	info := multipartUploadInfo(storedBucket, storedObject, uploadUUID, fi.ModTime)
	if storedBucket == "" || storedObject == "" {
		return info, false, true, nil
	}
	if !valid {
		return MultipartInfo{}, false, false, fmt.Errorf("%w: %s", errMultipartListingIdentity, candidate)
	}
	if _, ok := multipartUploadTime(uploadUUID); !ok {
		return info, false, true, nil
	}
	if bucket != "" && bucket != storedBucket {
		return MultipartInfo{}, false, false, nil
	}
	return info, true, false, nil
}

func (er erasureObjects) scanMultipartUploads(s *multipartScan, bucket string, poolIdx, setIdx int) ([]MultipartInfo, bool, error) {
	disks := er.getDisks()
	report := multipartScanSet{Pool: poolIdx, Set: setIdx, Drives: er.setDriveCount}
	var resultErr error
	defer func() {
		if resultErr != nil {
			report.Error = resultErr.Error()
		}
		s.sets = append(s.sets, report)
	}()
	var candidateMu sync.Mutex
	candidates := make(map[string]StorageAPI)
	errs := make([]error, len(disks))
	var wg sync.WaitGroup
	for i, disk := range disks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if disk == nil || !disk.IsOnline() {
				errs[i] = errDiskNotFound
				return
			}
			paths, err := s.listUploadDirs(disk, bucket)
			errs[i] = err
			if err != nil {
				return
			}
			candidateMu.Lock()
			defer candidateMu.Unlock()
			for _, p := range paths {
				candidates[p] = disk
			}
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err == nil {
			report.ScannedDrives++
		} else {
			report.UncoveredDrives = append(report.UncoveredDrives, i)
			if _, limited := err.(SlowDown); limited {
				resultErr = err
			}
		}
	}
	report.Candidates = len(candidates)
	// A successful scan must intersect every metadata read quorum, including
	// records left with R copies by a partially failed cancellation.
	if report.ScannedDrives < er.setDriveCount/2+1 && resultErr == nil {
		resultErr = toObjectErr(errErasureReadQuorum, bucket)
	}
	if resultErr != nil {
		return nil, false, resultErr
	}
	type candidate struct {
		path   string
		source StorageAPI
	}
	jobs := make(chan candidate)
	var mu sync.Mutex
	var uploads []MultipartInfo
	for range min(16, len(candidates)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range jobs {
				mu.Lock()
				stopped := resultErr != nil
				mu.Unlock()
				if stopped || s.ctx.Err() != nil {
					continue
				}
				upload, found, legacy, err := er.readMultipartUploadCandidate(s, bucket, c.path, c.source)
				if errors.Is(err, errFileNotFound) || errors.Is(err, errFileVersionNotFound) {
					continue
				}
				mu.Lock()
				switch {
				case err != nil && resultErr == nil:
					resultErr = err
				case legacy:
					report.LegacyUploads++
					if report.OldestLegacy.IsZero() || upload.Initiated.Before(report.OldestLegacy) {
						report.OldestLegacy = upload.Initiated
					}
				case found && !s.preflight:
					uploads = append(uploads, upload)
				}
				mu.Unlock()
			}
		}()
	}
dispatch:
	for p, source := range candidates {
		select {
		case jobs <- candidate{p, source}:
		case <-s.ctx.Done():
			break dispatch
		}
	}
	close(jobs)
	wg.Wait()
	if s.ctx.Err() != nil {
		resultErr = SlowDown{}
	}
	return uploads, report.LegacyUploads != 0, resultErr
}

// multipartPreflight scans all pools, sets and drives, independent of caches.
// A majority suffices for normal listing; upgrade readiness requires every
// drive to have been inspected. The operator must also upgrade all writers.
func (z *erasureServerPools) multipartPreflight(ctx context.Context) (multipartPreflightReport, error) {
	s, err := startMultipartScan(ctx, true)
	if err != nil {
		return multipartPreflightReport{}, err
	}
	defer s.close()
	report := multipartPreflightReport{Complete: true, Mode: "strict"}
	if globalAPIConfig.getMultipartListingLegacy() {
		report.Mode = "legacy"
	}
	for p, pool := range z.serverPools {
		for i, set := range pool.sets {
			_, _, _ = set.scanMultipartUploads(s, "", p, i)
		}
	}
	report.Sets = s.sets
	for _, set := range s.sets {
		report.LegacyUploads += set.LegacyUploads
		if set.Error != "" || set.ScannedDrives != set.Drives {
			report.Complete = false
		}
	}
	report.ScannedEntries = multipartScanEntryLimit - s.remaining.Load()
	report.Ready = report.Complete && report.LegacyUploads == 0
	return report, nil
}

// MultipartPreflightHandler is a read-only storage-admin diagnostic. It never
// accepts an arbitrary deletion path or changes upload lifetime settings.
func (a adminAPIHandlers) MultipartPreflightHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	obj, _ := validateAdminReq(ctx, w, r, policy.StorageInfoAdminAction)
	if obj == nil {
		return
	}
	z, ok := obj.(*erasureServerPools)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}
	report, err := z.multipartPreflight(ctx)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	b, err := json.Marshal(report)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	writeSuccessResponseJSON(w, b)
}
