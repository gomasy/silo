// Copyright (c) 2015-2021 MinIO, Inc.
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

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestSysErrors(t *testing.T) {
	pathErr := &os.PathError{Err: syscall.ENAMETOOLONG}
	ok := isSysErrTooLong(pathErr)
	if !ok {
		t.Fatalf("Unexpected error expecting %s", syscall.ENAMETOOLONG)
	}
	pathErr = &os.PathError{Err: syscall.ENOTDIR}
	ok = isSysErrNotDir(pathErr)
	if !ok {
		t.Fatalf("Unexpected error expecting %s", syscall.ENOTDIR)
	}
	if runtime.GOOS != globalWindowsOSName {
		pathErr = &os.PathError{Err: syscall.ENOTEMPTY}
		ok = isSysErrNotEmpty(pathErr)
		if !ok {
			t.Fatalf("Unexpected error expecting %s", syscall.ENOTEMPTY)
		}
	} else {
		pathErr = &os.PathError{Err: syscall.Errno(0x91)}
		ok = isSysErrNotEmpty(pathErr)
		if !ok {
			t.Fatal("Unexpected error expecting 0x91")
		}
	}
	if runtime.GOOS == globalWindowsOSName {
		pathErr = &os.PathError{Err: syscall.Errno(0x03)}
		ok = isSysErrPathNotFound(pathErr)
		if !ok {
			t.Fatal("Unexpected error expecting 0x03")
		}
	}
}

// TestCommitMetaMissingMapping verifies the RPC and object-layer mapping
// of errCommitMetaMissing, returned when the staged xl.meta disappears
// between the data rename and the meta-file commit in RenameData().
func TestCommitMetaMissingMapping(t *testing.T) {
	// The sentinel must survive a storage REST/grid round-trip; errors
	// cross the wire as strings and are remapped by toStorageErr().
	if got := toStorageErr(errors.New(errCommitMetaMissing.Error())); !errors.Is(got, errCommitMetaMissing) {
		t.Fatalf("expected %s, got %s", errCommitMetaMissing, got)
	}
	if got := toStorageErr(errCommitMetaMissing); got != errCommitMetaMissing {
		t.Fatalf("expected %s, got %s", errCommitMetaMissing, got)
	}

	// toObjectErr() must map the sentinel to InsufficientWriteQuorum
	// (503), never to ObjectNotFound (404).
	err := toObjectErr(errCommitMetaMissing, "bucket", "object")
	qerr, ok := err.(InsufficientWriteQuorum)
	if !ok {
		t.Fatalf("expected InsufficientWriteQuorum, got %s (%T)", err, err)
	}
	if qerr.Bucket != "bucket" || qerr.Object != "object" {
		t.Fatalf("unexpected bucket/object %s/%s", qerr.Bucket, qerr.Object)
	}
	apiErr := toAPIError(t.Context(), err)
	if apiErr.Code != "SlowDownWrite" || apiErr.HTTPStatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected SlowDownWrite/503, got %+v", apiErr)
	}
}

// missingCommitMetaContext removes staged metadata at an existing cancellation
// check, after WriteAll and (for non-inline objects) the data rename succeeded.
// This deterministically models concurrent cleanup without changing production
// code or relying on a filesystem race. The context itself remains live.
type missingCommitMetaContext struct {
	context.Context
	mu             sync.Mutex
	meta, dataDest string
	removed        bool
	removeErr      error
}

func (c *missingCommitMetaContext) Done() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.removed {
		if _, err := os.Stat(c.meta); err == nil {
			dataMoved := c.dataDest == ""
			if !dataMoved {
				_, err = os.Stat(c.dataDest)
				dataMoved = err == nil
			}
			if dataMoved {
				c.removeErr = os.Remove(c.meta)
				c.removed = c.removeErr == nil
			}
		}
	}
	return c.Context.Done()
}

func TestXLStorageRenameDataCommitMetaMissing(t *testing.T) {
	for _, tc := range []struct {
		name     string
		dataFile bool
	}{
		{"empty_object", false},
		{"data_directory", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			disk, root, err := newXLStorageTestSetup(t)
			if err != nil {
				t.Fatal(err)
			}
			defer disk.Close()
			s := disk.storage
			if err := s.MakeVol(t.Context(), "bucket"); err != nil {
				t.Fatal(err)
			}
			fi := newFileInfo("object", 2, 2)
			fi.DataDir, fi.VersionID = mustGetUUID(), mustGetUUID()
			fi.Erasure.Index = 1
			fi.ModTime = time.Now().UTC()
			fi.Fresh = true
			ctx := &missingCommitMetaContext{
				Context: t.Context(),
				meta:    filepath.Join(root, minioMetaTmpBucket, "staged", xlStorageFormatFile),
			}
			if tc.dataFile {
				fi.Size = 1
				fi.AddObjectPart(1, "", 1, 1, fi.ModTime, nil, nil)
				part := filepath.Join(root, minioMetaTmpBucket, "staged", fi.DataDir, "part.1")
				if err := os.MkdirAll(filepath.Dir(part), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(part, []byte("a"), 0o644); err != nil {
					t.Fatal(err)
				}
				ctx.dataDest = filepath.Join(root, "bucket", "object", fi.DataDir)
			}
			_, err = s.RenameData(ctx, minioMetaTmpBucket, "staged", fi, "bucket", "object", RenameOptions{})
			if !ctx.removed {
				t.Fatalf("metadata removal did not execute: removal error = %v, RenameData error = %v", ctx.removeErr, err)
			}
			if !errors.Is(err, errCommitMetaMissing) {
				t.Fatalf("expected commit metadata missing, got %v", err)
			}
			apiErr := toAPIError(t.Context(), toObjectErr(err, "bucket", "object"))
			if apiErr.Code != "SlowDownWrite" || apiErr.HTTPStatusCode != http.StatusServiceUnavailable {
				t.Fatalf("expected SlowDownWrite/503, got %+v", apiErr)
			}
			if _, err := os.Stat(filepath.Join(root, "bucket", "object", xlStorageFormatFile)); !os.IsNotExist(err) {
				t.Fatalf("failed commit must not produce destination metadata: %v", err)
			}
		})
	}
}
