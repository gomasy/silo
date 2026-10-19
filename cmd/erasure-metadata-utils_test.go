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
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
)

// Tests caclculating disk count.
func TestDiskCount(t *testing.T) {
	testCases := []struct {
		disks     []StorageAPI
		diskCount int
	}{
		// Test case - 1
		{
			disks:     []StorageAPI{&xlStorage{}, &xlStorage{}, &xlStorage{}, &xlStorage{}},
			diskCount: 4,
		},
		// Test case - 2
		{
			disks:     []StorageAPI{nil, &xlStorage{}, &xlStorage{}, &xlStorage{}},
			diskCount: 3,
		},
	}
	for i, testCase := range testCases {
		cdiskCount := diskCount(testCase.disks)
		if cdiskCount != testCase.diskCount {
			t.Errorf("Test %d: Expected %d, got %d", i+1, testCase.diskCount, cdiskCount)
		}
	}
}

// Test for reduceErrs, reduceErr reduces collection
// of errors into a single maximal error with in the list.
func TestReduceErrs(t *testing.T) {
	canceledErrs := make([]error, 0, 5)
	for i := range 5 {
		canceledErrs = append(canceledErrs, fmt.Errorf("error %d: %w", i, context.Canceled))
	}
	// List all of all test cases to validate various cases of reduce errors.
	testCases := []struct {
		errs        []error
		ignoredErrs []error
		err         error
	}{
		// Validate if have reduced properly.
		{[]error{
			errDiskNotFound,
			errDiskNotFound,
			errDiskFull,
		}, []error{}, errErasureReadQuorum},
		// Validate if have no consensus.
		{[]error{
			errDiskFull,
			errDiskNotFound,
			nil, nil,
		}, []error{}, errErasureReadQuorum},
		// Validate if have consensus and errors ignored.
		{[]error{
			errVolumeNotFound,
			errVolumeNotFound,
			errVolumeNotFound,
			errVolumeNotFound,
			errVolumeNotFound,
			errDiskNotFound,
			errDiskNotFound,
		}, []error{errDiskNotFound}, errVolumeNotFound},
		{[]error{}, []error{}, errErasureReadQuorum},
		{
			[]error{
				errFileNotFound, errFileNotFound, errFileNotFound,
				errFileNotFound, errFileNotFound, nil, nil, nil, nil, nil,
			},
			nil, nil,
		},
		// Checks if wrapped context cancellation errors are grouped as one.
		{canceledErrs, nil, context.Canceled},
	}
	// Validates list of all the testcases for returning valid errors.
	for i, testCase := range testCases {
		gotErr := reduceReadQuorumErrs(t.Context(), testCase.errs, testCase.ignoredErrs, 5)
		if gotErr != testCase.err {
			t.Errorf("Test %d : expected %s, got %s", i+1, testCase.err, gotErr)
		}
		gotNewErr := reduceWriteQuorumErrs(t.Context(), testCase.errs, testCase.ignoredErrs, 6)
		if gotNewErr != errErasureWriteQuorum {
			t.Errorf("Test %d : expected %s, got %s", i+1, errErasureWriteQuorum, gotErr)
		}
	}
}

// TestReduceWriteQuorumErrsCommitMetaMissing verifies that an in-quorum
// errCommitMetaMissing reduces to the sentinel, which renameData() converts
// to errErasureWriteQuorum, while below-quorum reduces directly to
// errErasureWriteQuorum.
func TestReduceWriteQuorumErrsCommitMetaMissing(t *testing.T) {
	errs := make([]error, 16)
	for i := range 9 {
		errs[i] = errCommitMetaMissing
	}
	if err := reduceWriteQuorumErrs(t.Context(), errs, objectOpIgnoredErrs, 9); !errors.Is(err, errCommitMetaMissing) {
		t.Errorf("expected %s, got %s", errCommitMetaMissing, err)
	}
	// The sentinel must be distinguishable from errFileNotFound so that
	// commit failures are never mapped to ObjectNotFound (404).
	if errors.Is(errCommitMetaMissing, errFileNotFound) {
		t.Error("errCommitMetaMissing must not match errFileNotFound")
	}

	// Below quorum the reduction must yield a write-quorum failure (503);
	// eight sentinels against eight committed drives is not a quorum.
	belowQuorum := make([]error, 16)
	for i := range 8 {
		belowQuorum[i] = errCommitMetaMissing
	}
	if err := reduceWriteQuorumErrs(t.Context(), belowQuorum, objectOpIgnoredErrs, 9); !errors.Is(err, errErasureWriteQuorum) {
		t.Errorf("expected %s, got %s", errErasureWriteQuorum, err)
	}
}

// renameDataFailingDisk is a StorageAPI stub returning a fixed error
// from RenameData, enough to drive renameData() in unit tests.
type renameDataFailingDisk struct {
	StorageAPI
	err       error
	undoWrite bool
}

func (d *renameDataFailingDisk) RenameData(context.Context, string, string, FileInfo, string, string, RenameOptions) (RenameDataResp, error) {
	return RenameDataResp{}, d.err
}

func (d *renameDataFailingDisk) DeleteVersion(_ context.Context, _, _ string, _ FileInfo, _ bool, opts DeleteOptions) error {
	d.undoWrite = opts.UndoWrite
	return nil
}

// TestRenameDataCommitMetaMissing verifies that renameData() converts an
// in-quorum errCommitMetaMissing into errErasureWriteQuorum (HTTP 503),
// while an in-quorum errFileNotFound keeps its meaning so that
// PutObject() still reports IncompleteBody for client disconnects.
func TestRenameDataCommitMetaMissing(t *testing.T) {
	for _, tc := range []struct {
		name     string
		failures int
		diskErr  error
		wantErr  error
	}{
		{"all_commit_metadata_missing", 16, errCommitMetaMissing, errErasureWriteQuorum},
		{"quorum_commit_metadata_missing", 9, errCommitMetaMissing, errErasureWriteQuorum},
		{"neither_side_has_quorum", 8, errCommitMetaMissing, errErasureWriteQuorum},
		{"successful_write_quorum", 7, errCommitMetaMissing, nil},
		{"staging_failure", 16, errFileNotFound, errFileNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metadata := make([]FileInfo, 16)
			disks := make([]StorageAPI, 16)
			for i := range disks {
				metadata[i] = FileInfo{Deleted: true} // valid without erasure info
				disk := &renameDataFailingDisk{}
				if i < tc.failures {
					disk.err = tc.diskErr
				}
				disks[i] = disk
			}
			_, _, _, err := renameData(t.Context(), disks, minioMetaTmpBucket, "tmp", metadata, "bucket", "object", 9)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected %v, got %v", tc.wantErr, err)
			}
			for i, disk := range disks {
				// Failed writes undo only the drives that committed. A
				// successful quorum must never be rolled back.
				wantUndo := tc.wantErr != nil && i >= tc.failures
				if got := disk.(*renameDataFailingDisk).undoWrite; got != wantUndo {
					t.Errorf("disk %d: UndoWrite = %v, want %v", i, got, wantUndo)
				}
			}
		})
	}
}

// TestHashOrder - test order of ints in array
func TestHashOrder(t *testing.T) {
	testCases := []struct {
		objectName  string
		hashedOrder []int
	}{
		// cases which should pass the test.
		// passing in valid object name.
		{"object", []int{14, 15, 16, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}},
		{"The Shining Script <v1>.pdf", []int{16, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
		{"Cost Benefit Analysis (2009-2010).pptx", []int{15, 16, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}},
		{"117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A", []int{3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 1, 2}},
		{"SHØRT", []int{11, 12, 13, 14, 15, 16, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
		{"There are far too many object names, and far too few bucket names!", []int{15, 16, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}},
		{"a/b/c/", []int{3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 1, 2}},
		{"/a/b/c", []int{6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 1, 2, 3, 4, 5}},
		{string([]byte{0xff, 0xfe, 0xfd}), []int{15, 16, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}},
	}

	// Tests hashing order to be consistent.
	for i, testCase := range testCases {
		hashedOrder := hashOrder(testCase.objectName, 16)
		if !reflect.DeepEqual(testCase.hashedOrder, hashedOrder) {
			t.Errorf("Test case %d: Expected \"%v\" but failed \"%v\"", i+1, testCase.hashedOrder, hashedOrder)
		}
	}

	// Tests hashing order to fail for when order is '-1'.
	if hashedOrder := hashOrder("This will fail", -1); hashedOrder != nil {
		t.Errorf("Test: Expect \"nil\" but failed \"%#v\"", hashedOrder)
	}

	if hashedOrder := hashOrder("This will fail", 0); hashedOrder != nil {
		t.Errorf("Test: Expect \"nil\" but failed \"%#v\"", hashedOrder)
	}
}

func TestShuffleDisks(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	nDisks := 16
	disks, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}
	objLayer, _, err := initObjectLayer(ctx, mustGetPoolEndpoints(0, disks...))
	if err != nil {
		removeRoots(disks)
		t.Fatal(err)
	}
	defer removeRoots(disks)
	z := objLayer.(*erasureServerPools)
	testShuffleDisks(t, z)
}

// Test shuffleDisks which returns shuffled slice of disks for their actual distribution.
func testShuffleDisks(t *testing.T, z *erasureServerPools) {
	disks := z.serverPools[0].GetDisks(0)()
	distribution := []int{16, 14, 12, 10, 8, 6, 4, 2, 1, 3, 5, 7, 9, 11, 13, 15}
	shuffledDisks := shuffleDisks(disks, distribution)
	// From the "distribution" above you can notice that:
	// 1st data block is in the 9th disk (i.e distribution index 8)
	// 2nd data block is in the 8th disk (i.e distribution index 7) and so on.
	if shuffledDisks[0] != disks[8] ||
		shuffledDisks[1] != disks[7] ||
		shuffledDisks[2] != disks[9] ||
		shuffledDisks[3] != disks[6] ||
		shuffledDisks[4] != disks[10] ||
		shuffledDisks[5] != disks[5] ||
		shuffledDisks[6] != disks[11] ||
		shuffledDisks[7] != disks[4] ||
		shuffledDisks[8] != disks[12] ||
		shuffledDisks[9] != disks[3] ||
		shuffledDisks[10] != disks[13] ||
		shuffledDisks[11] != disks[2] ||
		shuffledDisks[12] != disks[14] ||
		shuffledDisks[13] != disks[1] ||
		shuffledDisks[14] != disks[15] ||
		shuffledDisks[15] != disks[0] {
		t.Errorf("shuffleDisks returned incorrect order.")
	}
}

// TestEvalDisks tests the behavior of evalDisks
func TestEvalDisks(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	nDisks := 16
	disks, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}
	objLayer, _, err := initObjectLayer(ctx, mustGetPoolEndpoints(0, disks...))
	if err != nil {
		removeRoots(disks)
		t.Fatal(err)
	}
	defer removeRoots(disks)
	z := objLayer.(*erasureServerPools)
	testShuffleDisks(t, z)
}

func Test_hashOrder(t *testing.T) {
	for x := 1; x < 17; x++ {
		t.Run(fmt.Sprintf("%d", x), func(t *testing.T) {
			var first [17]int
			rng := rand.New(rand.NewSource(0))
			var tmp [16]byte
			rng.Read(tmp[:])
			prefix := hex.EncodeToString(tmp[:])
			for range 10000 {
				rng.Read(tmp[:])

				y := hashOrder(fmt.Sprintf("%s/%x", prefix, hex.EncodeToString(tmp[:3])), x)
				first[y[0]]++
			}
			t.Log("first:", first[:x])
		})
	}
}
