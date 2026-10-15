// Copyright (c) 2026 Julien Laurenceau and Feng Ruohang
//
// This file is part of Silo Object Storage.
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
	"testing"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/grid"
)

type scannerResponseTestDisk struct {
	StorageAPI
}

func (scannerResponseTestDisk) NSScanner(_ context.Context, cache dataUsageCache, updates chan<- dataUsageEntry, _ madmin.HealScanMode, _ func() bool) (dataUsageCache, error) {
	defer close(updates)
	updates <- dataUsageEntry{Size: int64(cache.Info.NextCycle)}
	cache.Info.NextCycle++
	return cache, nil
}

func TestNSScannerHandlerClearsStaleResponses(t *testing.T) {
	endpoint, err := NewEndpoint(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	globalLocalDrivesMu.Lock()
	oldDrives, oldSets := globalLocalDrivesMap, globalLocalSetDrives
	globalLocalDrivesMap = map[string]StorageAPI{endpoint.String(): scannerResponseTestDisk{}}
	globalLocalSetDrives = nil
	globalLocalDrivesMu.Unlock()
	t.Cleanup(func() {
		globalLocalDrivesMu.Lock()
		globalLocalDrivesMap, globalLocalSetDrives = oldDrives, oldSets
		globalLocalDrivesMu.Unlock()
	})

	oldRPC := storageNSScannerRPC
	t.Cleanup(func() { storageNSScannerRPC = oldRPC })
	// Every new allocation is dirty too: the check must not depend on
	// sync.Pool returning a previously used object.
	storageNSScannerRPC = grid.NewStream[*nsScannerOptions, grid.NoPayload, *nsScannerResp](grid.HandlerNSScanner,
		func() *nsScannerOptions { return &nsScannerOptions{} }, nil,
		func() *nsScannerResp {
			return &nsScannerResp{Update: &dataUsageEntry{Size: -1}, Final: &dataUsageCache{}}
		})
	server := storageRESTServer{endpoint: endpoint}
	for cycle := uint32(1); cycle <= 3; cycle++ {
		out := make(chan *nsScannerResp, 2)
		params := &nsScannerOptions{Cache: &dataUsageCache{Info: dataUsageCacheInfo{Name: "bucket", NextCycle: cycle}}}
		if err := server.NSScannerHandler(t.Context(), params, out); err != nil {
			t.Fatal(err)
		}
		if len(out) != 2 {
			t.Fatalf("cycle %d: got %d responses, want an update and a final cache", cycle, len(out))
		}
		for i := 0; i < 2; i++ {
			resp := <-out
			wire, err := resp.MarshalMsg(nil)
			storageNSScannerRPC.PutResponse(resp)
			if err != nil {
				t.Fatal(err)
			}
			var decoded nsScannerResp
			if _, err := decoded.UnmarshalMsg(wire); err != nil {
				t.Fatal(err)
			}
			if i == 0 {
				if decoded.Update == nil || decoded.Final != nil || decoded.Update.Size != int64(cycle) {
					t.Errorf("cycle %d: invalid update response: %+v", cycle, decoded)
				}
			} else if decoded.Update != nil || decoded.Final == nil || decoded.Final.Info.NextCycle != cycle+1 {
				t.Errorf("cycle %d: invalid final response: %+v", cycle, decoded)
			}
		}
	}
}
