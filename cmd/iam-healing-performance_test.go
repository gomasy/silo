// Copyright (c) 2026 PGSTY
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/madmin-go/v3"
)

// Measures steady-state index traversal, sorting and the capability request.
// The network peer acknowledges real batches but performs no disk I/O; this
// benchmark deliberately does not claim durable catch-up throughput.
func BenchmarkIAMRevisionConvergedHealing(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			ctx, sys, _ := prepareIAMRevisionFixture(b)
			_, err := sys.CreateUser(ctx, "benchmark-sync", madmin.AddOrUpdateUserReq{SecretKey: "valid-sync-password", Status: madmin.AccountEnabled})
			mustIAM(b, err)
			for i := range n {
				at := UTCNow().Add(time.Duration(i) * time.Nanosecond)
				data, err := json.Marshal(iamRevision{Deleted: true, UpdatedAt: at, RevokedBefore: at})
				mustIAM(b, err)
				sys.store.revisionIndex().observe(getUserIdentityPath(fmt.Sprintf("deleted-%06d", i), regUser), data)
			}
			var puts atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/minio/health/live" {
					w.WriteHeader(http.StatusOK)
					return
				}
				if r.Method == http.MethodPut {
					puts.Add(1)
				}
				_ = json.NewEncoder(w).Encode(iamRevisionResponse{iamRevisionStatus: iamRevisionStatus{Version: iamRevisionProtocol, Node: "node-1", Instance: "benchmark-peer", Digest: "constant"}})
			}))
			defer server.Close()
			c := &SiteReplicationSys{enabled: true, state: srState{ServiceAccountAccessKey: "benchmark-sync", Peers: map[string]madmin.PeerInfo{globalDeploymentID(): {DeploymentID: globalDeploymentID(), Name: "local"}, "remote": {DeploymentID: "remote", Name: "remote", Endpoint: server.URL}}}}
			mustIAM(b, c.healIAMDeletions(ctx))
			before := puts.Load()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				mustIAM(b, c.healIAMDeletions(ctx))
			}
			b.StopTimer()
			b.ReportMetric(float64(puts.Load()-before)/float64(b.N), "PUT/op")
			if puts.Load() != before {
				b.Fatal("steady-state healing replayed acknowledged records")
			}
		})
	}
}
