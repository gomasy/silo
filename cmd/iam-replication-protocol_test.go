// Copyright (c) 2026 PGSTY
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/madmin-go/v3"
)

func TestIAMRevisionProtocolDoesNotFallBackToLegacy(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path != "/minio/admin/v3/site-replication/peer/iam-revisions" {
			t.Errorf("unsafe fallback path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"Code":"NotImplemented","Message":"old server"}`))
	}))
	defer server.Close()
	client, err := madmin.New(strings.TrimPrefix(server.URL, "http://"), "test-access", "valid-test-secret", false)
	mustIAM(t, err)
	_, err = executeIAMRevisionRequest(context.Background(), client, http.MethodPut, &iamRevisionBatch{Version: iamRevisionProtocol, Items: []iamReplicationItem{{SRIAMItem: madmin.SRIAMItem{Type: iamUserBoundaryType}, UserRevocation: &iamUserBoundary{User: "recreated", Before: UTCNow()}}}})
	if err == nil || requests.Load() != 1 {
		t.Fatalf("old peer must reject without fallback, err=%v requests=%d", err, requests.Load())
	}
}

type iamNoHealingScanStore struct{ IAMStorageAPI }

func (s *iamNoHealingScanStore) listIAMConfigPaths(context.Context) ([]string, error) {
	panic("healing must use the loaded revision index")
}

func TestIAMRevisionHealingAcknowledgements(t *testing.T) {
	for _, balanced := range []bool{false, true} {
		t.Run(fmt.Sprintf("load_balanced_%t", balanced), func(t *testing.T) { testIAMRevisionHealingAcknowledgements(t, balanced) })
	}
}

func testIAMRevisionHealingAcknowledgements(t *testing.T, balanced bool) {
	ctx, sys, _ := prepareIAMRevisionFixture(t)
	_, err := sys.CreateUser(ctx, "ack-sync", madmin.AddOrUpdateUserReq{SecretKey: "valid-sync-password", Status: madmin.AccountEnabled})
	mustIAM(t, err)
	for i := range maxIAMRevisionBatch*2 + 1 {
		at := UTCNow().Add(time.Duration(i) * time.Nanosecond)
		mustIAM(t, sys.store.saveIAMConfig(ctx, &UserIdentity{Version: 1, Deleted: true, UpdatedAt: at, RevokedBefore: at}, getUserIdentityPath(fmt.Sprintf("ack-%04d", i), regUser)))
	}
	sys.store.IAMStorageAPI = &iamNoHealingScanStore{IAMStorageAPI: sys.store.IAMStorageAPI}
	var mu sync.Mutex
	var puts, gets int
	var applied int
	instance := "boot-1"
	failSecondBatch := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/minio/health/live" {
			w.WriteHeader(http.StatusOK)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if r.URL.Path != "/minio/admin/v3/site-replication/peer/iam-revisions" {
			t.Errorf("unexpected request: %s", r.URL.Path)
			w.WriteHeader(404)
			return
		}
		var failures []string
		if r.Method == http.MethodGet {
			gets++
		} else {
			puts++
			var batch iamRevisionBatch
			if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			if len(batch.Items) > maxIAMRevisionBatch {
				t.Error("batch exceeds limit")
			}
			if failSecondBatch && puts == 2 {
				failures = []string{"injected item error"}
			} else {
				applied += len(batch.Items)
			}
		}
		node := "node-1"
		if balanced {
			node = fmt.Sprintf("node-%d", (gets+puts)%2+1)
		}
		_ = json.NewEncoder(w).Encode(iamRevisionResponse{iamRevisionStatus: iamRevisionStatus{Version: iamRevisionProtocol, Node: node, Instance: node + instance, Digest: fmt.Sprintf("%d", applied)}, Errors: failures})
	}))
	defer server.Close()
	c := &SiteReplicationSys{enabled: true, state: srState{ServiceAccountAccessKey: "ack-sync", Peers: map[string]madmin.PeerInfo{globalDeploymentID(): {DeploymentID: globalDeploymentID(), Name: "local"}, "remote": {DeploymentID: "remote", Name: "remote", Endpoint: server.URL}}}}
	if err := c.healIAMDeletions(ctx); err == nil {
		t.Fatal("item failure was hidden")
	}
	mu.Lock()
	if puts != 3 || applied != maxIAMRevisionBatch+1 {
		t.Errorf("failed middle batch blocked later revocations: puts=%d applied=%d", puts, applied)
	}
	failSecondBatch = false
	applied++ // Unrelated remote mutation changes its digest.
	mu.Unlock()
	at := UTCNow()
	mustIAM(t, sys.store.saveIAMConfig(ctx, &UserIdentity{Version: 1, Deleted: true, UpdatedAt: at, RevokedBefore: at}, getUserIdentityPath("ack-new-local", regUser)))
	mustIAM(t, c.healIAMDeletions(ctx))
	mu.Lock()
	if puts != 5 {
		t.Errorf("did not resume at unacknowledged batch: puts=%d", puts)
	}
	mu.Unlock()
	mustIAM(t, c.healIAMDeletions(ctx))
	mu.Lock()
	if puts != 5 || gets != 3 {
		t.Errorf("converged records were replayed: puts=%d gets=%d", puts, gets)
	}
	instance = "boot-2"
	mu.Unlock()
	mustIAM(t, c.healIAMDeletions(ctx))
	mu.Lock()
	defer mu.Unlock()
	if puts != 8 {
		t.Fatalf("peer restart reused an old acknowledgement: puts=%d", puts)
	}
}

func TestIAMRevisionIndexRebuildsFromStorage(t *testing.T) {
	ctx, sys, obj := prepareIAMRevisionFixture(t)
	const user = "index-parent"
	req := madmin.AddOrUpdateUserReq{SecretKey: "valid-parent-password", Status: madmin.AccountEnabled}
	_, err := sys.CreateUser(ctx, user, req)
	mustIAM(t, err)
	mustIAM(t, sys.DeleteUser(ctx, user, false))
	before := sys.store.revisionIndex().snapshot()
	store := &IAMStoreSys{IAMStorageAPI: newIAMObjectStore(obj, MinIOUsersSysType)}
	mustIAM(t, store.LoadIAMCache(ctx, true))
	if iamRevisionDigest(before) != iamRevisionDigest(store.revisionIndex().snapshot()) {
		t.Fatal("ordinary IAM loading did not restore the deletion index")
	}
	_, err = store.AddUser(ctx, user, req)
	mustIAM(t, err)
	r := store.revisionIndex().get(getUserIdentityPath(user, regUser))
	if r.Deleted || r.RevokedBefore.IsZero() {
		t.Fatal("recreation discarded the retained boundary")
	}
	if r.Credentials.SecretKey != "" || r.Credentials.SessionToken != "" {
		t.Fatal("index retained credentials")
	}
}
