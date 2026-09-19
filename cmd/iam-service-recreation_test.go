// Copyright (c) 2026 PGSTY
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/auth"
)

// The receiver missed a deletion and still has the previous service key.
// A later full snapshot must replace it, including when the owner changed.
func TestIAMServiceAccountRecreation(t *testing.T) {
	for _, backend := range []string{"object", "etcd"} {
		t.Run(backend, func(t *testing.T) {
			ctx, sys, _ := prepareIAMRevisionFixture(t, backend)
			origin := UTCNow().Add(-time.Hour)
			for _, parent := range []string{"old-owner", "new-owner"} {
				_, err := sys.CreateUser(withIAMReplicationTime(ctx, origin), parent, madmin.AddOrUpdateUserReq{SecretKey: "valid-owner-password", Status: madmin.AccountEnabled})
				mustIAM(t, err)
			}
			const key = "reusable-service"
			old := &madmin.SRSvcAccChange{Create: &madmin.SRSvcAccCreate{Parent: "old-owner", AccessKey: key, SecretKey: "old-service-password"}}
			mustIAM(t, globalSiteReplicationSys.PeerSvcAccChangeHandler(ctx, old, origin))
			oldIdentity, _ := sys.store.GetUser(key)
			_, err := sys.PolicyDBSet(withIAMReplicationTime(ctx, origin), key, "readwrite", svcUser, false)
			mustIAM(t, err)
			boundary, newer := origin.Add(time.Minute), origin.Add(2*time.Minute)
			fresh := iamReplicationItem{SRIAMItem: madmin.SRIAMItem{Type: madmin.SRIAMItemSvcAcc, UpdatedAt: newer, SvcAccChange: &madmin.SRSvcAccChange{Create: &madmin.SRSvcAccCreate{Parent: "new-owner", AccessKey: key, SecretKey: "new-service-password", Status: auth.AccountOff}}}, RevokedBefore: boundary}
			mustIAM(t, applyIAMReplicationItem(ctx, fresh))
			// A sibling may have missed the notification of the committed
			// replacement. An equal-version retry must refresh that cache too.
			staleCache := sys.store.lock()
			staleCache.iamUsersMap[key] = oldIdentity
			sys.store.unlock()
			mustIAM(t, applyIAMReplicationItem(ctx, fresh)) // duplicate delivery is acknowledged
			if current, _ := sys.store.GetUser(key); current.Credentials.SecretKey != fresh.SvcAccChange.Create.SecretKey {
				t.Fatal("duplicate snapshot acknowledged without refreshing the stale cache")
			}
			mustIAM(t, globalSiteReplicationSys.PeerSvcAccChangeHandler(ctx, old, origin))
			mustIAM(t, globalSiteReplicationSys.PeerSvcAccChangeHandler(ctx, &madmin.SRSvcAccChange{Delete: &madmin.SRSvcAccDelete{AccessKey: key}}, boundary))
			mustIAM(t, sys.store.LoadIAMCache(ctx, false))
			u, ok := sys.store.GetUser(key)
			if !ok || u.Credentials.SecretKey != fresh.SvcAccChange.Create.SecretKey || u.Credentials.ParentUser != "new-owner" || u.Credentials.Status != auth.AccountOff || !u.UpdatedAt.Equal(newer) || !u.RevokedBefore.Equal(boundary) {
				t.Fatal("recreation did not retain the new identity, disabled status, source version and revocation")
			}
			if _, ok := sys.GetUser(ctx, key); ok {
				t.Fatal("replicated disabled service can authenticate")
			}
			cache := sys.store.rlock()
			_, mapped := cache.cachedMappedPolicy(key, svcUser, false)
			sys.store.runlock()
			if mapped {
				t.Fatal("recreated service inherited an older mapping")
			}
			_, err = sys.PolicyDBSet(withIAMReplicationTime(ctx, origin), key, "readwrite", svcUser, false)
			if !errors.Is(err, errIAMStaleUpdate) {
				t.Fatalf("old service mapping replay was accepted: %v", err)
			}
			_, _, err = sys.NewServiceAccount(ctx, "new-owner", nil, newServiceAccountOpts{accessKey: key, secretKey: "local-service-password"})
			if !errors.Is(err, errIAMServiceAccountNotAllowed) {
				t.Fatalf("local duplicate creation must remain rejected: %v", err)
			}
			// Outbound snapshots must carry the retained service boundary too.
			out, err := globalSiteReplicationSys.replicationItem(ctx, fresh.SRIAMItem)
			mustIAM(t, err)
			if !out.RevokedBefore.Equal(boundary) {
				t.Fatal("outbound service snapshot lost its revocation")
			}
		})
	}
}

func TestIAMServiceAccountReplicationRejectsOtherCredentialKinds(t *testing.T) {
	ctx, sys, _ := prepareIAMRevisionFixture(t)
	_, err := sys.CreateUser(ctx, "builtin-collision", madmin.AddOrUpdateUserReq{SecretKey: "valid-user-password", Status: madmin.AccountEnabled})
	mustIAM(t, err)
	secret, err := getTokenSigningKey()
	mustIAM(t, err)
	token, err := auth.GetNewCredentialsWithMetadata(map[string]any{"exp": UTCNow().Add(time.Hour).Unix(), parentClaim: "builtin-collision"}, secret)
	mustIAM(t, err)
	token.ParentUser = "builtin-collision"
	_, err = sys.SetTempUser(ctx, token.AccessKey, token, "")
	mustIAM(t, err)
	for _, key := range []string{"builtin-collision", token.AccessKey} {
		_, _, err := sys.NewServiceAccount(withIAMReplicationTime(ctx, UTCNow().Add(time.Minute)), "another-owner", nil, newServiceAccountOpts{accessKey: key, secretKey: "valid-service-password"})
		if !errors.Is(err, errIAMServiceAccountNotAllowed) {
			t.Fatalf("service replication replaced another credential kind: %v", err)
		}
	}
}

// SR configuration can be temporarily unreadable even though a service token
// is signed with its own valid secret. Do not acknowledge a failed cache load.
func TestIAMServiceAccountRetryReportsClaimLoadFailure(t *testing.T) {
	ctx, sys, _ := prepareIAMRevisionFixture(t)
	globalSiteReplicatorCred.RLock()
	previousSigningKey := globalSiteReplicatorCred.secretKey
	globalSiteReplicatorCred.RUnlock()
	globalSiteReplicatorCred.Set("")
	t.Cleanup(func() { globalSiteReplicatorCred.Set(previousSigningKey) })
	_, err := sys.CreateUser(ctx, "retry-owner", madmin.AddOrUpdateUserReq{SecretKey: "valid-owner-password", Status: madmin.AccountEnabled})
	mustIAM(t, err)
	opts := newServiceAccountOpts{accessKey: "retry-service", secretKey: "valid-service-password"}
	_, _, err = sys.NewServiceAccount(ctx, "retry-owner", nil, opts)
	mustIAM(t, err)
	old, _ := sys.store.GetUser(opts.accessKey)
	opts.secretKey = "replacement-service-password"
	at, err := sys.UpdateServiceAccount(ctx, opts.accessKey, updateServiceAccountOpts{secretKey: opts.secretKey})
	mustIAM(t, err)
	cache := sys.store.lock()
	cache.iamUsersMap[opts.accessKey] = old // Missed sibling notification.
	sys.store.unlock()
	globalSiteReplicationSys.Lock()
	globalSiteReplicationSys.enabled = true // No site-replicator credential is installed.
	globalSiteReplicationSys.Unlock()
	_, _, err = sys.NewServiceAccount(withIAMReplicationTime(ctx, at), "retry-owner", nil, opts)
	if err == nil {
		t.Fatal("acknowledged service retry despite failed claims loading")
	}
	if _, ok := sys.store.GetUser(opts.accessKey); ok {
		t.Fatal("failed cache refresh retained the superseded service secret")
	}
	globalSiteReplicationSys.Lock()
	globalSiteReplicationSys.enabled = false
	globalSiteReplicationSys.Unlock()
	_, _, err = sys.NewServiceAccount(withIAMReplicationTime(ctx, at), "retry-owner", nil, opts)
	mustIAM(t, err)
}

// A delayed snapshot still has its original absolute expiration. Reapplying
// the local minimum issuance lifetime would leave the old unexpired key alive.
func TestIAMServiceAccountReplicationPreservesExpiration(t *testing.T) {
	for _, backend := range []string{"object", "etcd"} {
		for _, action := range []string{"create", "update"} {
			for _, expired := range []bool{false, true} {
				name := backend + "/" + action + "/near_expiry"
				if expired {
					name = backend + "/" + action + "/expired"
				}
				t.Run(name, func(t *testing.T) {
					ctx, sys, _ := prepareIAMRevisionFixture(t, backend)
					origin := UTCNow().Add(-time.Hour)
					_, err := sys.CreateUser(ctx, "expiry-owner", madmin.AddOrUpdateUserReq{SecretKey: "valid-owner-password", Status: madmin.AccountEnabled})
					mustIAM(t, err)
					old := &madmin.SRSvcAccChange{Create: &madmin.SRSvcAccCreate{Parent: "expiry-owner", AccessKey: "expiry-service", SecretKey: "old-service-password"}}
					mustIAM(t, globalSiteReplicationSys.PeerSvcAccChangeHandler(ctx, old, origin))
					expires := UTCNow().Add(time.Minute)
					if expired {
						expires = UTCNow().Add(-time.Minute)
					}
					change := &madmin.SRSvcAccChange{Create: &madmin.SRSvcAccCreate{Parent: "expiry-owner", AccessKey: "expiry-service", SecretKey: "new-service-password", Expiration: &expires}}
					if action == "update" {
						change = &madmin.SRSvcAccChange{Update: &madmin.SRSvcAccUpdate{AccessKey: "expiry-service", SecretKey: "new-service-password", Expiration: &expires}}
					}
					mustIAM(t, globalSiteReplicationSys.PeerSvcAccChangeHandler(ctx, change, origin.Add(2*time.Minute)))
					u, ok := sys.store.GetUser("expiry-service")
					if !ok || u.Credentials.SecretKey != "new-service-password" || !u.Credentials.Expiration.Equal(expires) {
						t.Fatal("delayed snapshot lost its new secret or absolute expiration")
					}
					_, allowed := sys.GetUser(ctx, "expiry-service")
					if allowed == expired {
						t.Fatal("credential validity disagrees with its absolute expiration")
					}
					mustIAM(t, sys.store.LoadIAMCache(ctx, false))
					mustIAM(t, globalSiteReplicationSys.PeerSvcAccChangeHandler(ctx, old, origin))
					r, err := loadIAMRevision(ctx, sys.store, getUserIdentityPath("expiry-service", svcUser))
					mustIAM(t, err)
					if r.Credentials.SecretKey == old.Create.SecretKey || (expired && !r.Deleted) {
						t.Fatal("old non-expiring credential returned after reload")
					}
					_, _, err = sys.NewServiceAccount(ctx, "expiry-owner", nil, newServiceAccountOpts{accessKey: "local-expiry", secretKey: "valid-service-password", expiration: &expires})
					if !errors.Is(err, errInvalidSvcAcctExpiration) {
						t.Fatalf("local issuance lifetime check changed: %v", err)
					}
				})
			}
		}
	}
}

// Status-only summaries intentionally omit secrets. Different revisions must
// still trigger live healing, and disabled identities must be eligible sources.
func TestIAMServiceAccountHealingNewerSnapshot(t *testing.T) {
	ctx, sys, obj := prepareIAMRevisionFixture(t)
	origin := UTCNow().Add(-time.Hour)
	_, err := sys.CreateUser(ctx, "heal-svc-owner", madmin.AddOrUpdateUserReq{SecretKey: "valid-owner-password", Status: madmin.AccountEnabled})
	mustIAM(t, err)
	_, _, err = sys.NewServiceAccount(withIAMReplicationTime(ctx, origin), "heal-svc-owner", nil, newServiceAccountOpts{accessKey: "heal-service", secretKey: "valid-service-password"})
	mustIAM(t, err)
	at, err := sys.UpdateServiceAccount(ctx, "heal-service", updateServiceAccountOpts{status: auth.AccountOff})
	mustIAM(t, err)
	var sent atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/minio/health/live" {
			return
		}
		if r.URL.Path != "/minio/admin/v3/site-replication/peer/iam-revisions" || r.Method != http.MethodPut {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var batch iamRevisionBatch
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		for _, item := range batch.Items {
			if item.SvcAccChange != nil && item.SvcAccChange.Create != nil && item.SvcAccChange.Create.AccessKey == "heal-service" && item.SvcAccChange.Create.Status == auth.AccountOff && item.UpdatedAt.Equal(at) {
				sent.Add(1)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(iamRevisionResponse{iamRevisionStatus: iamRevisionStatus{Version: iamRevisionProtocol, Node: "node", Instance: "boot", Digest: "fixture"}})
	}))
	defer server.Close()
	peers := map[string]madmin.PeerInfo{globalDeploymentID(): {Name: "local", DeploymentID: globalDeploymentID()}, "remote": {Name: "remote", DeploymentID: "remote", Endpoint: server.URL}}
	c := &SiteReplicationSys{enabled: true, state: srState{ServiceAccountAccessKey: "heal-svc-owner", Peers: peers}}
	local := madmin.UserInfo{Status: madmin.AccountStatus(auth.AccountOff), UpdatedAt: at}
	remote := local
	remote.UpdatedAt = origin
	if isUserInfoReplicated(2, 2, []madmin.UserInfo{local, remote}) {
		t.Fatal("status-only summaries concealed different service revisions")
	}
	info := srStatusInfo{Sites: peers, UserStats: map[string]map[string]srUserStatsSummary{"heal-service": {globalDeploymentID(): {userInfo: srUserInfo{UserInfo: local}}, "remote": {SRUserStatsSummary: madmin.SRUserStatsSummary{UserInfoMismatch: true}, userInfo: srUserInfo{UserInfo: remote}}}}}
	mustIAM(t, c.healUsers(ctx, obj, "heal-service", info))
	if sent.Load() == 0 {
		t.Fatal("disabled newer service snapshot was not healed")
	}
}
