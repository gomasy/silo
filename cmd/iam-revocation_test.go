// Copyright (c) 2026 PGSTY
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/auth"
	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"
)

// Exercise the persisted IAM store and the same peer handler used by site heal.
// A delete must survive a cache reload and an older create arriving afterwards.
func TestIAMRevocationRejectsOfflineUser(t *testing.T) {
	resetTestGlobals()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	obj, disk, err := prepareFS(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(disk)
	defer obj.Shutdown(ctx)
	defer resetTestGlobals()
	user := "offline-revoked-user"
	req := madmin.AddOrUpdateUserReq{SecretKey: "test-password-valid", Status: madmin.AccountEnabled}
	created, err := globalIAMSys.CreateUser(ctx, user, req)
	if err != nil {
		t.Fatal(err)
	}
	if err = globalIAMSys.DeleteUser(ctx, user, false); err != nil {
		t.Fatal(err)
	}
	if err = globalIAMSys.store.LoadIAMCache(ctx, false); err != nil {
		t.Fatal(err)
	}
	if err = globalSiteReplicationSys.PeerIAMUserChangeHandler(ctx, &madmin.SRIAMUser{AccessKey: user, UserReq: &req}, created); err != nil {
		t.Fatal(err)
	}
	if _, err = globalIAMSys.GetUserInfo(ctx, user); !errors.Is(err, errNoSuchUser) {
		t.Fatalf("revoked user restored by old peer event: %v", err)
	}
}

func TestIAMRevocationHealingContinuesAfterPeerRejectsDelete(t *testing.T) {
	resetTestGlobals()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	obj, disk, err := prepareFS(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(disk)
	defer obj.Shutdown(ctx)
	defer resetTestGlobals()
	req := madmin.AddOrUpdateUserReq{SecretKey: "valid-test-password", Status: madmin.AccountEnabled}
	if _, err := globalIAMSys.CreateUser(ctx, "heal-sync", req); err != nil {
		t.Fatal(err)
	}
	if _, err := globalIAMSys.CreateUser(ctx, "heal-deleted", req); err != nil {
		t.Fatal(err)
	}
	if err := globalIAMSys.DeleteUser(ctx, "heal-deleted", false); err != nil {
		t.Fatal(err)
	}
	p, err := globalIAMSys.store.GetPolicy("readwrite")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := globalIAMSys.SetPolicy(ctx, "heal-new-policy", p); err != nil {
		t.Fatal(err)
	}
	var liveUpdates atomic.Int32
	peer := func(id string, rejectDelete bool) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			switch {
			case strings.HasSuffix(r.URL.Path, "/metainfo"):
				_ = json.NewEncoder(w).Encode(madmin.SRInfo{DeploymentID: id})
			case r.URL.Path == "/minio/admin/v3/site-replication/peer/iam-revisions":
				if r.Method == http.MethodGet {
					_ = json.NewEncoder(w).Encode(iamRevisionStatus{Version: iamRevisionProtocol, Node: "node-1", Instance: id, Digest: "fixture"})
					return
				}
				var batch iamRevisionBatch
				if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				for _, item := range batch.Items {
					if rejectDelete && iamDeletionPath(item.SRIAMItem) != "" {
						w.WriteHeader(http.StatusForbidden)
						_, _ = w.Write([]byte(`{"Code":"AccessDenied","Message":"delete rejected"}`))
						return
					}
					if id == "healthy" && item.Type == madmin.SRIAMItemPolicy && item.Name == "heal-new-policy" && len(item.Policy) > 0 {
						liveUpdates.Add(1)
					}
				}
				_ = json.NewEncoder(w).Encode(iamRevisionStatus{Version: iamRevisionProtocol, Node: "node-1", Instance: id, Digest: "fixture"})
			default:
				t.Errorf("unexpected peer request %s", r.URL.Path)
				w.WriteHeader(http.StatusNotFound)
			}
		}))
	}
	healthy, rejected := peer("healthy", false), peer("rejected", true)
	defer healthy.Close()
	defer rejected.Close()
	c := &SiteReplicationSys{enabled: true, state: srState{
		ServiceAccountAccessKey: "heal-sync",
		Peers: map[string]madmin.PeerInfo{
			globalDeploymentID(): {Name: "local", DeploymentID: globalDeploymentID()},
			"healthy":            {Name: "healthy", DeploymentID: "healthy", Endpoint: healthy.URL},
			"rejected":           {Name: "rejected", DeploymentID: "rejected", Endpoint: rejected.URL},
		},
	}}
	if err := c.healIAMSystem(ctx, obj); err == nil {
		t.Fatal("deletion failure was not reported")
	}
	if liveUpdates.Load() == 0 {
		t.Fatal("one peer rejecting a deletion blocked unrelated live IAM healing to a healthy peer")
	}
}

func TestIAMRevocationLifecycle(t *testing.T) {
	testIAMRevocationLifecycle(t, nil)
}

func TestIAMRevocationEtcdLifecycle(t *testing.T) {
	endpoint := os.Getenv("SILO_TEST_IAM_REVOCATION_ETCD")
	if endpoint == "" {
		t.Skip("set SILO_TEST_IAM_REVOCATION_ETCD to a disposable etcd endpoint")
	}
	connection, err := etcd.New(etcd.Config{Endpoints: strings.Split(endpoint, ","), DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	// The facade borrows the connection's services. Close the owning client,
	// not namespace.Watcher while IAM's canceled watch loop is winding down.
	ctx, cancel := context.WithCancel(connection.Ctx())
	defer cancel()
	client := etcd.NewCtxClient(ctx, etcd.WithZapLogger(connection.GetLogger()))
	prefix := fmt.Sprintf("/silo-revocation-test/%d/", time.Now().UnixNano())
	client.KV = namespace.NewKV(connection.KV, prefix)
	client.Watcher = namespace.NewWatcher(connection.Watcher, prefix)
	client.Lease = connection.Lease
	testIAMRevocationLifecycle(t, client)
}

func testIAMRevocationLifecycle(t *testing.T, client *etcd.Client) {
	resetTestGlobals()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	disks, err := getRandomDisks(1)
	if err != nil {
		t.Fatal(err)
	}
	disk := disks[0]
	obj, _, err := initObjectLayer(ctx, mustGetPoolEndpoints(0, disks...))
	if err == nil {
		initAllSubsystems(ctx)
		globalIAMSys.Init(ctx, obj, client, 2*time.Second)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(disk)
	defer obj.Shutdown(ctx)
	defer resetTestGlobals()
	sys, peer := globalIAMSys, &globalSiteReplicationSys
	must := func(t *testing.T, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	reload := func(t *testing.T) { t.Helper(); must(t, sys.store.LoadIAMCache(ctx, false)) }
	req := madmin.AddOrUpdateUserReq{SecretKey: "valid-test-password", Status: madmin.AccountEnabled}
	origin := UTCNow().Add(-time.Hour).Truncate(time.Millisecond)
	createUser := func(t *testing.T, name string) {
		t.Helper()
		must(t, peer.PeerIAMUserChangeHandler(ctx, &madmin.SRIAMUser{AccessKey: name, UserReq: &req}, origin))
	}
	assertAbsent := func(t *testing.T, name string) {
		t.Helper()
		if _, ok := sys.GetUser(ctx, name); ok {
			t.Fatalf("revoked credential %s is usable", name)
		}
	}

	t.Run("replay after recreation", func(t *testing.T) {
		testIAMRevocationReplayAfterRecreation(ctx, t, sys)
	})

	t.Run("origin timestamp and recreation", func(t *testing.T) {
		name := "revocation-recreate"
		createUser(t, name)
		ui, ok := sys.store.GetUser(name)
		if !ok || !ui.UpdatedAt.Equal(origin) {
			t.Fatalf("origin time changed: %v", ui.UpdatedAt)
		}
		must(t, sys.DeleteUser(ctx, name, false))
		reload(t)
		must(t, peer.PeerIAMUserChangeHandler(ctx, &madmin.SRIAMUser{AccessKey: name, UserReq: &req}, time.Time{}))
		assertAbsent(t, name)
		newTime := UTCNow().Add(time.Minute)
		must(t, peer.PeerIAMUserChangeHandler(ctx, &madmin.SRIAMUser{AccessKey: name, UserReq: &req}, newTime))
		must(t, peer.PeerIAMUserChangeHandler(ctx, &madmin.SRIAMUser{AccessKey: name, IsDeleteReq: true}, origin.Add(time.Second)))
		reload(t)
		ui, ok = sys.store.GetUser(name)
		if !ok || !ui.UpdatedAt.Equal(newTime) || ui.RevokedBefore.IsZero() {
			t.Fatalf("newer recreation lost, or deletion boundary missing: present=%v", ok)
		}
	})

	t.Run("groups policies and mappings", func(t *testing.T) {
		user, group, name := "revocation-member", "revocation-group", "revocation-policy"
		createUser(t, user)
		p, err := sys.store.GetPolicy("readwrite")
		must(t, err)
		must(t, peer.PeerAddPolicyHandler(ctx, name, &p, origin))
		add := &madmin.SRGroupInfo{UpdateReq: madmin.GroupAddRemove{Group: group, Members: []string{user}}}
		must(t, peer.PeerGroupInfoChangeHandler(ctx, add, origin))
		for _, isGroup := range []bool{false, true} {
			entity := user
			if isGroup {
				entity = group
			}
			mp := &madmin.SRPolicyMapping{UserOrGroup: entity, Policy: name, UserType: int(regUser), IsGroup: isGroup}
			must(t, peer.PeerPolicyMappingHandler(ctx, mp, origin))
			_, err = sys.PolicyDBSet(ctx, entity, "", regUser, isGroup)
			must(t, err)
			must(t, peer.PeerPolicyMappingHandler(ctx, mp, origin))
			if _, ok := sys.store.GetMappedPolicy(entity, isGroup); ok {
				t.Fatal("old grant restored")
			}
		}
		// This receiver never saw the member-removal event preceding deletion.
		must(t, peer.PeerGroupInfoChangeHandler(ctx, &madmin.SRGroupInfo{UpdateReq: madmin.GroupAddRemove{Group: group, IsRemove: true}}, UTCNow()))
		must(t, sys.DeletePolicy(ctx, name, true))
		reload(t)
		must(t, peer.PeerGroupInfoChangeHandler(ctx, add, origin))
		must(t, peer.PeerAddPolicyHandler(ctx, name, &p, origin))
		if _, err = sys.GetGroupDescription(group); !errors.Is(err, errNoSuchGroup) {
			t.Fatalf("group restored: %v", err)
		}
		if _, err = sys.store.GetPolicyDoc(name); !errors.Is(err, errNoSuchPolicy) {
			t.Fatalf("policy restored: %v", err)
		}
		paths, err := sys.store.listIAMConfigPaths(ctx)
		must(t, err)
		found := make(map[string]bool)
		for _, path := range paths {
			r, err := loadIAMRevision(ctx, sys.store, path)
			must(t, err)
			if item, ok := iamDeletionItem(path, r); ok {
				found[iamDeletionPath(item)] = true
				if item.UpdatedAt.IsZero() {
					t.Fatal("undated delete replay")
				}
			}
		}
		for _, path := range []string{getGroupInfoPath(group), getPolicyDocPath(name), getMappedPolicyPath(user, regUser, false), getMappedPolicyPath(group, regUser, true)} {
			if !found[path] {
				t.Errorf("deletion missing from heal: %s", path)
			}
		}
	})

	t.Run("parent revokes service accounts and STS", func(t *testing.T) {
		parent := "revocation-parent"
		createUser(t, parent)
		svc, svcAt, err := sys.NewServiceAccount(withIAMReplicationTime(ctx, origin), parent, nil, newServiceAccountOpts{accessKey: "revocation-service", secretKey: "valid-service-password"})
		must(t, err)
		secret, err := getTokenSigningKey()
		must(t, err)
		sts, err := auth.GetNewCredentialsWithMetadata(map[string]any{"exp": UTCNow().Add(time.Hour).Unix(), parentClaim: parent}, secret)
		must(t, err)
		sts.ParentUser = parent
		_, err = sys.SetTempUser(withIAMReplicationTime(ctx, origin), sts.AccessKey, sts, "readwrite")
		must(t, err)
		must(t, sys.DeleteUser(ctx, parent, false))
		reload(t)
		assertAbsent(t, parent)
		assertAbsent(t, svc.AccessKey)
		assertAbsent(t, sts.AccessKey)
		// Recreate the parent, then deliver old child events from the offline site.
		must(t, peer.PeerIAMUserChangeHandler(ctx, &madmin.SRIAMUser{AccessKey: parent, UserReq: &req}, UTCNow()))
		must(t, peer.PeerSvcAccChangeHandler(ctx, &madmin.SRSvcAccChange{Create: &madmin.SRSvcAccCreate{Parent: parent, AccessKey: svc.AccessKey, SecretKey: svc.SecretKey}}, svcAt))
		must(t, peer.PeerSTSAccHandler(ctx, &madmin.SRSTSCredential{AccessKey: sts.AccessKey, SecretKey: sts.SecretKey, ParentUser: parent, SessionToken: sts.SessionToken, ParentPolicyMapping: "readwrite"}, origin))
		reload(t)
		assertAbsent(t, svc.AccessKey)
		assertAbsent(t, sts.AccessKey)
		// A freshly issued credential is still supported after deliberate recreation.
		_, _, err = sys.NewServiceAccount(ctx, parent, nil, newServiceAccountOpts{accessKey: "new-service", secretKey: "valid-service-password"})
		must(t, err)
		if _, ok := sys.GetUser(ctx, "new-service"); !ok {
			t.Fatal("fresh service account rejected")
		}
	})

	t.Run("delete before first create", func(t *testing.T) {
		name := "revocation-unseen"
		must(t, peer.PeerIAMUserChangeHandler(ctx, &madmin.SRIAMUser{AccessKey: name, IsDeleteReq: true}, UTCNow()))
		createUser(t, name)
		assertAbsent(t, name)
		svc := "unseen-service"
		must(t, peer.PeerSvcAccChangeHandler(ctx, &madmin.SRSvcAccChange{Delete: &madmin.SRSvcAccDelete{AccessKey: svc}}, UTCNow()))
		must(t, peer.PeerSvcAccChangeHandler(ctx, &madmin.SRSvcAccChange{Create: &madmin.SRSvcAccCreate{Parent: "revocation-recreate", AccessKey: svc, SecretKey: "valid-service-password"}}, origin))
		assertAbsent(t, svc)
	})

	t.Run("recreation arrives before revocation", func(t *testing.T) {
		parent := "reordered-parent"
		createUser(t, parent)
		child, _, err := sys.NewServiceAccount(withIAMReplicationTime(ctx, origin), parent, nil, newServiceAccountOpts{accessKey: "reordered-child", secretKey: "valid-service-password"})
		must(t, err)
		newTime, deleteTime := UTCNow(), origin.Add(time.Minute)
		must(t, peer.PeerIAMUserChangeHandler(ctx, &madmin.SRIAMUser{AccessKey: parent, UserReq: &req}, newTime))
		must(t, peer.PeerIAMUserChangeHandler(ctx, &madmin.SRIAMUser{AccessKey: parent, IsDeleteReq: true}, deleteTime))
		assertAbsent(t, child.AccessKey)
		reload(t)
		assertAbsent(t, child.AccessKey)
		u, ok := sys.GetUser(ctx, parent)
		if !ok || !u.UpdatedAt.Equal(newTime) || !u.RevokedBefore.Equal(deleteTime) {
			t.Fatal("reordered revocation damaged the new parent or lost its boundary")
		}
		r, err := loadIAMRevision(ctx, sys.store, getUserIdentityPath(parent, regUser))
		must(t, err)
		item, ok := iamDeletionItem(getUserIdentityPath(parent, regUser), r)
		if !ok || !item.UpdatedAt.Equal(deleteTime) {
			t.Fatal("recreation erased deletion replay")
		}
	})

	t.Run("user cleanup does not supersede group deletion", func(t *testing.T) {
		user, group := "cascade-user", "cascade-group"
		createUser(t, user)
		must(t, peer.PeerGroupInfoChangeHandler(ctx, &madmin.SRGroupInfo{UpdateReq: madmin.GroupAddRemove{Group: group, Members: []string{user}}}, origin))
		// On the origin site the group was removed before the user, but the
		// recovering receiver processes those independent events in reverse.
		must(t, peer.PeerIAMUserChangeHandler(ctx, &madmin.SRIAMUser{AccessKey: user, IsDeleteReq: true}, origin.Add(2*time.Minute)))
		must(t, peer.PeerGroupInfoChangeHandler(ctx, &madmin.SRGroupInfo{UpdateReq: madmin.GroupAddRemove{Group: group, IsRemove: true}}, origin.Add(time.Minute)))
		reload(t)
		if _, err := sys.GetGroupDescription(group); !errors.Is(err, errNoSuchGroup) {
			t.Fatalf("deleted group survived reordered cleanup: %v", err)
		}
		groups, err := sys.ListGroups(ctx)
		must(t, err)
		for _, name := range groups {
			if name == group {
				t.Fatal("deleted group listed")
			}
		}
	})

	t.Run("parent revocation covers later updates to existing children", func(t *testing.T) {
		parent, key := "late-update-parent", "late-update-child"
		createUser(t, parent)
		_, _, err := sys.NewServiceAccount(withIAMReplicationTime(ctx, origin), parent, nil, newServiceAccountOpts{accessKey: key, secretKey: "valid-service-password"})
		must(t, err)
		// This site missed the deletion and subsequently edited an old child.
		_, err = sys.UpdateServiceAccount(withIAMReplicationTime(ctx, origin.Add(2*time.Minute)), key, updateServiceAccountOpts{description: "edited while the peer was offline"})
		must(t, err)
		must(t, peer.PeerIAMUserChangeHandler(ctx, &madmin.SRIAMUser{AccessKey: parent, IsDeleteReq: true}, origin.Add(time.Minute)))
		assertAbsent(t, key)
		must(t, peer.PeerIAMUserChangeHandler(ctx, &madmin.SRIAMUser{AccessKey: parent, UserReq: &req}, origin.Add(3*time.Minute)))
		reload(t)
		assertAbsent(t, key)
	})

	t.Run("old generation cannot return with a newer event timestamp", func(t *testing.T) {
		parent := "generation-parent"
		createUser(t, parent)
		deleteTime := origin.Add(time.Minute)
		must(t, peer.PeerIAMUserChangeHandler(ctx, &madmin.SRIAMUser{AccessKey: parent, IsDeleteReq: true}, deleteTime))
		must(t, peer.PeerIAMUserChangeHandler(ctx, &madmin.SRIAMUser{AccessKey: parent, UserReq: &req}, origin.Add(2*time.Minute)))
		// Another offline site issued this child under the original parent,
		// after this site's delete/recreate. Wall-clock ordering cannot identify it.
		late := origin.Add(3 * time.Minute)
		must(t, peer.PeerSvcAccChangeHandler(ctx, &madmin.SRSvcAccChange{Create: &madmin.SRSvcAccCreate{Parent: parent, AccessKey: "old-gen-service", SecretKey: "valid-service-password"}}, late))
		secret, err := getTokenSigningKey()
		must(t, err)
		sts, err := auth.GetNewCredentialsWithMetadata(map[string]any{"exp": UTCNow().Add(time.Hour).Unix(), parentClaim: parent}, secret)
		must(t, err)
		must(t, peer.PeerSTSAccHandler(ctx, &madmin.SRSTSCredential{AccessKey: sts.AccessKey, SecretKey: sts.SecretKey, ParentUser: parent, SessionToken: sts.SessionToken}, late))
		reload(t)
		assertAbsent(t, "old-gen-service")
		assertAbsent(t, sts.AccessKey)
		// A local issuer knows the new boundary and signs it into both kinds
		// of child. Untrusted inherited claims cannot select that boundary.
		child, _, err := sys.NewServiceAccount(ctx, parent, nil, newServiceAccountOpts{accessKey: "new-gen-service", secretKey: "valid-service-password", claims: map[string]any{iamParentRevocationClaim: "forged"}})
		must(t, err)
		newClaims := map[string]any{"exp": UTCNow().Add(time.Hour).Unix(), parentClaim: parent}
		must(t, setIAMParentRevocationClaim(ctx, sys.store, parent, newClaims))
		fresh, err := auth.GetNewCredentialsWithMetadata(newClaims, secret)
		must(t, err)
		fresh.ParentUser = parent
		_, err = sys.SetTempUser(ctx, fresh.AccessKey, fresh, "")
		must(t, err)
		reload(t)
		// A periodic reload retains the STS cache. Explicitly clear it to
		// exercise the cold credential load performed after process restart.
		cache := sys.store.lock()
		cache.iamSTSAccountsMap = make(map[string]UserIdentity)
		sys.store.unlock()
		for _, key := range []string{child.AccessKey, fresh.AccessKey} {
			u, ok := sys.GetUser(ctx, key)
			if !ok || !iamCredentialSurvivesRevocation(u.Credentials, deleteTime) {
				t.Fatalf("new-generation credential %s rejected", key)
			}
		}
	})

	t.Run("late revocation preserves proven new-generation children", func(t *testing.T) {
		for _, recreateFirst := range []bool{false, true} {
			parent := fmt.Sprintf("gen-parent-%t", recreateFirst)
			createUser(t, parent)
			deleteTime, createTime := origin.Add(time.Minute), origin.Add(2*time.Minute)
			if recreateFirst {
				must(t, peer.PeerIAMUserChangeHandler(ctx, &madmin.SRIAMUser{AccessKey: parent, UserReq: &req}, createTime))
			}
			key := fmt.Sprintf("gen-child-%t", recreateFirst)
			must(t, peer.PeerSvcAccChangeHandler(ctx, &madmin.SRSvcAccChange{Create: &madmin.SRSvcAccCreate{Parent: parent, AccessKey: key, SecretKey: "valid-service-password", Claims: map[string]any{iamParentRevocationClaim: deleteTime.Format(time.RFC3339Nano)}}}, createTime.Add(time.Second)))
			must(t, peer.PeerIAMUserChangeHandler(ctx, &madmin.SRIAMUser{AccessKey: parent, IsDeleteReq: true}, deleteTime))
			if !recreateFirst {
				assertAbsent(t, key)
				must(t, peer.PeerIAMUserChangeHandler(ctx, &madmin.SRIAMUser{AccessKey: parent, UserReq: &req}, createTime))
			}
			reload(t)
			if _, ok := sys.GetUser(ctx, key); !ok {
				t.Fatal("late revocation deleted a child issued by the recreated parent")
			}
		}
	})

	t.Run("cold loading preserves site-signed STS", func(t *testing.T) {
		parent := "cold-sts-parent"
		createUser(t, parent)
		secret := "site-signing-key-valid"
		_, _, err := sys.NewServiceAccount(ctx, globalActiveCred.AccessKey, nil, newServiceAccountOpts{
			accessKey: siteReplicatorSvcAcc, secretKey: secret, allowSiteReplicatorAccount: true,
		})
		must(t, err)
		setReplication := func(enabled bool) {
			globalSiteReplicationSys.Lock()
			globalSiteReplicationSys.enabled = enabled
			globalSiteReplicationSys.Unlock()
			globalSiteReplicatorCred.Set("")
		}
		setReplication(true)
		defer setReplication(false)
		cred, err := auth.GetNewCredentialsWithMetadata(map[string]any{"exp": UTCNow().Add(time.Hour).Unix(), parentClaim: parent}, secret)
		must(t, err)
		cred.ParentUser = parent
		_, err = sys.SetTempUser(ctx, cred.AccessKey, cred, "")
		must(t, err)
		// IAM can load before the site replication manager during startup.
		// A signing key that is not available yet must not delete live tokens.
		setReplication(false)
		for range 3 {
			unverified := make(map[string]UserIdentity)
			_ = sys.store.loadUser(ctx, cred.AccessKey, stsUser, unverified)
			if _, ok := unverified[cred.AccessKey]; ok {
				t.Fatal("accepted STS before the signing key became available")
			}
		}
		r, err := loadIAMRevision(ctx, sys.store, getUserIdentityPath(cred.AccessKey, stsUser))
		must(t, err)
		if r.Credentials.SessionToken == "" {
			t.Fatal("cold IAM load physically deleted a non-expired site-signed STS credential")
		}
		setReplication(true)
		loaded := make(map[string]UserIdentity)
		must(t, sys.store.loadUser(ctx, cred.AccessKey, stsUser, loaded))
		if _, ok := loaded[cred.AccessKey]; !ok {
			t.Fatal("STS credential did not recover when the signing key became available")
		}
	})

	t.Run("unverifiable STS stay denied and expired STS are removed", func(t *testing.T) {
		parent := "invalid-sts-parent"
		createUser(t, parent)
		// Keep the etcd watcher from cleaning half of the fixture before the
		// second record is seeded; this subtest exercises the loader directly.
		sys.store.lock()
		defer sys.store.unlock()
		for _, expired := range []bool{false, true} {
			cred, err := auth.GetNewCredentialsWithMetadata(map[string]any{"exp": UTCNow().Add(time.Hour).Unix(), parentClaim: parent}, "unavailable-test-signing-key")
			must(t, err)
			cred.ParentUser = parent
			if expired {
				cred.Expiration = UTCNow().Add(-time.Minute)
			}
			identityPath := getUserIdentityPath(cred.AccessKey, stsUser)
			mappingPath := getMappedPolicyPath(cred.AccessKey, stsUser, false)
			// Seed disk directly to exercise loading, including existing records
			// whose key is unknown. The write API should not accept such tokens.
			must(t, sys.store.saveIAMConfig(ctx, &UserIdentity{Version: 1, Credentials: cred, UpdatedAt: UTCNow()}, identityPath))
			must(t, sys.store.saveIAMConfig(ctx, &MappedPolicy{Version: 1, Policies: "readwrite"}, mappingPath))
			loaded := make(map[string]UserIdentity)
			_ = sys.store.loadUser(ctx, cred.AccessKey, stsUser, loaded)
			if _, ok := loaded[cred.AccessKey]; ok {
				t.Fatalf("invalid STS accepted, expired=%t", expired)
			}
			for _, path := range []string{identityPath, mappingPath} {
				var record map[string]any
				err := sys.store.loadIAMConfig(ctx, &record, path)
				if expired {
					if !errors.Is(err, errConfigNotFound) {
						t.Fatalf("expired STS data not cleaned up at %s: %v", path, err)
					}
				} else {
					must(t, err)
				}
			}
		}
	})
}
