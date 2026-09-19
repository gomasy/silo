// Copyright (c) 2026 PGSTY
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/grid"
	xnet "github.com/pgsty/silo-pkg/v3/net"
	"github.com/pgsty/silo-pkg/v3/policy"
	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"
)

func prepareIAMRevisionFixture(t testing.TB, backend ...string) (context.Context, *IAMSys, ObjectLayer) {
	t.Helper()
	resetTestGlobals()
	ctx, cancel := context.WithCancel(context.Background())
	disks, err := getRandomDisks(1)
	mustIAM(t, err)
	obj, _, err := initObjectLayer(ctx, mustGetPoolEndpoints(0, disks...))
	mustIAM(t, err)
	initAllSubsystems(ctx)
	// Deliberately omit the periodic refresh goroutine. Fault injection can
	// replace this fixture's storage interface without racing initialization.
	var client *etcd.Client
	if len(backend) != 0 && backend[0] == "etcd" {
		endpoint := os.Getenv("SILO_TEST_IAM_REVOCATION_ETCD")
		if endpoint == "" {
			cancel()
			obj.Shutdown(context.Background())
			os.RemoveAll(disks[0])
			t.Skip("set SILO_TEST_IAM_REVOCATION_ETCD to a disposable etcd endpoint")
		}
		client, err = etcd.New(etcd.Config{Endpoints: strings.Split(endpoint, ","), DialTimeout: 5 * time.Second})
		mustIAM(t, err)
		prefix := fmt.Sprintf("/silo-boundary-test/%d/", time.Now().UnixNano())
		client.KV = namespace.NewKV(client.KV, prefix)
		client.Watcher = namespace.NewWatcher(client.Watcher, prefix)
		t.Cleanup(func() { client.Delete(context.Background(), "", etcd.WithPrefix()); client.Close() })
	}
	globalIAMSys.initStore(obj, client)
	mustIAM(t, globalIAMSys.Load(ctx, true))
	t.Cleanup(func() { cancel(); obj.Shutdown(context.Background()); os.RemoveAll(disks[0]); resetTestGlobals() })
	return ctx, globalIAMSys, obj
}

func mustIAM(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

var errIAMInjectedWrite = errors.New("injected IAM persistence failure")

type iamFailingCleanupStore struct {
	IAMStorageAPI
	parentPath   string
	beforeCommit bool
}

func (s *iamFailingCleanupStore) saveIAMConfig(ctx context.Context, item any, path string, opts ...options) error {
	if s.beforeCommit || path != s.parentPath {
		return errIAMInjectedWrite
	}
	return s.IAMStorageAPI.saveIAMConfig(ctx, item, path, opts...)
}

func TestIAMRevocationCommitBoundary(t *testing.T) {
	for _, before := range []bool{true, false} {
		name := "after_identity_commit"
		if before {
			name = "before_identity_commit"
		}
		t.Run(name, func(t *testing.T) {
			ctx, sys, obj := prepareIAMRevisionFixture(t)
			const user = "commit-boundary-user"
			origin := UTCNow().Add(-time.Hour)
			req := madmin.AddOrUpdateUserReq{SecretKey: "valid-test-password", Status: madmin.AccountEnabled}
			_, err := sys.CreateUser(withIAMReplicationTime(ctx, origin), user, req)
			mustIAM(t, err)
			_, err = sys.PolicyDBSet(withIAMReplicationTime(ctx, origin.Add(time.Minute)), user, "readwrite", regUser, false)
			mustIAM(t, err)
			_, err = sys.AddUsersToGroup(withIAMReplicationTime(ctx, origin.Add(time.Minute)), "commit-group", []string{user})
			mustIAM(t, err)
			_, err = sys.PolicyDBSet(ctx, "commit-group", "readwrite", regUser, true)
			mustIAM(t, err)
			child, _, err := sys.NewServiceAccount(withIAMReplicationTime(ctx, origin), user, nil, newServiceAccountOpts{accessKey: "commit-child", secretKey: "valid-child-password"})
			mustIAM(t, err)
			args := policy.Args{AccountName: user, Action: policy.GetObjectAction, BucketName: "bucket", ObjectName: "object"}
			if !sys.IsAllowed(args) {
				t.Fatal("fixture has no grant")
			}
			siblingStore := &IAMStoreSys{IAMStorageAPI: newIAMObjectStore(obj, MinIOUsersSysType)}
			mustIAM(t, siblingStore.LoadIAMCache(ctx, true))
			sibling := &IAMSys{store: siblingStore, usersSysType: MinIOUsersSysType}
			tg, err := grid.SetupTestGrid(2)
			mustIAM(t, err)
			defer tg.Cleanup()
			var notifications atomic.Int32
			mustIAM(t, deleteUserRPC.Register(tg.Managers[1], func(r *grid.MSS) (grid.NoPayload, *grid.RemoteErr) {
				notifications.Add(1)
				if err := sibling.LoadUserAfterDelete(ctx, r.Get(peerRESTUser)); err != nil {
					return grid.NoPayload{}, grid.NewRemoteErr(err)
				}
				return grid.NoPayload{}, nil
			}))
			host, err := xnet.ParseHost(strings.TrimPrefix(tg.Hosts[1], "http://"))
			mustIAM(t, err)
			globalNotificationSys = &NotificationSys{peerClients: []*peerRESTClient{{host: host, gridConn: func() *grid.Connection { return tg.Managers[0].Connection(tg.Hosts[1]) }}}}
			original := sys.store.IAMStorageAPI
			sys.store.IAMStorageAPI = &iamFailingCleanupStore{IAMStorageAPI: original, parentPath: getUserIdentityPath(user, regUser), beforeCommit: before}
			boundary := origin.Add(2 * time.Minute)
			err = sys.DeleteUser(withIAMReplicationTime(ctx, boundary), user, true)
			if !errors.Is(err, errIAMInjectedWrite) {
				t.Fatalf("expected write failure, got %v", err)
			}
			sys.store.IAMStorageAPI = original
			r, err := loadIAMRevision(ctx, original, getUserIdentityPath(user, regUser))
			mustIAM(t, err)
			if before {
				if r.Deleted || !sys.IsAllowed(args) || !sibling.IsAllowed(args) || notifications.Load() != 0 {
					t.Fatal("failure before commit changed the identity or grant")
				}
				return
			}
			if !r.Deleted || !r.RevokedBefore.Equal(boundary) {
				t.Fatal("cleanup failure lost durable revocation")
			}
			if sys.IsAllowed(args) || sibling.IsAllowed(args) || notifications.Load() != 1 {
				t.Fatal("cleanup failure retained old permission")
			}
			// Subsequent fixture writes need no additional RPC handlers.
			globalNotificationSys = &NotificationSys{}
			// Recreate after the partial cleanup. The old mapping, group member
			// and child still exist in storage; none may authorize this identity.
			_, err = sys.CreateUser(withIAMReplicationTime(ctx, origin.Add(3*time.Minute)), user, req)
			mustIAM(t, err)
			reloaded := &IAMStoreSys{IAMStorageAPI: newIAMObjectStore(obj, MinIOUsersSysType)}
			mustIAM(t, reloaded.LoadIAMCache(ctx, true))
			fresh := &IAMSys{store: reloaded, usersSysType: MinIOUsersSysType}
			if fresh.IsAllowed(args) {
				t.Fatal("cold reload restored partially cleaned-up grants")
			}
			if _, ok := reloaded.GetUser(child.AccessKey); ok {
				t.Fatal("cold reload restored the old child")
			}
			gd, err := reloaded.GetGroupDescription("commit-group")
			mustIAM(t, err)
			if len(gd.Members) != 0 {
				t.Fatalf("listing exposed a revoked group relation: %v", gd.Members)
			}
			_, err = sys.AddUsersToGroup(ctx, "commit-group", []string{user})
			mustIAM(t, err)
			if !sys.IsAllowed(args) {
				t.Fatal("explicit new group grant was not accepted")
			}
		})
	}
}

func TestIAMGroupGrantVersionsSurviveSnapshotsAndRecreation(t *testing.T) {
	ctx, sys, _ := prepareIAMRevisionFixture(t)
	origin := UTCNow().Add(-time.Hour)
	req := madmin.AddOrUpdateUserReq{SecretKey: "valid-test-password", Status: madmin.AccountEnabled}
	for _, user := range []string{"grant-alice", "grant-bob"} {
		_, err := sys.CreateUser(withIAMReplicationTime(ctx, origin), user, req)
		mustIAM(t, err)
	}
	grant := origin.Add(time.Minute)
	_, err := sys.AddUsersToGroup(withIAMReplicationTime(ctx, grant), "grant-group", []string{"grant-alice"})
	mustIAM(t, err)
	_, err = sys.PolicyDBSet(ctx, "grant-group", "readwrite", regUser, true)
	mustIAM(t, err)
	boundary := origin.Add(2 * time.Minute)
	mustIAM(t, sys.DeleteUser(withIAMReplicationTime(ctx, boundary), "grant-alice", false))
	_, err = sys.CreateUser(withIAMReplicationTime(ctx, origin.Add(3*time.Minute)), "grant-alice", req)
	mustIAM(t, err)
	_, err = sys.AddUsersToGroup(withIAMReplicationTime(ctx, origin.Add(4*time.Minute)), "grant-group", []string{"grant-bob"})
	mustIAM(t, err)
	_, err = sys.SetGroupStatus(withIAMReplicationTime(ctx, origin.Add(5*time.Minute)), "grant-group", true)
	mustIAM(t, err)
	var gi GroupInfo
	mustIAM(t, sys.store.loadIAMConfig(ctx, &gi, getGroupInfoPath("grant-group")))
	if !gi.MemberGrants["grant-alice"].Equal(grant) {
		t.Fatal("unrelated group edits refreshed an old grant")
	}
	args := policy.Args{AccountName: "grant-alice", Action: policy.GetObjectAction, BucketName: "bucket", ObjectName: "object"}
	for _, stale := range []time.Time{grant, boundary, {}} {
		item := iamReplicationItem{SRIAMItem: madmin.SRIAMItem{Type: madmin.SRIAMItemGroupInfo, UpdatedAt: origin.Add(6 * time.Minute), GroupInfo: &madmin.SRGroupInfo{UpdateReq: madmin.GroupAddRemove{Group: "grant-group", Members: []string{"grant-alice", "grant-bob"}}}}, GroupSnapshot: true, GroupGrants: map[string]time.Time{"grant-alice": stale, "grant-bob": origin.Add(4 * time.Minute)}}
		mustIAM(t, applyIAMReplicationItem(ctx, item))
		mustIAM(t, sys.store.LoadIAMCache(ctx, false))
		if sys.IsAllowed(args) {
			t.Fatalf("snapshot restored revoked grant %s", stale)
		}
		gd, err := sys.GetGroupDescription("grant-group")
		mustIAM(t, err)
		if len(gd.Members) != 1 || gd.Members[0] != "grant-bob" {
			t.Fatalf("inconsistent effective members: %v", gd.Members)
		}
	}
	// Only an explicit post-revocation grant restores access.
	freshAt, err := sys.AddUsersToGroup(ctx, "grant-group", []string{"grant-alice"})
	mustIAM(t, err)
	if !sys.IsAllowed(args) {
		t.Fatal("explicit regrant rejected")
	}
	mustIAM(t, sys.store.LoadIAMCache(ctx, false))
	mustIAM(t, sys.store.loadIAMConfig(ctx, &gi, getGroupInfoPath("grant-group")))
	if !gi.MemberGrants["grant-alice"].Equal(freshAt) {
		t.Fatal("new grant version was not persisted")
	}
	if !gi.MemberGrants["grant-bob"].Equal(origin.Add(4 * time.Minute)) {
		t.Fatal("regranting Alice changed Bob's grant")
	}
}

func TestIAMGroupRevocationCommitAndRecreation(t *testing.T) {
	for _, backend := range []string{"object", "etcd"} {
		t.Run(backend, func(t *testing.T) { testIAMGroupRevocationCommitAndRecreation(t, backend) })
	}
}

func testIAMGroupRevocationCommitAndRecreation(t *testing.T, backend string) {
	ctx, sys, obj := prepareIAMRevisionFixture(t, backend)
	origin := UTCNow().Add(-time.Hour)
	user, group := "group-boundary-user", "group-boundary"
	_, err := sys.CreateUser(withIAMReplicationTime(ctx, origin), user, madmin.AddOrUpdateUserReq{SecretKey: "valid-user-password", Status: madmin.AccountEnabled})
	mustIAM(t, err)
	grant, boundary := origin.Add(time.Minute), origin.Add(2*time.Minute)
	_, err = sys.AddUsersToGroup(withIAMReplicationTime(ctx, grant), group, []string{user})
	mustIAM(t, err)
	// A newer mapping must not veto the authoritative group deletion.
	_, err = sys.PolicyDBSet(withIAMReplicationTime(ctx, origin.Add(3*time.Minute)), group, "readwrite", regUser, true)
	mustIAM(t, err)
	_, err = sys.RemoveUsersFromGroup(withIAMReplicationTime(ctx, boundary), group, nil)
	mustIAM(t, err)
	r, err := loadIAMRevision(ctx, sys.store, getGroupInfoPath(group))
	mustIAM(t, err)
	if !r.Deleted || !r.RevokedBefore.Equal(boundary) {
		t.Fatal("newer mapping swallowed group deletion")
	}
	_, err = sys.AddUsersToGroup(withIAMReplicationTime(ctx, origin.Add(4*time.Minute)), group, nil)
	mustIAM(t, err)
	for _, at := range []time.Time{grant, boundary, {}} {
		item := iamReplicationItem{SRIAMItem: madmin.SRIAMItem{Type: madmin.SRIAMItemGroupInfo, UpdatedAt: origin.Add(5 * time.Minute), GroupInfo: &madmin.SRGroupInfo{UpdateReq: madmin.GroupAddRemove{Group: group, Members: []string{user}}}}, GroupSnapshot: true, GroupGrants: map[string]time.Time{user: at}}
		mustIAM(t, applyIAMReplicationItem(ctx, item))
		gd, err := sys.GetGroupDescription(group)
		mustIAM(t, err)
		if len(gd.Members) != 0 {
			t.Fatalf("group recreation restored grant %s", at)
		}
	}
	_, err = sys.AddUsersToGroup(ctx, group, []string{user})
	mustIAM(t, err)
	args := policy.Args{AccountName: user, Action: policy.GetObjectAction, BucketName: "bucket", ObjectName: "object"}
	if !sys.IsAllowed(args) {
		t.Fatal("explicit group regrant was rejected")
	}
	// The newer live snapshot may arrive before an older group deletion.
	lateBoundary := origin.Add(6 * time.Minute)
	_, err = sys.RemoveUsersFromGroup(withIAMReplicationTime(ctx, lateBoundary), group, nil)
	mustIAM(t, err)
	r, err = loadIAMRevision(ctx, sys.store, getGroupInfoPath(group))
	mustIAM(t, err)
	if r.Deleted || !r.RevokedBefore.Equal(lateBoundary) {
		t.Fatal("late deletion lost the live group's revocation boundary")
	}
	// The old mapping is now revoked; a new explicit mapping restores access.
	if sys.IsAllowed(args) {
		t.Fatal("late group boundary retained an old mapping")
	}
	_, err = sys.PolicyDBSet(ctx, group, "readwrite", regUser, true)
	mustIAM(t, err)
	store := &IAMStoreSys{IAMStorageAPI: newIAMObjectStore(obj, MinIOUsersSysType)}
	if es, ok := sys.store.IAMStorageAPI.(*IAMEtcdStore); ok {
		store.IAMStorageAPI = newIAMEtcdStore(es.client, MinIOUsersSysType)
	}
	mustIAM(t, store.LoadIAMCache(ctx, true))
	fresh := &IAMSys{store: store, usersSysType: MinIOUsersSysType}
	if !fresh.IsAllowed(args) {
		t.Fatal("reload lost explicit grants after a retained group boundary")
	}
	item, err := globalSiteReplicationSys.replicationItem(ctx, madmin.SRIAMItem{Type: madmin.SRIAMItemGroupInfo, GroupInfo: &madmin.SRGroupInfo{UpdateReq: madmin.GroupAddRemove{Group: group}}, UpdatedAt: r.timestamp()})
	mustIAM(t, err)
	if !item.RevokedBefore.Equal(lateBoundary) || !item.GroupGrants[user].After(lateBoundary) {
		t.Fatal("group snapshot lost revision metadata")
	}
}

// A committed revision is observable before all cached dependents have been
// cleaned up. Every authorization read must apply that boundary in this window.
func TestIAMCachedMappingHonorsCommittedRevision(t *testing.T) {
	ctx, sys, _ := prepareIAMRevisionFixture(t)
	origin := UTCNow().Add(-time.Hour)
	parent := "cached-external-parent"
	_, err := sys.PolicyDBSet(withIAMReplicationTime(ctx, origin), parent, "readwrite", stsUser, false)
	mustIAM(t, err)
	policies, err := sys.PolicyDBGet(parent)
	mustIAM(t, err)
	if len(policies) == 0 {
		t.Fatal("fixture has no STS-parent mapping")
	}
	mustIAM(t, sys.store.saveIAMConfig(ctx, &MappedPolicy{Version: 1, Deleted: true, UpdatedAt: origin.Add(time.Minute)}, getMappedPolicyPath(parent, stsUser, false)))
	policies, err = sys.PolicyDBGet(parent)
	mustIAM(t, err)
	if len(policies) != 0 {
		t.Fatal("cached STS mapping ignored its own namespace tombstone")
	}

	user, group := "cached-group-user", "cached-group"
	_, err = sys.CreateUser(withIAMReplicationTime(ctx, origin), user, madmin.AddOrUpdateUserReq{SecretKey: "valid-user-password", Status: madmin.AccountEnabled})
	mustIAM(t, err)
	grant := origin.Add(5 * time.Minute)
	_, err = sys.AddUsersToGroup(withIAMReplicationTime(ctx, grant), group, []string{user})
	mustIAM(t, err)
	_, err = sys.PolicyDBSet(withIAMReplicationTime(ctx, origin), group, "readwrite", regUser, true)
	mustIAM(t, err)
	args := policy.Args{AccountName: user, Action: policy.GetObjectAction, BucketName: "bucket", ObjectName: "object"}
	if !sys.IsAllowed(args) {
		t.Fatal("fixture has no group grant")
	}
	// A late deletion preserves the newer member grant but revokes the older
	// policy mapping. Simulate the interval before mapping cleanup completes.
	gi := GroupInfo{Version: 1, Status: statusEnabled, Members: []string{user}, MemberGrants: map[string]time.Time{user: grant}, UpdatedAt: grant, RevokedBefore: origin.Add(2 * time.Minute)}
	mustIAM(t, sys.store.saveIAMConfig(ctx, &gi, getGroupInfoPath(group)))
	if sys.IsAllowed(args) {
		t.Fatal("cached group mapping ignored the committed group boundary")
	}
	gd, err := sys.GetGroupDescription(group)
	mustIAM(t, err)
	if gd.Policy != "" {
		t.Fatal("group listing exposed a revoked mapping")
	}
}

type (
	iamExpiryLockFailure struct {
		ObjectLayer
		path string
	}
	iamFailedExpiryLock struct{ RWLocker }
)

func (o *iamExpiryLockFailure) NewNSLock(bucket string, objects ...string) RWLocker {
	lock := o.ObjectLayer.NewNSLock(bucket, objects...)
	if bucket == minioMetaBucket && len(objects) == 1 && objects[0] == o.path+".revision-lock" {
		return &iamFailedExpiryLock{RWLocker: lock}
	}
	return lock
}

func (l *iamFailedExpiryLock) GetLock(context.Context, *dynamicTimeout) (LockContext, error) {
	return LockContext{}, errIAMInjectedWrite
}

func TestIAMExpiredCredentialCleanupDoesNotBlockLoading(t *testing.T) {
	ctx, sys, obj := prepareIAMRevisionFixture(t)
	_, err := sys.CreateUser(ctx, "healthy-user", madmin.AddOrUpdateUserReq{SecretKey: "healthy-user-password", Status: madmin.AccountEnabled})
	mustIAM(t, err)
	_, err = sys.PolicyDBSet(ctx, "healthy-user", "readwrite", regUser, false)
	mustIAM(t, err)
	c, _, err := sys.NewServiceAccount(ctx, "healthy-user", nil, newServiceAccountOpts{accessKey: "expired-service", secretKey: "expired-service-password"})
	mustIAM(t, err)
	c.Expiration = UTCNow().Add(-time.Hour)
	path := getUserIdentityPath(c.AccessKey, svcUser)
	mustIAM(t, sys.store.saveIAMConfig(ctx, &UserIdentity{Version: 1, Credentials: c, UpdatedAt: UTCNow()}, path))
	// A cold loader sees the existing version but cannot acquire the cleanup
	// write lock. Healthy users must still load; the expired one stays denied.
	fresh := &IAMStoreSys{IAMStorageAPI: newIAMObjectStore(&iamExpiryLockFailure{ObjectLayer: obj, path: path}, MinIOUsersSysType)}
	mustIAM(t, fresh.LoadIAMCache(ctx, true))
	if _, ok := fresh.GetUser("healthy-user"); !ok {
		t.Fatal("cleanup failure prevented healthy IAM state from loading")
	}
	if _, ok := fresh.GetUser(c.AccessKey); ok {
		t.Fatal("cleanup failure admitted an expired service account")
	}
	r, err := loadIAMRevision(ctx, fresh, path)
	mustIAM(t, err)
	if r.Deleted || !r.Credentials.IsExpired() {
		t.Fatal("failed cleanup lost the existing expired revision")
	}
}
