// Copyright (c) 2026 PGSTY
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/grid"
	xnet "github.com/pgsty/silo-pkg/v3/net"
)

// Count physical saves: comparing timestamps alone would miss identical
// tombstones being rewritten on every heal pass.
type iamRevisionWriteCounter struct {
	IAMStorageAPI
	data   []byte
	writes int
}

func (s *iamRevisionWriteCounter) loadIAMConfig(_ context.Context, item any, _ string) error {
	return json.Unmarshal(s.data, item)
}

func (s *iamRevisionWriteCounter) saveIAMConfig(_ context.Context, item any, _ string, _ ...options) error {
	data, err := json.Marshal(item)
	if err == nil {
		s.data = data
		s.writes++
	}
	return err
}

func TestIAMRevocationTombstoneReplayIsIdempotent(t *testing.T) {
	at := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	for _, record := range []struct {
		name string
		new  func(bool) any
	}{
		{"user", func(deleted bool) any { return &UserIdentity{Version: 1, Deleted: deleted} }},
		{"group", func(deleted bool) any { return &GroupInfo{Version: 1, Deleted: deleted} }},
		{"policy", func(deleted bool) any { return &PolicyDoc{Version: 1, Deleted: deleted} }},
		{"mapping", func(deleted bool) any { return &MappedPolicy{Version: 1, Deleted: deleted} }},
	} {
		t.Run(record.name, func(t *testing.T) {
			data, err := json.Marshal(iamRevision{Deleted: true, UpdatedAt: at, RevokedBefore: at})
			if err != nil {
				t.Fatal(err)
			}
			store := &iamRevisionWriteCounter{data: data}
			ctx := context.Background()
			for range 3 {
				// Site heal carries the original timestamp. Sibling notifications
				// have no timestamp; both must leave an applied deletion untouched.
				for _, replay := range []context.Context{withIAMReplicationTime(ctx, at), ctx} {
					if err := saveIAMRevision(replay, store, record.name, record.new(true)); err != nil {
						t.Fatal(err)
					}
				}
			}
			if store.writes != 0 || string(store.data) != string(data) {
				t.Fatalf("replayed tombstone changed storage: writes=%d, record=%s", store.writes, store.data)
			}
			for _, deleted := range []bool{false, true} {
				err := saveIAMRevision(withIAMReplicationTime(ctx, at.Add(-time.Second)), store, record.name, record.new(deleted))
				if !errors.Is(err, errIAMStaleUpdate) {
					t.Fatalf("older event accepted, deleted=%t: %v", deleted, err)
				}
			}
			if err := saveIAMRevision(withIAMReplicationTime(ctx, at), store, record.name, record.new(false)); !errors.Is(err, errIAMStaleUpdate) {
				t.Fatalf("equal-time recreation accepted: %v", err)
			}
			newer := at.Add(time.Minute)
			if err := saveIAMRevision(withIAMReplicationTime(ctx, newer), store, record.name, record.new(true)); err != nil {
				t.Fatal(err)
			}
			r, err := loadIAMRevision(ctx, store, record.name)
			if err != nil || store.writes != 1 || !r.timestamp().Equal(newer) || !r.Deleted {
				t.Fatalf("newer deletion did not advance storage: writes=%d, revision=%+v, error=%v", store.writes, r, err)
			}
			if err := saveIAMRevision(withIAMReplicationTime(ctx, newer.Add(time.Minute)), store, record.name, record.new(false)); err != nil {
				t.Fatalf("newer recreation rejected: %v", err)
			}
		})
	}
}

// Run with both object storage and etcd through TestIAMRevocation*Lifecycle.
// The object-store case uses the real peer RPC and deletion handler, so a
// spurious notification actually destroys the parent instead of only counting it.
func testIAMRevocationReplayAfterRecreation(ctx context.Context, t *testing.T, sys *IAMSys) {
	t.Helper()
	peer := &globalSiteReplicationSys
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	user := "heal-recreated-parent"
	req := madmin.AddOrUpdateUserReq{SecretKey: "valid-test-password", Status: madmin.AccountEnabled}
	origin := UTCNow().Add(-time.Hour).Truncate(time.Millisecond)
	deleted, recreated := origin.Add(time.Minute), origin.Add(3*time.Minute)
	create := func(at time.Time) {
		must(peer.PeerIAMUserChangeHandler(ctx, &madmin.SRIAMUser{AccessKey: user, UserReq: &req}, at))
	}
	revoke := func(at time.Time) {
		must(peer.PeerIAMUserChangeHandler(ctx, &madmin.SRIAMUser{AccessKey: user, IsDeleteReq: true}, at))
	}
	create(origin)
	revoke(deleted)
	create(recreated)
	child, _, err := sys.NewServiceAccount(ctx, user, nil, newServiceAccountOpts{
		accessKey: "heal-recreated-child", secretKey: "valid-service-password",
	})
	must(err)

	tg, err := grid.SetupTestGrid(2)
	must(err)
	t.Cleanup(tg.Cleanup)
	var deletes atomic.Int32
	server := &peerRESTServer{}
	must(deleteUserRPC.Register(tg.Managers[1], func(req *grid.MSS) (grid.NoPayload, *grid.RemoteErr) {
		deletes.Add(1)
		return server.DeleteUserHandler(req)
	}))
	// Future user updates still use the normal peer reload notification.
	must(loadUserRPC.Register(tg.Managers[1], server.LoadUserHandler))
	host, err := xnet.ParseHost(strings.TrimPrefix(tg.Hosts[1], "http://"))
	must(err)
	previousNotifications := globalNotificationSys
	globalNotificationSys = &NotificationSys{peerClients: []*peerRESTClient{{
		host: host,
		gridConn: func() *grid.Connection {
			return tg.Managers[0].Connection(tg.Hosts[1])
		},
	}}}
	t.Cleanup(func() { globalNotificationSys = previousNotifications })
	assertLive := func(key string) {
		t.Helper()
		if _, ok := sys.GetUser(ctx, key); !ok {
			t.Fatalf("live credential %s lost during deletion replay", key)
		}
	}
	assertNoDelete := func() {
		t.Helper()
		if n := deletes.Load(); n != 0 {
			t.Fatalf("retained revocation sent %d destructive sibling notifications", n)
		}
	}
	for range 3 {
		r, err := loadIAMRevision(ctx, sys.store, getUserIdentityPath(user, regUser))
		must(err)
		item, ok := iamDeletionItem(getUserIdentityPath(user, regUser), r)
		if !ok || item.IAMUser == nil || !item.UpdatedAt.Equal(deleted) {
			t.Fatal("recreated user lost its durable revocation replay")
		}
		must(peer.PeerIAMUserChangeHandler(ctx, item.IAMUser, item.UpdatedAt))
		must(sys.store.LoadIAMCache(ctx, false))
		assertLive(user)
		assertLive(child.AccessKey)
		assertNoDelete()
	}

	// A divergent site sends a previously unseen revocation between our old
	// boundary and recreation. Retain it and revoke old children, but never
	// turn it into an unversioned delete of the recreated parent.
	delayed := deleted.Add(time.Minute)
	revoke(delayed)
	assertNoDelete()
	must(sys.store.LoadIAMCache(ctx, false))
	assertLive(user)
	if _, ok := sys.GetUser(ctx, child.AccessKey); ok {
		t.Fatal("child from before the delayed revocation remains usable")
	}
	r, err := loadIAMRevision(ctx, sys.store, getUserIdentityPath(user, regUser))
	must(err)
	if r.Deleted || !r.RevokedBefore.Equal(delayed) || !r.timestamp().Equal(recreated) {
		t.Fatalf("retained revocation damaged the recreated identity: %+v", r)
	}

	// A genuinely newer deletion must still reach siblings and remove the
	// parent plus credentials issued under its latest revocation boundary.
	fresh, _, err := sys.NewServiceAccount(ctx, user, nil, newServiceAccountOpts{
		accessKey: "heal-fresh-child", secretKey: "valid-service-password",
	})
	must(err)
	latest := recreated.Add(time.Minute)
	revoke(latest)
	wantDeletes := int32(1)
	if sys.HasWatcher() {
		wantDeletes = 0
	}
	if n := deletes.Load(); n != wantDeletes {
		t.Fatalf("new deletion notifications=%d, want %d", n, wantDeletes)
	}
	for _, key := range []string{user, fresh.AccessKey} {
		if _, ok := sys.GetUser(ctx, key); ok {
			t.Fatalf("newer deletion left credential %s usable", key)
		}
	}
	// Exercise the actual sibling handler again against the already persisted
	// tombstone. Its context has no revision; it must not re-stamp the record.
	_, remoteErr := server.DeleteUserHandler(grid.NewMSSWith(map[string]string{peerRESTUser: user}))
	if remoteErr != nil {
		t.Fatal(remoteErr)
	}
	r, err = loadIAMRevision(ctx, sys.store, getUserIdentityPath(user, regUser))
	must(err)
	if !r.Deleted || !r.timestamp().Equal(latest) {
		t.Fatalf("sibling re-stamped the tombstone: got %s, want %s", r.timestamp(), latest)
	}
	create(latest.Add(time.Minute))
	_, remoteErr = server.DeleteUserHandler(grid.NewMSSWith(map[string]string{peerRESTUser: user}))
	if remoteErr != nil {
		t.Fatal(remoteErr)
	}
	assertLive(user)
	revoke(latest)
	must(sys.store.LoadIAMCache(ctx, false))
	assertLive(user)
}

// Counts what a retained revocation actually sends to sibling nodes.
func TestIAMRevocationRetainedReloadsSibling(t *testing.T) {
	resetTestGlobals()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	disks, err := getRandomDisks(1)
	if err != nil {
		t.Fatal(err)
	}
	obj, _, err := initObjectLayer(ctx, mustGetPoolEndpoints(0, disks...))
	if err != nil {
		t.Fatal(err)
	}
	initAllSubsystems(ctx)
	globalIAMSys.Init(ctx, obj, nil, 2*time.Second)
	defer os.RemoveAll(disks[0])
	defer obj.Shutdown(ctx)
	defer resetTestGlobals()

	sys, peer := globalIAMSys, &globalSiteReplicationSys
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	user := "retained-parent"
	req := madmin.AddOrUpdateUserReq{SecretKey: "valid-test-password", Status: madmin.AccountEnabled}
	origin := UTCNow().Add(-time.Hour).Truncate(time.Millisecond)
	deleted, recreated := origin.Add(time.Minute), origin.Add(3*time.Minute)
	create := func(at time.Time) {
		must(peer.PeerIAMUserChangeHandler(ctx, &madmin.SRIAMUser{AccessKey: user, UserReq: &req}, at))
	}
	revoke := func(at time.Time) {
		must(peer.PeerIAMUserChangeHandler(ctx, &madmin.SRIAMUser{AccessKey: user, IsDeleteReq: true}, at))
	}
	create(origin)
	revoke(deleted)
	create(recreated)
	child, _, err := sys.NewServiceAccount(ctx, user, nil, newServiceAccountOpts{
		accessKey: "retained-child", secretKey: "valid-service-password",
	})
	must(err)

	// A sibling shares persistent state but has an independent IAM cache.
	sibling := &IAMStoreSys{IAMStorageAPI: newIAMObjectStore(obj, sys.usersSysType)}
	must(sibling.LoadIAMCache(ctx, false))
	if _, ok := sibling.GetUser(child.AccessKey); !ok {
		t.Fatal("sibling fixture did not load child")
	}
	tg, err := grid.SetupTestGrid(2)
	must(err)
	t.Cleanup(tg.Cleanup)
	var deletes, loads atomic.Int32
	server := &peerRESTServer{}
	must(deleteUserRPC.Register(tg.Managers[1], func(r *grid.MSS) (grid.NoPayload, *grid.RemoteErr) {
		deletes.Add(1)
		return server.DeleteUserHandler(r)
	}))
	must(loadUserRPC.Register(tg.Managers[1], func(r *grid.MSS) (grid.NoPayload, *grid.RemoteErr) {
		loads.Add(1)
		// LoadUserHandler delegates to this same cache reload method.
		if err := sibling.UserNotificationHandler(ctx, r.Get(peerRESTUser), regUser); err != nil {
			return grid.NoPayload{}, grid.NewRemoteErr(err)
		}
		return grid.NoPayload{}, nil
	}))
	host, err := xnet.ParseHost(strings.TrimPrefix(tg.Hosts[1], "http://"))
	must(err)
	prev := globalNotificationSys
	globalNotificationSys = &NotificationSys{peerClients: []*peerRESTClient{{
		host:     host,
		gridConn: func() *grid.Connection { return tg.Managers[0].Connection(tg.Hosts[1]) },
	}}}
	t.Cleanup(func() { globalNotificationSys = prev })

	delayed := deleted.Add(time.Minute)
	revoke(delayed)

	t.Logf("sibling notifications after a retained revocation: destructive=%d reload=%d", deletes.Load(), loads.Load())
	if deletes.Load() != 0 {
		t.Errorf("destructive sibling delete sent: %d", deletes.Load())
	}
	if loads.Load() == 0 {
		t.Errorf("retained revocation did not notify the sibling")
	}
	if _, ok := sibling.GetUser(child.AccessKey); ok {
		t.Error("sibling still resolves revoked child")
	}
	if _, ok := sibling.GetUser(user); !ok {
		t.Error("sibling lost live parent")
	}
	if _, ok := sys.store.GetUser(user); !ok {
		t.Error("live parent lost")
	}
	if _, ok := sys.store.GetUser(child.AccessKey); ok {
		t.Error("revoked child still resolves on the receiving node")
	}
}
