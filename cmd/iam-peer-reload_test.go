// Copyright (c) 2026 PGSTY
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/grid"
	"github.com/pgsty/silo-pkg/v3/policy"
)

// Two independent IAM caches share the same real object backend, as sibling
// nodes do. Deliver the actual peer handler only after the source committed.
func TestIAMPeerDeleteNotificationReloadsCommittedState(t *testing.T) {
	for _, name := range []string{"deleted", "recreated", "recreated_without_grant"} {
		recreate := name != "deleted"
		t.Run(name, func(t *testing.T) {
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
			globalObjLayerMutex.Lock()
			globalObjectAPI = obj
			globalObjLayerMutex.Unlock()
			must := func(err error) {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
			}
			source := globalIAMSys
			const user = "peer-reload-user"
			req := madmin.AddOrUpdateUserReq{SecretKey: "original-test-password", Status: madmin.AccountEnabled}
			_, err = source.CreateUser(ctx, user, req)
			must(err)
			_, err = source.PolicyDBSet(ctx, user, "readwrite", regUser, false)
			must(err)
			_, err = source.AddUsersToGroup(ctx, "peer-reload-group", []string{user})
			must(err)
			_, err = source.PolicyDBSet(ctx, "peer-reload-group", "readwrite", regUser, true)
			must(err)
			svc, _, err := source.NewServiceAccount(ctx, user, nil, newServiceAccountOpts{accessKey: "peer-reload-service", secretKey: "service-test-password"})
			must(err)
			signingKey, err := getTokenSigningKey()
			must(err)
			sts, err := auth.GetNewCredentialsWithMetadata(map[string]any{"exp": UTCNow().Add(time.Hour).Unix(), parentClaim: user}, signingKey)
			must(err)
			sts.ParentUser = user
			_, err = source.SetTempUser(ctx, sts.AccessKey, sts, "")
			must(err)

			siblingStore := &IAMStoreSys{IAMStorageAPI: newIAMObjectStore(obj, MinIOUsersSysType)}
			must(siblingStore.LoadIAMCache(ctx, true))
			must(siblingStore.UserNotificationHandler(ctx, sts.AccessKey, stsUser))
			for _, key := range []string{user, svc.AccessKey, sts.AccessKey} {
				if _, ok := siblingStore.GetUser(key); !ok {
					t.Fatalf("fixture did not load %s", key)
				}
			}
			must(source.DeleteUser(ctx, user, false))
			if recreate {
				req.SecretKey = "recreated-test-password"
				_, err = source.CreateUser(ctx, user, req)
				must(err)
				if name == "recreated" {
					_, err = source.PolicyDBSet(ctx, user, "readonly", regUser, false)
					must(err)
				}
			}

			sibling := &IAMSys{store: siblingStore, usersSysType: MinIOUsersSysType}
			globalIAMSys = sibling
			defer func() { globalIAMSys = source }()
			server := &peerRESTServer{}
			for range 2 {
				_, remoteErr := server.DeleteUserHandler(grid.NewMSSWith(map[string]string{peerRESTUser: user}))
				if remoteErr != nil {
					t.Fatal(remoteErr)
				}
			}
			if recreate {
				u, ok := siblingStore.GetUser(user)
				if !ok || u.Credentials.SecretKey != req.SecretKey {
					t.Fatal("delayed deletion notification removed the recreated user")
				}
				loaded := make(map[string]UserIdentity)
				must(source.store.loadUser(ctx, user, regUser, loaded))
				if loaded[user].Credentials.SecretKey != req.SecretKey {
					t.Fatal("notification changed the persisted recreated identity")
				}
				if allowed := sibling.IsAllowed(policy.Args{AccountName: user, Action: policy.GetObjectAction, BucketName: "bucket", ObjectName: "object"}); allowed != (name == "recreated") {
					t.Fatal("notification did not load the recreated user's current grant")
				}
			}
			if sibling.IsAllowed(policy.Args{AccountName: user, Action: policy.PutObjectAction, BucketName: "bucket", ObjectName: "object"}) {
				t.Fatal("notification retained an old direct or group grant")
			}
			for _, key := range []string{svc.AccessKey, sts.AccessKey} {
				if _, ok := siblingStore.GetUser(key); ok {
					t.Fatalf("notification retained a revoked child: %s", key)
				}
			}
			if !recreate {
				for _, key := range []string{user} {
					if _, ok := siblingStore.GetUser(key); ok {
						t.Fatalf("notification retained a revoked cached identity: %s", key)
					}
				}
				if sibling.IsAllowed(policy.Args{AccountName: user, Action: policy.GetObjectAction, BucketName: "bucket", ObjectName: "object"}) {
					t.Fatal("notification retained the user's old grant")
				}
				cache := siblingStore.rlock()
				member := cache.iamUserGroupMemberships[user].Contains("peer-reload-group")
				siblingStore.runlock()
				if member {
					t.Fatal("notification retained the deleted user's group membership")
				}
			}
		})
	}
}
