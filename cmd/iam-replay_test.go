package cmd

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/pgsty/silo-pkg/v3/policy"
)

func TestReviewIAMRevokedUserReplay(t *testing.T) {
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
	user := "review-revoked-user"
	req := madmin.AddOrUpdateUserReq{SecretKey: "review-valid-password", Status: madmin.AccountEnabled}
	created, err := globalIAMSys.CreateUser(ctx, user, req)
	if err != nil {
		t.Fatal(err)
	}
	policyAt, err := globalIAMSys.PolicyDBSet(ctx, user, "readwrite", regUser, false)
	if err != nil {
		t.Fatal(err)
	}
	args := policy.Args{AccountName: user, Action: policy.GetObjectAction, BucketName: "review-bucket", ObjectName: "review-object"}
	if !globalIAMSys.IsAllowed(args) {
		t.Fatal("seed must allow object read")
	}
	if err := globalIAMSys.DeleteUser(ctx, user, false); err != nil {
		t.Fatal(err)
	}
	if err := globalIAMSys.store.LoadIAMCache(ctx, false); err != nil {
		t.Fatal(err)
	}
	if globalIAMSys.IsAllowed(args) {
		t.Fatal("deletion did not remove initial permission")
	}
	if err := globalSiteReplicationSys.PeerIAMUserChangeHandler(ctx, &madmin.SRIAMUser{AccessKey: user, UserReq: &req}, created); err != nil {
		t.Fatal(err)
	}
	if _, err := globalIAMSys.GetUserInfo(ctx, user); !errors.Is(err, errNoSuchUser) {
		t.Errorf("revoked user restored by an older replicated create, GetUserInfo error = %v", err)
	}
	if err := globalSiteReplicationSys.PeerPolicyMappingHandler(ctx, &madmin.SRPolicyMapping{UserOrGroup: user, UserType: int(regUser), Policy: "readwrite"}, policyAt); err != nil {
		t.Fatal(err)
	}
	if globalIAMSys.IsAllowed(args) {
		t.Error("older replicated identity and policy events restored revoked S3 read permission")
	}
}

func TestReviewIAMSourceTimestampOrder(t *testing.T) {
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
	user := "review-ordered-user"
	req := madmin.AddOrUpdateUserReq{SecretKey: "review-valid-password", Status: madmin.AccountEnabled}
	origin := UTCNow().Add(-time.Hour)
	if err := globalSiteReplicationSys.PeerIAMUserChangeHandler(ctx, &madmin.SRIAMUser{AccessKey: user, UserReq: &req}, origin); err != nil {
		t.Fatal(err)
	}
	if err := globalSiteReplicationSys.PeerIAMUserChangeHandler(ctx, &madmin.SRIAMUser{AccessKey: user, IsDeleteReq: true}, origin.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := globalIAMSys.GetUserInfo(ctx, user); !errors.Is(err, errNoSuchUser) {
		t.Fatalf("newer source deletion skipped after delayed creation, GetUserInfo error = %v", err)
	}
}

// A user's old group grant must not return after deletion and deliberate recreation.
func TestR3CandidateOldGroupReplayAfterRecreation(t *testing.T) {
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
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	user, group := "r3-group-member", "r3-granting-group"
	origin := UTCNow().Add(-time.Hour)
	req := madmin.AddOrUpdateUserReq{SecretKey: "valid-r3-user-password", Status: madmin.AccountEnabled}
	peer := &globalSiteReplicationSys
	must(peer.PeerIAMUserChangeHandler(ctx, &madmin.SRIAMUser{AccessKey: user, UserReq: &req}, origin))
	add := &madmin.SRGroupInfo{UpdateReq: madmin.GroupAddRemove{Group: group, Members: []string{user}}}
	must(peer.PeerGroupInfoChangeHandler(ctx, add, origin.Add(time.Minute)))
	must(peer.PeerPolicyMappingHandler(ctx, &madmin.SRPolicyMapping{UserOrGroup: group, IsGroup: true, UserType: int(regUser), Policy: "readwrite"}, origin.Add(time.Minute)))
	args := policy.Args{AccountName: user, Action: policy.GetObjectAction, BucketName: "r3-bucket", ObjectName: "probe"}
	if !globalIAMSys.IsAllowed(args) {
		t.Fatal("fixture must grant through group")
	}
	must(peer.PeerIAMUserChangeHandler(ctx, &madmin.SRIAMUser{AccessKey: user, IsDeleteReq: true}, origin.Add(2*time.Minute)))
	must(peer.PeerIAMUserChangeHandler(ctx, &madmin.SRIAMUser{AccessKey: user, UserReq: &req}, origin.Add(3*time.Minute)))
	must(globalIAMSys.store.LoadIAMCache(ctx, false))
	if globalIAMSys.IsAllowed(args) {
		t.Fatal("recreation must start without deleted group membership")
	}
	must(peer.PeerGroupInfoChangeHandler(ctx, add, origin.Add(time.Minute)))
	must(globalIAMSys.store.LoadIAMCache(ctx, false))
	if globalIAMSys.IsAllowed(args) {
		t.Fatal("old group event restored the deleted user's read grant after recreation and durable reload")
	}
}

// The user delete is also a revocation of its earlier group memberships.
func TestR3CandidateLateDeleteRetainsOldGroupGrant(t *testing.T) {
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
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	user, group := "r3-late-member", "r3-late-group"
	origin := UTCNow().Add(-time.Hour)
	req := madmin.AddOrUpdateUserReq{SecretKey: "valid-r3-user-password", Status: madmin.AccountEnabled}
	peer := &globalSiteReplicationSys
	must(peer.PeerIAMUserChangeHandler(ctx, &madmin.SRIAMUser{AccessKey: user, UserReq: &req}, origin))
	must(peer.PeerGroupInfoChangeHandler(ctx, &madmin.SRGroupInfo{UpdateReq: madmin.GroupAddRemove{Group: group, Members: []string{user}}}, origin.Add(time.Minute)))
	must(peer.PeerPolicyMappingHandler(ctx, &madmin.SRPolicyMapping{UserOrGroup: group, IsGroup: true, UserType: int(regUser), Policy: "readwrite"}, origin.Add(time.Minute)))
	args := policy.Args{AccountName: user, Action: policy.GetObjectAction, BucketName: "r3-bucket", ObjectName: "probe"}
	if !globalIAMSys.IsAllowed(args) {
		t.Fatal("fixture must grant through group")
	}
	must(peer.PeerIAMUserChangeHandler(ctx, &madmin.SRIAMUser{AccessKey: user, UserReq: &req}, origin.Add(3*time.Minute)))
	must(peer.PeerIAMUserChangeHandler(ctx, &madmin.SRIAMUser{AccessKey: user, IsDeleteReq: true}, origin.Add(2*time.Minute)))
	must(globalIAMSys.store.LoadIAMCache(ctx, false))
	if _, ok := globalIAMSys.GetUser(ctx, user); !ok {
		t.Fatal("newer identity must survive")
	}
	if globalIAMSys.IsAllowed(args) {
		t.Fatal("late user deletion retained the older group grant on the recreated identity")
	}
}
