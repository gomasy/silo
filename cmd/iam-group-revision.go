// Copyright (c) 2026 PGSTY
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"context"
	"maps"
	"slices"
	"time"

	"github.com/minio/minio-go/v7/pkg/set"
)

type (
	iamGroupGrantsKey   struct{}
	iamGroupMutationKey struct{}
	iamGroupMutation    struct {
		Members    []string
		Remove     bool
		StatusOnly bool
	}
)

// Merge the intended mutation with the record read under the distributed
// revision lock, not the older cache used to prepare the request.
func mergeIAMGroupMutation(ctx context.Context, previous GroupInfo, next *GroupInfo) {
	op, ok := ctx.Value(iamGroupMutationKey{}).(iamGroupMutation)
	if !ok || previous.Deleted || previous.Version == 0 {
		return
	}
	members := set.CreateStringSet(previous.Members...)
	grants := maps.Clone(previous.MemberGrants)
	if grants == nil {
		grants = make(map[string]time.Time)
	}
	switch {
	case op.StatusOnly:
		// Only the status changes.
	case op.Remove:
		for _, member := range op.Members {
			members.Remove(member)
			delete(grants, member)
		}
		next.Status = previous.Status
	default:
		requested := set.CreateStringSet(next.Members...)
		for _, member := range op.Members {
			if !requested.Contains(member) {
				continue
			}
			at := next.MemberGrants[member]
			if at.Before(grants[member]) {
				continue
			}
			members.Add(member)
			grants[member] = at
		}
		next.Status = previous.Status
	}
	next.Members, next.MemberGrants = members.ToSlice(), grants
	slices.Sort(next.Members)
}

// A non-nil map is supplied by the versioned peer envelope, including for
// snapshots. Missing times are unknown, never the snapshot's newer timestamp.
func withIAMGroupGrants(ctx context.Context, grants map[string]time.Time) context.Context {
	return context.WithValue(ctx, iamGroupGrantsKey{}, grants)
}

func (c *iamCache) effectiveGroupMembers(gi GroupInfo) []string {
	var members []string
	for _, member := range gi.Members {
		if c.groupMemberAllowed(member, gi.MemberGrants[member], gi.RevokedBefore) {
			members = append(members, member)
		}
	}
	return members
}

func (c *iamCache) effectiveUserGroups(user string) []string {
	var groups []string
	for group := range c.iamUserGroupMemberships[user] {
		gi, ok := c.iamGroupsMap[group]
		r := c.revisions.get(getGroupInfoPath(group))
		if r.RevokedBefore.After(gi.RevokedBefore) {
			gi.RevokedBefore = r.RevokedBefore
		}
		if ok && !r.Deleted && c.groupMemberAllowed(user, gi.MemberGrants[user], gi.RevokedBefore) {
			groups = append(groups, group)
		}
	}
	return groups
}

func (c *iamCache) addGroupMembers(ctx context.Context, gi GroupInfo, members []string) (GroupInfo, error) {
	grants, versioned := ctx.Value(iamGroupGrantsKey{}).(map[string]time.Time)
	if boundary, ok := ctx.Value(iamRecordBoundaryKey{}).(time.Time); ok && boundary.After(gi.RevokedBefore) {
		gi.RevokedBefore = boundary
	}
	origin, replicated := iamReplicationTime(ctx)
	gi.Members = slices.Clone(gi.Members)
	gi.MemberGrants = maps.Clone(gi.MemberGrants)
	if gi.MemberGrants == nil {
		gi.MemberGrants = make(map[string]time.Time)
	}
	current := set.CreateStringSet(gi.Members...)
	gi.UpdatedAt = UTCNow()
	if replicated {
		gi.UpdatedAt = origin
	}
	for _, member := range members {
		at := gi.UpdatedAt
		r := c.userRevocation(member)
		if replicated {
			switch {
			case versioned:
				at = grants[member]
				if at.After(origin) {
					return gi, errInvalidArgument
				}
			case !r.RevokedBefore.IsZero() || r.Deleted || !gi.RevokedBefore.IsZero():
				// Legacy snapshots cannot prove a post-revocation grant.
				continue
			case current.Contains(member):
				continue
			}
			if !c.groupMemberAllowed(member, at, gi.RevokedBefore) {
				continue
			}
		} else {
			if current.Contains(member) && c.groupMemberAllowed(member, gi.MemberGrants[member], gi.RevokedBefore) {
				continue // Editing the group is not reissuing every grant.
			}
			if !at.After(gi.RevokedBefore) {
				at = gi.RevokedBefore.Add(time.Nanosecond)
			}
			if !at.After(r.RevokedBefore) {
				at = r.RevokedBefore.Add(time.Nanosecond)
			}
			if !at.After(gi.MemberGrants[member]) {
				at = gi.MemberGrants[member].Add(time.Nanosecond)
			}
			if at.After(gi.UpdatedAt) {
				gi.UpdatedAt = at
			}
		}
		u, ok := c.iamUsersMap[member]
		if !ok {
			return gi, errNoSuchUser
		}
		if u.Credentials.IsTemp() || u.Credentials.IsServiceAccount() {
			return gi, errIAMActionNotAllowed
		}
		if previous := gi.MemberGrants[member]; previous.After(at) {
			continue
		}
		current.Add(member)
		gi.MemberGrants[member] = at
	}
	gi.Members = current.ToSlice()
	slices.Sort(gi.Members)
	return gi, nil
}
