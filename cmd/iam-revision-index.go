// Copyright (c) 2026 PGSTY
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/internal/auth"
)

// This index is rebuilt by the existing IAM loaders and updated by successful
// storage operations. It avoids a second full IAM walk during every heal pass.
// It is an optimization of the durable records, never a reason to delete them.
// The index contains no secrets or grants.
type iamParentRevision struct {
	deleted bool
	before  time.Time
}

type iamRevisionIndex struct {
	mu         sync.RWMutex
	items      map[string]iamRevision
	parents    map[string]iamParentRevision
	floors     map[string]time.Time
	generation uint64
}

func (idx *iamRevisionIndex) observe(path string, data []byte) {
	if !strings.HasPrefix(path, iamConfigPrefix+"/") {
		return
	}
	var r iamRevision
	if json.Unmarshal(data, &r) != nil {
		return // The caller reports malformed data using its normal decoder.
	}
	r.Credentials = auth.Credentials{ParentUser: r.Credentials.ParentUser, Expiration: r.Credentials.Expiration}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if strings.HasPrefix(path, iamConfigUsersPrefix) {
		// Keep a compact name-keyed view for the authentication hot path;
		// constructing a config path on every S3 request allocates needlessly.
		defer func() {
			name := strings.TrimSuffix(strings.TrimPrefix(path, iamConfigUsersPrefix), "/"+iamIdentityFile)
			if current, ok := idx.items[path]; ok {
				if idx.parents == nil {
					idx.parents = make(map[string]iamParentRevision)
				}
				idx.parents[name] = iamParentRevision{deleted: current.Deleted, before: current.RevokedBefore}
			} else {
				delete(idx.parents, name)
			}
		}()
	}
	if floor, ok := idx.floors[path]; ok && r.timestamp().Before(floor) {
		return
	}
	if previous, ok := idx.items[path]; ok {
		// A concurrent read that began before a write must not roll it back.
		if previous.timestamp().After(r.timestamp()) || (previous.Deleted && !r.Deleted && !r.timestamp().After(previous.timestamp())) {
			return
		}
		if previous.RevokedBefore.After(r.RevokedBefore) {
			r.RevokedBefore = previous.RevokedBefore
		}
		if previous.timestamp().Equal(r.timestamp()) && previous.Deleted == r.Deleted && previous.RevokedBefore.Equal(r.RevokedBefore) {
			return
		}
	}
	if r.Deleted && !r.ExpiresAt.IsZero() && UTCNow().After(r.ExpiresAt) {
		if _, tracked := idx.items[path]; tracked {
			delete(idx.items, path)
			idx.generation++
		}
		delete(idx.floors, path)
		return
	}
	if !r.Deleted && r.RevokedBefore.IsZero() {
		_, tracked := idx.items[path]
		_, hasFloor := idx.floors[path]
		if tracked || hasFloor {
			if idx.floors == nil {
				idx.floors = make(map[string]time.Time)
			}
			idx.floors[path] = r.timestamp()
		}
		if tracked {
			delete(idx.items, path)
			idx.generation++
		}
		return
	}
	if idx.items == nil {
		idx.items = make(map[string]iamRevision)
	}
	idx.items[path] = r
	delete(idx.floors, path)
	idx.generation++
}

func (idx *iamRevisionIndex) get(path string) iamRevision {
	if idx == nil {
		return iamRevision{}
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.items[path]
}

func (idx *iamRevisionIndex) snapshot() map[string]iamRevision {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	for path, r := range idx.items {
		if r.Deleted && !r.ExpiresAt.IsZero() && UTCNow().After(r.ExpiresAt) {
			delete(idx.items, path)
			delete(idx.floors, path)
			idx.generation++
		}
	}
	return maps.Clone(idx.items)
}

func (idx *iamRevisionIndex) count() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.items)
}

// A process-local generation plus the protocol's instance ID is sufficient
// for acknowledgements. Avoid hashing the entire index on every IAM write.
func (idx *iamRevisionIndex) digest() string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return fmt.Sprintf("%x:%x", idx.generation, len(idx.items))
}

func (idx *iamRevisionIndex) forget(path string) {
	idx.mu.Lock()
	if _, ok := idx.items[path]; ok {
		delete(idx.items, path)
		idx.generation++
	}
	delete(idx.floors, path)
	if strings.HasPrefix(path, iamConfigUsersPrefix) {
		delete(idx.parents, strings.TrimSuffix(strings.TrimPrefix(path, iamConfigUsersPrefix), "/"+iamIdentityFile))
	}
	idx.mu.Unlock()
}

func (c *iamCache) userRevocation(user string) iamRevision {
	r := c.revisions.parentRevision(user)
	if u, ok := c.iamUsersMap[user]; ok && u.RevokedBefore.After(r.RevokedBefore) {
		r.RevokedBefore = u.RevokedBefore
	}
	return r
}

func (c *iamCache) groupMemberAllowed(member string, grantedAt, groupBoundary time.Time) bool {
	r := c.userRevocation(member)
	return !r.Deleted && (r.RevokedBefore.IsZero() || grantedAt.After(r.RevokedBefore)) && (groupBoundary.IsZero() || grantedAt.After(groupBoundary))
}

func iamMappingParentPath(path string) string {
	kind, name, ok := strings.Cut(strings.TrimPrefix(path, iamConfigPolicyDBPrefix), "/")
	if !ok {
		return ""
	}
	name = strings.TrimSuffix(name, ".json")
	switch kind {
	case "users", "sts-users":
		return getUserIdentityPath(name, regUser)
	case "service-accounts":
		return getUserIdentityPath(name, svcUser)
	case "groups":
		return getGroupInfoPath(name)
	}
	return ""
}

func (idx *iamRevisionIndex) mappingAllowed(path string, mp MappedPolicy) bool {
	if mp.Deleted || idx.get(path).Deleted {
		return false
	}
	r := idx.get(iamMappingParentPath(path))
	return !r.Deleted && (r.RevokedBefore.IsZero() || mp.UpdatedAt.After(r.RevokedBefore))
}

// Apply the persisted commit boundary even before dependent cache cleanup has
// completed. The map namespace is part of the authorization record's identity.
func (c *iamCache) cachedMappedPolicy(name string, userType IAMUserType, isGroup bool) (MappedPolicy, bool) {
	var mp MappedPolicy
	var ok bool
	switch {
	case isGroup:
		mp, ok = c.iamGroupPolicyMap.Load(name)
	case userType == stsUser:
		mp, ok = c.iamSTSPolicyMap.Load(name)
	default:
		mp, ok = c.iamUserPolicyMap.Load(name)
	}
	if !ok || !c.revisions.mappingAllowed(getMappedPolicyPath(name, userType, isGroup), mp) {
		return MappedPolicy{}, false
	}
	return mp, true
}

func (idx *iamRevisionIndex) parentRevision(user string) iamRevision {
	if idx == nil {
		return iamRevision{}
	}
	idx.mu.RLock()
	p := idx.parents[user]
	idx.mu.RUnlock()
	return iamRevision{Deleted: p.deleted, RevokedBefore: p.before}
}
