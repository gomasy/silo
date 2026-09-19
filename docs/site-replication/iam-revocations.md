# Durable IAM revocations in site replication

SILO retains the version of a deleted IAM record so an offline site cannot
restore an older identity or grant when it reconnects. This covers built-in
users, service accounts, groups, policy documents, and policy mappings in their
actual user/STS-parent/group namespaces. It also revokes the deleted built-in
user's older service accounts, STS credentials and group grants across deliberate
same-name recreation.

## Ordering and persistence

Deletion records occupy the original IAM configuration paths. They contain the
originating timestamp, `Deleted`, and, where required, `RevokedBefore`; identity
deletion records contain no secret key or session token. Normal IAM listings and
authorization hide deleted records. Object storage and etcd both serialize each
path's version comparison and write with a distributed lock. The source timestamp
is persisted without replacing it with the receiving node's clock.

Older events cannot overwrite a newer revision. At an identical timestamp, a
deletion wins over a live record. A local deliberate recreation receives a
version newer than the stored deletion. An older user/group deletion arriving
after recreation retains its revocation boundary while preserving the newer
live record. Receiving an already-applied tombstone does not rewrite or advance
it. These rules also apply after cold loading persistent IAM state.

The user or group revision is the commit point of deletion. Cleanup of mappings
and children follows that commit; cleanup failure cannot undo it. The API returns
the cleanup error and still notifies sibling nodes to reload the committed
state. Such an error does **not** mean the identity is still active. Retrying the
operation is safe. When the API returns a committed-cleanup error, the admin
handler does not send its immediate cross-site hook; cross-site propagation
relies on the normal deletion-healing retry. Sibling deletion notifications reload current shared state,
so a delayed notification cannot delete a subsequently recreated identity.
Etcd siblings also receive persistent changes through watches. Failed
notifications/watches remain eventual propagation, not a distributed
instantaneous revocation transaction.

Keep node clocks synchronized and monitor offsets. Ordering uses source wall
clock timestamps, with monotonic advancement for local writes to the same path.
It does not establish causal order between concurrent writers at different sites
or resolve conflicting live updates with identical timestamps deterministically.

## Users, child credentials and groups

A recreated user retains `RevokedBefore`. New service accounts and built-in STS
issuances carry a signed `siloParentRevocation` claim identifying the parent
boundary known at issuance. Editing or replaying an old child does not update
this claim. Old children remain invalid even when their own update timestamp is
newer than the parent deletion; children issued for the recreated parent remain
valid. Claims are read from verified tokens on credential load/write.

A newer replicated service-account snapshot can replace an older service with
the same access key, including a deliberate change of owner or secret. Local
duplicate creates remain rejected. Snapshots preserve disabled status and the
service's own revocation boundary, so earlier mappings cannot attach to the new
service. Equal-version retries reload the committed identity without rewriting
it. Periodic live healing compares source versions even when the public status
summary is unchanged, and includes disabled identities as healing sources.
Service snapshots also preserve their absolute expiration. The receiving site
does not reapply the local minimum lifetime for a newly issued credential. A
newer already-expired snapshot still supersedes the old key, is denied by
authentication, and is collected into a durable service tombstone by normal
loading. Cache/claims loading failures are returned for retry, not acknowledged;
after a committed replacement the stale cached secret is evicted immediately.
Collisions with an existing built-in or cached STS identity report an error and
require an explicit administrative resolution; replication cannot change its
credential kind. Concurrent conflicting service updates with exactly the same
timestamp can retain different winners at different sites; the status summary
does not resolve that case.

Each group member has its own `MemberGrants` timestamp. Changing another member
or the group's enabled status does not reissue everyone else's grants. Effective
membership requires the grant to be newer than both the user's and the group's
retained boundaries. Listings and policy evaluation use the same effective
membership. Peer snapshots preserve grant times, including unknown legacy grant
times; they cannot treat a recent snapshot time as a fresh grant to a revoked
identity. A new explicit administrative group grant can restore access.

This does not implement a general conflict-resolution protocol for all group
membership edits. In particular, the inherited live-group snapshot merge adds
members and does not reconcile a missed ordinary member removal. Removing a
member from a live group during a site outage is a separate known limitation;
do not infer that this change resolves it. User/group deletion boundaries and
same-name recreation are covered here.

## Retention and expiration

Permanent identities, groups, policy documents and mappings have no automatic
tombstone TTL. A disconnected peer or an old backup may return arbitrarily late.
Successful replay acknowledgements are an optimization, **not** permission to
garbage-collect this history.

Natural expiration of an immutable STS token physically removes its token-key
record and any legacy token-key mapping, without generating a permanent
tombstone. An early STS revocation is retained until that token's expiration plus
the existing clock-skew allowance. Replaying the same revoked token with a later
event timestamp cannot recreate it. Etcd uses an expiration lease; object storage
collects expired STS tombstones during its existing credential loading/purge.
A record with unknown expiration is retained conservatively. Cleanup writes are
best effort and use a short lock budget. If one fails, that load stops optional
reclamation, reports the error and still loads healthy users; expired credentials
stay denied and retain their existing durable version. The next load retries.
Healthy cleanup has no fixed record quota. The reusable STS
parent policy mapping is not assigned the token's TTL by deletion cleanup.
External-IDP disablement is an early revocation, not natural token expiration,
and its cached STS and service accounts are included in cleanup. Expiring service
accounts retain a durable revision because their access keys are reusable and
an older version might have no expiration.

Direct per-token `RevokeTokens` delivery between sites is not a new guarantee of
this change. The guarantee for built-in parent deletion follows from the durable
parent boundary, including children not currently present in the deleting node's
cache.

## Healing, failures and operational cost

Each process starts one healing loop. Losing its distributed leadership lease
pauses work until leadership is reacquired; it does not permanently terminate
healing. Configuration reloads do not create extra loops. The 30-second interval
starts after leadership is acquired and after each completed pass. Initial lock
retries and endpoint recovery can add further delay; it is not a convergence SLA.

The normal IAM loaders maintain an in-memory index of deletion records and
retained boundaries, without secrets. Healing uses this index; it does not add a
second full walk of `config/iam/` every cycle. Existing full IAM loading still
scans persistent records, including tombstones, at startup and on refresh.

Each peer receives batches of at most 128 records. The sender remembers which
path/version each peer acknowledged. Unrelated new changes at either site do not
reset that progress. A failed batch remains pending while later independent
batches can progress; a lost response may cause safe idempotent replay. A pass
has a bounded duration, and its successful acknowledgements survive that timeout.
The protocol reports each node name and process instance. Switching between
known node instances behind a load balancer preserves acknowledgements. A new
node instance conservatively invalidates prior acknowledgements once; repeated
switches among those known instances do not reset progress. Restore persistent state only with the affected processes
stopped, so a restore cannot reuse an old process acknowledgement.

Steady-state healing still traverses/sorts the retained in-memory set and checks
the peer's protocol status. It suppresses repeated deletion PUTs once acknowledged.
Memory use scales with retained paths and peers; startup storage reads scale with
history. This release does not provide general history compaction.
After upgrading, older live records whose receiving sites originally assigned
different timestamps can require an initial reconciliation wave. Allow for its
storage writes and sibling notifications when planning the maintenance window.

The cluster IAM metrics include `revocation_records`,
`revocation_heal_failures`, `revocation_heal_duration_millis`, and
`revocation_heal_last_success_timestamp_seconds`. Errors are also logged. A
nominal 30-second scheduler interval is not a convergence deadline: outages,
large backlogs, lock contention and failed requests can require more passes.

Cached credential lookup checks the in-memory parent revision index and performs
no additional storage read. STS issuance and cold credential loading still consult
the persistent parent revision. Revision I/O and distributed lock waits release
the IAM cache lock while a separate local writer mutex preserves write order.
These operations have bounded contexts, including etcd lock and lease cleanup.

## Protocol and supported upgrade

The server-owned versioned route is
`/minio/admin/v3/site-replication/peer/iam-revisions`. It carries source versions,
member grant times and distinct user/group revocation items without changing the
admin client SDK or S3 API. Older servers reject this route. The sender reports
the failure and does not fall back to a route that would discard the metadata.
The existing legacy IAM route remains readable for best-effort compatibility;
this does not confer the new guarantees on an older peer.

All participating servers must be upgraded for the guarantee in this document.
Mixed old/new nodes sharing an IAM backend and rolling downgrade are unsupported:
older binaries do not interpret tombstones or signed parent boundaries correctly.
Use a maintenance window for coordinated upgrade:

1. Pause IAM changes and isolate any offline site or backup whose state is unknown.
2. Back up each site's complete IAM storage and required encryption material. A
   live IAM admin export omits deletion history and is not an adequate backup.
3. Stop all nodes sharing each site's IAM backend, replace their binaries, and
   restart them on the upgraded version. Complete this for every participating
   site before relying on the new revocation semantics.
4. Check IAM loading, site-replication errors and revocation convergence. Verify
   representative old credentials are denied and deliberately reissued ones work.
5. Resolve pre-upgrade revocations explicitly. Absence cannot reconstruct an
   already-lost deletion version: remove surviving old records on the sites that
   still have them, and rebuild stale offline peers from approved state before
   admitting them. Do not reconnect an unknown old snapshot just to discover its
   deleted credentials.

Credentials issued by an older server for a recreated parent lack the required
signed boundary and must be reissued by an upgraded server. Parents without any
retained revocation history preserve existing credential behavior.

A pristine built-in policy remains protected from local deletion. If an
administrator explicitly overrides that policy and later deletes the override,
the durable deletion now suppresses automatic recreation of the built-in policy
on reload. This prevents reload from undoing the deletion. Restore the policy by
an explicit policy-create operation if desired. Local deletion of a nonexistent
policy remains idempotent and does not create a new tombstone; replicated
unknown deletions retain their version.

For rollback, stop and isolate the affected sites and assess changes since the
backup before restoring compatible state. Restoring an older backup can itself
lose later revocations and requires reconciliation/rekeying before access is
reopened. Do not delete tombstones online or convert only live IAM records to
make an older binary start. Server, client, Console, package and deployment
acceptance remain separate delivery gates of the maintained PGSTY stack.

## Regression and performance checks

Focused coverage is in `iam-revocation_test.go`, `iam-revision_test.go`,
`iam-revision-lock_test.go`, `iam-revision-boundary_test.go`,
`iam-replication-protocol_test.go`, `iam-credential-retention_test.go`,
`iam-peer-reload_test.go`, and `iam-replay_test.go`. Set
`SILO_TEST_IAM_REVOCATION_ETCD` to a disposable etcd endpoint to include backend
lifecycle/locking/boundary tests; they use isolated key namespaces.

`BenchmarkIAMCachedCredential` and `BenchmarkIAMSetTempUser` can be run against
the pre-change source for a comparable local baseline.
`BenchmarkIAMRevisionConvergedHealing` covers 1,000 and 10,000 retained records;
it measures steady-state index/network work and asserts zero repeated PUTs. Its
fake peer does not measure durable catch-up throughput.
`BenchmarkIAMColdLoadExpiredServices` measures loading and cleanup with 100 or
1,000 expired reusable credentials; run it with `-benchtime=1x`. Use actual multi-site
signed S3/STS tests and deployment-specific latency/scale measurements in
addition to these component tests.
