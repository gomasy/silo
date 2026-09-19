# Removing access-frequency pool tiering

PR #60 introduced an opt-in scheduler that moved objects between local server pools according to GET frequency. It and its feature-specific fixes have been removed. This does not remove ordinary lifecycle expiration, transitions to remote tiers, rebalance, decommission, or the general multi-pool correctness fixes from PR #178.

The [introduction and rollback record](https://silo.pgsty.com/compatibility/access-tiering-removal/) documents the commit history, scope decision, review corrections and unresolved validation findings.

The published Server 20260903 predates this feature. These instructions concern main/snapshot deployments that included #60; upgrading from the published version does not require access-tier configuration cleanup.

## Before upgrading a build with access tiering

1. Save a copy of the server ILM configuration and each affected bucket's lifecycle XML. Use an API client that preserves the nonstandard XML; do not rely on a client model that silently omits unknown elements.
2. On the old server, set `ilm access_tiering=off` and remove or disable any access-tier environment overrides. Allow in-progress moves to finish before replacing nodes. This reduces movement intermediate states; the removal itself changes no storage RPC protocol.
3. Remove top-level `AccessTierQuota` and rule-level `AccessTransition` elements. **Delete rules whose only action was `AccessTransition`**. For a mixed rule, retain its filter, status, ID and ordinary expiration/transition actions. An access-only rule loads harmlessly after upgrade, but becomes an actionless rule and fails validation on the next lifecycle edit. If no rules remain, delete the lifecycle configuration through the S3 API.
4. Use a coordinated maintenance window: stop the deployment, install the same new binary on every node, then restart all nodes. The existing bootstrap check compares binary checksums; in a four-node test the first new node could not finish starting among three old nodes. Do not assume that an unchanged RPC protocol permits replacing one node at a time and waiting for it to become ready. This removal does not relax that check. Apply the same environment changes on every node: bootstrap also compares server environment settings, so removing an old override on only some nodes can block startup even with matching binaries. The check runs only during startup and is not a safety guarantee for nodes already running different binaries.
5. After restarting, verify object reads, bucket listing, ILM worker settings and a lifecycle edit. Check storage access from every request-serving node to each pool's drives: successful reads or bucket listing establish less than complete drive reachability, and admin disk summaries aggregate server-local state. Retirement acceptance used unique probes and storage trace to confirm every node-to-drive path before and after version deletion. Startup connection times varied, so a fixed sleep is insufficient. Complete distributed upgrade acceptance for the exact binaries before production rollout.

## What happens to stored state

| State | Behavior after removal |
| --- | --- |
| Ten old ILM keys | `access_tiering`, `access_pools`, `access_max_size`, `access_promote_watermark`, `access_bin_width`, `access_bins`, `access_flush`, `access_min_residency`, `access_workers`, `access_max_tracked` are accepted but ignored. Existing transition/expiration worker settings are preserved. |
| Admin configuration | Deprecated keys may still appear in `mcli admin config get ilm`; setting them may succeed but has no effect, even with `access_tiering=on`. Remove obsolete environment settings from deployment manifests. |
| Lifecycle XML | `AccessTierQuota` and `AccessTransition` are ignored when read and omitted when re-encoded. The same parser handles new PUT requests, so these extensions are also silently discarded there; access-only rules still fail action validation. |
| Data-usage cache | Both v8 and v9 caches are read, preserving ordinary counts, sizes, histograms and remote-tier statistics. The retired hot-tier byte count is discarded; subsequent writes use v8. No feature-driven full statistics rebuild is required. |
| Objects already moved | Remain in their current pools with the same versions and timestamps. There is no bulk move-back or object metadata rewrite. |
| Internal leftovers | `x-minio-internal-ilm-atier` and `.minio.sys/config/ilm/access/` counter objects may remain unused. They do not require a cleanup service or an object scan. |

Interrupted rebalance/decommission can leave the same version in more than one pool independently of access tiering. Removing the scheduler does not remove such existing copies. General Object Lock, conditional-delete, metadata reconciliation and shared remote-tier reference protections remain in place.

## Version deletion scope

Ordinary single-object `DELETE ?versionId=...` reconciles the addressed UUID, null version or delete marker across pools. Unqualified DELETE of a directory marker (a key ending in `/`) also addresses its null version and uses this path. A successful request applies the deletion to every resolved pool copy under the existing per-pool quorum rules; other version IDs remain. If outbound delete replication is pending, copies retain `VersionPurgePending` until the existing replication worker completes the purge. Success does not guarantee immediate physical removal from every drive.

If a pool is unreadable, these requests can return 503 even when another pool has a readable copy. Insufficient read quorum returns `503 SlowDownRead`; other failures retain their corresponding error codes. This extends an existing failure surface: previously the result could depend on whether the unreadable pool preceded the successful pool in traversal order; it now fails consistently. Retry after recovery. Cleanup failures also return an error. Ordinary unqualified DELETE retains its existing semantics. Batch `DeleteObjects` already fans out across pools.

Incoming replicated deletes, lifecycle expiration, free-version cleanup and movement-internal calls retain their existing contracts. In particular, an incoming replicated version delete can leave movement duplicates in other pools; this change does not solve that separate case. Expiration scanners process their own pools and may remove duplicate expired copies in later cycles; free-version cleanup remains local to a pool. Do not treat the ordinary DELETE repair as a guarantee for every source of deletion.
