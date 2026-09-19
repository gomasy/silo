# Changelog

## Unreleased

Preparation target: `RELEASE.2026-09-16T00-00-00Z` (package version
`20260916000000.0.0`). The entries below describe the candidate changes since
the latest published Server.
**The latest published Server remains 20260903.** These changes are not in its
binaries, packages or images. See the [component matrix](https://silo.pgsty.com/compatibility/versions/)
and [complete commit range](https://github.com/pgsty/silo/compare/RELEASE.2026-09-03T13-18-01Z...main).

### Authorization and security

- Synchronize CPU metrics reads with resource-metrics updates (#210), preventing
  concurrent map access from terminating the server during Prometheus scraping.
  Metric names, values and authentication requirements are unchanged.
- Restrict embedded Console's anonymous sharing proxy to object-content GETs
  at the configured S3 origin, and reject every redirect. Internal metrics,
  system paths and non-download S3 operations cannot be reached through it.
  Normal public, presigned and versioned downloads remain available without a
  new setting; a full sharing-disable switch is not introduced. See
  [Console #56](https://github.com/pgsty/silo-console/pull/56) and the
  [design record](https://github.com/pgsty/silo-console/issues/52).
  Thanks to Jiri Pejchal (@jiri-pejchal) for the report.
- Persist IAM deletion revisions and parent revocation boundaries so stale site
  events cannot restore deleted identities, policies or their older grants
  (#191, #192). Peer deletion notifications reload committed storage; deliberate
  recreation requires a newer revision, and credentials issued before the
  parent's revocation remain invalid.
  **Coordinated upgrade required:** upgrade every participating node and site.
  Mixed old/new nodes sharing an IAM backend and rolling downgrade are
  unsupported. Back up complete IAM storage and encryption material; an admin
  export of live records omits deletion history. Reissue credentials for
  recreated parents and explicitly reconcile pre-upgrade revocations whose
  history is already lost. Restoring an older backup can lose later revocations;
  keep affected sites isolated until reconciliation/rekeying is complete. See
  [the operator runbook](https://silo.pgsty.com/operations/replication/iam-upgrade/).
- Enforce an absolute HTTP/1 request-header deadline through the connection
  wrapper (#196). Repeated small reads no longer extend that deadline, and
  `--read-header-timeout` / `MINIO_READ_HEADER_TIMEOUT` now reaches the HTTP
  server. HTTP/1 request bodies retain the rolling idle timeout; this does not
  impose a total upload/download duration. A shorter setting also constrains
  TLS handshake reads. The wrapper's strict header mode is not applied to HTTP/2.
- Reject unsigned `x-amz-*` request headers that could turn a signed PUT into a
  copy of another object accessible to the signer (SN-2026-011). The latest
  public Server is affected; the fix is on main. See [the advisory ledger](https://silo.pgsty.com/about/security-advisories/).
- Align signed request fields with policy conditions and enforce header-only
  presigned payload checksums. See [the signed-header review](https://silo.pgsty.com/blog/design/signed-header-coverage/).
- **Breaking policy semantics:** separate self-service `admin:ChangeMyPassword`
  from `admin:CreateUser`. Built-in read-only policies follow the split. Preserve
  both denies if the previous combined restriction must survive upgrades or
  rollback. Saved policies are not rewritten. Deploy with the matching Console
  and pkg; see [the migration guide](docs/iam/password-permissions.md).

### Object storage and replication

- Make `ListMultipartUploads` discover quorum-valid uploads from durable state
  across pools, erasure sets and drives, then apply S3 prefix, delimiter,
  marker, ordering and 1,000-entry pagination semantics globally (#198). New uploads
  store their canonical bucket and key as reserved fields in the existing
  quorum-written `xl.meta`; completion removes those upload-only fields. Native
  markers remain usable after their upload is completed or canceled. Strict
  listing returns a diagnostic 503 for legacy uploads or uncertain coverage;
  the default remains the released exact-key/cache-based `legacy` behavior.
  Opt into strict mode only through `MINIO_API_MULTIPART_LISTING=strict`, after
  upgrading every writer, draining old uploads, checking the read-only admin
  `multipart-preflight` report and validating scan capacity. The process-only
  setting is not persisted into shared API configuration. Historical
  `multipart_listing` keys are ignored and can be removed with a targeted
  `mcli admin config reset ALIAS api multipart_listing` before rollback. Per-process
  admission, directory-entry, worker and time budgets bound scan scheduling;
  each page still scans durable state. See [issue #79](https://github.com/pgsty/silo/issues/79)
  and its [design record](https://silo.pgsty.com/blog/design/list-multipart-uploads/).
  Thanks to mr javad seydi (@mrjavadseydi) for the original implementation.
- Retain released read-quorum and best-effort multipart cancellation in default
  legacy mode. Strict mode requires majority deletion acknowledgements and
  permits retries below read quorum. When most drives were already empty,
  failed deletion of an observed remnant now returns 503 instead of being
  masked by empty-drive successes. Wrong-key or wrong-bucket cancellation
  preserves the valid upload's cache entry; successful cancellation notifies
  peers with the request context after the distributed lock is released.
  The HTTP response for an absent
  upload remains 204; this is not proof of physical cleanup. **Known boundary:**
  delayed creation writes can still restore an upload after cancellation;
  this change does not add a durable creation fence.
- Preserve object tags during multi-pool metadata reconciliation by reading the
  resolved tag field together with its revision (#189). Previously, reconciliation
  could replace existing tags with an empty value.
- Preserve the tag revision on SSE-KMS metadata replication (#193), and advance
  tag revisions monotonically on local PUT/DELETE tagging (#196). Empty tags
  participate in reconciliation as an ordered deletion, preventing older
  events from restoring removed tags. SSE-C key rotation also retains the tag
  revision. Malformed historical revisions can fail and retry; their missing
  history is not reconstructed by the upgrade.
- Complete delete-marker version purges and preserve their identity and retry
  state through MRF recovery (#196). Recovery accepts a 405 marker response only
  when its version, bucket, object name and modification time match the task.
  Purge audit status is normalized from `COMPLETE` to `COMPLETED`.
  Thanks to Julien Laurenceau (@julienlau) for the investigation and proposed
  fix in #184 that helped shape this follow-up.
- Restore only the six replication-specific metadata fields after ordinary
  request metadata extraction (#194). This prevents transport-only `aws-chunked`
  from being stored as Content-Encoding while preserving the signed-header
  protections. Trusted Snowball entries no longer inherit the outer archive's
  ordinary metadata. Thanks to Mikhail Khadarenka (@chodorenko) for the fix in #187.
  **Existing data:** these repairs prevent new errors; they do not scan or rewrite
  historical object metadata, recover lost tags or prove that old purge work has
  converged. Follow the [read-only audit procedure](https://silo.pgsty.com/operations/replication/replica-metadata-audit/)
  before planning any repair of stored state.

- Make exact-version delete-marker purges converge in replicated buckets
  (`eb4f5e5b3`, `254b19ac0`, `358ab38fb`). A purge no longer creates the
  marker on drives that lacked it; a retried purge of a missing version is
  acknowledged only when a write-quorum majority of drives report it absent;
  purge results merge removed and reliably absent replies; healing a marker
  preserves its stored replication and purge metadata; and a queued marker
  creation is re-checked against the source under the replication lock before
  it is sent, so a purge that already reached the targets is not undone by a
  stale task from another frontend, a GET/LIST heal, the scanner or MRF.
  Purging a data version whose earlier purge is still pending reports it as a
  data version. **Known limitations:** creations already in flight or replayed
  from another site, and minority marker copies left by a crash after a
  majority-acknowledged purge, are tracked in #217. See
  [the replication reliability record](https://silo.pgsty.com/blog/design/replication-reliability/).
- Keep a null object version that has listing quorum when a newer minority of
  drives sorts first (`8d06424b1`). The resolver recounts per header only when
  the original selection lacks quorum, every non-empty drive stream holds
  exactly one ordinary null version and all share the same erasure layout;
  mixed histories keep their previous behavior. **Known limitation:** a
  successful ListObjects can still omit readable keys during rolling restarts
  with concurrent overwrites (#218). Do not run destination-deleting sync tools
  against a listing taken during a rolling restart; list again once the
  cluster is stable.
- Carry object tags through rebalance and decommission for ordinary and
  multipart writes (`fced86303`). Both migration entry points restore the tags
  and their revision fields when rewriting the object in the destination pool.
  Tags dropped by earlier migrations are not recovered; audit tag-dependent
  lifecycle and policy rules for pools migrated with an older build.

- Evaluate conditional multipart completion against the logical current object
  across all pools while holding the existing object lock. A stale `If-Match`
  can no longer replace newer data in another pool, and the current ETag is no
  longer rejected because the upload resides next to an older copy. Conditions
  are evaluated once; a current delete marker counts as an absent object.
  **Availability change:** if any pool's metadata cannot be read, conditional
  completion fails even when another pool can still serve GET/HEAD. This also
  applies when the unreadable pool may not hold the object: absence cannot be
  verified. Retry after the pool recovers. Unconditional completion and the
  single-pool path retain their existing behavior.

- Evaluate ordinary multi-pool conditional PUT against the logical current
  object across all pools, including draining pools, under the existing object
  lock (#207). A stale destination copy no longer accepts a stale ETag or rejects
  the current one; a current delete marker is treated as absence.
  **Availability change:** if any pool's object metadata cannot be verified,
  the condition fails even when GET can use another pool; read-quorum failures
  return 503. Restore readability or heal before retrying. Unconditional PUT,
  single-pool conditions and internal replication retain their existing behavior.
  A public condition with a destination `versionId` compares the current object
  while preserving the requested write version. This change does not retire
  stale copies in other pools, undo historical accepted overwrites or provide
  a new global clock-ordering guarantee. The multipart-completion repair in #190
  neither introduced nor repaired this separate PUT defect.

- Reconcile ordinary single-object version DELETE across all pools, including
  null versions, delete markers and unqualified directory-marker DELETE. This
  applies the deletion to every resolved pool copy under existing quorum
  rules. Pending outbound delete replication retains versions until the
  existing replication worker completes their purge; a successful response
  does not imply immediate physical removal from every drive. Unreadable
  pools now consistently return 503 instead of depending on pool traversal
  order; insufficient read quorum returns `SlowDownRead`. This extends the
  existing failure surface. Retry after recovery.
  Cleanup failures also return an error. Batch deletion already fans out across
  pools; replication and scanner cleanup keep their existing contracts. See
  [scope and limitations](docs/bucket/lifecycle/access-tiering-removal.md#version-deletion-scope).

- Remove the opt-in GET-frequency pool-tiering feature from PR #60, including
  its tracker, mover, scanner hooks, configuration, XML actions and metrics.
  Accept and ignore retired configuration/XML and preserve ordinary statistics
  when reading v9 caches. See [migration notes](docs/bucket/lifecycle/access-tiering-removal.md).
  The [decision record](https://silo.pgsty.com/compatibility/access-tiering-removal/) preserves
  the feature's introduction, subsequent fixes, rollback scope and review history.
- Preserve the independent multi-pool write, metadata, healing and conditional
  deletion fixes from PR #178, including shared remote-tier reference protection.
- Enforce `If-Match` on DELETE, preserve retention and independently ordered
  Object Lock/tag updates, and correctly retransmit encrypted replicas.
- Preserve plaintext part sizes and raw SSE-C replicas; prevent SSE-C
  compression, honor key-rotation checksums, and complete attributes pagination.
- Repair federated CopyObject checksums, destination timestamps, reserved
  metadata, encrypted-object forwarding, legal hold and KMS context.
- Make resync counters, target selection, cancellation and worker lifetimes
  reflect actual work, and report bounded MRF drops.
- Converge bucket metadata with deterministic source state, deletion tombstones,
  creation time recovery and diagnostics. The mixed-version export gate requires
  coordinated upgrades before tombstones are exported. See [the #77 record](https://silo.pgsty.com/blog/design/bucket-metadata-convergence/).
- Include per-bucket CORS in metadata export/import, close metadata publication
  and logger races, and report effective bucket quotas in metrics.

### Console, dependencies and delivery

- Restore embedded Console login over loopback TLS, trusted-proxy handling and
  all four WebSocket connection limits. Preserve Go TLS defaults across transports.
- Directly require `github.com/pgsty/silo-pkg/v3` v3.14.1; select released Console
  v2.4.1 (`v0.0.0-20260916075814-1360e26d976d`) and mcli 20260916
  (`v0.0.0-20260916070421-e952aa78f10a`) with explicit PGSTY replacements.
  The embedded frontend identifies itself as Console v2.4.1.
- Pin upstream minio-go `v7.3.1-0.20260915093545-32e1f32cb176` to handle
  CopyObject errors embedded in HTTP 200 responses. Update JWX to v3.3.0 for
  JSON field-name escaping, strfmt to v0.27.2 for Go 1.27 hostname validation,
  and LZ4 to v4.1.30 for frame-reader, partial-read and concurrency fixes.
  Retain the earlier Go x/* and bounded AMQP frame updates, Go 1.27.1, and
  go-systemd v22.6.0's NetBSD compatibility replacement.
- Refresh container base digests and build static curl 8.22.0 from verified
  source for both Linux architectures. Pin the published mcli 20260916 archives
  and hashes in the container and update the client installer default.
- Prepare Helm chart 7.0.3 with Server and client defaults for the September 16
  batch. Publish the chart only after the corresponding Server image exists.
- Pin GoReleaser v2.18.1 and its action commit identically in snapshot and release
  workflows. Dependency-only PRs now run the Test Release Pipeline too.

Validation of earlier source revisions does not establish acceptance of this
candidate. Final source, package, image and multi-process checks are tracked
separately in [#203](https://github.com/pgsty/silo/issues/203). No Server release
or production rollout is implied by this preparation target.

## RELEASE.2026-09-03T13-18-01Z

Published source: `9b11dc9469e650815b775cb47b039610644f5da4`.
[Complete release notes](https://silo.pgsty.com/blog/release/silo-20260903/) ·
[GitHub release](https://github.com/pgsty/silo/releases/tag/RELEASE.2026-09-03T13-18-01Z)

This release ships Go 1.27.1, silo-pkg v3.13.2, upstream minio-go `0e78d3f18efe`,
mcli 20260903 and embedded Console source `464a59d73ada` (v2.3.0 version identity).
Installing the newer standalone mcli or Console does not replace components
inside this existing Server binary or image.

Earlier releases: [release archive](https://github.com/pgsty/silo/releases).
