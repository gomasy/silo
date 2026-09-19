# Automatic Site Replication

This feature allows multiple independent Silo sites (or clusters) that are using the same external IDentity Provider (IDP) to be configured as replicas. In this situation the set of replica sites are referred to as peer sites or just sites. When site-replication is enabled on a set of sites, the following changes are replicated to all other sites:

- Creation and deletion of buckets and objects
- Creation and deletion of all IAM users, groups, policies and their mappings to users or groups
- Creation of STS credentials
- Creation and deletion of service accounts (except those owned by the root user)
- Changes to Bucket features such as:
  - Bucket Policies
  - Bucket Tags
  - Bucket Object-Lock configurations (including retention and legal hold configuration)
  - Bucket Encryption configuration

> NOTE: Bucket versioning is automatically enabled for all new and existing buckets on all replicated sites.

The following Bucket features will **not be replicated**, is designed to differ between sites:

- Bucket notification configuration
- Bucket lifecycle (ILM) configuration

## Pre-requisites

- Initially, only **one** of the sites added for replication may have data. After site-replication is successfully configured, this data is replicated to the other (initially empty) sites. Subsequently, objects may be written to any of the sites, and they will be replicated to all other sites.

- **Removing a site** is not allowed from a set of replicated sites once configured.
- All sites must be using the **same** external IDP(s) if any.
- For [SSE-S3 or SSE-KMS encryption via KMS](https://silo.pgsty.com/operations/server-side-encryption/ "Silo KMS Guide"), all sites **must**  have access to a central KMS deployment. This can be achieved via a central KES server or multiple KES servers (say one per site) connected via a central KMS (Vault) server.

## Configuring Site Replication

- Configure an alias in `mc` for each of the sites. For example if you have three Silo sites, you may run:

```sh
mc alias set silo1 https://silo1.example.com:9000 adminuser adminpassword
mc alias set silo2 https://silo2.example.com:9000 adminuser adminpassword
mc alias set silo3 https://silo3.example.com:9000 adminuser adminpassword
```

or

```sh
export MC_HOST_silo1=https://adminuser:adminpassword@silo1.example.com
export MC_HOST_silo2=https://adminuser:adminpassword@silo2.example.com
export MC_HOST_silo3=https://adminuser:adminpassword@silo3.example.com
```

- Add site replication configuration with:

```sh
mc admin replicate add silo1 silo2 silo3
```

- Once the above command returns success, you may query site replication configuration with:

```sh
mc admin replicate info silo1
```

** Note **
Previously, site replication required the root credentials of peer sites to be identical. This is no longer necessary because STS tokens are now signed with the site replicator service account credentials, thus allowing flexibility in the independent management of root accounts across sites and the ability to disable root accounts eventually.

However, this means that STS tokens signed previously by root credentials will no longer be valid upon upgrading to the latest version with this change. Please re-generate them as you usually do. Additionally, if site replication is ever removed - the STS tokens will become invalid, regenerate them as you usually do.

## Bucket metadata source times and deletion recovery

Policy, tags, encryption, quota, versioning and Object Lock use the originating
field timestamp. Peer apply and healing compare under the bucket metadata lock;
duplicates and older events do not rewrite the field. Real changes win over
creation-time defaults. Equal-time deletions win over live values; equal-time
live conflicts use a deterministic content key. An empty creation-time default
is never a deletion. Versioning and Object Lock cannot be deleted by an empty
replication event.

`MINIO_SITE_REPLICATION_METADATA_TOMBSTONES=off` is the startup default. After
**all nodes at every participating site** run the fixed server, have consistent
settings within each site, and old requests have drained, restart them with
`MINIO_SITE_REPLICATION_METADATA_TOMBSTONES=on`. This exports the saved deletion
times for absent tags, encryption and quota, and includes real Policy/tag/SSE/
quota deletions in initial synchronization. It lets healing recover deletions
missed during an outage. The setting does not detect remote capabilities.

With the setting off, the new timestamp ordering still applies. Ordinary delete
events still replicate, and Policy deletion times remain exported as before.
Only the newly exposed deletion information is withheld. Hidden tag/SSE/quota
tombstones can cause repeated stale heal RPCs that the fixed receiver rejects;
zero RPCs on a subsequent heal is only expected when full state is visible.
Do not enable the setting while old nodes remain: old quota heal can leave a
stale parsed quota in memory after deletion.

Before rolling back to an older server, turn the setting off on every fixed
node and restart it, then downgrade. This removes the newly exposed deletion
information; it does not repair bugs in the older software. The maintained and
release-tested target is the coordinated PGSTY stack. Compatibility with
unmodified upstream MinIO is best effort.

Legacy events without a source timestamp are still accepted and assigned a
monotonic local time, independently of this setting. In particular, older
servers sent tag heal events without `UpdatedAt`. Their arrival-time pollution,
previously polluted timestamps, and genuine differences in bucket creation
identity cannot be reconstructed automatically. Inspect the sites, resolve
bucket identity conflicts first, then resubmit the intended configuration or
delete at the authoritative site. A local write advances beyond an existing
future field timestamp. Source times before the target bucket's creation are
ignored; an unknown creation time is recovered from the physical bucket, or the
operation fails without writing. Recovery uses a physical approximation (the
bucket directory's modification time), which can differ across drives and be
later than the real creation time. A source event older than this recovered
value is still ignored; its timestamp is not used to invent an earlier bucket
identity. Recovery happens on the write path and during initial site sync. While
a bucket's stored metadata still carries no creation time, site status reports
it that way and periodic healing skips that bucket in both directions; the first
successful configuration write on it, local or replicated, or initial site sync
records the physical time and returns the bucket to the normal path.

The server emits bounded warnings for `legacy-zero`, `before-created`,
`indeterminate`, `unreachable` and `peer-error`. Each reason keeps its own log
key, so a peer that did not report cannot hide an unusable peer state or a real
heal RPC failure for the same bucket and field. A peer that simply does not
have the bucket yet is a normal transient and is not reported here. Keys and
error messages remain stable for each bucket/field/reason; timestamps and peer
details are log attributes. Existing hourly logger cleanup applies. Normal
duplicates, older events and resolved ties are quiet. Invalid existing field
states emit `indeterminate` even when no valid source can be selected; empty
baselines alone remain quiet and do not cause a heal RPC.

A local PUT of a policy whose parsed statements are empty now consistently
means deletion: PUT succeeds and GET returns the existing NotFound response.
This matches the established peer-event interpretation. Zero-value quota
JSON (`{}`, `null`, or a valid zero quota document) remains a live document;
it is not silently sent as a deletion. Bulk omission preserves a field,
whereas an explicit Policy JSON `null` deletes it. These rules use the existing
wire fields and on-disk metadata format.

Policy GET and admin export use the same validated encoder as replication.
Statement and set arrays may appear in a different order from older output;
policy evaluation is unchanged. This also makes policies using the parser's
existing NotAction/NotResource alternatives writable and readable. Public
replication status compares the same stable policy key as heal, so a peer's
equivalent legacy statement order does not remain a false mismatch.
