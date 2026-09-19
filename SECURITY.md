# Security Policy

Silo is an independent, community-maintained object-storage server derived from
the open-source MinIO server. Upstream MinIO security contacts do not handle
Silo-specific fixes or release notes.

## Supported Versions

Security fixes are tracked on the active development branch and summarized in
[the security advisory ledger](https://silo.pgsty.com/about/security-advisories/). Only the current
Silo release line is supported unless an advisory says otherwise.

## Inherited Fix Evidence

The canonical ledger also records security fixes inherited from upstream when
they are part of the Silo release baseline. Source and fork commits are linked
separately even when the fork preserves the original commit object and SHA.

- [CVE-2025-62506](https://github.com/advisories/GHSA-jjjj-jwhf-8rgr):
  upstream [PR #21642](https://github.com/minio/minio/pull/21642) merged as
  [`minio/minio@c1a49490`](https://github.com/minio/minio/commit/c1a49490c78e9c3ebcad86ba0662319138ace190),
  inherited unchanged as
  [`pgsty/silo@c1a49490`](https://github.com/pgsty/silo/commit/c1a49490c78e9c3ebcad86ba0662319138ace190),
  and is present in every Silo community release beginning with
  [`RELEASE.2025-12-03T12-00-00Z`](https://github.com/pgsty/silo/releases/tag/RELEASE.2025-12-03T12-00-00Z).
  The inherited [service-account](https://github.com/pgsty/silo/blob/c1a49490c78e9c3ebcad86ba0662319138ace190/cmd/admin-handlers-users_test.go#L211-L212)
  and [STS](https://github.com/pgsty/silo/blob/c1a49490c78e9c3ebcad86ba0662319138ace190/cmd/sts-handlers_test.go#L45-L46)
  regression groups remain part of `go test ./cmd`; see the
  [canonical ledger](https://silo.pgsty.com/about/security-advisories/#inherited)
  for the operator-facing record.

## Reporting a Vulnerability

For vulnerabilities in this fork:

1. Follow the fork-specific expectations in [VULNERABILITY_REPORT.md](VULNERABILITY_REPORT.md).
2. Prefer this repository's [private GitHub security advisory](https://github.com/pgsty/silo/security/advisories/new) workflow.
3. If private reporting is unavailable, contact the maintainers through the
   repository without publishing exploit details until a private channel is
   established.
4. If you confirm the issue also affects upstream `minio/minio`, report it upstream separately.

## Disclosure Process

Fork-specific fixes and user-visible upgrade notes are published in [the security advisory ledger](https://silo.pgsty.com/about/security-advisories/). The fork-specific triage and remediation process is described in [VULNERABILITY_REPORT.md](VULNERABILITY_REPORT.md).
