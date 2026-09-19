# SILO Repository Guide

## Project map

- Server: `pgsty/silo` (this checkout, default branch `main`)
- Console: `pgsty/silo-console` (usual sibling checkout `../silo-console`)
- Client: `pgsty/mc`, shipped as `mcli` (usual sibling checkout `../mc`)
- Shared packages: `pgsty/silo-pkg` (usual sibling checkout `../silo-pkg`)
- Documentation: `pgsty/silo.pgsty.com` (usual sibling checkout `../silo.pgsty.com`),
  published at <https://silo.pgsty.com/>

The server executable, package, systemd service, and container image use `silo`;
the public Docker image is `docker.io/pgsty/silo`. The server module remains
`github.com/minio/minio`. Consult the current `go.mod` and release configuration
for selected component versions and compatibility replacements.

## Supported stack and compatibility policy

The maintained, release-gating product graph is the coordinated PGSTY stack:
`silo` + `silo-console` + `mc` + `silo-pkg`.

Keep compatibility with upstream MinIO/MC on a best-effort basis. Preserve
inexpensive wire, configuration, CLI, migration, and import compatibility when
it helps users, and document known differences. Do not infer from source
lineage, MinIO-compatible protocols, retained `MINIO_*`/`MC_*` names, or an
inherited upstream test that unmodified upstream MinIO is a supported release
target.

Upstream-only compatibility checks may remain as advisory evidence, but they
must not force a downgrade of a maintained SILO component, a fork-only API
workaround, or a release block. Make upstream compatibility a hard gate only
when the user explicitly requests that scope.

## Dependency selection

- When PGSTY maintains a component, import and require it directly under its own
  module path. In particular, prefer `github.com/pgsty/silo-pkg/v3` over
  `github.com/minio/pkg/v3` in maintained SILO source.
- Do not use an upstream package merely to make unmodified upstream MinIO or MC
  compile. Unavoidable transitive upstream modules should be documented and
  kept separate from the maintained product dependency.
- `github.com/minio/minio-go/v7` is the explicit exception: use the verified
  upstream module/commit while it contains the required fixes; do not recreate
  a SILO fork without a concrete functional divergence.
- Check the current `go.mod` before changing versions. Coordinate breaking
  import-path changes across package, client, Console, and server releases.

## Documentation and delivery

The companion site owns product, operations, migration, security-advisory,
design, and release documentation. Maintain English and Chinese content in
that repository and link to its canonical URLs from this one. The site's
`content/docs/` is the documentation entry point; detailed pages live under
`content/operations/`, `administration/`, `reference/`, `compatibility/`,
`about/`, and `blog/`, following the existing structure.

Keep repository entry points and contributor instructions here. Retained
upstream documents, examples, and test fixtures under `docs/` may be updated
when a code or tooling change requires it; do not build a second documentation
site in this tree.

Investigation plans, prompts, AI session transcripts, execution logs, temporary
reports, and private security material belong outside the checkout, for example
in a task-specific directory under `~/tmp/`. Do not recreate
`docs/investigations/`, `docs/security/`, or `docs/rebranding.md`, or move their
working material to another repository directory. Extract reusable, verified
knowledge into the companion site; keep private evidence outside public Git.

Compatibility pages may continue to describe similarities with upstream, but
must label that compatibility as best effort. The supported and tested server
for Console and mcli administration is `pgsty/silo`.

Before removing or moving documentation, check repository links and scripts,
the companion site's links and anchors, and references from this guide. Use
fixed commit URLs for historical evidence that should survive deletion from
the current tree. Run the site's `make check` for site changes and this
repository's `make rebrand-guard` for documentation cleanup that changes the
identifier inventory. When new public URLs are involved, publish the site
before publishing source changes that depend on those URLs.

Keep changes in the repository that owns the affected surface. Treat every
repository commit, tag, release, image, documentation update, and deployment as
a separate deliverable. Follow `CONTRIBUTING.md`, including DCO sign-off
(`git commit -s`). `CLAUDE.md` imports this guide so both agents use one policy.
