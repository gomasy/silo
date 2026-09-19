# Data-usage v9 retirement fixture

Generated using unmodified `scanDataFolder` and `dataUsageCache.serializeTo` from
Server commit `89637554d60c27cfc51d2281d0a4fe15e415f06d` (Go 1.27.1, darwin/arm64).
The scanner visits three real local files; its size callback supplies synthetic
version/delete-marker/remote-tier summaries, following `TestDataUsageCacheSerialize`.
It is a scanner/cache compatibility fixture, not a distributed object-store test.

The old scanner counts 74,962 bytes, 3 objects, 5 versions and 2 delete markers.
Its nonzero retired `hts` totals 9,426 bytes. Remote tier `COLD` contains 65,536
bytes, one version and one object. The cache includes nested children, both
histograms, a fixed timestamp and scanner cycle 42. The JSON is the old scanner's
complete expected cache (including `HotTierSize`, which the new reader ignores).

Binary SHA-256: `4c9c7e94cea7758fe9b498b19f639188764b2787582783e6cc950f2c44d52a8b`.

To regenerate, create a detached worktree at that exact source commit, copy
`generate.go.txt` to `cmd/retirement-fixture_test.go`, and run:

```sh
SILO_RETIRE_FIXTURE_DIR=/absolute/output/directory go test ./cmd -run '^TestGenerateAccessRetirementV9Fixture$' -count=1 -v
```

The historical production writer adds the version byte, compresses with zstd
and encodes msgp, including the nonzero `hts` field. Do not regenerate using the
retired implementation or by changing the header of a v8 payload. Map order may
change serialized bytes across regeneration; compare the decoded full cache.
