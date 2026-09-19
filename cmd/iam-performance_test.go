// Copyright (c) 2026 PGSTY
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/auth"
)

// Uses APIs shared with the pre-revision tree so the same benchmark can be
// overlaid on that tree for a comparable local baseline.
func prepareIAMPerformanceFixture(b *testing.B) (context.Context, *IAMSys) {
	b.Helper()
	resetTestGlobals()
	ctx, cancel := context.WithCancel(context.Background())
	disks, err := getRandomDisks(1)
	if err != nil {
		b.Fatal(err)
	}
	obj, _, err := initObjectLayer(ctx, mustGetPoolEndpoints(0, disks...))
	if err != nil {
		b.Fatal(err)
	}
	initAllSubsystems(ctx)
	globalIAMSys.initStore(obj, nil)
	if err := globalIAMSys.Load(ctx, true); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { cancel(); obj.Shutdown(context.Background()); os.RemoveAll(disks[0]); resetTestGlobals() })
	return ctx, globalIAMSys
}

func BenchmarkIAMCachedCredential(b *testing.B) {
	for _, kind := range []string{"user", "service", "sts"} {
		b.Run(kind, func(b *testing.B) {
			ctx, sys := prepareIAMPerformanceFixture(b)
			const parent = "benchmark-parent"
			_, err := sys.CreateUser(ctx, parent, madmin.AddOrUpdateUserReq{SecretKey: "benchmark-user-password", Status: madmin.AccountEnabled})
			if err != nil {
				b.Fatal(err)
			}
			key := parent
			if kind == "service" {
				c, _, err := sys.NewServiceAccount(ctx, parent, nil, newServiceAccountOpts{accessKey: "benchmark-service", secretKey: "benchmark-service-password"})
				if err != nil {
					b.Fatal(err)
				}
				key = c.AccessKey
			}
			if kind == "sts" {
				secret, err := getTokenSigningKey()
				if err != nil {
					b.Fatal(err)
				}
				c, err := auth.GetNewCredentialsWithMetadata(map[string]any{"exp": UTCNow().Add(time.Hour).Unix(), parentClaim: parent}, secret)
				if err != nil {
					b.Fatal(err)
				}
				c.ParentUser = parent
				if _, err := sys.SetTempUser(ctx, c.AccessKey, c, ""); err != nil {
					b.Fatal(err)
				}
				key = c.AccessKey
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, ok := sys.store.GetUser(key); !ok {
					b.Fatal("credential missing")
				}
			}
		})
	}
}

func BenchmarkIAMSetTempUser(b *testing.B) {
	ctx, sys := prepareIAMPerformanceFixture(b)
	const parent = "benchmark-sts-parent"
	_, err := sys.CreateUser(ctx, parent, madmin.AddOrUpdateUserReq{SecretKey: "benchmark-user-password", Status: madmin.AccountEnabled})
	if err != nil {
		b.Fatal(err)
	}
	secret, err := getTokenSigningKey()
	if err != nil {
		b.Fatal(err)
	}
	cred, err := auth.GetNewCredentialsWithMetadata(map[string]any{"exp": UTCNow().Add(time.Hour).Unix(), parentClaim: parent}, secret)
	if err != nil {
		b.Fatal(err)
	}
	cred.ParentUser = parent
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := sys.SetTempUser(ctx, cred.AccessKey, cred, "readwrite"); err != nil {
			b.Fatal(err)
		}
	}
}

// Run with -benchtime=1x. Preparation is outside the timer; each measured load
// sees a fresh set of expired reusable service-account records.
func BenchmarkIAMColdLoadExpiredServices(b *testing.B) {
	for _, count := range []int{100, 1000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			ctx, sys := prepareIAMPerformanceFixture(b)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				for j := 0; j < count; j++ {
					key := fmt.Sprintf("expired-benchmark-%d-%d", i, j)
					u := UserIdentity{Version: 1, UpdatedAt: UTCNow().Add(-2 * time.Hour), Credentials: auth.Credentials{AccessKey: key, SecretKey: "expired-benchmark-password", ParentUser: "absent-idp-parent", Expiration: UTCNow().Add(-time.Hour), Status: auth.AccountOn}}
					if err := sys.store.saveIAMConfig(ctx, &u, getUserIdentityPath(key, svcUser)); err != nil {
						b.Fatal(err)
					}
				}
				b.StartTimer()
				if err := sys.store.LoadIAMCache(ctx, true); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
