// Copyright (c) 2026 Ruohang Feng
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/minio/minio/internal/config"
)

func TestMultipartListingMigrationMode(t *testing.T) {
	t.Setenv(EnvAPIRequestsMax, "")
	t.Setenv(EnvAPISyncEvents, "")
	t.Setenv(EnvAPIObjectMaxVersions, "")
	for _, tc := range []struct {
		name, stored, override, want string
		invalid                      bool
	}{
		{name: "default", want: "legacy"},
		{name: "explicit-legacy", override: "legacy", want: "legacy"},
		{name: "explicit-strict", override: "strict", want: "strict"},
		{name: "retired-stored-strict", stored: "strict", want: "legacy"},
		{name: "retired-stored-legacy", stored: "legacy", want: "legacy"},
		{name: "retired-stored-invalid", stored: "automatic", want: "legacy"},
		{name: "environment-override", stored: "legacy", override: "strict", want: "strict"},
		{name: "invalid-environment", stored: "strict", override: "automatic", want: "legacy", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(EnvAPIMultipartListing, tc.override)
			kvs := DefaultKVS.Clone()
			kvs.Set(apiRequestsMax, "17")
			kvs.Set(apiSyncEvents, config.EnableOn)
			kvs.Set(apiObjectMaxVersions, "71")
			if tc.stored != "" {
				kvs.Set(apiMultipartListing, tc.stored)
			}
			cfg, err := LookupConfig(kvs)
			if (err != nil) != tc.invalid || cfg.MultipartListing != tc.want {
				t.Fatalf("mode=%q err=%v, want %q invalid=%v", cfg.MultipartListing, err, tc.want, tc.invalid)
			}
			if cfg.RequestsMax != 17 || !cfg.SyncEvents || cfg.ObjectMaxVersions != 71 {
				t.Fatalf("migration setting discarded unrelated config: %+v", cfg)
			}
		})
	}
}

func TestMultipartListingConfigPersistence(t *testing.T) {
	t.Setenv(EnvAPIRequestsMax, "")
	t.Setenv(EnvAPISyncEvents, "")
	t.Setenv(EnvAPIObjectMaxVersions, "")
	t.Setenv(EnvAPIMultipartListing, "strict")
	previous := config.DefaultKVS
	config.DefaultKVS = map[string]config.KVS{config.APISubSys: DefaultKVS}
	t.Cleanup(func() { config.DefaultKVS = previous })
	for _, help := range Help {
		if help.Key == apiMultipartListing {
			t.Fatal("process-only setting advertised as shared config")
		}
	}
	for _, input := range []string{
		"",
		"api requests_max=17 sync_events=on object_max_versions=71",
		`api requests_max=17 comment="later disable multipart_listing=strict"`,
	} {
		t.Run(input, func(t *testing.T) {
			c := config.New().Clone()
			if _, err := c.ReadConfig(strings.NewReader(input)); err != nil {
				t.Fatal(err)
			}
			// Exercise the JSON save/load and Merge paths used for shared config.
			data, err := json.Marshal(c)
			if err != nil {
				t.Fatal(err)
			}
			var loaded config.Config
			if err = json.Unmarshal(data, &loaded); err != nil {
				t.Fatal(err)
			}
			kvs := loaded.Merge()[config.APISubSys][config.Default]
			if _, present := kvs.Lookup(apiMultipartListing); present {
				t.Fatal("process-only setting persisted in shared config")
			}
			cfg, err := LookupConfig(kvs)
			if err != nil || cfg.MultipartListing != "strict" {
				t.Fatalf("environment mode lost: %+v %v", cfg, err)
			}
			if input != "" && cfg.RequestsMax != 17 {
				t.Fatalf("unrelated setting lost: %+v", cfg)
			}
			if strings.Contains(input, "comment=") && kvs.Get(config.Comment) != "later disable multipart_listing=strict" {
				t.Fatalf("literal comment changed: %q", kvs.Get(config.Comment))
			}
		})
	}
	// A key persisted by a previous development build is tolerated, retained for
	// explicit cleanup, and removable without resetting other API settings.
	c := config.New().Clone()
	kvs := c[config.APISubSys][config.Default]
	kvs.Set(apiMultipartListing, "strict")
	kvs.Set(apiRequestsMax, "17")
	c[config.APISubSys][config.Default] = kvs
	c = c.Merge()
	if err := c.DelKVS("api multipart_listing"); err != nil {
		t.Fatal(err)
	}
	kvs = c.Merge()[config.APISubSys][config.Default]
	if _, present := kvs.Lookup(apiMultipartListing); present || kvs.Get(apiRequestsMax) != "17" {
		t.Fatalf("targeted reset changed unrelated config or restored retired key: %v", kvs)
	}
}
