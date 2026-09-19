// Copyright (c) 2026 Feng Ruohang
//
// This file is part of Silo Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package ilm

import (
	"testing"

	"github.com/minio/minio/internal/config"
)

func TestLookupConfigRetiredAccessKeys(t *testing.T) {
	t.Setenv(EnvILMTransitionWorkers, "")
	t.Setenv(EnvILMExpirationWorkers, "")
	kvs := config.KVS{
		{Key: "transition_workers", Value: "37"},
		{Key: "expiration_workers", Value: "23"},
		{Key: "access_tiering", Value: "on"},
		{Key: "access_pools", Value: "0,1"},
		{Key: "access_max_size", Value: "500GiB"},
		{Key: "access_promote_watermark", Value: "85"},
		{Key: "access_bin_width", Value: "1m"},
		{Key: "access_bins", Value: "12"},
		{Key: "access_flush", Value: "1m"},
		{Key: "access_min_residency", Value: "24h"},
		{Key: "access_workers", Value: "10"},
		{Key: "access_max_tracked", Value: "1000000"},
	}
	cfg, err := LookupConfig(kvs)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TransitionWorkers != 37 || cfg.ExpirationWorkers != 23 {
		t.Fatalf("worker settings lost: %+v", cfg)
	}
	// An ordinary ILM edit must keep working with stored retired keys.
	kvs.Set("transition_workers", "41")
	cfg, err = LookupConfig(kvs)
	if err != nil || cfg.TransitionWorkers != 41 || cfg.ExpirationWorkers != 23 {
		t.Fatalf("ILM edit: %+v, %v", cfg, err)
	}
	kvs.Set("access_unknown", "on")
	if _, err := LookupConfig(kvs); err == nil {
		t.Fatal("unrecognized key accepted")
	}
}
