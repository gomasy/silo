// Copyright (c) 2026 Gomasy
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

package scanner

import (
	"testing"
	"time"

	"github.com/minio/minio/internal/config"
)

func TestLookupConfigDeprecatedValues(t *testing.T) {
	for _, env := range []string{EnvSpeed, EnvDelay, EnvDelayLegacy, EnvMaxWait, EnvMaxWaitLegacy, EnvCycle} {
		t.Setenv(env, "")
	}
	kvs := config.KVS{
		{Key: Speed, Value: "default"},
		{Key: Delay, Value: "30"},
		{Key: MaxWait, Value: "15s"},
		{Key: Cycle, Value: "10m"},
	}
	cfg, err := LookupConfig(kvs)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Delay != 30 || cfg.MaxWait != 15*time.Second || cfg.Cycle != 10*time.Minute {
		t.Fatalf("deprecated values overwritten: %+v", cfg)
	}
	if cfg.ExcessVersions != 100 || cfg.ExcessFolders != 50000 {
		t.Fatalf("alert thresholds lost: %+v", cfg)
	}

	// An explicit speed takes precedence over the deprecated values.
	kvs.Set(Speed, "slow")
	cfg, err = LookupConfig(kvs)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Delay != 10 || cfg.Cycle != time.Minute {
		t.Fatalf("speed not honored: %+v", cfg)
	}
}
