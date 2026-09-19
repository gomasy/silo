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

package cmd

import (
	"os"
	"testing"
	"time"

	"github.com/minio/cli"
	xhttp "github.com/minio/minio/internal/http"
)

func TestServerReadHeaderTimeoutConfig(t *testing.T) {
	for _, tc := range []struct {
		name, env, idle string
		args            []string
		want            time.Duration
		fmtgen          bool
	}{
		{name: "default", want: xhttp.DefaultReadHeaderTimeout},
		{name: "flag", args: []string{"--read-header-timeout=100ms"}, want: 100 * time.Millisecond},
		{name: "environment", env: "170ms", want: 170 * time.Millisecond},
		{name: "flag-over-environment", env: "170ms", args: []string{"--read-header-timeout=80ms"}, want: 80 * time.Millisecond},
		{name: "yaml-retains-flag", args: []string{"--config=testdata/config/1.yaml", "--read-header-timeout=100ms"}, want: 100 * time.Millisecond},
		{name: "zero-fallback", args: []string{"--read-header-timeout=0s"}},
		{name: "zero-idle-default-header", idle: "0s", want: xhttp.DefaultReadHeaderTimeout},
		{name: "negative-idle-default-header", idle: "-1s", want: xhttp.DefaultReadHeaderTimeout},
		{name: "fmt-gen-unregistered-duration", env: "100ms", fmtgen: true},
		{name: "negative-disabled", args: []string{"--read-header-timeout=-1s"}, want: -time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, key := range []string{"MINIO_ARGS", "MINIO_VOLUMES", "MINIO_ENDPOINTS", "MINIO_CONFIG", "MINIO_ERASURE_SET_DRIVE_COUNT"} {
				t.Setenv(key, "")
			}
			t.Setenv("MINIO_READ_HEADER_TIMEOUT", tc.env)
			if tc.env == "" {
				if err := os.Unsetenv("MINIO_READ_HEADER_TIMEOUT"); err != nil {
					t.Fatal(err)
				}
			}
			idle := tc.idle
			if idle == "" {
				idle = "2s"
			}
			t.Setenv("MINIO_IDLE_TIMEOUT", idle)
			idleWant, err := time.ParseDuration(idle)
			if err != nil {
				t.Fatal(err)
			}
			commandName, flags := "server", serverCmd.Flags
			if tc.fmtgen {
				commandName, flags = "fmt-gen", fmtGenFlags
				idleWant = 0
			}
			var got serverCtxt
			called := false
			app := cli.NewApp()
			app.Commands = []cli.Command{{Name: commandName, Flags: flags, Action: func(ctx *cli.Context) error {
				called = true
				if parsed := ctx.Duration("read-header-timeout"); parsed != tc.want {
					t.Errorf("CLI read-header-timeout=%s, expected=%s", parsed, tc.want)
				}
				return buildServerCtxt(ctx, &got)
			}}}
			args := append([]string{"silo", commandName}, tc.args...)
			args = append(args, t.TempDir())
			if err := app.Run(args); err != nil {
				t.Fatal(err)
			}
			if !called {
				t.Fatal("server action did not run")
			}
			if got.ReadHeaderTimeout != tc.want {
				t.Errorf("parsed ReadHeaderTimeout = %s, want %s", got.ReadHeaderTimeout, tc.want)
			}
			if got.IdleTimeout != idleWant {
				t.Errorf("parsed IdleTimeout = %s, want %s", got.IdleTimeout, idleWant)
			}
		})
	}
}
