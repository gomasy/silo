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

package deadlineconn

import (
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

type deadlineRecorder struct {
	net.Conn
	read, write time.Time
}

func (c *deadlineRecorder) SetReadDeadline(t time.Time) error  { c.read = t; return nil }
func (c *deadlineRecorder) SetWriteDeadline(t time.Time) error { c.write = t; return nil }
func (c *deadlineRecorder) SetDeadline(t time.Time) error      { c.read = t; c.write = t; return nil }
func (c *deadlineRecorder) Read([]byte) (int, error)           { return 0, io.EOF }
func (c *deadlineRecorder) Write(b []byte) (int, error)        { return len(b), nil }

func TestStrictReadDeadline(t *testing.T) {
	for _, both := range []bool{false, true} {
		name := "SetReadDeadline"
		if both {
			name = "SetDeadline"
		}
		t.Run(name, func(t *testing.T) {
			raw := &deadlineRecorder{}
			conn := New(raw).WithReadDeadline(time.Second).WithWriteDeadline(time.Second)
			conn.SetReadDeadlineStrict(true)
			absolute := time.Now().Add(100 * time.Millisecond)
			if both {
				conn.SetDeadline(absolute)
			} else {
				conn.SetReadDeadline(absolute)
			}
			conn.Read(nil)
			if !raw.read.Equal(absolute) {
				t.Fatalf("absolute read deadline extended: %s, want %s", raw.read, absolute)
			}
			// Strictness only affects reads. Existing rolling writes remain unchanged.
			conn.SetWriteDeadline(absolute)
			conn.Write([]byte("x"))
			if !raw.write.After(absolute) {
				t.Fatal("strict read mode changed write semantics")
			}
			// Body phase can renew even when the header deadline was shorter than idle.
			conn.SetReadDeadlineStrict(false)
			conn.Read(nil)
			if !raw.read.After(absolute) {
				t.Fatal("body deadline did not renew")
			}
			// Keep-alive installs a new absolute deadline; do not reuse the first header's cap.
			next := time.Now().Add(200 * time.Millisecond)
			conn.SetReadDeadlineStrict(true)
			conn.SetReadDeadline(next)
			conn.Read(nil)
			if !raw.read.Equal(next) {
				t.Fatalf("second deadline = %s, want %s", raw.read, next)
			}
			// A longer header deadline must still allow the socket idle bound.
			later := time.Now().Add(time.Hour)
			conn.SetReadDeadline(later)
			conn.Read(nil)
			if !raw.read.Before(later) || raw.read.Before(time.Now()) {
				t.Fatal("idle deadline not applied within absolute cap")
			}
			// Explicit zero disables both the absolute cap and idle renewal.
			conn.SetReadDeadline(time.Time{})
			conn.Read(nil)
			if !raw.read.IsZero() {
				t.Fatal("zero deadline re-enabled idle timeout")
			}
			past := time.Unix(1, 0)
			conn.SetReadDeadline(past)
			if _, err := conn.Read(nil); err == nil {
				t.Fatal("past deadline did not abort")
			}
		})
	}
}

func TestDefaultReadDeadlineStillRenews(t *testing.T) {
	raw := &deadlineRecorder{}
	conn := New(raw).WithReadDeadline(time.Second)
	absolute := time.Now().Add(100 * time.Millisecond)
	conn.SetReadDeadline(absolute)
	conn.Read(nil)
	if !raw.read.After(absolute) {
		t.Fatal("default internode-style read no longer renews")
	}
	if Unwrap(conn) != raw {
		t.Fatal("Unwrap changed the underlying connection")
	}
	conn.SetReadDeadline(time.Time{})
	conn.Read(nil)
	if !raw.read.IsZero() {
		t.Fatal("default zero deadline no longer disables timeout")
	}
}

func TestStrictExpiredFutureReadDeadline(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	conn := New(server).WithReadDeadline(time.Second)
	conn.SetReadDeadlineStrict(true)
	absolute := time.Now().Add(30 * time.Millisecond)
	conn.SetReadDeadline(absolute)
	time.Sleep(60 * time.Millisecond)
	_, err := conn.Read(make([]byte, 1))
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("expired absolute read: %v", err)
	}
	if time.Since(absolute) > 300*time.Millisecond {
		t.Fatal("expired future deadline was extended by idle renewal")
	}
}

func TestConcurrentStrictReadDeadline(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	conn := New(server).WithReadDeadline(time.Second)
	conn.SetReadDeadlineStrict(true)
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 100 {
			conn.SetReadDeadline(time.Now().Add(time.Second))
			conn.SetReadDeadline(time.Time{})
			conn.SetReadDeadlineStrict(false)
			conn.SetReadDeadlineStrict(true)
		}
		conn.Close()
	})
	for {
		if _, err := conn.Read(make([]byte, 1)); err != nil {
			break
		}
	}
	wg.Wait()
}

// Force multiple real update intervals without resetting the explicit cap.
func TestStrictReadDeadlineRepeatedRenewal(t *testing.T) {
	raw := &deadlineRecorder{}
	conn := New(raw).WithReadDeadline(2 * time.Second)
	conn.SetReadDeadlineStrict(true)
	absolute := time.Now().Add(time.Second)
	conn.SetReadDeadline(absolute)
	for range 3 {
		conn.Read(nil)
		if !raw.read.Equal(absolute) {
			t.Fatalf("idle renewal changed absolute deadline: %s, want %s", raw.read, absolute)
		}
		time.Sleep(300 * time.Millisecond)
	}
}
