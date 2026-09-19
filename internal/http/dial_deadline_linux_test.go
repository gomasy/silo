//go:build linux

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

package http

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/minio/minio/internal/deadlineconn"
)

// The optional drive dialer must retain its legacy rolling read semantics.
func TestInternodeDialReadDeadline(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			accepted <- nil
			return
		}
		accepted <- conn
	}()
	dial := NewInternodeDialContext(time.Second, TCPOptions{DriveOPTimeout: func() time.Duration { return 200 * time.Millisecond }})
	conn, err := dial(t.Context(), "tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	peer := <-accepted
	if peer == nil {
		t.Fatal("accept failed")
	}
	defer peer.Close()
	if _, ok := conn.(*deadlineconn.DeadlineConn); !ok {
		t.Fatalf("drive connection type %T", conn)
	}
	sent := make(chan error, 1)
	go func() {
		time.Sleep(150 * time.Millisecond)
		if _, err := io.WriteString(peer, "a"); err != nil {
			sent <- err
			return
		}
		time.Sleep(700 * time.Millisecond)
		_, err := io.WriteString(peer, "b")
		sent <- err
	}()
	conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	var b [1]byte
	if _, err := io.ReadFull(conn, b[:]); err != nil || b[0] != 'a' {
		t.Fatalf("rolling explicit deadline: %q %v", b, err)
	}
	conn.SetReadDeadline(time.Time{})
	if _, err := io.ReadFull(conn, b[:]); err != nil || b[0] != 'b' {
		t.Fatalf("disabled read deadline: %q %v", b, err)
	}
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(time.Minute))
	_, err = conn.Read(b[:])
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("drive idle timeout: %v", err)
	}
}
