// Copyright (c) 2026 PGSTY
// SPDX-License-Identifier: AGPL-3.0-only

package target

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

// A broker must not make the notification client allocate or read an oversized
// frame, including before connection.tune negotiates the frame size limit.
func TestAMQPRejectsOversizedHandshakeFrame(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })

	uri, err := amqp091.ParseURI("amqp://" + listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	target, err := NewAMQPTarget("oversized-frame", AMQPArgs{Enable: true, URL: uri},
		func(context.Context, error, string, ...any) {})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { target.Close() })

	brokerDone := make(chan error, 1)
	go func() {
		brokerDone <- func() error {
			conn, err := listener.Accept()
			if err != nil {
				return err
			}
			defer conn.Close()
			if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				return err
			}
			var protocol [8]byte
			if _, err := io.ReadFull(conn, protocol[:]); err != nil {
				return err
			}
			if protocol != [8]byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1} {
				return fmt.Errorf("unexpected AMQP protocol header: %x", protocol)
			}
			// A method frame declaring an 8 KiB payload exceeds the 4 KiB
			// pre-negotiation limit. Send only its header: rejection must
			// happen without allocating or waiting for the declared body.
			if _, err := conn.Write([]byte{1, 0, 0, 0, 0, 0x20, 0}); err != nil {
				return err
			}
			var response [1]byte
			if n, err := conn.Read(response[:]); n != 0 || !errors.Is(err, io.EOF) {
				return fmt.Errorf("client did not close after oversized frame header: read %d bytes, error %v", n, err)
			}
			return nil
		}()
	}()

	active, err := target.IsActive()
	if active || err == nil {
		t.Errorf("oversized handshake frame accepted: active=%v, error=%v", active, err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatal(err)
	}
}
