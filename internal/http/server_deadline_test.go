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
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio/internal/deadlineconn"
)

// Real sockets exercise net/http's TLS, read-ahead and connection-state transitions.
func startDeadlineServer(t *testing.T, secure bool, idle, header time.Duration, handler stdhttp.Handler, hook func(net.Conn, stdhttp.ConnState), configure ...func(*Server)) (*Server, string) {
	t.Helper()
	srv := NewServer([]string{"127.0.0.1:0"}).UseHandler(handler).
		UseTCPOptions(TCPOptions{IdleTimeout: idle}).UseIdleTimeout(idle).
		UseReadTimeout(idle).UseWriteTimeout(idle).UseReadHeaderTimeout(header)
	srv.ConnState = hook
	for _, fn := range configure {
		fn(srv)
	}
	if secure {
		cert, err := getTLSCert()
		if err != nil {
			t.Fatal(err)
		}
		srv.UseTLSConfig(&tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{"http/1.1", "h2"}})
	}
	serve, err := srv.Init(context.Background(), func(_ string, err error) { t.Error(err) })
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- serve() }()
	t.Cleanup(func() {
		srv.Close()
		select {
		case err := <-done:
			if !errors.Is(err, stdhttp.ErrServerClosed) {
				t.Errorf("Serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Serve did not stop")
		}
	})
	return srv, srv.listener.Addr().String()
}

func dialDeadlineServer(t *testing.T, addr string, secure bool) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if secure {
		tc := tls.Client(conn, &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"http/1.1"}})
		if err := tc.Handshake(); err != nil {
			t.Fatal(err)
		}
		return tc
	}
	return conn
}

func writeDeadlineRequest(t *testing.T, conn net.Conn, data string) {
	t.Helper()
	if _, err := io.WriteString(conn, data); err != nil {
		t.Fatal(err)
	}
}

func readDeadlineResponse(t *testing.T, r *bufio.Reader) {
	t.Helper()
	resp, err := stdhttp.ReadResponse(r, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != stdhttp.StatusNoContent {
		t.Fatalf("status: %s", resp.Status)
	}
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatal(err)
	}
}

func requireDeadlineRejection(t *testing.T, conn net.Conn) {
	t.Helper()
	resp, err := stdhttp.ReadResponse(bufio.NewReader(conn), nil)
	if err == nil {
		resp.Body.Close()
		t.Fatalf("request accepted: %s", resp.Status)
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Fatalf("client timed out before server rejected request: %v", err)
	}
}

func TestServerReadHeaderDeadline(t *testing.T) {
	for _, secure := range []bool{false, true} {
		for _, second := range []bool{false, true} {
			for _, trickle := range []bool{false, true} {
				t.Run(fmt.Sprintf("tls=%t/second=%t/trickle=%t", secure, second, trickle), func(t *testing.T) {
					t.Parallel()
					var calls atomic.Int32
					_, addr := startDeadlineServer(t, secure, 2*time.Second, 650*time.Millisecond, stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) { calls.Add(1); w.WriteHeader(204) }), nil)
					conn := dialDeadlineServer(t, addr, secure)
					expected := int32(0)
					if second {
						writeDeadlineRequest(t, conn, "GET /first HTTP/1.1\r\nHost: localhost\r\n\r\n")
						readDeadlineResponse(t, bufio.NewReader(conn))
						expected = 1
						// Header time starts anew for the second request, after the keep-alive wait.
						time.Sleep(750 * time.Millisecond)
					}
					writeDeadlineRequest(t, conn, "GET /slow HTTP/1.1\r\nHost: localhost\r\nX-Slow: ")
					if trickle {
						for range 10 {
							time.Sleep(100 * time.Millisecond)
							if _, err := io.WriteString(conn, "x"); err != nil {
								break
							}
						}
					} else {
						time.Sleep(950 * time.Millisecond)
					}
					_, _ = io.WriteString(conn, "done\r\n\r\n")
					requireDeadlineRejection(t, conn)
					if got := calls.Load(); got != expected {
						t.Fatalf("handler calls = %d, want %d", got, expected)
					}
				})
			}
		}
	}
}

func TestServerKeepAliveDeadline(t *testing.T) {
	for _, secure := range []bool{false, true} {
		t.Run(fmt.Sprintf("tls=%t", secure), func(t *testing.T) {
			t.Parallel()
			_, addr := startDeadlineServer(t, secure, 800*time.Millisecond, 200*time.Millisecond, stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) { w.WriteHeader(204) }), nil)
			conn := dialDeadlineServer(t, addr, secure)
			br := bufio.NewReader(conn)
			for range 2 {
				writeDeadlineRequest(t, conn, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
				readDeadlineResponse(t, br)
				time.Sleep(300 * time.Millisecond)
			}
			// An incomplete method must not renew the keep-alive deadline on each byte.
			writeDeadlineRequest(t, conn, "G")
			time.Sleep(300 * time.Millisecond)
			_, _ = io.WriteString(conn, "E")
			time.Sleep(300 * time.Millisecond)
			_, _ = io.WriteString(conn, "T")
			time.Sleep(300 * time.Millisecond)
			conn.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
			requireDeadlineRejection(t, conn)
		})
	}
}

func runContinuousUpload(t *testing.T, secure, chunked, expect bool, idle, period time.Duration, chunks int) {
	t.Helper()
	nread := make(chan int64, 1)
	_, addr := startDeadlineServer(t, secure, idle, 2*time.Second, stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		n, err := io.Copy(io.Discard, r.Body)
		if err != nil {
			t.Errorf("body read: %v", err)
			w.WriteHeader(400)
			return
		}
		nread <- n
		w.WriteHeader(204)
	}), nil)
	conn := dialDeadlineServer(t, addr, secure)
	conn.SetDeadline(time.Now().Add(time.Duration(chunks)*period + 5*time.Second))
	headers := "POST / HTTP/1.1\r\nHost: localhost\r\n"
	if chunked {
		headers += "Transfer-Encoding: chunked\r\n"
	} else {
		headers += fmt.Sprintf("Content-Length: %d\r\n", chunks)
	}
	if expect {
		headers += "Expect: 100-continue\r\n"
	}
	writeDeadlineRequest(t, conn, headers+"\r\n")
	br := bufio.NewReader(conn)
	if expect {
		resp, err := stdhttp.ReadResponse(br, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 100 {
			t.Fatalf("expected 100 Continue, got %s", resp.Status)
		}
	}
	start := time.Now()
	for range chunks {
		time.Sleep(period)
		if chunked {
			writeDeadlineRequest(t, conn, "1\r\nx\r\n")
		} else {
			writeDeadlineRequest(t, conn, "x")
		}
	}
	if chunked {
		writeDeadlineRequest(t, conn, "0\r\n\r\n")
	}
	readDeadlineResponse(t, br)
	if n := <-nread; n != int64(chunks) {
		t.Fatalf("read %d bytes, want %d", n, chunks)
	}
	if elapsed := time.Since(start); elapsed <= idle {
		t.Fatalf("upload took %s, must exceed idle %s", elapsed, idle)
	} else {
		t.Logf("continuous upload %s > idle %s", elapsed, idle)
	}
	// Verify a fresh request after body EOF and read-deadline cancellation.
	writeDeadlineRequest(t, conn, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
	readDeadlineResponse(t, br)
}

func TestServerContinuousUpload(t *testing.T) {
	for _, secure := range []bool{false, true} {
		for _, chunked := range []bool{false, true} {
			for _, expect := range []bool{false, true} {
				t.Run(fmt.Sprintf("tls=%t/chunked=%t/expect=%t", secure, chunked, expect), func(t *testing.T) {
					t.Parallel()
					runContinuousUpload(t, secure, chunked, expect, 300*time.Millisecond, 80*time.Millisecond, 16)
				})
			}
		}
	}
}

func TestServerDefaultIdleLongUpload(t *testing.T) {
	if os.Getenv("SILO_TEST_LONG_UPLOAD") != "1" {
		t.Skip("set SILO_TEST_LONG_UPLOAD=1 for >30s default-idle regression")
	}
	t.Parallel()
	for _, secure := range []bool{false, true} {
		t.Run(fmt.Sprintf("tls=%t", secure), func(t *testing.T) {
			t.Parallel()
			runContinuousUpload(t, secure, false, false, DefaultIdleTimeout, time.Second, 33)
		})
	}
}

func TestServerIdleBodyDeadline(t *testing.T) {
	for _, secure := range []bool{false, true} {
		t.Run(fmt.Sprintf("tls=%t", secure), func(t *testing.T) {
			t.Parallel()
			result := make(chan error, 1)
			_, addr := startDeadlineServer(t, secure, 200*time.Millisecond, time.Second, stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
				_, err := io.Copy(io.Discard, r.Body)
				result <- err
				w.WriteHeader(400)
			}), nil)
			conn := dialDeadlineServer(t, addr, secure)
			writeDeadlineRequest(t, conn, "PUT / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\n\r\nx")
			select {
			case err := <-result:
				var ne net.Error
				if !errors.As(err, &ne) || !ne.Timeout() {
					t.Fatalf("expected server body timeout, got %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("idle body did not time out")
			}
		})
	}
}

func TestServerBackgroundReadNoDeadline(t *testing.T) {
	for _, secure := range []bool{false, true} {
		for _, body := range []bool{false, true} {
			t.Run(fmt.Sprintf("tls=%t/body=%t", secure, body), func(t *testing.T) {
				t.Parallel()
				_, addr := startDeadlineServer(t, secure, 150*time.Millisecond, time.Second, stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
					if _, err := io.Copy(io.Discard, r.Body); err != nil {
						t.Error(err)
						return
					}
					select {
					case <-r.Context().Done():
						t.Errorf("background read canceled handler: %v", r.Context().Err())
						return
					case <-time.After(700 * time.Millisecond):
					}
					w.WriteHeader(204)
				}), nil)
				conn := dialDeadlineServer(t, addr, secure)
				req := "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"
				if body {
					req = "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1\r\n\r\nx"
				}
				writeDeadlineRequest(t, conn, req)
				readDeadlineResponse(t, bufio.NewReader(conn))
			})
		}
	}
}

func TestServerTLSHandshakeReadDeadline(t *testing.T) {
	_, addr := startDeadlineServer(t, true, 2*time.Second, 200*time.Millisecond, stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) { t.Error("unexpected handler") }), nil)
	conn := dialDeadlineServer(t, addr, false)
	conn.SetReadDeadline(time.Now().Add(time.Second))
	// Partial TLS record header: server must wait for bytes and enforce its own deadline.
	writeDeadlineRequest(t, conn, "\x16\x03")
	var b [1]byte
	_, err := conn.Read(b[:])
	if err == nil {
		t.Fatal("expected handshake failure")
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Fatalf("client timeout before server handshake deadline: %v", err)
	}
}

func TestServerConnStateHook(t *testing.T) {
	for _, secure := range []bool{false, true} {
		t.Run(fmt.Sprintf("tls=%t", secure), func(t *testing.T) {
			states := make(chan stdhttp.ConnState, 16)
			_, addr := startDeadlineServer(t, secure, time.Second, 500*time.Millisecond, stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) { w.WriteHeader(204) }), func(_ net.Conn, s stdhttp.ConnState) { states <- s })
			conn := dialDeadlineServer(t, addr, secure)
			br := bufio.NewReader(conn)
			for range 2 {
				writeDeadlineRequest(t, conn, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
				readDeadlineResponse(t, br)
			}
			for _, want := range []stdhttp.ConnState{stdhttp.StateNew, stdhttp.StateActive, stdhttp.StateIdle, stdhttp.StateActive, stdhttp.StateIdle} {
				select {
				case got := <-states:
					if got != want {
						t.Fatalf("state %v, want %v", got, want)
					}
				case <-time.After(time.Second):
					t.Fatalf("missing state %v", want)
				}
			}
		})
	}
}

func TestServerHTTP2Deadlines(t *testing.T) {
	bodyErr := make(chan error, 1)
	started := make(chan struct{})
	var connections atomic.Int32
	_, addr := startDeadlineServer(t, true, 400*time.Millisecond, 200*time.Millisecond, stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		if r.ProtoMajor != 2 {
			t.Errorf("expected HTTP/2, got %s", r.Proto)
		}
		if r.Method == "PUT" {
			// Isolate the native read timer: otherwise the equal-duration write
			// timer can win and close the stream with a different error.
			if err := stdhttp.NewResponseController(w).SetWriteDeadline(time.Time{}); err != nil {
				bodyErr <- err
				close(started)
				return
			}
			close(started)
			_, err := io.Copy(io.Discard, r.Body)
			bodyErr <- err
			if err != nil {
				return
			}
		}
		w.WriteHeader(204)
	}), func(_ net.Conn, state stdhttp.ConnState) {
		if state == stdhttp.StateNew {
			connections.Add(1)
		}
	}, func(srv *Server) { srv.IdleTimeout = 3 * time.Second })
	// Configure only HTTP/2 so the server's HTTP/1-first ALPN preference
	// cannot turn this into an HTTP/1 smoke test.
	protocols := new(stdhttp.Protocols)
	protocols.SetHTTP2(true)
	tr := &stdhttp.Transport{Protocols: protocols, TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	t.Cleanup(tr.CloseIdleConnections)
	client := &stdhttp.Client{Transport: tr, Timeout: 3 * time.Second}
	for range 2 {
		resp, err := client.Get("https://" + addr)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.TLS == nil || resp.TLS.NegotiatedProtocol != "h2" || resp.Proto != "HTTP/2.0" || resp.StatusCode != 204 {
			t.Fatalf("unexpected %s %s", resp.Proto, resp.Status)
		}
	}
	pr, pw := io.Pipe()
	defer pr.Close()
	defer pw.Close()
	req, err := stdhttp.NewRequest("PUT", "https://"+addr, pr)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		resp, err := client.Do(req)
		if resp != nil {
			resp.Body.Close()
		}
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP/2 PUT handler did not start")
	}
	// A stalled stream must not set a deadline on other multiplexed requests.
	resp, err := client.Get("https://" + addr)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 204 {
		t.Fatalf("healthy HTTP/2 stream: %s", resp.Status)
	}
	// Native HTTP/2 still applies its existing per-stream ReadTimeout.
	select {
	case err := <-bodyErr:
		var ne net.Error
		if !errors.As(err, &ne) || !ne.Timeout() {
			t.Fatalf("expected native HTTP/2 read timeout, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("native HTTP/2 stream timeout was lost")
	}
	pw.Close()
	<-done
	resp, err = client.Get("https://" + addr)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if connections.Load() != 1 {
		t.Fatalf("HTTP/2 stream timeout replaced the connection: %d connections", connections.Load())
	}
}

func TestServerEarlyBodyClose(t *testing.T) {
	for _, secure := range []bool{false, true} {
		t.Run(fmt.Sprintf("tls=%t", secure), func(t *testing.T) {
			t.Parallel()
			_, addr := startDeadlineServer(t, secure, 200*time.Millisecond, time.Second, stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
				if err := r.Body.Close(); err != nil {
					t.Error(err)
				}
				w.WriteHeader(204)
			}), nil)
			conn := dialDeadlineServer(t, addr, secure)
			br := bufio.NewReader(conn)
			writeDeadlineRequest(t, conn, "PUT / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\n\r\nx")
			time.Sleep(300 * time.Millisecond)
			writeDeadlineRequest(t, conn, "y")
			readDeadlineResponse(t, br)
			writeDeadlineRequest(t, conn, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
			readDeadlineResponse(t, br)
		})
	}
}

func TestServerPipelinedDeadline(t *testing.T) {
	for _, secure := range []bool{false, true} {
		t.Run(fmt.Sprintf("tls=%t", secure), func(t *testing.T) {
			_, addr := startDeadlineServer(t, secure, 300*time.Millisecond, 200*time.Millisecond, stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
				if _, err := io.Copy(io.Discard, r.Body); err != nil {
					t.Error(err)
					w.WriteHeader(400)
					return
				}
				w.WriteHeader(204)
			}), nil)
			conn := dialDeadlineServer(t, addr, secure)
			// Second request headers arrive in the first socket read; body continues
			// past ReadTimeout to require the buffered request's StateActive hook.
			writeDeadlineRequest(t, conn, "GET /first HTTP/1.1\r\nHost: localhost\r\n\r\nPUT /second HTTP/1.1\r\nHost: localhost\r\nContent-Length: 8\r\n\r\n")
			br := bufio.NewReader(conn)
			readDeadlineResponse(t, br)
			for range 8 {
				time.Sleep(80 * time.Millisecond)
				writeDeadlineRequest(t, conn, "x")
			}
			readDeadlineResponse(t, br)
		})
	}
}

func TestServerHijackedDeadline(t *testing.T) {
	for _, secure := range []bool{false, true} {
		t.Run(fmt.Sprintf("tls=%t", secure), func(t *testing.T) {
			t.Parallel()
			done := make(chan error, 1)
			_, addr := startDeadlineServer(t, secure, 150*time.Millisecond, time.Second, stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
				conn, rw, err := stdhttp.NewResponseController(w).Hijack()
				if err != nil {
					done <- err
					return
				}
				defer conn.Close()
				if !secure {
					if _, ok := deadlineconn.Unwrap(conn).(*net.TCPConn); !ok {
						done <- errors.New("grid-style Unwrap no longer returns TCPConn")
						return
					}
				}
				_, err = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: test\r\n\r\n")
				if err == nil {
					err = rw.Flush()
				}
				if err != nil {
					done <- err
					return
				}
				b, err := rw.ReadByte()
				if err == nil {
					_, err = conn.Write([]byte{b})
				}
				done <- err
			}), nil)
			conn := dialDeadlineServer(t, addr, secure)
			writeDeadlineRequest(t, conn, "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: test\r\n\r\n")
			br := bufio.NewReader(conn)
			resp, err := stdhttp.ReadResponse(br, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != 101 {
				t.Fatalf("expected upgrade, got %s", resp.Status)
			}
			time.Sleep(700 * time.Millisecond)
			writeDeadlineRequest(t, conn, "x")
			b, err := br.ReadByte()
			if err != nil || b != 'x' {
				t.Fatalf("hijacked echo: %q, %v", b, err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func runContinuousDownload(t *testing.T, secure bool, idle, period time.Duration, chunks int) {
	t.Helper()
	_, addr := startDeadlineServer(t, secure, idle, 2*time.Second, stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(chunks))
		for range chunks {
			time.Sleep(period)
			if _, err := io.WriteString(w, "x"); err != nil {
				t.Error(err)
				return
			}
			if err := stdhttp.NewResponseController(w).Flush(); err != nil {
				t.Error(err)
				return
			}
		}
	}), nil)
	conn := dialDeadlineServer(t, addr, secure)
	conn.SetDeadline(time.Now().Add(time.Duration(chunks)*period + 5*time.Second))
	start := time.Now()
	writeDeadlineRequest(t, conn, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
	resp, err := stdhttp.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	n, err := io.Copy(io.Discard, resp.Body)
	if err != nil || n != int64(chunks) {
		t.Fatalf("download: %d bytes, %v", n, err)
	}
	if elapsed := time.Since(start); elapsed <= idle {
		t.Fatalf("download must outlast idle: %s <= %s", elapsed, idle)
	} else {
		t.Logf("continuous download %s > idle %s", elapsed, idle)
	}
}

func TestServerContinuousDownload(t *testing.T) {
	for _, secure := range []bool{false, true} {
		t.Run(fmt.Sprintf("tls=%t", secure), func(t *testing.T) {
			t.Parallel()
			runContinuousDownload(t, secure, 300*time.Millisecond, 80*time.Millisecond, 16)
		})
	}
}

func TestServerDefaultIdleLongDownload(t *testing.T) {
	if os.Getenv("SILO_TEST_LONG_UPLOAD") != "1" {
		t.Skip("set SILO_TEST_LONG_UPLOAD=1 for >30s default-idle transfer regressions")
	}
	t.Parallel()
	for _, secure := range []bool{false, true} {
		t.Run(fmt.Sprintf("tls=%t", secure), func(t *testing.T) {
			t.Parallel()
			runContinuousDownload(t, secure, DefaultIdleTimeout, time.Second, 33)
		})
	}
}
