// Copyright (c) 2026 Pigsty
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/minio/minio/internal/config/etcd"
)

func TestOutboundTLSKeyExchangeDefaults(t *testing.T) {
	for _, debug := range []string{"tlsmlkem=0", "tlsmlkem=1"} {
		t.Run(debug, func(t *testing.T) {
			t.Setenv("GODEBUG", debug)
			for _, version := range []uint16{tls.VersionTLS12, tls.VersionTLS13} {
				t.Run(tls.VersionName(version), func(t *testing.T) {
					hellos := make(chan []tls.CurveID, 1)
					server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
						_, _ = io.WriteString(w, "ok")
					}))
					server.TLS = &tls.Config{
						MinVersion: version, MaxVersion: version,
						GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
							select {
							case hellos <- slices.Clone(hello.SupportedCurves):
							default:
							}
							return nil, nil
						},
					}
					server.StartTLS()
					t.Cleanup(server.Close)
					roots := x509.NewCertPool()
					roots.AddCert(server.Certificate())

					dir := t.TempDir()
					certFile, keyFile := filepath.Join(dir, "public.crt"), filepath.Join(dir, "private.key")
					cert := server.TLS.Certificates[0]
					key, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
					if err != nil {
						t.Fatal(err)
					}
					for path, block := range map[string]*pem.Block{
						certFile: {Type: "CERTIFICATE", Bytes: cert.Certificate[0]},
						keyFile:  {Type: "PRIVATE KEY", Bytes: key},
					} {
						if err := os.WriteFile(path, pem.EncodeToMemory(block), 0o600); err != nil {
							t.Fatal(err)
						}
					}
					t.Setenv(etcd.EnvEtcdEndpoints, server.URL)
					t.Setenv(etcd.EnvEtcdClientCert, "")
					t.Setenv(etcd.EnvEtcdClientCertKey, "")
					etcdConfig, err := etcd.LookupConfig(etcd.DefaultKVS, roots)
					if err != nil {
						t.Fatal(err)
					}
					for name, transport := range map[string]*http.Transport{
						"external":          NewHTTPTransport(),
						"internode":         NewInternodeHTTPTransport(1)().(*http.Transport),
						"replication":       NewRemoteTargetHTTPTransport(false)(),
						"cloud-client-cert": NewHTTPTransportWithClientCerts(certFile, keyFile).(*http.Transport),
						"etcd":              {TLSClientConfig: etcdConfig.TLS},
					} {
						t.Run(name, func(t *testing.T) {
							defer transport.CloseIdleConnections()
							transport.Proxy = nil
							transport.TLSClientConfig.RootCAs = roots
							client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
							resp, err := client.Get(server.URL)
							if err != nil {
								t.Fatal(err)
							}
							defer resp.Body.Close()
							if resp.StatusCode != http.StatusOK || resp.TLS.Version != version {
								t.Fatalf("status %d, TLS %s", resp.StatusCode, tls.VersionName(resp.TLS.Version))
							}
							curves := <-hellos
							if got, want := slices.Contains(curves, tls.X25519MLKEM768), debug == "tlsmlkem=1"; got != want {
								t.Errorf("ML-KEM offered = %v, want %v; curves %v", got, want, curves)
							}
						})
					}
				})
			}
		})
	}
}

func TestServerTLSKeyExchangeDefaults(t *testing.T) {
	for _, debug := range []string{"tlsmlkem=0", "tlsmlkem=1"} {
		t.Run(debug, func(t *testing.T) {
			t.Setenv("GODEBUG", debug)
			// Reuse httptest's certificate with the actual Server TLS constructor.
			seed := httptest.NewTLSServer(http.NotFoundHandler())
			cert := seed.TLS.Certificates[0]
			seed.Close()
			server := httptest.NewUnstartedServer(http.NotFoundHandler())
			server.TLS = newTLSConfig(func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return &cert, nil })
			server.StartTLS()
			defer server.Close()
			roots := x509.NewCertPool()
			leaf, err := x509.ParseCertificate(cert.Certificate[0])
			if err != nil {
				t.Fatal(err)
			}
			roots.AddCert(leaf)
			for _, curve := range []tls.CurveID{tls.X25519MLKEM768, tls.CurveP256} {
				conn, err := tls.Dial("tcp", server.Listener.Addr().String(), &tls.Config{
					RootCAs: roots, MinVersion: tls.VersionTLS13, CurvePreferences: []tls.CurveID{curve},
				})
				wantSuccess := curve == tls.CurveP256 || debug == "tlsmlkem=1"
				if (err == nil) != wantSuccess {
					t.Errorf("curve %v: error %v, want success %v", curve, err, wantSuccess)
				}
				if conn != nil {
					_ = conn.Close()
				}
			}
		})
	}
}
