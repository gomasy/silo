// Copyright (c) 2026 PGSTY
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/minio/madmin-go/v3"
	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/client/v3/namespace"
)

type iamRevisionLockObserver struct {
	ObjectLayer
	path    string
	waiting chan struct{}
	once    sync.Once
}

func (o *iamRevisionLockObserver) NewNSLock(bucket string, objects ...string) RWLocker {
	lock := o.ObjectLayer.NewNSLock(bucket, objects...)
	if bucket == minioMetaBucket && len(objects) == 1 && objects[0] == o.path {
		return &iamRevisionObservedLock{RWLocker: lock, observe: func() { o.once.Do(func() { close(o.waiting) }) }}
	}
	return lock
}

type iamRevisionObservedLock struct {
	RWLocker
	observe func()
}

func (l *iamRevisionObservedLock) GetLock(ctx context.Context, timeout *dynamicTimeout) (LockContext, error) {
	l.observe()
	return l.RWLocker.GetLock(ctx, timeout)
}

type iamRevisionWatchObserver struct {
	etcd.Watcher
	waiting chan struct{}
	once    sync.Once
}

func (w *iamRevisionWatchObserver) Watch(ctx context.Context, key string, opts ...etcd.OpOption) etcd.WatchChan {
	w.once.Do(func() { close(w.waiting) })
	return w.Watcher.Watch(ctx, key, opts...)
}

// Simulate an unavailable cleanup RPC. Mutex.Lock calls Delete after its wait
// is canceled; that RPC must inherit a deadline too, not Client.Ctx() forever.
type iamRevisionCleanupBlocker struct {
	etcd.KV
	release chan struct{}
}

func (b *iamRevisionCleanupBlocker) Delete(ctx context.Context, key string, opts ...etcd.OpOption) (*etcd.DeleteResponse, error) {
	if strings.Contains(key, "/iam-revision-locks/") {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-b.release:
		}
	}
	return b.KV.Delete(ctx, key, opts...)
}

type iamRevisionReadBlocker struct {
	IAMStorageAPI
	path    string
	after   int
	waiting chan struct{}
}

func (b *iamRevisionReadBlocker) loadIAMConfig(ctx context.Context, item any, path string) error {
	if path == b.path {
		b.after--
		if b.after == 0 {
			close(b.waiting)
			<-ctx.Done()
			return ctx.Err()
		}
	}
	return b.IAMStorageAPI.loadIAMConfig(ctx, item, path)
}

func TestIAMRevisionReadDoesNotBlockAuthentication(t *testing.T) {
	for _, stage := range []struct {
		name   string
		offset time.Duration
	}{{"deletion", time.Minute}, {"retained_revocation", -time.Minute}} {
		t.Run(stage.name, func(t *testing.T) {
			resetTestGlobals()
			t.Cleanup(resetTestGlobals)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			disks, err := getRandomDisks(1)
			if err != nil {
				t.Fatal(err)
			}
			obj, _, err := initObjectLayer(ctx, mustGetPoolEndpoints(0, disks...))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				obj.Shutdown(context.Background())
				os.RemoveAll(disks[0])
			})
			store := &IAMStoreSys{IAMStorageAPI: newIAMObjectStore(obj, MinIOUsersSysType)}
			const user = "read-blocked-parent"
			created, err := store.AddUser(ctx, user, madmin.AddOrUpdateUserReq{SecretKey: "original-password", Status: madmin.AccountEnabled})
			if err != nil {
				t.Fatal(err)
			}
			blocked := &iamRevisionReadBlocker{IAMStorageAPI: store.IAMStorageAPI, path: getUserIdentityPath(user, regUser), after: 1, waiting: make(chan struct{})}
			store.IAMStorageAPI = blocked
			done := make(chan error, 1)
			go func() {
				done <- store.DeleteUser(withIAMReplicationTime(ctx, created.Add(stage.offset)), user, regUser)
			}()
			defer func() { cancel(); <-done }()
			select {
			case <-blocked.waiting:
			case <-time.After(5 * time.Second):
				t.Fatal("revision read was not attempted")
			}
			read := make(chan bool, 1)
			go func() {
				u, ok := store.GetUser(user)
				read <- ok && u.Credentials.SecretKey == "original-password"
			}()
			select {
			case ok := <-read:
				if !ok {
					t.Fatal("pending revision read changed the cached identity")
				}
			case <-time.After(time.Second):
				t.Fatal("revision read blocked cached authentication")
			}
		})
	}
}

func TestIAMRevisionLockContention(t *testing.T) {
	for _, backend := range []string{"object", "etcd"} {
		t.Run(backend, func(t *testing.T) {
			endpoint := os.Getenv("SILO_TEST_IAM_REVOCATION_ETCD")
			if backend == "etcd" && endpoint == "" {
				t.Skip("set SILO_TEST_IAM_REVOCATION_ETCD to a disposable etcd endpoint")
			}
			for _, outcome := range []string{"release", "cancel", "default_timeout"} {
				t.Run(outcome, func(t *testing.T) {
					resetTestGlobals()
					t.Cleanup(resetTestGlobals)
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					oldTimeout := defaultContextTimeout
					defaultContextTimeout = 2 * time.Second
					t.Cleanup(func() { defaultContextTimeout = oldTimeout })
					must := func(err error) {
						t.Helper()
						if err != nil {
							t.Fatal(err)
						}
					}
					const user = "contended-user"
					path := getUserIdentityPath(user, regUser)
					waiting := make(chan struct{})
					var store *IAMStoreSys
					var hold func() func()
					unblockCleanup := func() {}
					if backend == "object" {
						disks, err := getRandomDisks(1)
						must(err)
						obj, _, err := initObjectLayer(ctx, mustGetPoolEndpoints(0, disks...))
						must(err)
						t.Cleanup(func() {
							obj.Shutdown(context.Background())
							os.RemoveAll(disks[0])
						})
						observed := &iamRevisionLockObserver{ObjectLayer: obj, path: path + ".revision-lock", waiting: waiting}
						store = &IAMStoreSys{IAMStorageAPI: newIAMObjectStore(obj, MinIOUsersSysType)}
						hold = func() func() {
							lock := obj.NewNSLock(minioMetaBucket, observed.path)
							lc, err := lock.GetLock(ctx, newDynamicTimeout(time.Second, time.Second))
							must(err)
							store.IAMStorageAPI.(*IAMObjectStore).objAPI = observed
							return func() { lock.Unlock(lc) }
						}
					} else {
						client, err := etcd.New(etcd.Config{Endpoints: strings.Split(endpoint, ","), DialTimeout: time.Second})
						must(err)
						t.Cleanup(func() { client.Close() })
						prefix := fmt.Sprintf("/silo-lock-test/%d/", time.Now().UnixNano())
						client.KV = namespace.NewKV(client.KV, prefix)
						client.Watcher = namespace.NewWatcher(client.Watcher, prefix)
						store = &IAMStoreSys{IAMStorageAPI: newIAMEtcdStore(client, MinIOUsersSysType)}
						hold = func() func() {
							session, err := concurrency.NewSession(client, concurrency.WithContext(ctx))
							must(err)
							lock := concurrency.NewMutex(session, fmt.Sprintf("%s/iam-revision-locks/%x", minioConfigPrefix, sha256.Sum256([]byte(path))))
							must(lock.Lock(ctx))
							client.Watcher = &iamRevisionWatchObserver{Watcher: client.Watcher, waiting: waiting}
							blocker := &iamRevisionCleanupBlocker{KV: client.KV, release: make(chan struct{})}
							client.KV = blocker
							unblockCleanup = sync.OnceFunc(func() { close(blocker.release) })
							t.Cleanup(unblockCleanup)
							return func() { session.Close() }
						}
					}
					request := func(secret string) madmin.AddOrUpdateUserReq {
						return madmin.AddOrUpdateUserReq{SecretKey: secret, Status: madmin.AccountEnabled}
					}
					_, err := store.AddUser(ctx, user, request("original-password"))
					must(err)
					release := sync.OnceFunc(hold())
					t.Cleanup(release)
					writeCtx, cancelWrite := context.WithCancel(ctx)
					defer cancelWrite()
					first, second := make(chan error, 1), make(chan error, 1)
					var writers sync.WaitGroup
					t.Cleanup(func() {
						cancelWrite()
						unblockCleanup()
						release()
						writers.Wait()
					})
					writers.Go(func() {
						_, err := store.AddUser(writeCtx, user, request("first-password"))
						first <- err
					})
					select {
					case <-waiting:
					case <-time.After(5 * time.Second):
						t.Fatal("writer did not attempt the held revision lock")
					}
					// A second writer must queue without taking the cache's RWMutex:
					// Go's writer preference would otherwise block every new reader.
					writers.Go(func() {
						_, err := store.AddUser(ctx, user, request("second-password"))
						second <- err
					})
					select {
					case err := <-second:
						t.Fatalf("second writer bypassed the first: %v", err)
					case <-time.After(50 * time.Millisecond):
					}
					read := make(chan UserIdentity, 1)
					go func() {
						u, _ := store.GetUser(user)
						read <- u
					}()
					select {
					case u := <-read:
						if u.Credentials.SecretKey != "original-password" {
							t.Fatal("pending write changed the cached credential")
						}
					case <-time.After(time.Second):
						t.Fatal("distributed lock contention blocked cached authentication")
					}
					switch outcome {
					case "release":
						release()
					case "cancel":
						cancelWrite()
					}
					select {
					case err := <-first:
						if outcome == "release" {
							must(err)
						} else if err == nil {
							t.Fatal("canceled or timed-out write succeeded")
						}
					case <-time.After(5 * time.Second):
						t.Fatal("lock wait or cancellation cleanup exceeded its deadline")
					}
					release()
					select {
					case err := <-second:
						must(err)
					case <-time.After(5 * time.Second):
						t.Fatal("queued writer did not recover after the first completed")
					}
					cached, ok := store.GetUser(user)
					if !ok || cached.Credentials.SecretKey != "second-password" {
						t.Fatal("cached write order was lost")
					}
					var persisted UserIdentity
					must(store.loadIAMConfig(ctx, &persisted, path))
					if persisted.Credentials.SecretKey != cached.Credentials.SecretKey || !persisted.UpdatedAt.Equal(cached.UpdatedAt) {
						t.Fatal("persistent and cached revisions differ")
					}
				})
			}
		})
	}
}
