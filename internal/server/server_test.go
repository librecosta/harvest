package server

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

type fakeDatabase struct {
	ping   func(context.Context) error
	closed atomic.Int32
}

func (db *fakeDatabase) Ping(ctx context.Context) error {
	if db.ping != nil {
		return db.ping(ctx)
	}
	return nil
}

func (db *fakeDatabase) Close() { db.closed.Add(1) }

func TestBindFailureClosesDependenciesOnce(t *testing.T) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	db := new(fakeDatabase)
	if err := serve(t.Context(), ln.Addr().String(), db, slog.New(slog.DiscardHandler), nil); err == nil {
		t.Fatal("server accepted an occupied listener address")
	}
	if count := db.closed.Load(); count != 1 {
		t.Fatalf("closed dependencies %d times", count)
	}
}

func TestCanceledStartupClosesDependenciesOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	cancel()
	db := new(fakeDatabase)
	done := make(chan error, 1)
	go func() {
		done <- serve(ctx, "127.0.0.1:0", db, slog.New(slog.DiscardHandler), nil)
	}()
	select {
	case err := <-done:
		if err != nil || db.closed.Load() != 1 {
			t.Fatalf("canceled startup returned %v, close count = %d", err, db.closed.Load())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("startup missed shutdown")
	}
}

func TestGracefulShutdownDrainsBeforeClosingDependencies(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	started := make(chan string, 1)
	draining := make(chan struct{})
	logger := slog.New(lifecycleLog{Handler: slog.DiscardHandler, started: started, draining: draining})
	probing := make(chan struct{})
	release := make(chan struct{})
	db := &fakeDatabase{ping: func(ctx context.Context) error {
		close(probing)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	done := make(chan error, 1)
	go func() { done <- serve(ctx, "127.0.0.1:0", db, logger, nil) }()

	var address string
	select {
	case address = <-started:
	case <-ctx.Done():
		t.Fatal("OnListen did not run")
	}
	client := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	response := make(chan int, 1)
	go func() {
		resp, err := client.Get("http://" + address + "/readyz")
		if err != nil {
			response <- 0
			return
		}
		resp.Body.Close()
		response <- resp.StatusCode
	}()
	select {
	case <-probing:
	case <-ctx.Done():
		t.Fatal("readiness handler did not start")
	}
	cancel()
	cancel()
	select {
	case <-draining:
	case <-time.After(2 * time.Second):
		t.Fatal("OnPreShutdown did not run")
	}
	if db.closed.Load() != 0 {
		t.Fatal("database closed before the in-flight readiness request")
	}
	close(release)
	if status := <-response; status != http.StatusServiceUnavailable {
		t.Fatalf("readiness during shutdown returned %d", status)
	}
	select {
	case err := <-done:
		if err != nil || db.closed.Load() != 1 {
			t.Fatalf("shutdown returned %v, close count = %d", err, db.closed.Load())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("graceful shutdown did not finish")
	}
}

type lifecycleLog struct {
	slog.Handler
	started  chan string
	draining chan struct{}
}

func (log lifecycleLog) Enabled(context.Context, slog.Level) bool { return true }

func (log lifecycleLog) Handle(_ context.Context, record slog.Record) error {
	switch record.Message {
	case "HTTP listening":
		record.Attrs(func(attr slog.Attr) bool {
			if attr.Key == "address" {
				log.started <- attr.Value.String()
			}
			return true
		})
	case "HTTP draining":
		close(log.draining)
	}
	return nil
}
