package localrun

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStopOnlyOwnedRun(t *testing.T) {
	root := t.TempDir()
	t.Chdir(root)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cleanup, err := Start(cancel)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Start(cancel); err == nil {
		cleanup()
		t.Fatal("second local run acquired the same checkout lock")
	}
	state, err := stateDir()
	if err != nil {
		cleanup()
		t.Fatal(err)
	}
	info, err := readEndpoint(filepath.Join(state, "run.json"))
	if err != nil {
		cleanup()
		t.Fatal(err)
	}
	defer func() {
		if cleanup != nil {
			cleanup()
		}
	}()
	conn, err := net.DialTimeout("tcp4", info.Address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	accepted := requestStop(conn, state+"-other-checkout", info.Key)
	conn.Close()
	if accepted || ctx.Err() != nil {
		t.Fatal("different checkout's request stopped the owner")
	}
	for i := 0; i < 2; i++ {
		if ok, err := Stop(); err != nil || !ok {
			t.Fatalf("Stop = %t, %v", ok, err)
		}
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("authenticated stop did not cancel the owner")
	}
	cleanup()
	cleanup = nil
	if err := publish(filepath.Join(state, "run.json"), info); err != nil {
		t.Fatal(err)
	}
	if ok, err := Stop(); err != nil || ok {
		t.Fatalf("stale record caused a stop attempt: %t, %v", ok, err)
	}
	if err := os.Remove(filepath.Join(state, "run.json")); err != nil {
		t.Fatal(err)
	}
}
