// Package localrun lets a foreground development server accept stop requests
// from its own checkout without inspecting or signaling PIDs.
package localrun

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	json "encoding/json/v2"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"time"

	"github.com/gofrs/flock"
)

const handshakeTimeout = 2 * time.Second

type endpoint struct {
	Address string   `json:"address"`
	Key     [32]byte `json:"key"`
}

func stateDir() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	cwd, err = filepath.EvalSymlinks(cwd)
	if err != nil {
		return "", err
	}
	return filepath.Join(cwd, ".harvest"), nil
}

// Start holds the checkout lock until cleanup and publishes a random loopback
// control address. The control channel only cancels this process's own context.
func Start(cancel func()) (cleanup func(), err error) {
	dir, err := stateDir()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	lock := flock.New(filepath.Join(dir, "run.lock"))
	acquired, err := lock.TryLock()
	if err != nil {
		return nil, err
	}
	if !acquired {
		return nil, errors.New("this checkout already has a local run")
	}
	defer func() {
		if err != nil {
			_ = lock.Unlock()
		}
	}()

	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	info := endpoint{Address: ln.Addr().String()}
	rand.Read(info.Key[:])
	path := filepath.Join(dir, "run.json")
	if err := publish(path, info); err != nil {
		_ = ln.Close()
		return nil, err
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			authorized := acceptStop(conn, dir, info.Key)
			_ = conn.Close()
			if authorized {
				cancel()
			}
		}
	}()
	return func() {
		_ = ln.Close()
		<-done
		_ = os.Remove(path)
		_ = lock.Unlock()
	}, nil
}

func publish(path string, info endpoint) error {
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), "run-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	_, writeErr := f.Write(data)
	if err := errors.Join(writeErr, f.Close()); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

// Stop reports whether an authenticated local run accepted the request.
func Stop() (bool, error) {
	dir, err := stateDir()
	if err != nil {
		return false, err
	}
	lockPath := filepath.Join(dir, "run.lock")
	if _, err := os.Stat(lockPath); errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	lock := flock.New(lockPath)
	acquired, err := lock.TryLock()
	if err != nil {
		return false, err
	}
	if acquired {
		return false, lock.Unlock()
	}

	info, err := readEndpoint(filepath.Join(dir, "run.json"))
	if err != nil {
		return false, err
	}
	conn, err := net.DialTimeout("tcp4", info.Address, handshakeTimeout)
	if err != nil {
		return false, errors.New("cannot reach the checkout control endpoint")
	}
	defer conn.Close()
	if !requestStop(conn, dir, info.Key) {
		return false, errors.New("could not authenticate and stop this checkout's local run")
	}
	return true, nil
}

func readEndpoint(path string) (endpoint, error) {
	var info endpoint
	f, err := os.Open(path)
	if err != nil {
		return info, errors.New("checkout control endpoint is not published")
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, 4097))
	if err != nil || len(data) > 4096 || json.Unmarshal(data, &info, json.RejectUnknownMembers(true)) != nil {
		return info, errors.New("invalid checkout control record")
	}
	address, err := netip.ParseAddrPort(info.Address)
	if err != nil || address.Addr() != netip.MustParseAddr("127.0.0.1") ||
		address.Port() == 0 || info.Key == [32]byte{} {
		return info, errors.New("invalid checkout control endpoint")
	}
	return info, nil
}

// Both sides prove possession of the key. The request is tied to a fresh nonce
// from the server and the canonical working directory, including across copies.
func acceptStop(conn net.Conn, dir string, key [32]byte) bool {
	_ = conn.SetDeadline(time.Now().Add(handshakeTimeout))
	var challenge [64]byte
	if _, err := io.ReadFull(conn, challenge[:32]); err != nil {
		return false
	}
	rand.Read(challenge[32:])
	proof := append(challenge[32:], sign(key, dir, "server", challenge)...)
	if _, err := conn.Write(proof); err != nil {
		return false
	}
	var request [32]byte
	if _, err := io.ReadFull(conn, request[:]); err != nil ||
		!hmac.Equal(request[:], sign(key, dir, "stop", challenge)) {
		return false
	}
	_, _ = conn.Write([]byte{1})
	return true
}

func requestStop(conn net.Conn, dir string, key [32]byte) bool {
	_ = conn.SetDeadline(time.Now().Add(handshakeTimeout))
	var challenge, reply [64]byte
	rand.Read(challenge[:32])
	if _, err := conn.Write(challenge[:32]); err != nil {
		return false
	}
	if _, err := io.ReadFull(conn, reply[:]); err != nil {
		return false
	}
	copy(challenge[32:], reply[:32])
	if !hmac.Equal(reply[32:], sign(key, dir, "server", challenge)) {
		return false
	}
	if _, err := conn.Write(sign(key, dir, "stop", challenge)); err != nil {
		return false
	}
	var ack [1]byte
	_, err := io.ReadFull(conn, ack[:])
	return err == nil && ack[0] == 1
}

func sign(key [32]byte, dir, purpose string, challenge [64]byte) []byte {
	mac := hmac.New(sha256.New, key[:])
	mac.Write([]byte("harvest-local-v1\x00" + purpose + "\x00" + dir + "\x00"))
	mac.Write(challenge[:])
	return mac.Sum(nil)
}
