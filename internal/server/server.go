package server

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofiber/fiber/v3"

	"github.com/librecosta/harvest/internal/config"
	"github.com/librecosta/harvest/internal/database"
	"github.com/librecosta/harvest/internal/routers"
)

type dependencies interface {
	Ping(context.Context) error
	Close()
}

func Run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	pool, err := database.Open(ctx, cfg.Database)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	return serve(ctx, cfg.HTTP.Listen, pool, logger)
}

func serve(ctx context.Context, address string, deps dependencies, logger *slog.Logger) error {
	var ready atomic.Bool
	closeDeps := sync.OnceFunc(deps.Close)
	defer closeDeps() // Also handles a bind failure or early return before Fiber hooks run.

	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", address)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return errors.New("cannot bind HTTP listener")
	}
	defer listener.Close()

	app := fiber.New(fiber.Config{
		JSONEncoder:   func(v any) ([]byte, error) { return json.Marshal(v) },
		JSONDecoder:   func(data []byte, v any) error { return json.Unmarshal(data, v) },
		ReadTimeout:   5 * time.Second,
		WriteTimeout:  5 * time.Second,
		IdleTimeout:   15 * time.Second,
		StrictRouting: true,
		CaseSensitive: true,
	})
	routers.Register(app, func(probeCtx context.Context) bool {
		if !ready.Load() {
			return false
		}
		probeCtx, cancel := context.WithTimeout(probeCtx, config.ProbeTimeout)
		defer cancel()
		return deps.Ping(probeCtx) == nil && ready.Load()
	})

	app.Hooks().OnListen(func(data fiber.ListenData) error {
		ready.Store(ctx.Err() == nil)
		logger.Info("HTTP listening", "address", net.JoinHostPort(data.Host, data.Port))
		return nil
	})
	app.Hooks().OnPreShutdown(func() error {
		ready.Store(false)
		logger.Info("HTTP draining")
		return nil
	})
	app.Hooks().OnPostShutdown(func(err error) error {
		closeDeps()
		logger.Info("HTTP stopped", "drained", err == nil)
		return nil
	})

	// fasthttp registers its listener before the first Accept. Wait for that
	// point so cancellation during OnListen cannot miss the listener.
	ln := &acceptListener{Listener: listener, started: make(chan struct{})}
	finished := make(chan error, 1)
	go func() {
		finished <- app.Listener(ln, fiber.ListenConfig{DisableStartupMessage: true})
	}()
	select {
	case err := <-finished:
		return listenerResult(err)
	case <-ctx.Done():
	}
	select {
	case err := <-finished:
		return listenerResult(err)
	case <-ln.started:
	}
	shutdownErr := app.ShutdownWithTimeout(config.ShutdownTimeout)
	listenErr := <-finished
	if shutdownErr != nil {
		return errors.New("HTTP graceful shutdown did not complete")
	}
	return listenerResult(listenErr)
}

func listenerResult(err error) error {
	if err != nil {
		return errors.New("HTTP listener stopped unexpectedly")
	}
	return nil
}

type acceptListener struct {
	net.Listener
	once    sync.Once
	started chan struct{}
}

func (ln *acceptListener) Accept() (net.Conn, error) {
	ln.once.Do(func() { close(ln.started) })
	return ln.Listener.Accept()
}
