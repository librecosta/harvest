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
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/librecosta/harvest/internal/config"
	"github.com/librecosta/harvest/internal/database"
	"github.com/librecosta/harvest/internal/execution"
	"github.com/librecosta/harvest/internal/handlers"
	"github.com/librecosta/harvest/internal/provider"
	"github.com/librecosta/harvest/internal/routers"
	"github.com/librecosta/harvest/internal/worker"
)

type dependencies interface {
	Ping(context.Context) error
	Close()
}

type schemaPool struct{ *pgxpool.Pool }

func (p schemaPool) Ping(ctx context.Context) error {
	return database.CheckSchema(ctx, p.Pool)
}

type services struct {
	engine *execution.Engine
	api    *handlers.API
	local  *config.Worker
}

func Run(ctx context.Context, cfg config.Config, registry *provider.Registry, logger *slog.Logger) error {
	if cfg.LocalWorker != nil {
		if err := worker.Validate(*cfg.LocalWorker, registry); err != nil {
			return err
		}
	}
	pool, err := database.Open(ctx, cfg.Database)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	check, cancel := context.WithTimeout(ctx, 3*time.Second)
	err = database.CheckSchema(check, pool)
	cancel()
	if err != nil {
		pool.Close()
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	engine := execution.New(pool, registry, cfg.Principals, cfg.Network)
	if err := engine.PrepareNetwork(ctx); err != nil {
		pool.Close()
		return err
	}
	if cfg.LocalWorker != nil {
		for _, principal := range cfg.Principals {
			if principal.Token == cfg.LocalWorker.Token {
				if err := engine.ValidateLocalWorker(principal, *cfg.LocalWorker); err != nil {
					pool.Close()
					return errors.New("local_worker offers exceed the configured worker principal policy")
				}
				break
			}
		}
	}
	api := handlers.NewAPI(engine, cfg.Principals, logger)
	return serve(ctx, cfg.HTTP.Listen, schemaPool{pool}, logger, &services{engine, api, cfg.LocalWorker})
}

func serve(ctx context.Context, address string, deps dependencies, logger *slog.Logger, svc *services) error {
	var ready atomic.Bool
	var loops sync.WaitGroup
	runtimeCtx, cancelRuntime := context.WithCancel(context.WithoutCancel(ctx))
	drain := sync.OnceFunc(func() {
		ready.Store(false)
		if svc != nil {
			svc.engine.Drain()
		}
		cancelRuntime()
	})
	closeDeps := sync.OnceFunc(func() {
		drain()
		loops.Wait()
		deps.Close()
	})
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
		ErrorHandler:  handlers.FiberError,
		JSONEncoder:   func(v any) ([]byte, error) { return json.Marshal(v) },
		JSONDecoder:   func(data []byte, v any) error { return json.Unmarshal(data, v) },
		BodyLimit:     execution.MaxBodySize,
		ReadTimeout:   5 * time.Second,
		WriteTimeout:  5 * time.Second,
		IdleTimeout:   15 * time.Second,
		StrictRouting: true,
		CaseSensitive: true,
	})
	var api *handlers.API
	if svc != nil {
		api = svc.api
	}
	routers.Register(app, func(probeCtx context.Context) bool {
		if !ready.Load() {
			return false
		}
		probeCtx, cancel := context.WithTimeout(probeCtx, config.ProbeTimeout)
		defer cancel()
		return deps.Ping(probeCtx) == nil && ready.Load()
	}, api)

	app.Hooks().OnListen(func(data fiber.ListenData) error {
		if ctx.Err() == nil {
			ready.Store(true)
			if svc != nil {
				loops.Go(func() { maintain(runtimeCtx, svc.engine, logger) })
				if svc.local != nil {
					loops.Go(func() {
						if err := worker.Run(runtimeCtx, *svc.local, svc.engine.Registry, logger); err != nil {
							logger.Error("embedded worker stopped", "reason", err.Error())
						}
					})
				}
			}
		}
		logger.Info("HTTP listening", "address", net.JoinHostPort(data.Host, data.Port))
		return nil
	})
	app.Hooks().OnPreShutdown(func() error {
		drain()
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
	drain()
	if svc != nil {
		waitForAttempts(svc.engine, logger)
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

func maintain(ctx context.Context, engine *execution.Engine, logger *slog.Logger) {
	reconcile := time.NewTimer(0)
	defer reconcile.Stop()
	prune := time.NewTicker(time.Minute)
	defer prune.Stop()
	permits := time.NewTicker(10 * time.Second)
	defer permits.Stop()
	failed := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-reconcile.C:
			run, cancel := context.WithTimeout(ctx, 2*time.Second)
			err := engine.Reconcile(run)
			cancel()
			delay := time.Second
			if err != nil {
				if !failed && ctx.Err() == nil {
					logger.Warn("reconciliation temporarily unavailable")
				}
				delay = 3 * time.Second
			} else if failed {
				logger.Info("reconciliation recovered")
			}
			failed = err != nil
			reconcile.Reset(delay)
		case <-permits.C:
			run, cancel := context.WithTimeout(ctx, 2*time.Second)
			err := engine.PrunePermits(run)
			cancel()
			if err != nil && ctx.Err() == nil {
				logger.Warn("permit pruning unavailable")
			}
		case <-prune.C:
			run, cancel := context.WithTimeout(ctx, 2*time.Second)
			err := engine.Prune(run)
			cancel()
			if err != nil && ctx.Err() == nil {
				logger.Warn("execution history pruning unavailable")
			}
		}
	}
}

func waitForAttempts(engine *execution.Engine, logger *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), execution.PreDrain)
	defer cancel()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		probe, stop := context.WithTimeout(ctx, 500*time.Millisecond)
		active, err := engine.ActiveLeases(probe)
		stop()
		if err != nil || active == 0 {
			return
		}
		select {
		case <-ctx.Done():
			logger.Info("attempt pre-drain expired")
			return
		case <-ticker.C:
		}
	}
}
