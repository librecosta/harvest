package command

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/librecosta/harvest/internal/config"
	"github.com/librecosta/harvest/internal/database"
	"github.com/librecosta/harvest/internal/execution"
	"github.com/librecosta/harvest/internal/localrun"
	"github.com/librecosta/harvest/internal/provider"
	"github.com/librecosta/harvest/internal/server"
	"github.com/librecosta/harvest/internal/worker"
)

func Run(args []string, registry *provider.Registry, logger *slog.Logger) error {
	// pgx inherits libpq variables before applying a connection string. Clear
	// them in this process before command execution so YAML owns the settings.
	for _, item := range os.Environ() {
		key, _, _ := strings.Cut(item, "=")
		if strings.HasPrefix(strings.ToUpper(key), "PG") {
			if err := os.Unsetenv(key); err != nil {
				return errors.New("cannot clear ambient PostgreSQL settings")
			}
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	root := rootCommand(registry, logger)
	root.SetArgs(args)
	return root.ExecuteContext(ctx)
}

func rootCommand(registry *provider.Registry, logger *slog.Logger) *cobra.Command {
	root := &cobra.Command{
		Use: "harvest", Short: "Harvest data acquisition engine",
		SilenceErrors: true, SilenceUsage: true,
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
	}
	var path string
	root.PersistentFlags().StringVar(&path, "config", "", "path to an explicit YAML configuration file")
	var local bool
	serve := &cobra.Command{
		Use: "serve", Short: "Run the Harvest control plane", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load(path)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithCancel(cmd.Context())
			defer cancel()
			if local {
				cleanup, err := localrun.Start(cancel)
				if err != nil {
					return err
				}
				defer cleanup()
			}
			return server.Run(ctx, cfg, registry, logger)
		},
	}
	serve.Flags().BoolVar(&local, "local", false, "enable this working directory's authenticated stop command")

	workerCmd := &cobra.Command{
		Use: "worker", Short: "Run a remote Harvest worker", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.LoadWorker(path)
			if err != nil {
				return err
			}
			return worker.Run(cmd.Context(), cfg, registry, logger)
		},
	}
	var workerProbe bool
	health := &cobra.Command{
		Use: "healthcheck", Short: "Check server or worker control-plane readiness", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if workerProbe {
				cfg, err := config.LoadWorker(path)
				if err != nil {
					return err
				}
				return worker.Probe(cmd.Context(), cfg, registry)
			}
			cfg, err := config.Load(path)
			if err != nil {
				return err
			}
			return server.Probe(cmd.Context(), cfg.HTTP)
		},
	}
	health.Flags().BoolVar(&workerProbe, "worker", false, "probe with an explicit worker YAML and credential")
	migrate := &cobra.Command{
		Use: "migrate [status|up]", Short: "Inspect or apply the embedded Goose migrations",
		Args: cobra.ExactArgs(1), ValidArgs: []string{"status", "up"},
		RunE: func(cmd *cobra.Command, args []string) error {
			if args[0] != "status" && args[0] != "up" {
				return errors.New("migrate requires status or up")
			}
			cfg, err := config.Load(path)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 2*time.Minute)
			defer cancel()
			return database.Migrate(ctx, cfg.Database, args[0], cmd.OutOrStdout())
		},
	}
	stop := &cobra.Command{
		Use: "stop", Short: "Request graceful shutdown of this checkout's local server", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			requested, err := localrun.Stop()
			if err != nil {
				return err
			}
			message := "No local Harvest run in this checkout."
			if requested {
				message = "Requested graceful shutdown."
			}
			_, err = fmt.Fprintln(cmd.OutOrStdout(), message)
			return err
		},
	}
	policy := &cobra.Command{Use: "policy", Short: "Initialize the database-owned operational policy"}
	policy.AddCommand(&cobra.Command{
		Use: "init", Short: "Import the legacy server network block once", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load(path)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
			defer cancel()
			pool, err := database.Open(ctx, cfg.Database)
			if err != nil {
				return err
			}
			defer pool.Close()
			if err := database.CheckSchema(ctx, pool); err != nil {
				return err
			}
			engine := execution.New(pool, registry, cfg.Principals, cfg.Retention)
			if err := engine.InitNetworkPolicy(ctx, cfg.Network); err != nil {
				return err
			}
			_, err = fmt.Fprintln(cmd.OutOrStdout(), "Initialized network policy revision 1.")
			return err
		},
	})
	root.AddCommand(serve, workerCmd, health, migrate, stop, policy)
	return root
}
