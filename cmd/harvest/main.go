package main

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
	"github.com/librecosta/harvest/internal/localrun"
	"github.com/librecosta/harvest/internal/server"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	// pgx inherits libpq variables before applying a connection string. Clear
	// them in this process, before command execution, so YAML owns the settings.
	for _, item := range os.Environ() {
		key, _, _ := strings.Cut(item, "=")
		if strings.HasPrefix(strings.ToUpper(key), "PG") {
			if err := os.Unsetenv(key); err != nil {
				logger.Error("cannot clear ambient PostgreSQL settings")
				os.Exit(1)
			}
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := rootCommand(logger).ExecuteContext(ctx)
	stop()
	if err != nil {
		logger.Error("command failed", "reason", err.Error())
		os.Exit(1)
	}
}

func rootCommand(logger *slog.Logger) *cobra.Command {
	root := &cobra.Command{
		Use: "harvest", Short: "Harvest data acquisition engine",
		SilenceErrors: true, SilenceUsage: true,
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
	}
	var path string
	root.PersistentFlags().StringVar(&path, "config", "", "path to an explicit YAML configuration file")
	var local bool
	serve := &cobra.Command{
		Use: "serve", Short: "Run the Harvest server", Args: cobra.NoArgs,
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
			return server.Run(ctx, cfg, logger)
		},
	}
	serve.Flags().BoolVar(&local, "local", false, "enable this working directory's authenticated stop command")

	health := &cobra.Command{
		Use: "healthcheck", Short: "Check the configured server's local readiness endpoint", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load(path)
			if err != nil {
				return err
			}
			return server.Probe(cmd.Context(), cfg.HTTP)
		},
	}
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
	root.AddCommand(serve, health, migrate, stop)
	return root
}
