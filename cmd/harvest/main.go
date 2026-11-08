package main

import (
	"log/slog"
	"os"

	"github.com/librecosta/harvest/internal/command"
	"github.com/librecosta/harvest/internal/provider"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	registry, err := provider.New(provider.Builtins()...)
	if err == nil {
		err = command.Run(os.Args[1:], registry, logger)
	}
	if err != nil {
		logger.Error("command failed", "reason", err.Error())
		os.Exit(1)
	}
}
