package database

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/librecosta/harvest/internal/config"
)

// Open requires the CLI to clear libpq environment variables before pgx parses
// its connection settings. Explicit credentials and TLS bypass home-directory defaults.
func Open(ctx context.Context, settings config.Database) (*pgxpool.Pool, error) {
	return open(ctx, settings, false)
}

func open(ctx context.Context, settings config.Database, readOnly bool) (*pgxpool.Pool, error) {
	endpoint := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(settings.User, settings.Password),
		Host:     net.JoinHostPort(settings.Host, strconv.Itoa(settings.Port)),
		Path:     "/" + settings.Name,
		RawQuery: "sslmode=disable&passfile=&connect_timeout=3&application_name=harvest",
	}
	cfg, err := pgxpool.ParseConfig(endpoint.String())
	if err != nil {
		return nil, errors.New("invalid PostgreSQL connection configuration")
	}
	if settings.TLSMode == "verify-full" {
		cfg.ConnConfig.TLSConfig = &tls.Config{
			ServerName: settings.Host,
			MinVersion: tls.VersionTLS12,
		}
	}
	if readOnly {
		cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	}
	cfg.MaxConns = int32(settings.MaxConns)
	cfg.AfterConnect = func(_ context.Context, conn *pgx.Conn) error {
		conn.TypeMap().RegisterType(&pgtype.Type{Name: "timestamptz", OID: pgtype.TimestamptzOID, Codec: &pgtype.TimestamptzCodec{ScanLocation: time.UTC}})
		version := conn.PgConn().ParameterStatus("server_version")
		major, _, _ := strings.Cut(version, ".")
		if major != "18" {
			return errors.New("Harvest requires PostgreSQL 18")
		}
		return nil
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, errors.New("cannot initialize PostgreSQL pool")
	}
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(probeCtx); err != nil {
		pool.Close()
		return nil, errors.New("cannot connect to PostgreSQL 18 with the configured credentials and TLS mode")
	}
	return pool, nil
}
