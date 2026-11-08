package server

import (
	"context"
	"errors"
	"net/http"

	"github.com/librecosta/harvest/internal/config"
	"github.com/librecosta/harvest/internal/routers"
)

func Probe(ctx context.Context, cfg config.HTTP) error {
	client := &http.Client{
		Transport:     &http.Transport{DisableKeepAlives: true},
		Timeout:       config.ProbeTimeout + config.ProbeTimeout/2,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+cfg.ProbeAddress()+routers.ReadyPath, nil)
	if err != nil {
		return errors.New("invalid local readiness probe address")
	}
	resp, err := client.Do(req)
	if err != nil {
		return errors.New("local readiness probe failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.New("Harvest is not ready")
	}
	return nil
}
