package worker

import (
	"bytes"
	"context"
	json "encoding/json/v2"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/librecosta/harvest/internal/config"
	"github.com/librecosta/harvest/internal/execution"
	"github.com/librecosta/harvest/internal/provider"
)

type client struct {
	http  *http.Client
	base  string
	token string
}

func newClient(cfg config.Worker) client {
	transport := &http.Transport{
		DialContext:         (&net.Dialer{Timeout: 2 * time.Second}).DialContext,
		TLSHandshakeTimeout: 2 * time.Second,
		MaxIdleConnsPerHost: int(cfg.Concurrency)*2 + 1,
		MaxConnsPerHost:     int(cfg.Concurrency)*2 + 1,
		IdleConnTimeout:     30 * time.Second,
		DisableCompression:  true,
	}
	return client{
		http: &http.Client{
			Transport: transport, Timeout: 4 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		base:  strings.TrimSuffix(cfg.ControlURL, "/") + "/internal/v1",
		token: cfg.Token,
	}
}

type responseError struct {
	status int
	code   string
}

func (e responseError) Error() string { return "control plane rejected worker request" }

func (e responseError) temporary() bool { return e.status >= 500 }

func transient(err error) bool {
	var response responseError
	return !errors.As(err, &response) || response.temporary()
}

func responseCode(err error, code string) bool {
	var response responseError
	return errors.As(err, &response) && response.code == code
}

func (c client) request(ctx context.Context, method, path string, in, out any) (bool, error) {
	var payload []byte
	if in != nil {
		var err error
		payload, err = json.Marshal(in)
		if err != nil || len(payload) > execution.MaxBodySize {
			return false, responseError{status: 400}
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, bytes.NewReader(payload))
	if err != nil {
		return false, responseError{status: 400}
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return false, errors.New("control plane unavailable")
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, execution.MaxBodySize+1))
	if err != nil || len(data) > execution.MaxBodySize {
		return false, errors.New("invalid control plane response")
	}
	switch resp.StatusCode {
	case http.StatusNoContent:
		return false, nil
	case http.StatusOK:
		if out == nil || json.Unmarshal(data, out, json.RejectUnknownMembers(true)) != nil {
			return false, errors.New("invalid control plane response")
		}
		return true, nil
	default:
		var problem struct {
			Code string `json:"code"`
		}
		_ = json.Unmarshal(data, &problem)
		return false, responseError{resp.StatusCode, problem.Code}
	}
}

func Probe(ctx context.Context, cfg config.Worker, registry *provider.Registry) error {
	if err := Validate(cfg, registry); err != nil {
		return err
	}
	c := newClient(cfg)
	defer c.http.CloseIdleConnections()
	_, err := c.request(ctx, http.MethodGet, "/readyz", nil, nil)
	return err
}
