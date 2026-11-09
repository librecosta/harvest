package egress

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"strings"
)

// ReadBounded applies the same hard ceiling independently to wire bytes and
// decoded bytes. It never starts a new request or interprets a document.
func ReadBounded(res *http.Response, limit int64, gzipFile bool) ([]byte, error) {
	if res.ContentLength > limit {
		return nil, ErrBudget
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > limit {
		return nil, ErrBudget
	}
	switch strings.ToLower(strings.TrimSpace(res.Header.Get("Content-Encoding"))) {
	case "", "identity":
		if !gzipFile || !bytes.HasPrefix(raw, []byte{0x1f, 0x8b}) {
			return raw, nil
		}
	case "gzip":
	default:
		return nil, ErrResponse
	}
	reader, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return nil, ErrResponse
	}
	defer reader.Close()
	decoded, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, ErrResponse
	}
	if int64(len(decoded)) > limit {
		return nil, ErrBudget
	}
	return decoded, nil
}
