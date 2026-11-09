package provider

import (
	"context"
	"crypto/tls"
	"encoding/json/jsontext"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/librecosta/harvest/internal/egress"
)

type certificateFacts struct {
	Origin            string    `json:"origin"`
	Hostname          string    `json:"hostname"`
	Verified          bool      `json:"verified"`
	Issuer            string    `json:"issuer"`
	NotBefore         time.Time `json:"not_before"`
	NotAfter          time.Time `json:"not_after"`
	DNSNames          []string  `json:"dns_names,omitempty"`
	DNSNamesTruncated bool      `json:"dns_names_truncated"`
	CertificateSHA256 string    `json:"certificate_sha256"`
	SPKISHA256        string    `json:"spki_sha256"`
}

type tlsEvidence struct {
	certificateFacts
	TLSVersion   string `json:"connection_tls_version"`
	ALPN         string `json:"connection_alpn,omitempty"`
	EgressKind   string `json:"egress_kind"`
	EgressRegion string `json:"egress_region,omitempty"`
}

func acquireTLS(ctx context.Context, task Task, client egress.Client) Result {
	scopes := []SegmentScope{webScope(task, "tls")}
	r, err := http.NewRequestWithContext(ctx, http.MethodHead, task.ExternalKey+"/", nil)
	if err != nil {
		return webResult(scopes, "policy_denied")
	}
	res, err := client.Do(r)
	if err != nil {
		return webResult(scopes, webNetworkCode(err))
	}
	defer res.Body.Close()
	return webResult(scopes, "", tlsSegment(task, scopes[0], res))
}

func tlsSegment(task Task, scope SegmentScope, res *http.Response) Segment {
	if res.TLS == nil || len(res.TLS.VerifiedChains) == 0 || len(res.TLS.PeerCertificates) == 0 {
		return webSegment(scope, "unavailable", "tls_verification_failed", nil)
	}
	cert := res.TLS.PeerCertificates[0]
	u, _ := url.Parse(task.ExternalKey)
	path := egress.ResponsePath(res)
	v := tlsEvidence{
		certificateFacts: certificateFacts{Origin: task.ExternalKey, Hostname: u.Hostname(), Verified: true,
			Issuer: cleanText(cert.Issuer.CommonName, 256), NotBefore: cert.NotBefore.UTC(), NotAfter: cert.NotAfter.UTC(),
			CertificateSHA256: hashWeb(cert.Raw), SPKISHA256: hashWeb(cert.RawSubjectPublicKeyInfo)},
		TLSVersion: tls.VersionName(res.TLS.Version), ALPN: res.TLS.NegotiatedProtocol,
		EgressKind: path.Kind, EgressRegion: path.Region,
	}
	for _, name := range cert.DNSNames {
		name = strings.ToLower(name)
		if !validSAN(name) {
			continue
		}
		if len(v.DNSNames) == 20 {
			v.DNSNamesTruncated = true
			break
		}
		v.DNSNames = append(v.DNSNames, name)
	}
	v.DNSNames = sortedUnique(v.DNSNames)
	return webSegment(scope, "present", "", v)
}

func validSAN(s string) bool {
	name := strings.TrimPrefix(s, "*.")
	return !strings.Contains(name, ":") && egress.ValidHostPort(&url.URL{Host: name})
}

func normalizeTLS(task Task, state string, value jsontext.Value) (jsontext.Value, jsontext.Value, error) {
	v, err := ObservationObject[tlsEvidence](value)
	u, uerr := url.Parse(task.ExternalKey)
	valid := err == nil && uerr == nil && u.Scheme == "https" && state == "present" &&
		v.Origin == task.ExternalKey && v.Hostname == u.Hostname() && v.Verified &&
		v.NotAfter.After(v.NotBefore) && validText(v.Issuer, 256) &&
		webDigest(v.CertificateSHA256) && webDigest(v.SPKISHA256) && len(v.DNSNames) <= 20 &&
		(v.TLSVersion == "TLS 1.2" || v.TLSVersion == "TLS 1.3") && (v.ALPN == "" || v.ALPN == "http/1.1") &&
		validVantage(v.EgressKind) && (v.EgressRegion == "" || v.EgressRegion == task.Scope.EgressRegion)
	for _, name := range v.DNSNames {
		valid = valid && validSAN(name)
	}
	v.DNSNames = sortedUnique(v.DNSNames)
	return webData(valid, v, v.certificateFacts)
}
