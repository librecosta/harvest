package provider

import (
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/librecosta/harvest/internal/egress"
)

type rdapEvidence struct {
	Domain          string             `json:"domain"`
	RegistryService string             `json:"registry_service"`
	Source          string             `json:"source"`
	Status          []string           `json:"status,omitempty"`
	Registrar       *registrarEvidence `json:"registrar,omitempty"`
	Nameservers     []string           `json:"nameservers,omitempty"`
	RegisteredAt    *time.Time         `json:"registered_at,omitempty"`
	UpdatedAt       *time.Time         `json:"updated_at,omitempty"`
	ExpiresAt       *time.Time         `json:"expires_at,omitempty"`
}

type registrarEvidence struct {
	ID   string `json:"iana_id"`
	Name string `json:"name,omitempty"`
}

type rdapEntity struct {
	Roles     []string `json:"roles"`
	PublicIDs []struct {
		Type       string `json:"type"`
		Identifier string `json:"identifier"`
	} `json:"publicIds"`
	VCard jsontext.Value `json:"vcardArray"`
}

func acquireRDAP(ctx context.Context, t Task, client egress.Client) Result {
	scope := discoveryScope(t, "rdap")
	name, err := egress.RegistrableDomain(t.ExternalKey)
	service, ok := client.(egress.RegistryClient)
	if err != nil || !ok {
		return discoveryResult(scope, "policy_denied", nil)
	}
	res, err := service.LookupRDAP(ctx)
	if err != nil {
		return discoveryResult(scope, discoveryNetworkCode(err), nil)
	}
	defer res.Body.Close()
	if code := discoveryStatus(res.StatusCode); code != "" {
		return discoveryFailure(scope, res, code)
	}
	if media := mediaType(res); media != "application/rdap+json" && media != "application/json" {
		return discoveryResult(scope, "source_invalid", nil)
	}
	body, code := discoveryBody(res, egress.RDAPMaxBytes, false)
	if code != "" {
		return discoveryResult(scope, code, nil)
	}
	v, err := parseRDAP(body, finalURL(res).String(), name)
	if err != nil {
		return discoveryResult(scope, "source_invalid", nil)
	}
	return discoveryResult(scope, "", v)
}

func parseRDAP(data []byte, endpoint, name string) (rdapEvidence, error) {
	v := rdapEvidence{Domain: name, RegistryService: endpoint, Source: "iana_bootstrap"}
	var raw struct {
		Class       string   `json:"objectClassName"`
		Name        string   `json:"ldhName"`
		Status      []string `json:"status"`
		Nameservers []struct {
			Name string `json:"ldhName"`
		} `json:"nameservers"`
		Events []struct {
			Action string `json:"eventAction"`
			Date   string `json:"eventDate"`
		} `json:"events"`
		Entities []rdapEntity `json:"entities"`
	}
	if json.Unmarshal(data, &raw) != nil || raw.Class != "domain" || !strings.EqualFold(strings.TrimSuffix(raw.Name, "."), name) {
		return v, errors.New("invalid RDAP domain object")
	}
	for _, s := range raw.Status {
		s = strings.ToLower(s)
		if validRDAPStatus(s) {
			v.Status = append(v.Status, s)
		}
	}
	v.Status = sortedUnique(v.Status)
	for _, ns := range raw.Nameservers {
		if key, err := domain(ns.Name); err == nil {
			v.Nameservers = append(v.Nameservers, key)
		}
	}
	v.Nameservers = sortedUnique(v.Nameservers)
	v.Nameservers = v.Nameservers[:min(len(v.Nameservers), 24)]
	for _, event := range raw.Events {
		parsed, err := time.Parse(time.RFC3339Nano, event.Date)
		if err != nil || parsed.Year() < 1900 || parsed.Year() > 9999 {
			continue
		}
		d := parsed.UTC()
		switch strings.ToLower(event.Action) {
		case "registration":
			if v.RegisteredAt == nil || d.Before(*v.RegisteredAt) {
				v.RegisteredAt = &d
			}
		case "last changed":
			if v.UpdatedAt == nil || d.After(*v.UpdatedAt) {
				v.UpdatedAt = &d
			}
		case "expiration":
			if v.ExpiresAt == nil || d.After(*v.ExpiresAt) {
				v.ExpiresAt = &d
			}
		}
	}
	for _, entity := range raw.Entities {
		if r := publicRegistrar(entity); r != nil {
			if v.Registrar == nil || r.ID < v.Registrar.ID || r.ID == v.Registrar.ID && r.Name < v.Registrar.Name {
				v.Registrar = r
			}
		}
	}
	return v, nil
}

func publicRegistrar(v rdapEntity) *registrarEvidence {
	isRegistrar := false
	for _, role := range v.Roles {
		switch strings.ToLower(role) {
		case "registrar":
			isRegistrar = true
		case "registrant", "administrative", "technical", "billing":
			return nil
		}
	}
	if !isRegistrar {
		return nil
	}
	var id string
	for _, public := range v.PublicIDs {
		if strings.EqualFold(public.Type, "IANA Registrar ID") && validRegistrarID(public.Identifier) {
			id = public.Identifier
			break
		}
	}
	if id == "" {
		return nil
	}
	r := &registrarEvidence{ID: id}
	var card []jsontext.Value
	if json.Unmarshal(v.VCard, &card) != nil || len(card) != 2 || string(card[0]) != "\"vcard\"" {
		return r
	}
	var properties [][]jsontext.Value
	if json.Unmarshal(card[1], &properties) != nil {
		return r
	}
	for _, item := range properties {
		if len(item) != 4 || string(item[0]) != "\"fn\"" {
			continue
		}
		var name string
		if json.Unmarshal(item[3], &name) == nil && !strings.Contains(name, "@") {
			r.Name = cleanText(name, 256)
			break
		}
	}
	return r
}

func validRegistrarID(id string) bool {
	n, err := strconv.ParseUint(id, 10, 32)
	return err == nil && n > 0 && strconv.FormatUint(n, 10) == id
}

func validRDAPStatus(s string) bool {
	switch s {
	case "active", "inactive", "ok", "locked", "hold", "pending create", "pending delete", "pending renew",
		"pending transfer", "pending update", "pending restore", "redemption period", "add period", "auto renew period",
		"renew period", "transfer period", "client delete prohibited", "client hold", "client renew prohibited",
		"client transfer prohibited", "client update prohibited", "server delete prohibited", "server hold",
		"server renew prohibited", "server transfer prohibited", "server update prohibited", "validated", "renew prohibited",
		"update prohibited", "transfer prohibited", "delete prohibited", "associated", "proxy", "private", "removed", "obscured":
		return true
	}
	return false
}

func normalizeRDAP(t Task, state string, data jsontext.Value) (jsontext.Value, jsontext.Value, error) {
	v, err := ObservationObject[rdapEvidence](data)
	name, nameErr := egress.RegistrableDomain(t.ExternalKey)
	_, endpointErr := egress.RegistryEndpoint(v.RegistryService, name)
	valid := err == nil && nameErr == nil && endpointErr == nil && state == "present" && v.Source == "iana_bootstrap" && v.Domain == name &&
		len(v.Status) <= 40 && len(v.Nameservers) <= 24
	for _, s := range v.Status {
		valid = valid && validRDAPStatus(s)
	}
	for _, ns := range v.Nameservers {
		key, e := domain(ns)
		valid = valid && e == nil && key == ns
	}
	for _, d := range []**time.Time{&v.RegisteredAt, &v.UpdatedAt, &v.ExpiresAt} {
		if *d != nil {
			parsed := (*d).UTC()
			valid = valid && parsed.Year() >= 1900 && parsed.Year() <= 9999
			*d = &parsed
		}
	}
	if v.Registrar != nil {
		valid = valid && validRegistrarID(v.Registrar.ID) && validText(v.Registrar.Name, 256) && !strings.Contains(v.Registrar.Name, "@")
	}
	v.Status, v.Nameservers = sortedUnique(v.Status), sortedUnique(v.Nameservers)
	return webData(valid, v, nil)
}
