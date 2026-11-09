package provider

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"net/netip"
	"slices"
	"strings"

	"golang.org/x/net/dns/dnsmessage"

	"github.com/librecosta/harvest/internal/egress"
)

const maxDNSRecords = 32

var recordTypes = map[string]dnsmessage.Type{
	"a": dnsmessage.TypeA, "aaaa": dnsmessage.TypeAAAA, "cname": dnsmessage.TypeCNAME,
	"mx": dnsmessage.TypeMX, "ns": dnsmessage.TypeNS, "soa": dnsmessage.TypeSOA,
	"txt": dnsmessage.TypeTXT, "caa": 257,
}

type dnsParams struct {
	Types []string `json:"types"`
}

type dnsRecord struct {
	Owner       string  `json:"owner"`
	TTL         uint32  `json:"ttl"`
	Address     string  `json:"address,omitempty"`
	Name        string  `json:"name,omitempty"`
	Preference  uint16  `json:"preference,omitempty"`
	SOA         *dnsSOA `json:"soa,omitempty"`
	ValueSHA256 string  `json:"value_sha256,omitempty"`
	ValueBytes  int     `json:"value_bytes,omitempty"`
	Tag         string  `json:"tag,omitempty"`
	Flags       uint8   `json:"flags,omitempty"`
}

type dnsSOA struct {
	Server  string `json:"server"`
	Mailbox string `json:"mailbox"`
	Serial  uint32 `json:"serial"`
	Refresh uint32 `json:"refresh"`
	Retry   uint32 `json:"retry"`
	Expire  uint32 `json:"expire"`
	Minimum uint32 `json:"minimum"`
}

type dnsEvidence struct {
	Domain       string      `json:"domain"`
	Type         string      `json:"type"`
	Resolver     string      `json:"resolver"`
	Vantage      string      `json:"vantage"`
	RCode        string      `json:"rcode"`
	Status       string      `json:"status"`
	AD           bool        `json:"authenticated_data"`
	Records      []dnsRecord `json:"records"`
	EgressKind   string      `json:"egress_kind"`
	EgressRegion string      `json:"egress_region"`
}

func normalizeDNSParams(input jsontext.Value) (jsontext.Value, error) {
	p, err := Object[dnsParams](input)
	if err != nil || len(p.Types) > len(recordTypes) {
		return nil, errors.New("invalid DNS type set")
	}
	if len(p.Types) == 0 {
		p.Types = []string{"a", "aaaa", "caa", "cname", "mx", "ns", "soa", "txt"}
	}
	for i, name := range p.Types {
		name = strings.ToLower(name)
		if _, ok := recordTypes[name]; !ok {
			return nil, errors.New("unsupported DNS type")
		}
		p.Types[i] = name
	}
	p.Types = sortedUnique(p.Types)
	return json.Marshal(p)
}

func dnsScopes(t Task) []SegmentScope {
	params, _ := Object[dnsParams](t.Params)
	scopes := make([]SegmentScope, 0, len(params.Types))
	for _, kind := range params.Types {
		scope := webScope(t, "dns")
		scope.Dimensions["rrtype"] = kind
		scope.Dimensions["resolver"] = resolverID
		scopes = append(scopes, scope)
	}
	return scopes
}

func acquireDNS(ctx context.Context, task Task, client egress.Client) Result {
	scopes := dnsScopes(task)
	name, err := dnsmessage.NewName(task.ExternalKey + ".")
	if err != nil || len(scopes) == 0 {
		return Result{State: "failed", Code: "invalid_payload"}
	}
	var code string
	var segments []Segment
	for _, scope := range scopes {
		// A source refusal, quota block or invalid route ends the fanout.
		switch code {
		case "upstream_throttled", "upstream_auth", "network_quota_deferred", "network_budget_exhausted", "policy_denied":
			segments = append(segments, webSegment(scope, "not_covered", "", nil))
			continue
		}
		if ctx.Err() != nil {
			code = "timeout"
			segments = append(segments, webSegment(scope, "not_covered", "", nil))
			continue
		}
		seg, reason := lookupDoH(ctx, client, name, scope)
		if reason != "" {
			code = reason
		}
		segments = append(segments, seg)
	}
	return webResult(scopes, code, segments...)
}

func lookupDoH(ctx context.Context, client egress.Client, name dnsmessage.Name, scope SegmentScope) (Segment, string) {
	unavailable := func(code string) (Segment, string) { return webSegment(scope, "unavailable", code, nil), code }
	invalid := func() (Segment, string) { return webSegment(scope, "invalid", "dns_invalid", nil), "dns_invalid" }
	kind := scope.Dimensions["rrtype"]
	question := dnsmessage.Question{Name: name, Type: recordTypes[kind], Class: dnsmessage.ClassINET}
	query := dnsmessage.Message{
		Header:    dnsmessage.Header{RecursionDesired: true, AuthenticData: true},
		Questions: []dnsmessage.Question{question},
	}
	data, err := query.Pack()
	if err != nil {
		return invalid()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://dns.google"+resolverPath+"?dns="+base64.RawURLEncoding.EncodeToString(data), nil)
	if err != nil {
		return invalid()
	}
	req.Header.Set("Accept", "application/dns-message")
	res, err := client.Do(req)
	if err != nil {
		if errors.Is(err, egress.ErrResponse) {
			return invalid()
		}
		code := webNetworkCode(err)
		if code == "transport_error" || code == "proxy_unavailable" || strings.HasPrefix(code, "tls_") {
			code = "resolver_unavailable"
		}
		return unavailable(code)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		code := webStatusCode(res.StatusCode)
		if code == "" {
			code = "resolver_unavailable"
		}
		return unavailable(code)
	}
	data, err = io.ReadAll(io.LimitReader(res.Body, dnsBodyLimit+1))
	if err != nil || len(data) > dnsBodyLimit {
		if errors.Is(err, egress.ErrUnavailable) {
			return unavailable("resolver_unavailable")
		}
		return invalid()
	}
	var reply dnsmessage.Message
	if reply.Unpack(data) != nil || !reply.Response || reply.ID != query.ID || reply.OpCode != 0 ||
		reply.Truncated || !reply.RecursionAvailable || len(reply.Questions) != 1 || reply.Questions[0].Type != question.Type ||
		reply.Questions[0].Class != question.Class || !strings.EqualFold(reply.Questions[0].Name.String(), name.String()) {
		return invalid()
	}
	for _, extra := range reply.Additionals {
		if extra.Header.Type == dnsmessage.TypeOPT && extra.Header.TTL>>24 != 0 {
			return invalid()
		}
	}
	switch reply.RCode {
	case dnsmessage.RCodeSuccess, dnsmessage.RCodeNameError:
	case dnsmessage.RCodeServerFailure:
		return unavailable("resolver_servfail")
	case dnsmessage.RCodeRefused:
		return unavailable("resolver_refused")
	default:
		return invalid()
	}
	path := egress.ResponsePath(res)
	value := dnsEvidence{
		Domain: strings.TrimSuffix(name.String(), "."), Type: kind, Resolver: resolverID, Vantage: "recursive_doh",
		RCode: "noerror", Status: "answer", AD: reply.AuthenticData, EgressKind: path.Kind, EgressRegion: path.Region,
		Records: []dnsRecord{},
	}
	if reply.RCode == dnsmessage.RCodeNameError {
		value.RCode, value.Status = "nxdomain", "nxdomain"
		return webSegment(scope, "confirmed_empty", "dns_response", value), ""
	}
	value.Records, err = dnsAnswers(question, reply.Answers)
	if err != nil {
		return invalid()
	}
	if len(value.Records) == 0 {
		value.Status = "nodata"
		return webSegment(scope, "confirmed_empty", "dns_response", value), ""
	}
	return webSegment(scope, "present", "", value), ""
}

func dnsAnswers(question dnsmessage.Question, answers []dnsmessage.Resource) ([]dnsRecord, error) {
	owner := dnsName(question.Name)
	aliases := make(map[string]string)
	for _, rr := range answers {
		cname, ok := rr.Body.(*dnsmessage.CNAMEResource)
		if !ok || rr.Header.Class != dnsmessage.ClassINET {
			continue
		}
		from, to := dnsName(rr.Header.Name), dnsName(cname.CNAME)
		if old := aliases[from]; old != "" && old != to {
			return nil, errors.New("ambiguous DNS alias")
		}
		aliases[from] = to
	}
	seen := map[string]bool{owner: true}
	if question.Type != dnsmessage.TypeCNAME {
		for to := aliases[owner]; to != ""; to = aliases[owner] {
			if seen[to] || len(seen) >= 9 {
				return nil, errors.New("DNS alias limit")
			}
			seen[to], owner = true, to
		}
	}
	result := make([]dnsRecord, 0)
	for _, rr := range answers {
		if rr.Header.Type != question.Type || rr.Header.Class != question.Class || dnsName(rr.Header.Name) != owner {
			continue
		}
		record := dnsRecord{Owner: owner, TTL: rr.Header.TTL}
		switch r := rr.Body.(type) {
		case *dnsmessage.AResource:
			record.Address = netip.AddrFrom4(r.A).String()
		case *dnsmessage.AAAAResource:
			record.Address = netip.AddrFrom16(r.AAAA).String()
		case *dnsmessage.CNAMEResource:
			record.Name = dnsName(r.CNAME)
		case *dnsmessage.NSResource:
			record.Name = dnsName(r.NS)
		case *dnsmessage.MXResource:
			record.Name, record.Preference = dnsName(r.MX), r.Pref
		case *dnsmessage.SOAResource:
			record.SOA = &dnsSOA{dnsName(r.NS), dnsName(r.MBox), r.Serial, r.Refresh, r.Retry, r.Expire, r.MinTTL}
		case *dnsmessage.TXTResource:
			var txt []byte
			for _, chunk := range r.TXT {
				txt = binary.BigEndian.AppendUint16(txt, uint16(len(chunk)))
				txt = append(txt, chunk...)
				record.ValueBytes += len(chunk)
			}
			record.ValueSHA256 = hashWeb(txt)
		case *dnsmessage.UnknownResource:
			if r.Type != 257 || len(r.Data) < 3 || r.Data[1] == 0 || int(r.Data[1])+2 > len(r.Data) {
				return nil, errors.New("invalid DNS CAA")
			}
			tagEnd := int(r.Data[1]) + 2
			record.Flags, record.Tag = r.Data[0], strings.ToLower(string(r.Data[2:tagEnd]))
			record.ValueSHA256, record.ValueBytes = hashWeb(r.Data[tagEnd:]), len(r.Data)-tagEnd
		default:
			return nil, errors.New("unexpected DNS data")
		}
		if len(result) == maxDNSRecords || !validRecord(question.Type, record) {
			return nil, errors.New("DNS record outside bounds")
		}
		result = append(result, record)
	}
	return result, nil
}

func dnsName(name dnsmessage.Name) string {
	s := strings.ToLower(name.String())
	if s == "." {
		return s
	}
	return strings.TrimSuffix(s, ".")
}

func validDNSName(s string) bool {
	if s == "." {
		return true
	}
	if s == "" || len(s) > 253 {
		return false
	}
	for label := range strings.SplitSeq(s, ".") {
		if len(label) == 0 || len(label) > 63 {
			return false
		}
		for _, r := range label {
			if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
				return false
			}
		}
	}
	return true
}

func validRecord(t dnsmessage.Type, v dnsRecord) bool {
	if !validDNSName(v.Owner) {
		return false
	}
	switch t {
	case dnsmessage.TypeA, dnsmessage.TypeAAAA:
		ip, err := netip.ParseAddr(v.Address)
		return err == nil && ip.Zone() == "" && ip.String() == v.Address && (t == dnsmessage.TypeA) == ip.Is4() &&
			v.Name == "" && v.SOA == nil && v.ValueSHA256 == "" && v.Tag == "" && v.ValueBytes == 0 && v.Flags == 0 && v.Preference == 0
	case dnsmessage.TypeCNAME, dnsmessage.TypeNS, dnsmessage.TypeMX:
		return validDNSName(v.Name) && v.Address == "" && v.SOA == nil && v.ValueSHA256 == "" && v.Tag == "" &&
			v.ValueBytes == 0 && v.Flags == 0 && (t == dnsmessage.TypeMX || v.Preference == 0)
	case dnsmessage.TypeSOA:
		return v.SOA != nil && validDNSName(v.SOA.Server) && validDNSName(v.SOA.Mailbox) &&
			v.Address == "" && v.Name == "" && v.ValueSHA256 == "" && v.Tag == "" && v.ValueBytes == 0 && v.Flags == 0 && v.Preference == 0
	case dnsmessage.TypeTXT, 257:
		if !webDigest(v.ValueSHA256) || v.ValueBytes < 0 || v.ValueBytes > dnsBodyLimit ||
			v.Address != "" || v.Name != "" || v.SOA != nil || v.Preference != 0 {
			return false
		}
		if t == dnsmessage.TypeTXT {
			return v.Tag == "" && v.Flags == 0
		}
		if v.Tag == "" || len(v.Tag) > 15 {
			return false
		}
		for _, r := range v.Tag {
			if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9') {
				return false
			}
		}
		return true
	}
	return false
}

func normalizeDNS(task Task, scope SegmentScope, state string, value jsontext.Value) (jsontext.Value, jsontext.Value, error) {
	v, err := ObservationObject[dnsEvidence](value)
	valid := err == nil && v.Domain == task.ExternalKey && v.Type == scope.Dimensions["rrtype"] &&
		v.Resolver == resolverID && v.Vantage == "recursive_doh" && v.Records != nil && len(v.Records) <= maxDNSRecords &&
		validVantage(v.EgressKind) && (v.EgressRegion == "" || v.EgressRegion == task.Scope.EgressRegion)
	switch state {
	case "present":
		valid = valid && v.RCode == "noerror" && v.Status == "answer" && len(v.Records) > 0
	case "confirmed_empty":
		valid = valid && len(v.Records) == 0 && (v.RCode == "noerror" && v.Status == "nodata" || v.RCode == "nxdomain" && v.Status == "nxdomain")
	default:
		valid = false
	}
	for _, record := range v.Records {
		valid = valid && validRecord(recordTypes[v.Type], record)
	}
	keys := make([]string, 0, len(v.Records))
	for _, record := range v.Records {
		record.TTL = 0
		stable, _ := json.Marshal(record, json.Deterministic(true))
		keys = append(keys, string(stable))
	}
	return webData(valid, v, struct {
		RCode   string
		Records []string
	}{v.RCode, sortedUnique(slices.Clip(keys))})
}
