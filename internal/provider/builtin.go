package provider

import (
	"errors"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"

	"golang.org/x/net/idna"
)

func Builtins() []Manifest {
	return []Manifest{
		SteamManifest(nil),
		WebManifest(nil),
	}
}

func steamApp(key string) (string, error) {
	for _, c := range key {
		if c < '0' || c > '9' {
			return "", errors.New("Steam AppID must be positive decimal")
		}
	}
	id, err := strconv.ParseUint(key, 10, 32)
	if err != nil || id == 0 {
		return "", errors.New("Steam AppID must be a positive uint32")
	}
	return strconv.FormatUint(id, 10), nil
}

func domain(key string) (string, error) {
	ascii, err := idna.Lookup.ToASCII(key)
	host := strings.TrimSuffix(strings.ToLower(ascii), ".")
	if err != nil || len(host) > 253 || !strings.Contains(host, ".") || net.ParseIP(host) != nil {
		return "", errors.New("invalid public DNS hostname")
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return "", errors.New("invalid DNS label")
		}
		for _, c := range label {
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
				return "", errors.New("invalid DNS label")
			}
		}
	}
	tld := host[strings.LastIndexByte(host, '.')+1:]
	switch tld {
	case "local", "localhost", "internal", "test", "invalid", "example", "onion", "arpa", "lan":
		return "", errors.New("reserved DNS hostname")
	}
	if strings.Trim(tld, "0123456789") == "" {
		return "", errors.New("numeric top-level label")
	}
	return host, nil
}

var restrictedIPs = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"), netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"), netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("64:ff9b::/96"), netip.MustParsePrefix("100::/64"),
	netip.MustParsePrefix("2001:db8::/32"), netip.MustParsePrefix("2002::/16"),
}

func origin(key string) (string, error) {
	u, err := url.Parse(key)
	if err != nil || u.User != nil || u.Host == "" || u.RawQuery != "" || u.ForceQuery ||
		strings.Contains(key, "#") || (u.EscapedPath() != "" && u.EscapedPath() != "/") {
		return "", errors.New("invalid HTTP origin")
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", errors.New("origin scheme must be http or https")
	}
	host := u.Hostname()
	if ip, err := netip.ParseAddr(host); err == nil {
		ip = ip.Unmap()
		if ip.Zone() != "" || !ip.IsGlobalUnicast() || ip.IsPrivate() {
			return "", errors.New("non-public origin IP")
		}
		for _, prefix := range restrictedIPs {
			if prefix.Contains(ip) {
				return "", errors.New("reserved origin IP")
			}
		}
		host = ip.String()
	} else {
		host, err = domain(host)
		if err != nil {
			return "", err
		}
	}
	port := u.Port()
	if port != "" || strings.HasSuffix(u.Host, ":") {
		n, err := strconv.ParseUint(port, 10, 16)
		if err != nil || n == 0 {
			return "", errors.New("invalid origin port")
		}
		port = strconv.FormatUint(n, 10)
		if scheme == "https" && n == 443 || scheme == "http" && n == 80 {
			port = ""
		}
	}
	if port != "" {
		host = net.JoinHostPort(host, port)
	} else if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	return scheme + "://" + host, nil
}
