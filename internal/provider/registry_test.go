package provider

import "testing"

func TestBuiltinIdentityAndDiscovery(t *testing.T) {
	r, err := New(Builtins()...)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ p, resource, input, want string }{
		{"steam", "app", "0000004294967295", "4294967295"},
		{"web", "domain", "WWW.Bücher.DE.", "www.xn--bcher-kva.de"},
		{"web", "domain", "bücher.de", "xn--bcher-kva.de"},
		{"web", "origin", "HTTPS://WWW.Bücher.DE:0443/", "https://www.xn--bcher-kva.de"},
		{"web", "origin", "http://www.bücher.de:8080/", "http://www.xn--bcher-kva.de:8080"},
		{"web", "origin", "https://[2606:4700:4700:0::1111]", "https://[2606:4700:4700::1111]"},
		{"steam", "app", "4294967296", ""},
		{"steam", "app", "0", ""},
		{"steam", "app", " 1", ""},
		{"steam", "app", "+1", ""},
		{"web", "domain", "https://www.bücher.de", ""},
		{"web", "domain", "corp.internal", ""},
		{"web", "origin", "https://[::ffff:127.0.0.1]", ""},
		{"web", "origin", "https://100.64.3.4", ""},
		{"web", "origin", "https://u:p@www.example.com/", ""},
		{"web", "origin", "https://www.example.com/?", ""},
		{"web", "origin", "https://www.example.com/#", ""},
		{"web", "origin", "https://www.example.com/%2F", ""},
	} {
		got, err := r.Normalize(tc.p, tc.resource, tc.input)
		if tc.want == "" && err == nil || tc.want != "" && (err != nil || got != tc.want) {
			t.Errorf("%s %q: got %q, %v, want %q", tc.resource, tc.input, got, err, tc.want)
		}
	}
	for _, p := range r.Describe() {
		for _, c := range p.Capabilities {
			if c.Status != "executable" {
				t.Fatalf("unexpected capability status for %s: %s", c.Capability, c.Status)
			}
		}
	}
}
