package provider

import "testing"

func TestRegistryVersionedOffers(t *testing.T) {
	m := SteamManifest(nil)
	r, err := New(m)
	if err != nil {
		t.Fatal(err)
	}
	v1, ok := r.SelectCapability("steam.details", 1, nil)
	if !ok || v1.OutputSchema != 1 || v1.Implementation != "steam_v1" {
		t.Fatal("legacy default must stay pinned")
	}
	explicit := int32(2)
	v2, ok := r.SelectCapability("steam.details", 1, &explicit)
	if !ok || v2.Key() != SteamMediaV2Key {
		t.Fatal("explicit media schema should resolve to its compiled Offer")
	}
	if full, ok := r.CapabilityByKey(SteamMediaV2Key); !ok || full.Offer != v2.Offer {
		t.Fatal("full key did not resolve to the requested executor")
	}
	if _, ok := r.SelectCapability("steam.details", 2, &explicit); ok {
		t.Fatal("invalid request and output schema pair resolved")
	}
	m.Capabilities = append(m.Capabilities, v2)
	if _, err := New(m); err == nil {
		t.Fatal("duplicate Offer registered")
	}
	m.Capabilities[len(m.Capabilities)-1].Implementation = "ambiguous"
	if _, err := New(m); err == nil {
		t.Fatal("ambiguous schema selector registered")
	}
}
