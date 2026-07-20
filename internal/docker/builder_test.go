package docker

import (
	"reflect"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/matrix-org/complement/b"

	"maunium.net/go/mautrix/appservice"
)

func TestASRegistrationLabelRoundTrip(t *testing.T) {
	const literalBackslashN = `literal\nvalue`
	source := b.ApplicationService{
		ID:               "bridge",
		HSToken:          `hs'"\token`,
		ASToken:          `as\token"'`,
		URL:              `http://bridge.example/quote'"/slash\value/` + literalBackslashN,
		SenderLocalpart:  "bridge",
		RateLimited:      false,
		SendEphemeral:    true,
		EnableEncryption: true,
		EnableMSC4190:    true,
		Namespaces: &b.ApplicationServiceNamespaces{
			Users: []b.ApplicationServiceNamespace{
				{Regex: `^@bridge_["'\\]+` + literalBackslashN + `:hs$`, Exclusive: true},
			},
			Aliases: []b.ApplicationServiceNamespace{
				{Regex: `^#bridge_["'\\]+` + literalBackslashN + `:hs$`, Exclusive: false},
			},
			Rooms: []b.ApplicationServiceNamespace{
				{Regex: `^!room_["'\\]+` + literalBackslashN + `:hs$`, Exclusive: false},
			},
		},
		Protocols: []string{`ir"c`, `path\protocol`, literalBackslashN},
	}

	labels := labelsForApplicationServices(b.Homeserver{
		Name:                "hs",
		ApplicationServices: []b.ApplicationService{source},
	})
	registrationYAML := asIDToRegistrationFromLabels(labels)[source.ID]
	var registration appservice.Registration
	if err := yaml.Unmarshal([]byte(registrationYAML), &registration); err != nil {
		t.Fatalf("decoded registration is not YAML: %v\n%s", err, registrationYAML)
	}

	if registration.ID != source.ID || registration.ServerToken != source.HSToken || registration.AppToken != source.ASToken {
		t.Errorf("identity fields changed during label round trip: %+v", registration)
	}
	if registration.URL != source.URL || registration.SenderLocalpart != source.SenderLocalpart {
		t.Errorf("connection fields changed during label round trip: %+v", registration)
	}
	if registration.RateLimited == nil || *registration.RateLimited != source.RateLimited {
		t.Errorf("rate_limited changed during label round trip: %v", registration.RateLimited)
	}
	if !registration.EphemeralEvents || !registration.SoruEphemeralEvents || !registration.MSC3202 || !registration.MSC4190 {
		t.Errorf("registration flags changed during label round trip: %+v", registration)
	}
	if !reflect.DeepEqual(registration.Protocols, source.Protocols) {
		t.Errorf("protocols changed during label round trip: got %q want %q", registration.Protocols, source.Protocols)
	}
	assertNamespaceRoundTrip(t, registration.Namespaces.UserIDs, source.Namespaces.Users)
	assertNamespaceRoundTrip(t, registration.Namespaces.RoomAliases, source.Namespaces.Aliases)
	assertNamespaceRoundTrip(t, registration.Namespaces.RoomIDs, source.Namespaces.Rooms)
}

func TestASRegistrationLabelDefaultNamespace(t *testing.T) {
	source := b.ApplicationService{ID: "bridge"}
	labels := labelsForApplicationServices(b.Homeserver{
		Name:                "hs",
		ApplicationServices: []b.ApplicationService{source},
	})
	var registration appservice.Registration
	if err := yaml.Unmarshal([]byte(asIDToRegistrationFromLabels(labels)[source.ID]), &registration); err != nil {
		t.Fatalf("decoded default registration is not YAML: %v", err)
	}
	assertNamespaceRoundTrip(t, registration.Namespaces.UserIDs, []b.ApplicationServiceNamespace{{Regex: ".*"}})
	if len(registration.Namespaces.RoomAliases) != 0 || len(registration.Namespaces.RoomIDs) != 0 {
		t.Errorf("default registration gained room namespaces: %+v", registration.Namespaces)
	}
}

func assertNamespaceRoundTrip(
	t *testing.T,
	got appservice.NamespaceList,
	want []b.ApplicationServiceNamespace,
) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("namespace count changed: got %d want %d", len(got), len(want))
	}
	for index := range want {
		if got[index].Regex != want[index].Regex || got[index].Exclusive != want[index].Exclusive {
			t.Errorf("namespace %d changed: got %+v want %+v", index, got[index], want[index])
		}
	}
}
