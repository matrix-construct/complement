//go:build complemau

package b

// complemau: a homeserver wired to an application service whose receiving
// endpoint is a real mautrix appservice server stood up inside the test. The
// blueprint bakes the registration (tokens, sender, ephemeral opt-in) at build
// time. Complement replaces the token values while validating the blueprint,
// so the test receiver reads them back from the normalized blueprint.
const (
	// ComplemauASID is the registration id.
	ComplemauASID = "complemau"
	// ComplemauSender is the sender_localpart the bridge acts as by default.
	ComplemauSender = "complemau"
	// ComplemauSenderID is the fully-qualified sender user on hs1.
	ComplemauSenderID = "@complemau:hs1"
	// ComplemauASPort is the fixed TCP port the mautrix receiver listens on.
	// It must match the port embedded in ComplemauASURL below.
	ComplemauASPort = 49973
	// ComplemauASURL is where the homeserver pushes transactions. The host is
	// COMPLEMENT_HOSTNAME_RUNNING_COMPLEMENT (host.docker.internal), which the
	// testee container resolves to the host running the test via host-gateway.
	ComplemauASURL = "http://host.docker.internal:49973"
)

// BlueprintHSWithComplemauBridge is one homeserver (hs1) with a single local
// user and a bridge appservice opted in to ephemeral events (receipts, typing).
// The appservice namespace is the harness default (users `.*`, non-exclusive),
// so the sender and any ghost are in scope.
var BlueprintHSWithComplemauBridge = MustValidate(Blueprint{
	Name: "hs_with_complemau_bridge",
	Homeservers: []Homeserver{
		{
			Name: "hs1",
			Users: []User{
				{
					Localpart:   "@alice",
					DisplayName: "Alice",
				},
			},
			ApplicationServices: []ApplicationService{
				{
					ID:              ComplemauASID,
					URL:             ComplemauASURL,
					SenderLocalpart: ComplemauSender,
					RateLimited:     false,
					SendEphemeral:   true,
				},
			},
		},
	},
})
