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
	// ComplemauSecondASID is the registration id for the flag-off receiver.
	ComplemauSecondASID = "complemau-off"
	// ComplemauSecondSender is the second registration's sender localpart.
	ComplemauSecondSender = "complemau_off"
	// ComplemauSecondSenderID is the second sender user on hs1.
	ComplemauSecondSenderID = "@complemau_off:hs1"
	// ComplemauSecondASPort is the fixed port for the second receiver.
	ComplemauSecondASPort = 49974
	// ComplemauASURL is where the homeserver pushes transactions. The host is
	// COMPLEMENT_HOSTNAME_RUNNING_COMPLEMENT (host.docker.internal), which the
	// testee container resolves to the host running the test via host-gateway.
	ComplemauASURL = "http://host.docker.internal:49973"
	// ComplemauSecondASURL is where the second receiver accepts transactions.
	ComplemauSecondASURL = "http://host.docker.internal:49974"
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

// BlueprintHSWithTwoComplemauBridges registers two overlapping appservices.
// The first opts into ephemeral, MSC3202, and MSC4190 behavior. The second
// leaves those registration flags off, allowing tests to compare delivery and
// verify that one stalled receiver does not block the other.
var BlueprintHSWithTwoComplemauBridges = MustValidate(Blueprint{
	Name: "hs_with_two_complemau_bridges",
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
					ID:               ComplemauASID,
					URL:              ComplemauASURL,
					SenderLocalpart:  ComplemauSender,
					SendEphemeral:    true,
					EnableEncryption: true,
					EnableMSC4190:    true,
					Namespaces: &ApplicationServiceNamespaces{
						Users: []ApplicationServiceNamespace{{Regex: "^@complemau_.*:hs1$"}},
					},
					Protocols: []string{"complemau"},
				},
				{
					ID:              ComplemauSecondASID,
					URL:             ComplemauSecondASURL,
					SenderLocalpart: ComplemauSecondSender,
					Namespaces: &ApplicationServiceNamespaces{
						Users: []ApplicationServiceNamespace{{Regex: "^@complemau_.*:hs1$"}},
					},
				},
			},
		},
	},
})
