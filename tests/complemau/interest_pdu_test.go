//go:build complemau

package complemau_tests

import (
	"testing"
	"time"

	"github.com/matrix-org/complement"
	"github.com/matrix-org/complement/b"
	"github.com/matrix-org/complement/client"
	"github.com/matrix-org/complement/helpers"
	"github.com/matrix-org/complement/runtime"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"maunium.net/go/mautrix/event"
)

var (
	complemauInterestLocalBlueprint = newComplemauInterestBlueprint(
		"hs_with_complemau_interest",
		false,
	)
	complemauInterestFederationBlueprint = newComplemauInterestBlueprint(
		"hs_with_complemau_interest_federation",
		true,
	)
)

func newComplemauInterestBlueprint(name string, federated bool) b.Blueprint {
	homeservers := []b.Homeserver{
		{
			Name: "hs1",
			Users: []b.User{
				{Localpart: "@alice", DisplayName: "Alice"},
			},
			ApplicationServices: []b.ApplicationService{
				{
					ID:              b.ComplemauASID,
					URL:             b.ComplemauASURL,
					SenderLocalpart: b.ComplemauSender,
					RateLimited:     false,
					SendEphemeral:   true,
					Namespaces: &b.ApplicationServiceNamespaces{
						Users: []b.ApplicationServiceNamespace{
							{Regex: `^@as_.*:hs1$`, Exclusive: false},
						},
						Aliases: []b.ApplicationServiceNamespace{
							{Regex: `^#as_.*:hs1$`, Exclusive: false},
						},
					},
				},
			},
		},
	}
	if federated {
		homeservers = append(homeservers, b.Homeserver{
			Name: "hs2",
			Users: []b.User{
				{Localpart: "@charlie", DisplayName: "Charlie"},
			},
		})
	}
	return b.MustValidate(b.Blueprint{Name: name, Homeservers: homeservers})
}

func TestComplemauAppservicePDUInterest(t *testing.T) {
	// tuwunel and Synapse read the Complement appservice registration file;
	// Dendrite does not yet. https://github.com/matrix-org/complement/issues/514
	runtime.SkipIf(t, runtime.Dendrite)

	deployment := complement.OldDeploy(t, complemauInterestLocalBlueprint)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	bridgeClient := deployment.AppServiceUser(t, "hs1", b.ComplemauSenderID)
	registration := complemauInterestLocalBlueprint.Homeservers[0].ApplicationServices[0]
	bridge := startComplemauBridgeWithRegistration(
		t,
		bridgeClient.BaseURL,
		registration,
		b.ComplemauASPort,
	)
	defer bridge.stop()

	t.Run("sender namespace", func(t *testing.T) {
		const ghostLocalpart = "as_sender"
		const ghostID = "@as_sender:hs1"
		registerAppserviceGhost(t, bridgeClient, ghostLocalpart)

		roomID := alice.MustCreateRoom(t, map[string]interface{}{"preset": "public_chat"})
		alice.MustInviteRoom(t, roomID, ghostID)
		joinComplemauInterestUser(t, bridgeClient, roomID, ghostID)

		eventID := sendComplemauInterestMessage(
			t,
			bridgeClient,
			roomID,
			ghostID,
			"sender namespace message",
			"sender_namespace",
		)
		got := mustReceiveComplemauInterestPDU(t, bridge, eventID)
		if string(got.Sender) != ghostID {
			t.Errorf("complemau: sender namespace PDU sender = %s, want %s", got.Sender, ghostID)
		}
	})

	t.Run("sender nonmatch", func(t *testing.T) {
		const markerLocalpart = "as_marker"
		const markerUserID = "@as_marker:hs1"
		registerAppserviceGhost(t, bridgeClient, markerLocalpart)

		markerRoomID := alice.MustCreateRoom(t, map[string]interface{}{"preset": "public_chat"})
		alice.MustInviteRoom(t, markerRoomID, markerUserID)
		joinComplemauInterestUser(t, bridgeClient, markerRoomID, markerUserID)

		uninterestedRoomID := alice.MustCreateRoom(t, map[string]interface{}{
			"preset":          "public_chat",
			"room_alias_name": "xmpp_unrelated",
		})
		uninterestedEventID := alice.SendEventSynced(t, uninterestedRoomID, b.Event{
			Type: "m.room.message",
			Content: map[string]interface{}{
				"msgtype": "m.text",
				"body":    "uninteresting sender message",
			},
		})
		markerEventID := sendComplemauInterestMessage(
			t,
			bridgeClient,
			markerRoomID,
			markerUserID,
			"interest marker",
			"sender_nonmatch_marker",
		)
		mustReceiveComplemauMarkerWithout(t, bridge, markerEventID, uninterestedEventID)
	})

	t.Run("membership target namespace", func(t *testing.T) {
		const inviteeLocalpart = "as_bob"
		inviteeID := "@as_bob:hs1"
		registerAppserviceGhost(t, bridgeClient, inviteeLocalpart)

		roomID := alice.MustCreateRoom(t, map[string]interface{}{"preset": "public_chat"})
		inviteEventID := alice.SendEventSynced(t, roomID, b.Event{
			Type:     "m.room.member",
			StateKey: &inviteeID,
			Content: map[string]interface{}{
				"membership": "invite",
			},
		})
		got := mustReceiveComplemauInterestPDU(t, bridge, inviteEventID)
		if got.StateKey == nil || *got.StateKey != inviteeID {
			t.Errorf("complemau: membership target state key = %v, want %s", got.StateKey, inviteeID)
		}
		if string(got.Sender) != alice.UserID {
			t.Errorf("complemau: membership target sender = %s, want %s", got.Sender, alice.UserID)
		}
	})

	t.Run("alias namespace", func(t *testing.T) {
		roomID := alice.MustCreateRoom(t, map[string]interface{}{
			"preset":          "public_chat",
			"room_alias_name": "as_bridge_room",
		})
		eventID := alice.SendEventSynced(t, roomID, b.Event{
			Type: "m.room.message",
			Content: map[string]interface{}{
				"msgtype": "m.text",
				"body":    "alias namespace message",
			},
		})
		got := mustReceiveComplemauInterestPDU(t, bridge, eventID)
		if string(got.RoomID) != roomID {
			t.Errorf("complemau: alias namespace PDU room = %s, want %s", got.RoomID, roomID)
		}
	})

	t.Run("joined namespace member", func(t *testing.T) {
		const ghostLocalpart = "as_carol"
		const ghostID = "@as_carol:hs1"
		registerAppserviceGhost(t, bridgeClient, ghostLocalpart)

		roomID := alice.MustCreateRoom(t, map[string]interface{}{"preset": "public_chat"})
		alice.MustInviteRoom(t, roomID, ghostID)
		joinComplemauInterestUser(t, bridgeClient, roomID, ghostID)
		eventID := alice.SendEventSynced(t, roomID, b.Event{
			Type: "m.room.message",
			Content: map[string]interface{}{
				"msgtype": "m.text",
				"body":    "message from unrelated room member",
			},
		})
		got := mustReceiveComplemauInterestPDU(t, bridge, eventID)
		if string(got.Sender) != alice.UserID {
			t.Errorf("complemau: member list PDU sender = %s, want %s", got.Sender, alice.UserID)
		}
	})

	t.Run("sender localpart identity", func(t *testing.T) {
		botID := b.ComplemauSenderID
		roomID := alice.MustCreateRoom(t, map[string]interface{}{"preset": "public_chat"})
		inviteEventID := alice.SendEventSynced(t, roomID, b.Event{
			Type:     "m.room.member",
			StateKey: &botID,
			Content: map[string]interface{}{
				"membership": "invite",
			},
		})
		got := mustReceiveComplemauInterestPDU(t, bridge, inviteEventID)
		if got.StateKey == nil || *got.StateKey != botID {
			t.Errorf("complemau: sender identity invite state key = %v, want %s", got.StateKey, botID)
		}
	})
}

func TestComplemauAppservicePDUInterestRemoteSender(t *testing.T) {
	// tuwunel and Synapse read the Complement appservice registration file;
	// Dendrite does not yet. https://github.com/matrix-org/complement/issues/514
	runtime.SkipIf(t, runtime.Dendrite)

	deployment := complement.OldDeploy(t, complemauInterestFederationBlueprint)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	remote := deployment.Register(t, "hs2", helpers.RegistrationOpts{})
	bridgeClient := deployment.AppServiceUser(t, "hs1", b.ComplemauSenderID)
	registration := complemauInterestFederationBlueprint.Homeservers[0].ApplicationServices[0]
	bridge := startComplemauBridgeWithRegistration(
		t,
		bridgeClient.BaseURL,
		registration,
		b.ComplemauASPort,
	)
	defer bridge.stop()

	const ghostLocalpart = "as_federated"
	const ghostID = "@as_federated:hs1"
	registerAppserviceGhost(t, bridgeClient, ghostLocalpart)

	roomID := alice.MustCreateRoom(t, map[string]interface{}{"preset": "public_chat"})
	alice.MustInviteRoom(t, roomID, ghostID)
	joinComplemauInterestUser(t, bridgeClient, roomID, ghostID)
	remote.MustJoinRoom(t, roomID, []spec.ServerName{
		deployment.GetFullyQualifiedHomeserverName(t, "hs1"),
	})

	eventID := remote.SendEventSynced(t, roomID, b.Event{
		Type: "m.room.message",
		Content: map[string]interface{}{
			"msgtype": "m.text",
			"body":    "message from remote unrelated member",
		},
	})
	got := mustReceiveComplemauInterestPDU(t, bridge, eventID)
	if string(got.Sender) != remote.UserID {
		t.Errorf("complemau: remote member PDU sender = %s, want %s", got.Sender, remote.UserID)
	}
}

func joinComplemauInterestUser(t *testing.T, bridge *client.CSAPI, roomID, userID string) {
	t.Helper()
	bridge.MustDo(
		t,
		"POST",
		[]string{"_matrix", "client", "v3", "rooms", roomID, "join"},
		client.WithJSONBody(t, struct{}{}),
		asUser(userID),
	)
}

func sendComplemauInterestMessage(
	t *testing.T,
	bridge *client.CSAPI,
	roomID string,
	userID string,
	body string,
	transactionID string,
) string {
	t.Helper()
	res := bridge.MustDo(
		t,
		"PUT",
		[]string{"_matrix", "client", "v3", "rooms", roomID, "send", "m.room.message", transactionID},
		client.WithJSONBody(t, map[string]interface{}{
			"msgtype": "m.text",
			"body":    body,
		}),
		asUser(userID),
	)
	return client.GetJSONFieldStr(t, client.ParseJSON(t, res), "event_id")
}

func mustReceiveComplemauInterestPDU(
	t *testing.T,
	bridge *complemauBridge,
	eventID string,
) *event.Event {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("complemau: timed out waiting for PDU %s", eventID)
			return nil
		}
		got := bridge.mustReceivePDU(t, remaining)
		if string(got.ID) == eventID {
			return got
		}
	}
}

func mustReceiveComplemauMarkerWithout(
	t *testing.T,
	bridge *complemauBridge,
	markerEventID string,
	absentEventID string,
) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("complemau: timed out waiting for marker PDU %s", markerEventID)
			return
		}
		got := bridge.mustReceivePDU(t, remaining)
		switch string(got.ID) {
		case absentEventID:
			t.Errorf("complemau: uninterested PDU %s arrived before marker %s", absentEventID, markerEventID)
		case markerEventID:
			return
		}
	}
}
