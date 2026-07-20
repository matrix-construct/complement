//go:build complemau

package complemau_tests

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/matrix-org/complement"
	"github.com/matrix-org/complement/b"
	"github.com/matrix-org/complement/client"
	"github.com/matrix-org/complement/helpers"
	"github.com/matrix-org/complement/runtime"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

func TestComplemauAppserviceReceivesTyping(t *testing.T) {
	runtime.SkipIf(t, runtime.Dendrite)

	deployment := complement.OldDeploy(t, b.BlueprintHSWithComplemauBridge)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	bridgeUser := deployment.AppServiceUser(t, "hs1", b.ComplemauSenderID)
	bridge := startComplemauBridge(t, bridgeUser.BaseURL)
	defer bridge.stop()

	const ghostLocalpart = "complemau_typist"
	ghostID := id.UserID("@" + ghostLocalpart + ":hs1")
	registerAppserviceGhost(t, bridgeUser, ghostLocalpart)

	roomID := alice.MustCreateRoom(t, map[string]interface{}{"preset": "public_chat"})
	bridgeUser.MustDo(t, "POST",
		[]string{"_matrix", "client", "v3", "rooms", roomID, "join"},
		client.WithJSONBody(t, struct{}{}), asUser(ghostID.String()),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ghost := bridge.as.Intent(ghostID)
	if _, err := ghost.UserTyping(ctx, id.RoomID(roomID), true, 20*time.Second); err != nil {
		t.Fatalf("complemau: failed to start ghost typing: %v", err)
	}

	started := bridge.mustReceiveTyping(t, 20*time.Second)
	typingPresenceAssertTyping(t, started, id.RoomID(roomID), ghostID, true)

	if _, err := ghost.UserTyping(ctx, id.RoomID(roomID), false, 0); err != nil {
		t.Fatalf("complemau: failed to stop ghost typing: %v", err)
	}
	stopped := bridge.mustReceiveTyping(t, 20*time.Second)
	typingPresenceAssertTyping(t, stopped, id.RoomID(roomID), ghostID, false)
}

func TestComplemauAppserviceScopesTypingByInterest(t *testing.T) {
	runtime.SkipIf(t, runtime.Dendrite)

	deployment := complement.OldDeploy(t, b.BlueprintHSWithTwoComplemauBridges)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	bridgeUser := deployment.AppServiceUser(t, "hs1", b.ComplemauSenderID)
	bridge, flagOffBridge := startComplemauBridges(t, bridgeUser.BaseURL)
	defer bridge.stop()
	defer flagOffBridge.stop()

	const markerLocalpart = "complemau_typing_marker"
	markerUserID := id.UserID("@" + markerLocalpart + ":hs1")
	registerAppserviceGhost(t, bridgeUser, markerLocalpart)

	interestedRoomID := alice.MustCreateRoom(t, map[string]interface{}{"preset": "public_chat"})
	bridgeUser.MustDo(t, "POST",
		[]string{"_matrix", "client", "v3", "rooms", interestedRoomID, "join"},
		client.WithJSONBody(t, struct{}{}), asUser(markerUserID.String()),
	)
	uninterestedRoomID := alice.MustCreateRoom(t, map[string]interface{}{"preset": "public_chat"})

	aliceMautrix := typingPresenceMautrixClient(t, alice)
	markerGhost := bridge.as.Intent(markerUserID)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := aliceMautrix.UserTyping(ctx, id.RoomID(uninterestedRoomID), true, 20*time.Second); err != nil {
		t.Fatalf("complemau: failed to start typing in uninterested room: %v", err)
	}
	if _, err := markerGhost.UserTyping(ctx, id.RoomID(interestedRoomID), true, 20*time.Second); err != nil {
		t.Fatalf("complemau: failed to send typing marker: %v", err)
	}

	seenUninterested := false
	deadline := time.Now().Add(20 * time.Second)
	for {
		typing := bridge.mustReceiveTyping(t, time.Until(deadline))
		if typing.RoomID == id.RoomID(uninterestedRoomID) {
			seenUninterested = true
			continue
		}
		if typing.RoomID == id.RoomID(interestedRoomID) &&
			typingPresenceTypingContains(typing, markerUserID) {
			break
		}
	}
	if seenUninterested {
		t.Errorf("complemau: appservice received m.typing for uninterested room %s", uninterestedRoomID)
	}

	if _, err := aliceMautrix.UserTyping(ctx, id.RoomID(uninterestedRoomID), false, 0); err != nil {
		t.Errorf("complemau: failed to stop typing in uninterested room: %v", err)
	}
	if _, err := markerGhost.UserTyping(ctx, id.RoomID(interestedRoomID), false, 0); err != nil {
		t.Errorf("complemau: failed to stop typing marker: %v", err)
	}
}

// TestComplemauMasqueradeDoesNotInferPresence covers each ordinary client
// request path that may refresh presence. Appservice authentication must not
// make a ghost appear online unless the bridge deliberately sets its presence.
func TestComplemauMasqueradeDoesNotInferPresence(t *testing.T) {
	runtime.SkipIf(t, runtime.Dendrite)

	deployment := complement.OldDeploy(t, b.BlueprintHSWithComplemauBridge)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	bridgeUser := deployment.AppServiceUser(t, "hs1", b.ComplemauSenderID)
	bridge := startComplemauBridge(t, bridgeUser.BaseURL)
	defer bridge.stop()

	const ghostLocalpart = "complemau_presence_ghost"
	ghostID := id.UserID("@" + ghostLocalpart + ":hs1")
	registerAppserviceGhost(t, bridgeUser, ghostLocalpart)

	roomID := alice.MustCreateRoom(t, map[string]interface{}{"preset": "public_chat"})
	bridgeUser.MustDo(t, "POST",
		[]string{"_matrix", "client", "v3", "rooms", roomID, "join"},
		client.WithJSONBody(t, struct{}{}), asUser(ghostID.String()),
	)
	eventID := alice.SendEventSynced(t, roomID, b.Event{
		Type: "m.room.message",
		Content: map[string]interface{}{
			"msgtype": "m.text",
			"body":    "presence regression anchor",
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	ghost := bridge.as.Intent(ghostID)
	actions := []struct {
		name string
		run  func(*testing.T)
	}{
		{
			name: "receipt",
			run: func(t *testing.T) {
				postReadReceipt(t, bridgeUser, roomID, eventID, asUser(ghostID.String()))
			},
		},
		{
			name: "read marker",
			run: func(t *testing.T) {
				res := bridgeUser.MustDo(t, "POST",
					[]string{"_matrix", "client", "v3", "rooms", roomID, "read_markers"},
					client.WithJSONBody(t, map[string]interface{}{"m.fully_read": eventID}),
					asUser(ghostID.String()),
				)
				res.Body.Close()
			},
		},
		{
			name: "typing",
			run: func(t *testing.T) {
				if _, err := ghost.UserTyping(ctx, id.RoomID(roomID), true, 10*time.Second); err != nil {
					t.Fatalf("complemau: failed to start masqueraded typing: %v", err)
				}
				if _, err := ghost.UserTyping(ctx, id.RoomID(roomID), false, 0); err != nil {
					t.Fatalf("complemau: failed to stop masqueraded typing: %v", err)
				}
			},
		},
		{
			name: "profile update",
			run: func(t *testing.T) {
				if err := ghost.SetDisplayName(ctx, "Complemau presence ghost"); err != nil {
					t.Fatalf("complemau: failed to update ghost profile: %v", err)
				}
			},
		},
		{
			name: "sync",
			run: func(t *testing.T) {
				res := bridgeUser.MustDo(t, "GET",
					[]string{"_matrix", "client", "v3", "sync"},
					asUser(ghostID.String()),
				)
				res.Body.Close()
			},
		},
	}

	for _, action := range actions {
		t.Run(action.name, func(t *testing.T) {
			typingPresenceSetGhostOffline(t, ctx, ghost, alice, ghostID)
			action.run(t)
			if presence := typingPresenceGetPresence(t, alice, ghostID); presence == event.PresenceOnline {
				t.Errorf("complemau: appservice %s inferred presence=online for %s (tuwunel#515)", action.name, ghostID)
			}
		})
	}
}

// This test intentionally records a known tuwunel gap in the complemau
// baseline. Tuwunel does not currently enqueue m.presence for appservices. A
// pass means MSC2409 presence delivery has landed and the baseline can advance.
func TestComplemauAppserviceReceivesPresence(t *testing.T) {
	runtime.SkipIf(t, runtime.Dendrite)

	deployment := complement.OldDeploy(t, b.BlueprintHSWithComplemauBridge)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	bridgeUser := deployment.AppServiceUser(t, "hs1", b.ComplemauSenderID)
	bridge := startComplemauBridge(t, bridgeUser.BaseURL)
	defer bridge.stop()

	const markerLocalpart = "complemau_presence_marker"
	markerUserID := id.UserID("@" + markerLocalpart + ":hs1")
	registerAppserviceGhost(t, bridgeUser, markerLocalpart)

	roomID := alice.MustCreateRoom(t, map[string]interface{}{"preset": "public_chat"})
	bridgeUser.MustDo(t, "POST",
		[]string{"_matrix", "client", "v3", "rooms", roomID, "join"},
		client.WithJSONBody(t, struct{}{}), asUser(markerUserID.String()),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	markerGhost := bridge.as.Intent(markerUserID)
	res := alice.MustDo(t, "PUT",
		[]string{"_matrix", "client", "v3", "presence", alice.UserID, "status"},
		client.WithJSONBody(t, map[string]interface{}{
			"presence": string(event.PresenceOffline),
		}),
	)
	res.Body.Close()
	if presence := typingPresenceGetPresence(t, alice, id.UserID(alice.UserID)); presence != event.PresenceOffline {
		t.Fatalf("complemau: presence is not enabled, setting offline yielded %q", presence)
	}
	before, err := markerGhost.SendText(ctx, id.RoomID(roomID), "presence boundary before transition")
	if err != nil {
		t.Fatalf("complemau: failed to send initial presence boundary: %v", err)
	}
	typingPresenceTransactionsThroughPDU(t, bridge, before.EventID, nil)

	alice.MustSync(t, client.SyncReq{TimeoutMillis: "0", SetPresence: string(event.PresenceOnline)})
	if presence := typingPresenceGetPresence(t, alice, id.UserID(alice.UserID)); presence != event.PresenceOnline {
		t.Fatalf("complemau: sync did not set %s online, got %q", alice.UserID, presence)
	}

	after, err := markerGhost.SendText(ctx, id.RoomID(roomID), "presence boundary after transition")
	if err != nil {
		t.Fatalf("complemau: failed to send final presence boundary: %v", err)
	}
	found := false
	typingPresenceTransactionsThroughPDU(t, bridge, after.EventID, func(transaction *complemauTransaction) {
		for _, edu := range transaction.Body.EphemeralEvents {
			if edu.Type.Type != event.EphemeralEventPresence.Type || edu.Sender != id.UserID(alice.UserID) {
				continue
			}
			content := edu.Content.GetRaw()
			if content["presence"] == string(event.PresenceOnline) {
				found = true
			}
		}
	})
	if !found {
		t.Errorf("complemau: appservice did not receive Alice's m.presence before the later PDU marker")
	}
}

func typingPresenceMautrixClient(t *testing.T, csapi *client.CSAPI) *mautrix.Client {
	t.Helper()
	cli, err := mautrix.NewClient(csapi.BaseURL, id.UserID(csapi.UserID), csapi.AccessToken)
	if err != nil {
		t.Fatalf("complemau: failed to create mautrix client: %v", err)
	}
	cli.DeviceID = id.DeviceID(csapi.DeviceID)
	cli.Client = csapi.Client
	return cli
}

func typingPresenceAssertTyping(
	t *testing.T,
	typing *event.Event,
	roomID id.RoomID,
	userID id.UserID,
	wantUser bool,
) {
	t.Helper()
	if typing.Type.Type != event.EphemeralEventTyping.Type {
		t.Errorf("complemau: got EDU type %s, want m.typing", typing.Type.Type)
	}
	if typing.RoomID != roomID {
		t.Errorf("complemau: got typing room %s, want %s", typing.RoomID, roomID)
	}
	if got := typingPresenceTypingContains(typing, userID); got != wantUser {
		t.Errorf("complemau: typing users %v, presence of %s is %t, want %t",
			typing.Content.AsTyping().UserIDs, userID, got, wantUser)
	}
}

func typingPresenceTypingContains(typing *event.Event, userID id.UserID) bool {
	for _, current := range typing.Content.AsTyping().UserIDs {
		if current == userID {
			return true
		}
	}
	return false
}

func typingPresenceSetGhostOffline(
	t *testing.T,
	ctx context.Context,
	ghost *appservice.IntentAPI,
	observer *client.CSAPI,
	ghostID id.UserID,
) {
	t.Helper()
	if err := ghost.SetPresence(ctx, mautrix.ReqPresence{Presence: event.PresenceOffline}); err != nil {
		t.Fatalf("complemau: failed to set ghost offline: %v", err)
	}
	if presence := typingPresenceGetPresence(t, observer, ghostID); presence != event.PresenceOffline {
		t.Fatalf("complemau: setting %s offline yielded %q", ghostID, presence)
	}
}

func typingPresenceGetPresence(t *testing.T, observer *client.CSAPI, userID id.UserID) event.Presence {
	t.Helper()
	res := observer.Do(t, "GET",
		[]string{"_matrix", "client", "v3", "presence", userID.String(), "status"},
	)
	if res.StatusCode != http.StatusOK {
		res.Body.Close()
		t.Fatalf("complemau: presence lookup for %s returned %s", userID, res.Status)
	}
	return event.Presence(client.GetJSONFieldStr(t, client.ParseJSON(t, res), "presence"))
}

func typingPresenceTransactionsThroughPDU(
	t *testing.T,
	bridge *complemauBridge,
	markerID id.EventID,
	inspect func(*complemauTransaction),
) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		transaction := bridge.mustReceiveTransaction(t, time.Until(deadline))
		if inspect != nil {
			inspect(transaction)
		}
		for _, pdu := range transaction.Body.Events {
			if pdu.ID == markerID {
				return
			}
		}
	}
}
