//go:build complemau

package complemau_tests

import (
	"net/http"
	"testing"

	"github.com/matrix-org/complement"
	"github.com/matrix-org/complement/b"
	"github.com/matrix-org/complement/client"
	"github.com/matrix-org/complement/helpers"
	"github.com/matrix-org/complement/runtime"
)

// TestComplemauMasqueradeDoesNotInferPresence asserts that an
// appservice-authenticated request masquerading as a local user does not, by
// itself, mark that user online. A double-puppet bridge posts a read receipt as
// its puppet while the puppet's real devices are offline; the homeserver must
// not treat that appservice traffic as human device activity.
//
// Motivating regression: matrix-construct/tuwunel#515, where every masqueraded
// receipt/read-marker/typing/profile/sync request refreshed the puppet's
// inferred presence to online/currently_active, which additionally defeated
// suppress_push_when_active. An appservice may still set presence deliberately
// through the explicit presence endpoint; it is ordinary masqueraded C-S traffic
// that must not imply activity.
func TestComplemauMasqueradeDoesNotInferPresence(t *testing.T) {
	// tuwunel and Synapse read the Complement appservice registration file;
	// Dendrite does not yet. https://github.com/matrix-org/complement/issues/514
	runtime.SkipIf(t, runtime.Dendrite)

	deployment := complement.OldDeploy(t, b.BlueprintHSWithComplemauBridge)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	bridge := deployment.AppServiceUser(t, "hs1", b.ComplemauSenderID)
	br := startComplemauBridge(t, bridge.BaseURL)
	defer br.stop()

	// The bridge provisions and puppets a ghost whose only activity is the
	// appservice request below; it has no real device to be online from.
	const ghostLocalpart = "complemau_ghost"
	ghostID := "@" + ghostLocalpart + ":hs1"
	registerAppserviceGhost(t, bridge, ghostLocalpart)

	roomID := alice.MustCreateRoom(t, map[string]interface{}{"preset": "public_chat"})
	alice.MustSyncUntil(t, client.SyncReq{}, client.SyncJoinedTo(alice.UserID, roomID))
	bridge.MustDo(t, "POST",
		[]string{"_matrix", "client", "v3", "rooms", roomID, "join"},
		client.WithJSONBody(t, struct{}{}), asUser(ghostID),
	)

	eventID := alice.SendEventSynced(t, roomID, b.Event{
		Type: "m.room.message",
		Content: map[string]interface{}{
			"msgtype": "m.text",
			"body":    "ping",
		},
	})

	// The bridge acknowledges the message on the ghost's behalf.
	postReadReceipt(t, bridge, roomID, eventID, asUser(ghostID))

	// That appservice traffic alone must not have inferred the ghost online.
	res := alice.Do(t, "GET",
		[]string{"_matrix", "client", "v3", "presence", ghostID, "status"},
	)
	if res.StatusCode == http.StatusNotFound {
		res.Body.Close()
		return
	}
	if res.StatusCode != http.StatusOK {
		res.Body.Close()
		t.Fatalf("complemau: presence lookup for %s returned %s", ghostID, res.Status)
	}
	presence := client.GetJSONFieldStr(t, client.ParseJSON(t, res), "presence")
	if presence == "online" {
		t.Errorf("complemau: appservice masquerade inferred presence=online for %s (tuwunel#515)", ghostID)
	}
}
