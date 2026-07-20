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
)

// TestComplemauAppserviceReceivesReceiptOnce stands up a real mautrix
// appservice server as the bridge endpoint and drives an appservice m.read
// receipt, asserting the homeserver pushes exactly one m.receipt ephemeral EDU
// and does not re-emit one for an equal (non-advancing) read position.
//
// This is the "complemau" harness: Complement supplies the homeserver lifecycle
// and container networking, while mautrix supplies the authentic appservice
// receive path. Asserting against what mautrix actually receives exercises real
// bridge compatibility rather than a re-implementation of the appservice wire
// protocol.
//
// Motivating regression: matrix-construct/tuwunel#516, where an equal-or-older
// public read receipt was stored with a fresh stream position and re-emitted to
// the bridge, producing an unbounded acknowledgement loop with a double-puppet
// bridge. The spec requires m.receipt be delivered under the same interest rules
// as regular events:
// https://spec.matrix.org/v1.18/application-service-api/#pushing-ephemeral-data
func TestComplemauAppserviceReceivesReceiptOnce(t *testing.T) {
	// tuwunel and Synapse read the Complement appservice registration file;
	// Dendrite does not yet. https://github.com/matrix-org/complement/issues/514
	runtime.SkipIf(t, runtime.Dendrite)

	deployment := complement.OldDeploy(t, b.BlueprintHSWithComplemauBridge)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	// The as_token client acting as the bridge's sender_localpart user, which the
	// registration namespace covers and which an appservice may act as without a
	// prior /register.
	bridge := deployment.AppServiceUser(t, "hs1", b.ComplemauSenderID)

	br := startComplemauBridge(t, bridge.BaseURL)
	defer br.stop()

	// A room the bridge user joins, so the appservice is interested in its
	// ephemeral data.
	roomID := alice.MustCreateRoom(t, map[string]interface{}{"preset": "public_chat"})
	alice.MustSyncUntil(t, client.SyncReq{}, client.SyncJoinedTo(alice.UserID, roomID))
	bridge.MustJoinRoom(t, roomID, nil)

	eventID := alice.SendEventSynced(t, roomID, b.Event{
		Type: "m.room.message",
		Content: map[string]interface{}{
			"msgtype": "m.text",
			"body":    "hello from alice",
		},
	})

	// The bridge acknowledges the message on behalf of its user. The homeserver
	// must push exactly one m.receipt EDU for the advancing read position.
	postReadReceipt(t, bridge, roomID, eventID)
	got := br.mustReceiveReceipt(t, 10*time.Second)
	if _, ok := got.Content.Raw[eventID]; !ok {
		t.Errorf("complemau: first m.receipt did not reference the acknowledged event %s: %v", eventID, got.Content.Raw)
	}

	// The same acknowledgement again does not advance the read position, so the
	// homeserver must neither store nor re-emit it (tuwunel#516).
	postReadReceipt(t, bridge, roomID, eventID)
	br.mustNotReceiveReceipt(t, 5*time.Second)
}
