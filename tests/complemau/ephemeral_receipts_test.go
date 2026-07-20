//go:build complemau

package complemau_tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/matrix-org/complement"
	"github.com/matrix-org/complement/b"
	"github.com/matrix-org/complement/client"
	"github.com/matrix-org/complement/helpers"
	"github.com/matrix-org/complement/runtime"

	"maunium.net/go/mautrix/event"
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

	eventIDs := make([]string, 3)
	for i := range eventIDs {
		eventIDs[i] = alice.Unsafe_SendEventUnsynced(t, roomID, b.Event{
			Type: "m.room.message",
			Content: map[string]interface{}{
				"msgtype": "m.text",
				"body":    fmt.Sprintf("receipt position %d", i),
			},
		})
	}

	// The bridge acknowledges the middle event on behalf of its user. The
	// homeserver must push exactly one correctly shaped m.receipt EDU.
	postReadReceipt(t, bridge, roomID, eventIDs[1])
	got := br.mustReceiveReceipt(t, 10*time.Second)
	receiptUserData(t, got, eventIDs[1], bridge.UserID)

	// Older and equal acknowledgements do not advance the read position. A later
	// advancing acknowledgement is the marker proving neither stale position was
	// emitted before it.
	postReadReceipt(t, bridge, roomID, eventIDs[0])
	postReadReceipt(t, bridge, roomID, eventIDs[1])
	postReadReceipt(t, bridge, roomID, eventIDs[2])
	for {
		got = br.mustReceiveReceipt(t, 10*time.Second)
		forbidden := false
		for _, eventID := range eventIDs[:2] {
			if _, ok := got.Content.Raw[eventID]; ok {
				t.Errorf("complemau: homeserver re-emitted a non-advancing receipt for %s (tuwunel#516): %v", eventID, got.Content.Raw)
				forbidden = true
			}
		}
		if _, ok := got.Content.Raw[eventIDs[2]]; ok {
			receiptUserData(t, got, eventIDs[2], bridge.UserID)
			break
		}
		if !forbidden {
			t.Errorf("complemau: received an unrelated receipt before marker %s: %v", eventIDs[2], got.Content.Raw)
		}
	}
}

// TestComplemauReceiptBatchingHasNoLoss drives enough independent receipt
// positions to require multiple appservice transactions without depending on a
// particular homeserver batch limit. The union across transaction boundaries
// must contain every position exactly once.
func TestComplemauReceiptBatchingHasNoLoss(t *testing.T) {
	runtime.SkipIf(t, runtime.Dendrite)

	deployment := complement.OldDeploy(t, b.BlueprintHSWithComplemauBridge)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	bridge := deployment.AppServiceUser(t, "hs1", b.ComplemauSenderID)
	br := startComplemauBridge(t, bridge.BaseURL)
	defer br.stop()

	// Hold the first delivery while one user acknowledges independent positions
	// in distinct rooms. Distinct rooms prevent the receipt stream from
	// coalescing the burst into one EDU.
	release := br.stallNextTransaction(t)
	const receiptCount = 300
	want := make(map[complemauReceiptPosition]struct{}, receiptCount)
	var markerRoomID string
	for i := 0; i < receiptCount; i++ {
		roomID := alice.MustCreateRoom(t, map[string]interface{}{"preset": "private_chat"})
		eventID := alice.Unsafe_SendEventUnsynced(t, roomID, b.Event{
			Type: "m.room.message",
			Content: map[string]interface{}{
				"msgtype": "m.text",
				"body":    fmt.Sprintf("receipt batch position %d", i),
			},
		})
		postReadReceipt(t, alice, roomID, eventID)
		want[complemauReceiptPosition{roomID: roomID, eventID: eventID, userID: alice.UserID}] = struct{}{}
		markerRoomID = roomID
	}
	release()

	markerEventID := alice.Unsafe_SendEventUnsynced(t, markerRoomID, b.Event{
		Type: "m.room.message",
		Content: map[string]interface{}{
			"msgtype": "m.text",
			"body":    "receipt batch terminal marker",
		},
	})

	seen := make(map[complemauReceiptPosition]struct{}, receiptCount)
	receiptTransactions := 0
	maxReceiptsPerTransaction := 0
	deadline := time.Now().Add(60 * time.Second)
	for {
		transaction := br.mustReceiveTransaction(t, time.Until(deadline))
		receiptsInTransaction := 0
		for _, ephemeral := range transaction.Body.EphemeralEvents {
			if ephemeral.Type != event.EphemeralEventReceipt {
				continue
			}
			receiptsInTransaction++
			recordComplemauReceiptPositions(t, ephemeral, want, seen)
		}
		if receiptsInTransaction > 0 {
			receiptTransactions++
			maxReceiptsPerTransaction = max(maxReceiptsPerTransaction, receiptsInTransaction)
		}
		if complemauTxnHasEvent(transaction, markerEventID) {
			break
		}
	}
	if len(seen) != len(want) {
		t.Fatalf("complemau: received %d distinct receipt positions, want %d", len(seen), len(want))
	}
	if receiptTransactions < 2 {
		t.Fatalf("complemau: %d receipts arrived in only %d transaction", len(seen), receiptTransactions)
	}
	t.Logf(
		"complemau: received %d receipt positions across %d transactions, observed batch cap %d",
		len(seen),
		receiptTransactions,
		maxReceiptsPerTransaction,
	)
}

type complemauReceiptPosition struct {
	roomID  string
	eventID string
	userID  string
}

func recordComplemauReceiptPositions(
	t *testing.T,
	receipt *event.Event,
	want map[complemauReceiptPosition]struct{},
	seen map[complemauReceiptPosition]struct{},
) {
	t.Helper()
	for eventID, rawEventReceipts := range receipt.Content.Raw {
		eventReceipts, ok := rawEventReceipts.(map[string]interface{})
		if !ok {
			t.Fatalf("complemau: m.receipt has non-object event data for %s: %v", eventID, rawEventReceipts)
		}
		readers, ok := eventReceipts["m.read"].(map[string]interface{})
		if !ok {
			t.Fatalf("complemau: m.receipt has no m.read object for event %s: %v", eventID, eventReceipts)
		}
		for userID, rawData := range readers {
			if _, ok := rawData.(map[string]interface{}); !ok {
				t.Fatalf("complemau: receipt data for %s is not an object: %v", userID, rawData)
			}
			position := complemauReceiptPosition{
				roomID:  string(receipt.RoomID),
				eventID: eventID,
				userID:  userID,
			}
			if _, ok := want[position]; !ok {
				t.Fatalf("complemau: receipt batch leaked unexpected position %+v", position)
			}
			if _, duplicate := seen[position]; duplicate {
				t.Fatalf("complemau: receipt batch duplicated position %+v", position)
			}
			seen[position] = struct{}{}
		}
	}
}

func receiptReaders(t *testing.T, receipt *event.Event, eventID string) map[string]interface{} {
	t.Helper()
	eventReceipts, ok := receipt.Content.Raw[eventID].(map[string]interface{})
	if !ok {
		t.Fatalf("complemau: m.receipt has no object for event %s: %v", eventID, receipt.Content.Raw)
	}
	readers, ok := eventReceipts["m.read"].(map[string]interface{})
	if !ok {
		t.Fatalf("complemau: m.receipt has no m.read object for event %s: %v", eventID, eventReceipts)
	}
	return readers
}

func receiptUserData(t *testing.T, receipt *event.Event, eventID, userID string) map[string]interface{} {
	t.Helper()
	data, ok := receiptReaders(t, receipt, eventID)[userID].(map[string]interface{})
	if !ok {
		t.Fatalf("complemau: m.receipt has no object for reader %s: %v", userID, receipt.Content.Raw)
	}
	if timestamp, ok := data["ts"].(float64); ok && timestamp <= 0 {
		t.Errorf("complemau: m.receipt has an implausible timestamp for %s: %v", userID, timestamp)
	}
	return data
}
