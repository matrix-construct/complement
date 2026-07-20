//go:build complemau

package complemau_tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/matrix-org/complement"
	"github.com/matrix-org/complement/b"
	"github.com/matrix-org/complement/client"
	"github.com/matrix-org/complement/helpers"
	"github.com/matrix-org/complement/runtime"
)

const complemauTxnTimeout = 20 * time.Second

func TestComplemauTransactionSingleInflightAndBatching(t *testing.T) {
	runtime.SkipIf(t, runtime.Dendrite)

	deployment := complement.OldDeploy(t, b.BlueprintHSWithComplemauBridge)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	bridgeUser := deployment.AppServiceUser(t, "hs1", b.ComplemauSenderID)
	bridge := startComplemauBridge(t, bridgeUser.BaseURL)
	defer bridge.stop()
	roomID := primeComplemauTxnRoom(t, alice, bridgeUser, bridge)

	release := bridge.stallNextTransaction(t)
	firstEventID := sendComplemauTxnMessage(t, alice, roomID, "first in flight")
	first := mustReceiveComplemauTxnWithEvent(t, bridge, firstEventID, complemauTxnTimeout)
	if !complemauTxnHasEvent(first, firstEventID) {
		t.Fatalf("complemau: first transaction did not contain %s", firstEventID)
	}

	secondEventID := sendComplemauTxnMessage(t, alice, roomID, "queued second")
	markerEventID := sendComplemauTxnMessageSynced(t, alice, roomID, "queued marker")
	releasedAt := time.Now()
	release()

	queued := mustReceiveComplemauTxnWithEvent(t, bridge, secondEventID, complemauTxnTimeout)
	if queued.ReceivedAt.Before(releasedAt) {
		t.Fatalf("complemau: a second transaction arrived while the first response was stalled")
	}
	secondPosition := complemauTxnEventPosition(queued, secondEventID)
	markerPosition := complemauTxnEventPosition(queued, markerEventID)
	if secondPosition < 0 || markerPosition < 0 {
		t.Fatalf(
			"complemau: queued events were not batched together after the first acknowledgement: second=%d marker=%d",
			secondPosition,
			markerPosition,
		)
	}
	if secondPosition >= markerPosition {
		t.Fatalf("complemau: queued event order changed inside the batch: second=%d marker=%d", secondPosition, markerPosition)
	}
}

func TestComplemauTransactionBatchCapAndOrdering(t *testing.T) {
	runtime.SkipIf(t, runtime.Dendrite)

	deployment := complement.OldDeploy(t, b.BlueprintHSWithComplemauBridge)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	bridgeUser := deployment.AppServiceUser(t, "hs1", b.ComplemauSenderID)
	bridge := startComplemauBridge(t, bridgeUser.BaseURL)
	defer bridge.stop()
	roomID := primeComplemauTxnRoom(t, alice, bridgeUser, bridge)

	release := bridge.stallNextTransaction(t)
	gateEventID := sendComplemauTxnMessage(t, alice, roomID, "batch gate")
	mustReceiveComplemauTxnWithEvent(t, bridge, gateEventID, complemauTxnTimeout)

	const eventCount = 150
	eventIDs := make([]string, 0, eventCount)
	positions := make(map[string]int, eventCount)
	for sequence := range eventCount {
		eventID := sendComplemauTxnMessage(t, alice, roomID, fmt.Sprintf("batch event %03d", sequence))
		positions[eventID] = sequence
		eventIDs = append(eventIDs, eventID)
	}
	markerEventID := sendComplemauTxnMessageSynced(t, alice, roomID, "batch terminal marker")
	release()

	seen := make(map[string]struct{}, eventCount)
	seenTransactionIDs := make(map[string]struct{})
	ordered := make([]string, 0, eventCount)
	observedCap := 0
	relevantTransactions := 0
	sawMarker := false
	deadline := time.Now().Add(60 * time.Second)
	for !sawMarker {
		transaction := bridge.mustReceiveTransaction(t, time.Until(deadline))
		relevant := false
		for _, event := range transaction.Body.Events {
			eventID := string(event.ID)
			if eventID == markerEventID {
				sawMarker = true
				relevant = true
				continue
			}
			if _, expected := positions[eventID]; !expected {
				continue
			}
			if _, duplicate := seen[eventID]; duplicate {
				t.Fatalf("complemau: event %s appeared in more than one appservice transaction", eventID)
			}
			seen[eventID] = struct{}{}
			ordered = append(ordered, eventID)
			relevant = true
		}
		if !relevant {
			continue
		}
		if _, duplicate := seenTransactionIDs[transaction.ID]; duplicate {
			t.Fatalf("complemau: successful transaction id %s was reused", transaction.ID)
		}
		seenTransactionIDs[transaction.ID] = struct{}{}
		relevantTransactions++
		transactionSize := len(transaction.Body.Events)
		if observedCap == 0 {
			observedCap = transactionSize
		} else if transactionSize > observedCap {
			t.Fatalf(
				"complemau: later transaction size %d exceeded the first observed full batch size %d",
				transactionSize,
				observedCap,
			)
		}
	}

	if relevantTransactions < 2 {
		t.Fatalf("complemau: %d queued events were not split across transaction batches", eventCount)
	}
	if len(seen) != len(eventIDs) {
		t.Fatalf("complemau: received %d of %d queued events before the terminal marker", len(seen), len(eventIDs))
	}
	if len(ordered) != len(eventIDs) {
		t.Fatalf("complemau: ordered event count is %d, want %d", len(ordered), len(eventIDs))
	}
	for position, eventID := range ordered {
		if positions[eventID] != position {
			t.Fatalf(
				"complemau: event stream reordered at position %d, got original position %d",
				position,
				positions[eventID],
			)
		}
	}
	t.Logf("complemau: observed appservice PDU batch size %d across %d transactions", observedCap, relevantTransactions)
}

func TestComplemauTransactionQueuesAreIndependent(t *testing.T) {
	runtime.SkipIf(t, runtime.Dendrite)

	deployment := complement.OldDeploy(t, b.BlueprintHSWithTwoComplemauBridges)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	firstUser := deployment.AppServiceUser(t, "hs1", b.ComplemauSenderID)
	secondUser := deployment.AppServiceUser(t, "hs1", b.ComplemauSecondSenderID)
	first, second := startComplemauBridges(t, firstUser.BaseURL)
	defer first.stop()
	defer second.stop()
	// The second sender is AS2's own user and also matches AS1's shared user
	// namespace, so its membership makes the room interesting to both services.
	roomID := primeComplemauTxnRoom(t, alice, secondUser, first, second)

	release := first.stallNextTransaction(t)
	eventID := sendComplemauTxnMessageSynced(t, alice, roomID, "independent queue marker")
	firstTransaction := mustReceiveComplemauTxnWithEvent(t, first, eventID, complemauTxnTimeout)
	secondTransaction := mustReceiveComplemauTxnWithEvent(t, second, eventID, complemauTxnTimeout)
	releasedAt := time.Now()
	release()

	if secondTransaction.ReceivedAt.After(releasedAt) {
		t.Fatalf("complemau: second appservice was blocked behind the first appservice response")
	}
	if secondTransaction.ReceivedAt.Before(firstTransaction.ReceivedAt) {
		t.Logf("complemau: independent second appservice received the event before the stalled appservice")
	}
}

func TestComplemauTransactionRetryPreservesIDAndContent(t *testing.T) {
	runtime.SkipIf(t, runtime.Dendrite)

	deployment := complement.OldDeploy(t, b.BlueprintHSWithComplemauBridge)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	bridgeUser := deployment.AppServiceUser(t, "hs1", b.ComplemauSenderID)
	bridge := startComplemauBridge(t, bridgeUser.BaseURL)
	defer bridge.stop()
	roomID := primeComplemauTxnRoom(t, alice, bridgeUser, bridge)

	responseDone := bridge.failNextTransaction(t, http.StatusInternalServerError)
	failedEventID := sendComplemauTxnMessageSynced(t, alice, roomID, "failed transaction")
	failed := mustReceiveComplemauTxnWithEvent(t, bridge, failedEventID, complemauTxnTimeout)
	failedBody := mustMarshalComplemauTxnBody(t, failed)
	bridge.mustAwaitTransactionResponse(t, responseDone)
	followupEventID := sendComplemauTxnMessageSynced(t, alice, roomID, "retry dispatch marker")

	var retry *complemauTransaction
	var followup *complemauTransaction
	deadline := time.Now().Add(complemauTxnTimeout)
	for retry == nil || followup == nil {
		transaction := bridge.mustReceiveTransaction(t, time.Until(deadline))
		if transaction.ID == failed.ID {
			retry = transaction
		}
		if complemauTxnHasEvent(transaction, followupEventID) {
			followup = transaction
		}
	}

	if !bytes.Equal(failedBody, mustMarshalComplemauTxnBody(t, retry)) {
		t.Fatalf("complemau: retry %s changed the failed transaction content", retry.ID)
	}
	if followup.ID == failed.ID {
		t.Fatalf("complemau: followup event reused the failed transaction id %s", failed.ID)
	}
}

func TestComplemauPingForcesTransactionRetry(t *testing.T) {
	runtime.SkipIf(t, runtime.Dendrite)

	deployment := complement.OldDeploy(t, b.BlueprintHSWithComplemauBridge)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	bridgeUser := deployment.AppServiceUser(t, "hs1", b.ComplemauSenderID)
	bridge := startComplemauBridge(t, bridgeUser.BaseURL)
	defer bridge.stop()
	roomID := primeComplemauTxnRoom(t, alice, bridgeUser, bridge)

	responseDone := bridge.failNextTransaction(t, http.StatusInternalServerError)
	failedEventID := sendComplemauTxnMessageSynced(t, alice, roomID, "ping gap transaction")
	failed := mustReceiveComplemauTxnWithEvent(t, bridge, failedEventID, complemauTxnTimeout)
	failedBody := mustMarshalComplemauTxnBody(t, failed)
	bridge.mustAwaitTransactionResponse(t, responseDone)

	// Synapse connects a successful ping to force_retry. This remains a known
	// failure until tuwunel's ping worker wakes its appservice sending queue.
	bridge.ensureReady(t)
	retry := mustReceiveComplemauTxnWithID(t, bridge, failed.ID, complemauTxnTimeout)
	if !bytes.Equal(failedBody, mustMarshalComplemauTxnBody(t, retry)) {
		t.Fatalf("complemau: ping retry %s changed the failed transaction content", retry.ID)
	}
}

func TestComplemauTransactionDedupAndUniquifier(t *testing.T) {
	runtime.SkipIf(t, runtime.Dendrite)

	deployment := complement.OldDeploy(t, b.BlueprintHSWithTwoComplemauBridges)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	firstUser := deployment.AppServiceUser(t, "hs1", b.ComplemauSenderID)
	first, second := startComplemauBridges(t, firstUser.BaseURL)
	defer first.stop()
	defer second.stop()

	const (
		ghostID   = "@complemau_txn_ghost:hs1"
		deviceID  = "COMPLEMAU_TXN"
		eventType = "com.example.complemau.txn"
	)
	first.mustCreateGhostDevice(t, ghostID, deviceID, "Complemau transaction device")
	// Two setup messages form a sender barrier. Arrival of the second proves the
	// first response and any device summary queued by device creation were
	// acknowledged before the identical payloads below.
	sendComplemauToDeviceMarker(t, alice, eventType, ghostID, deviceID, "setup one")
	mustReceiveComplemauTxnWithToDeviceMarker(t, second, "setup one", complemauTxnTimeout)
	sendComplemauToDeviceMarker(t, alice, eventType, ghostID, deviceID, "setup two")
	mustReceiveComplemauTxnWithToDeviceMarker(t, second, "setup two", complemauTxnTimeout)

	sendComplemauToDeviceMarker(t, alice, eventType, ghostID, deviceID, "identical")
	firstTransaction := mustReceiveComplemauTxnWithToDeviceMarker(
		t, second, "identical", complemauTxnTimeout,
	)
	assertComplemauToDeviceOnly(t, firstTransaction)
	replayComplemauTxn(t, second, firstTransaction)
	replayed := mustReceiveComplemauTxnWithID(t, second, firstTransaction.ID, complemauTxnTimeout)
	if replayed.ID != firstTransaction.ID {
		t.Fatalf("complemau: replayed transaction id is %s, want %s", replayed.ID, firstTransaction.ID)
	}

	// The wire payload is identical, but the homeserver must include its internal
	// uniquifier when deriving the path id so the receiver does not drop it.
	sendComplemauToDeviceMarker(t, alice, eventType, ghostID, deviceID, "identical")
	secondTransaction := mustReceiveComplemauTxnWithToDeviceMarker(
		t, second, "identical", complemauTxnTimeout,
	)
	assertComplemauToDeviceOnly(t, secondTransaction)
	if !bytes.Equal(
		mustMarshalComplemauTxnBody(t, firstTransaction),
		mustMarshalComplemauTxnBody(t, secondTransaction),
	) {
		t.Fatal("complemau: nominally identical transactions had different wire bodies")
	}
	if secondTransaction.ID == firstTransaction.ID {
		t.Fatalf("complemau: distinct identical transactions collided on id %s", firstTransaction.ID)
	}

	sendComplemauToDeviceMarker(t, alice, eventType, ghostID, deviceID, "terminal")
	mustReceiveComplemauTxnWithToDeviceMarker(t, second, "terminal", complemauTxnTimeout)

	identicalCount := 0
	for {
		event := second.mustReceiveToDevice(t, complemauTxnTimeout)
		marker, _ := event.Content.Raw["complemau_marker"].(string)
		if marker == "identical" {
			identicalCount++
		}
		if marker == "terminal" {
			break
		}
	}
	if identicalCount != 2 {
		t.Fatalf(
			"complemau: receiver applied the identical payload %d times before the terminal marker, want 2",
			identicalCount,
		)
	}
}

func primeComplemauTxnRoom(
	t *testing.T,
	alice *client.CSAPI,
	bridgeUser *client.CSAPI,
	receivers ...*complemauBridge,
) string {
	t.Helper()
	roomID := alice.MustCreateRoom(t, map[string]interface{}{"preset": "public_chat"})
	alice.MustSyncUntil(t, client.SyncReq{}, client.SyncJoinedTo(alice.UserID, roomID))
	bridgeUser.MustJoinRoom(t, roomID, nil)
	markerEventID := sendComplemauTxnMessageSynced(t, alice, roomID, "transaction readiness marker")
	for _, receiver := range receivers {
		mustReceiveComplemauTxnWithEvent(t, receiver, markerEventID, complemauTxnTimeout)
	}
	return roomID
}

func sendComplemauTxnMessage(t *testing.T, sender *client.CSAPI, roomID string, body string) string {
	t.Helper()
	return sender.Unsafe_SendEventUnsynced(t, roomID, b.Event{
		Type: "m.room.message",
		Content: map[string]interface{}{
			"msgtype": "m.text",
			"body":    body,
		},
	})
}

func sendComplemauTxnMessageSynced(t *testing.T, sender *client.CSAPI, roomID string, body string) string {
	t.Helper()
	return sender.SendEventSynced(t, roomID, b.Event{
		Type: "m.room.message",
		Content: map[string]interface{}{
			"msgtype": "m.text",
			"body":    body,
		},
	})
}

func mustReceiveComplemauTxnWithEvent(
	t *testing.T,
	bridge *complemauBridge,
	eventID string,
	timeout time.Duration,
) *complemauTransaction {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("complemau: timed out after %s waiting for event %s in an appservice transaction", timeout, eventID)
		}
		transaction := bridge.mustReceiveTransaction(t, remaining)
		if complemauTxnHasEvent(transaction, eventID) {
			return transaction
		}
	}
}

func mustReceiveComplemauTxnWithID(
	t *testing.T,
	bridge *complemauBridge,
	transactionID string,
	timeout time.Duration,
) *complemauTransaction {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("complemau: timed out after %s waiting for appservice transaction %s", timeout, transactionID)
		}
		transaction := bridge.mustReceiveTransaction(t, remaining)
		if transaction.ID == transactionID {
			return transaction
		}
	}
}

func mustReceiveComplemauTxnWithToDeviceMarker(
	t *testing.T,
	bridge *complemauBridge,
	marker string,
	timeout time.Duration,
) *complemauTransaction {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("complemau: timed out after %s waiting for to-device marker %q", timeout, marker)
		}
		transaction := bridge.mustReceiveTransaction(t, remaining)
		for _, event := range transaction.Body.MSC2409ToDeviceEvents {
			if got, _ := event.Content.Raw["complemau_marker"].(string); got == marker {
				return transaction
			}
		}
	}
}

func complemauTxnHasEvent(transaction *complemauTransaction, eventID string) bool {
	return complemauTxnEventPosition(transaction, eventID) >= 0
}

func complemauTxnEventPosition(transaction *complemauTransaction, eventID string) int {
	for position, event := range transaction.Body.Events {
		if string(event.ID) == eventID {
			return position
		}
	}
	return -1
}

func complemauTxnIDQueued(bridge *complemauBridge, transactionID string) bool {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for {
		transaction, ok := bridge.transactions.pop(ctx)
		if !ok {
			return false
		}
		if transaction.ID == transactionID {
			return true
		}
	}
}

func mustMarshalComplemauTxnBody(t *testing.T, transaction *complemauTransaction) []byte {
	t.Helper()
	body, err := json.Marshal(transaction.Body)
	if err != nil {
		t.Fatalf("complemau: failed to encode transaction %s: %v", transaction.ID, err)
	}
	return body
}

func replayComplemauTxn(t *testing.T, bridge *complemauBridge, transaction *complemauTransaction) {
	t.Helper()
	body := mustMarshalComplemauTxnBody(t, transaction)
	endpoint := fmt.Sprintf(
		"http://127.0.0.1:%d/_matrix/app/v1/transactions/%s",
		bridge.port,
		url.PathEscape(transaction.ID),
	)
	request, err := http.NewRequest(http.MethodPut, endpoint, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("complemau: failed to construct transaction replay: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+bridge.as.Registration.ServerToken)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("complemau: transaction replay failed: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		t.Fatalf("complemau: transaction replay returned %s", response.Status)
	}
}

func sendComplemauToDeviceMarker(
	t *testing.T,
	sender *client.CSAPI,
	eventType string,
	userID string,
	deviceID string,
	marker string,
) {
	t.Helper()
	sender.MustSendToDeviceMessages(t, eventType, map[string]map[string]map[string]interface{}{
		userID: {
			deviceID: {"complemau_marker": marker},
		},
	})
}

func assertComplemauToDeviceOnly(t *testing.T, transaction *complemauTransaction) {
	t.Helper()
	if len(transaction.Body.MSC2409ToDeviceEvents) != 1 {
		t.Fatalf(
			"complemau: transaction %s contains %d unstable to-device events, want 1",
			transaction.ID,
			len(transaction.Body.MSC2409ToDeviceEvents),
		)
	}
	if len(transaction.Body.Events) != 0 || len(transaction.Body.EphemeralEvents) != 0 ||
		transaction.Body.MSC3202DeviceLists != nil || len(transaction.Body.MSC3202DeviceOTKCount) != 0 ||
		len(transaction.Body.MSC3202FallbackKeys) != 0 {
		t.Fatalf("complemau: transaction %s was not to-device-only", transaction.ID)
	}
}
