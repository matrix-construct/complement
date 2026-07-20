//go:build complemau

package complemau_tests

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/matrix-org/complement"
	"github.com/matrix-org/complement/b"
	"github.com/matrix-org/complement/client"
	"github.com/matrix-org/complement/helpers"
	"github.com/matrix-org/complement/runtime"
	"maunium.net/go/mautrix/event"
)

const complemauToDeviceTimeout = 60 * time.Second

func TestComplemauAppserviceReceivesToDevice(t *testing.T) {
	runtime.SkipIf(t, runtime.Dendrite)

	blueprint := newComplemauToDeviceBlueprint()
	deployment := complement.OldDeploy(t, blueprint)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	bridgeUser := deployment.AppServiceUser(t, "hs1", b.ComplemauSenderID)
	registration := blueprint.Homeservers[0].ApplicationServices[0]
	bridge := startComplemauBridgeWithRegistration(
		t, bridgeUser.BaseURL, registration, b.ComplemauASPort,
	)
	defer bridge.stop()

	const (
		ghostID  = "@complemau_to_device:hs1"
		deviceID = "COMPLEMAU_TO_DEVICE"
	)
	bridge.mustCreateGhostDevice(t, ghostID, deviceID, "Complemau to-device")

	keyRequest := map[string]interface{}{
		"complemau_case": "key request",
		"some_key":       "some really interesting value",
	}
	alice.MustSendToDeviceMessages(t, "m.room_key_request", complemauToDeviceMessages(
		ghostID, deviceID, keyRequest,
	))
	assertComplemauToDeviceEvent(
		t,
		bridge.mustReceiveToDevice(t, complemauToDeviceTimeout),
		"m.room_key_request",
		alice.UserID,
		ghostID,
		deviceID,
		keyRequest,
	)

	encrypted := map[string]interface{}{
		"algorithm":      "m.megolm.v1.aes-sha2",
		"ciphertext":     "complemau ciphertext",
		"complemau_case": "encrypted",
		"sender_key":     "complemau sender key",
	}
	alice.MustSendToDeviceMessages(t, "m.room.encrypted", complemauToDeviceMessages(
		ghostID, deviceID, encrypted,
	))
	assertComplemauToDeviceEvent(
		t,
		bridge.mustReceiveToDevice(t, complemauToDeviceTimeout),
		"m.room.encrypted",
		alice.UserID,
		ghostID,
		deviceID,
		encrypted,
	)

	reverse := map[string]interface{}{"complemau_case": "reverse"}
	bridgeUser.MustDo(
		t,
		"PUT",
		[]string{"_matrix", "client", "v3", "sendToDevice", "m.room_key_request", "reverse"},
		client.WithJSONBody(t, map[string]interface{}{
			"messages": complemauToDeviceMessages(alice.UserID, alice.DeviceID, reverse),
		}),
		asUserDevice(ghostID, deviceID),
	)

	terminal := map[string]interface{}{"complemau_case": "terminal"}
	alice.MustSendToDeviceMessages(t, "m.room_key_request", complemauToDeviceMessages(
		ghostID, deviceID, terminal,
	))
	mustReceiveComplemauToDeviceMarkerWithout(t, bridge, "terminal", "reverse")
}

func TestComplemauToDeviceBurstFansOut(t *testing.T) {
	runtime.SkipIf(t, runtime.Dendrite)
	// Known tuwunel gap: a queued burst can repeat one logical event under a new
	// transaction ID. Keep this strict so the test flips green with the fix.

	deployment := complement.OldDeploy(t, b.BlueprintHSWithTwoComplemauBridges)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	bridgeUser := deployment.AppServiceUser(t, "hs1", b.ComplemauSenderID)
	first, second := startComplemauBridges(t, bridgeUser.BaseURL)
	defer first.stop()
	defer second.stop()

	const (
		burstCount = 150
		ghostID    = "@complemau_to_device_burst:hs1"
	)
	devices := make(map[string]map[string]interface{}, burstCount)
	for sequence := range burstCount {
		deviceID := fmt.Sprintf("COMPLEMAU_BURST_%03d", sequence)
		first.mustCreateGhostDevice(t, ghostID, deviceID, "Complemau burst device")
		devices[deviceID] = map[string]interface{}{
			"complemau_case": fmt.Sprintf("burst %03d", sequence),
		}
	}

	firstTransactions, stopFirst := startComplemauTransactionDrain(t, first, burstCount*4)
	defer stopFirst()
	secondTransactions, stopSecond := startComplemauTransactionDrain(t, second, burstCount*4)
	defer stopSecond()

	setup := map[string]interface{}{"complemau_case": "burst setup"}
	alice.MustSendToDeviceMessages(t, "m.room_key_request", complemauToDeviceMessages(
		ghostID, "COMPLEMAU_BURST_000", setup,
	))
	mustReceiveComplemauToDeviceTransactionMarker(t, firstTransactions, "burst setup")
	mustReceiveComplemauToDeviceTransactionMarker(t, secondTransactions, "burst setup")

	releaseFirst := first.stallNextTransaction(t)
	releaseSecond := second.stallNextTransaction(t)
	stall := map[string]interface{}{"complemau_case": "burst calibration stall"}
	alice.MustSendToDeviceMessages(t, "m.room_key_request", complemauToDeviceMessages(
		ghostID, "COMPLEMAU_BURST_000", stall,
	))
	mustReceiveComplemauToDeviceTransactionMarker(
		t, firstTransactions, "burst calibration stall",
	)
	mustReceiveComplemauToDeviceTransactionMarker(
		t, secondTransactions, "burst calibration stall",
	)

	calibration := make(map[string]map[string]interface{}, burstCount)
	for deviceID := range devices {
		calibration[deviceID] = map[string]interface{}{
			"complemau_case": "calibration " + deviceID,
		}
	}
	alice.MustSendToDeviceMessages(t, "m.room_key_request", map[string]map[string]map[string]interface{}{
		ghostID: calibration,
	})
	calibrationTerminal := map[string]interface{}{"complemau_case": "calibration terminal"}
	alice.MustSendToDeviceMessages(t, "m.room_key_request", complemauToDeviceMessages(
		ghostID, "COMPLEMAU_BURST_000", calibrationTerminal,
	))
	releaseFirst()
	releaseSecond()
	firstDeclaredCap := assertComplemauToDeviceBurst(
		t,
		firstTransactions,
		alice.UserID,
		ghostID,
		calibration,
		"calibration terminal",
		0,
	)
	secondDeclaredCap := assertComplemauToDeviceBurst(
		t,
		secondTransactions,
		alice.UserID,
		ghostID,
		calibration,
		"calibration terminal",
		0,
	)
	calibrationFence := map[string]interface{}{"complemau_case": "calibration fence"}
	alice.MustSendToDeviceMessages(t, "m.room_key_request", complemauToDeviceMessages(
		ghostID, "COMPLEMAU_BURST_000", calibrationFence,
	))
	mustReceiveComplemauToDeviceTransactionMarker(
		t, firstTransactions, "calibration fence",
	)
	mustReceiveComplemauToDeviceTransactionMarker(
		t, secondTransactions, "calibration fence",
	)

	alice.MustSendToDeviceMessages(t, "m.room_key_request", map[string]map[string]map[string]interface{}{
		ghostID: devices,
	})
	terminal := map[string]interface{}{"complemau_case": "burst terminal"}
	alice.MustSendToDeviceMessages(t, "m.room_key_request", complemauToDeviceMessages(
		ghostID, "COMPLEMAU_BURST_000", terminal,
	))

	firstCap := assertComplemauToDeviceBurst(
		t,
		firstTransactions,
		alice.UserID,
		ghostID,
		devices,
		"burst terminal",
		firstDeclaredCap,
	)
	secondCap := assertComplemauToDeviceBurst(
		t,
		secondTransactions,
		alice.UserID,
		ghostID,
		devices,
		"burst terminal",
		secondDeclaredCap,
	)
	t.Logf(
		"complemau: calibrated to-device caps %d and %d; observed burst sizes %d and %d",
		firstDeclaredCap,
		secondDeclaredCap,
		firstCap,
		secondCap,
	)
}

func TestComplemauToDeviceBurstExcludesUninterestedAppservice(t *testing.T) {
	runtime.SkipIf(t, runtime.Dendrite)

	blueprint := newComplemauToDeviceIsolationBlueprint()
	deployment := complement.OldDeploy(t, blueprint)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	bridgeUser := deployment.AppServiceUser(t, "hs1", b.ComplemauSenderID)
	interested, uninterested := startComplemauBridges(t, bridgeUser.BaseURL)
	defer interested.stop()
	defer uninterested.stop()

	const (
		burstCount     = 150
		ghostID        = "@complemau_to_device_isolation:hs1"
		probeGhostID   = "@unrelated_to_device_probe:hs1"
		probeDeviceID  = "UNRELATED_PROBE"
		terminalDevice = "COMPLEMAU_ISOLATION_000"
	)
	devices := make(map[string]map[string]interface{}, burstCount)
	for sequence := range burstCount {
		deviceID := fmt.Sprintf("COMPLEMAU_ISOLATION_%03d", sequence)
		interested.mustCreateGhostDevice(t, ghostID, deviceID, "Complemau isolation device")
		devices[deviceID] = map[string]interface{}{
			"complemau_case": fmt.Sprintf("isolation burst %03d", sequence),
		}
	}
	uninterested.mustCreateGhostDevice(t, probeGhostID, probeDeviceID, "Complemau isolation probe")

	transactions, stop := startComplemauTransactionDrain(t, interested, burstCount*4)
	defer stop()
	alice.MustSendToDeviceMessages(t, "m.room_key_request", map[string]map[string]map[string]interface{}{
		ghostID: devices,
	})
	terminal := map[string]interface{}{"complemau_case": "isolation terminal"}
	alice.MustSendToDeviceMessages(t, "m.room_key_request", complemauToDeviceMessages(
		ghostID, terminalDevice, terminal,
	))
	assertComplemauToDeviceBurst(
		t, transactions, alice.UserID, ghostID, devices, "isolation terminal", 0,
	)

	probe := map[string]interface{}{"complemau_case": "uninterested probe"}
	alice.MustSendToDeviceMessages(t, "m.room_key_request", complemauToDeviceMessages(
		probeGhostID, probeDeviceID, probe,
	))
	deadline := time.Now().Add(complemauToDeviceTimeout)
	for {
		got := uninterested.mustReceiveToDevice(t, time.Until(deadline))
		marker, _ := got.Content.Raw["complemau_case"].(string)
		if marker != "uninterested probe" {
			t.Fatalf("complemau: uninterested appservice received burst entry %q", marker)
		}
		assertComplemauToDeviceEvent(
			t, got, "m.room_key_request", alice.UserID, probeGhostID, probeDeviceID, probe,
		)
		break
	}
}

func TestComplemauToDeviceManyRecipients(t *testing.T) {
	runtime.SkipIf(t, runtime.Dendrite)

	blueprint := newComplemauToDeviceBlueprint()
	deployment := complement.OldDeploy(t, blueprint)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	bridgeUser := deployment.AppServiceUser(t, "hs1", b.ComplemauSenderID)
	registration := blueprint.Homeservers[0].ApplicationServices[0]
	bridge := startComplemauBridgeWithRegistration(
		t, bridgeUser.BaseURL, registration, b.ComplemauASPort,
	)
	defer bridge.stop()

	recipients := []struct {
		userID   string
		deviceID string
	}{
		{userID: "@complemau_many_one:hs1", deviceID: "COMPLEMAU_MANY_ONE"},
		{userID: "@complemau_many_two:hs1", deviceID: "COMPLEMAU_MANY_TWO"},
		{userID: "@complemau_many_three:hs1", deviceID: "COMPLEMAU_MANY_THREE"},
	}
	messages := make(map[string]map[string]map[string]interface{}, len(recipients))
	expected := make(map[string]map[string]interface{}, len(recipients))
	for position, recipient := range recipients {
		bridge.mustCreateGhostDevice(t, recipient.userID, recipient.deviceID, "Complemau recipient")
		content := map[string]interface{}{
			"complemau_case": fmt.Sprintf("recipient %d", position),
		}
		messages[recipient.userID] = map[string]map[string]interface{}{
			recipient.deviceID: content,
		}
		expected[complemauToDeviceRecipientKey(recipient.userID, recipient.deviceID)] = content
	}

	alice.MustSendToDeviceMessages(t, "m.room_key_request", messages)
	terminal := map[string]interface{}{"complemau_case": "many recipients terminal"}
	alice.MustSendToDeviceMessages(t, "m.room_key_request", complemauToDeviceMessages(
		recipients[0].userID, recipients[0].deviceID, terminal,
	))
	seen := make(map[string]struct{}, len(expected))
	for {
		got := bridge.mustReceiveToDevice(t, complemauToDeviceTimeout)
		marker, _ := got.Content.Raw["complemau_case"].(string)
		if marker == "many recipients terminal" {
			break
		}
		key := complemauToDeviceRecipientKey(string(got.ToUserID), string(got.ToDeviceID))
		content, ok := expected[key]
		if !ok {
			t.Fatalf(
				"complemau: unexpected to-device recipient %s on device %s",
				got.ToUserID,
				got.ToDeviceID,
			)
		}
		if _, duplicate := seen[key]; duplicate {
			t.Fatalf("complemau: duplicate to-device recipient %s", key)
		}
		seen[key] = struct{}{}
		assertComplemauToDeviceEvent(
			t,
			got,
			"m.room_key_request",
			alice.UserID,
			string(got.ToUserID),
			string(got.ToDeviceID),
			content,
		)
	}
	if len(seen) != len(expected) {
		t.Fatalf("complemau: received %d of %d to-device recipients", len(seen), len(expected))
	}
}

func newComplemauToDeviceBlueprint() b.Blueprint {
	blueprint := b.BlueprintHSWithTwoComplemauBridges
	blueprint.Name = "hs_with_complemau_to_device_bridge"
	blueprint.Homeservers = append([]b.Homeserver(nil), blueprint.Homeservers...)
	services := blueprint.Homeservers[0].ApplicationServices
	blueprint.Homeservers[0].ApplicationServices = append([]b.ApplicationService(nil), services[:1]...)
	return blueprint
}

func newComplemauToDeviceIsolationBlueprint() b.Blueprint {
	blueprint := b.BlueprintHSWithTwoComplemauBridges
	blueprint.Name = "hs_with_complemau_to_device_isolation"
	blueprint.Homeservers = append([]b.Homeserver(nil), blueprint.Homeservers...)
	services := append(
		[]b.ApplicationService(nil),
		blueprint.Homeservers[0].ApplicationServices...,
	)
	services[1].Namespaces = &b.ApplicationServiceNamespaces{
		Users: []b.ApplicationServiceNamespace{
			{Regex: `^@unrelated_.*:hs1$`, Exclusive: false},
		},
	}
	blueprint.Homeservers[0].ApplicationServices = services
	return blueprint
}

func complemauToDeviceMessages(
	userID string,
	deviceID string,
	content map[string]interface{},
) map[string]map[string]map[string]interface{} {
	return map[string]map[string]map[string]interface{}{
		userID: {
			deviceID: content,
		},
	}
}

func assertComplemauToDeviceEvent(
	t *testing.T,
	got *event.Event,
	wantType string,
	wantSender string,
	wantUserID string,
	wantDeviceID string,
	wantContent map[string]interface{},
) {
	t.Helper()
	if got.Type.Type != wantType {
		t.Errorf("complemau: to-device type = %s, want %s", got.Type.Type, wantType)
	}
	if string(got.Sender) != wantSender {
		t.Errorf("complemau: to-device sender = %s, want %s", got.Sender, wantSender)
	}
	if string(got.ToUserID) != wantUserID {
		t.Errorf("complemau: to-device user = %s, want %s", got.ToUserID, wantUserID)
	}
	if string(got.ToDeviceID) != wantDeviceID {
		t.Errorf("complemau: to-device device = %s, want %s", got.ToDeviceID, wantDeviceID)
	}
	if !reflect.DeepEqual(got.Content.Raw, wantContent) {
		t.Errorf("complemau: to-device content = %#v, want %#v", got.Content.Raw, wantContent)
	}
}

func mustReceiveComplemauToDeviceMarkerWithout(
	t *testing.T,
	bridge *complemauBridge,
	marker string,
	absent string,
) {
	t.Helper()
	deadline := time.Now().Add(complemauToDeviceTimeout)
	for {
		got := bridge.mustReceiveToDevice(t, time.Until(deadline))
		gotMarker, _ := got.Content.Raw["complemau_case"].(string)
		switch gotMarker {
		case absent:
			t.Errorf("complemau: uninterested to-device message %q arrived before marker %q", absent, marker)
		case marker:
			return
		}
	}
}

func startComplemauTransactionDrain(
	t *testing.T,
	bridge *complemauBridge,
	capacity int,
) (<-chan *complemauTransaction, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(bridge.ctx)
	transactions := make(chan *complemauTransaction, capacity)
	go func() {
		defer close(transactions)
		for {
			transaction, ok := bridge.transactions.pop(ctx)
			if !ok {
				return
			}
			select {
			case transactions <- transaction:
			case <-ctx.Done():
				return
			}
		}
	}()
	return transactions, cancel
}

func assertComplemauToDeviceBurst(
	t *testing.T,
	transactions <-chan *complemauTransaction,
	wantSender string,
	wantUserID string,
	expected map[string]map[string]interface{},
	terminalMarker string,
	declaredCap int,
) int {
	t.Helper()
	seen := make(map[string]string, len(expected))
	seenTransactions := make(map[string]*complemauTransaction)
	observedCap := 0
	timer := time.NewTimer(complemauToDeviceTimeout)
	defer timer.Stop()
	for {
		select {
		case transaction, ok := <-transactions:
			if !ok {
				t.Fatalf("complemau: transaction drain closed after %d of %d to-device messages", len(seen), len(expected))
			}
			if previous, replay := seenTransactions[transaction.ID]; replay {
				if !reflect.DeepEqual(previous.Body, transaction.Body) {
					t.Fatalf("complemau: transaction %s changed while being retried", transaction.ID)
				}
				continue
			}
			seenTransactions[transaction.ID] = transaction
			if len(transaction.Body.ToDeviceEvents) != 0 {
				t.Fatalf("complemau: transaction %s used the stable to-device field", transaction.ID)
			}
			events := transaction.Body.MSC2409ToDeviceEvents
			if len(events) == 0 {
				continue
			}
			if declaredCap > 0 && len(events) > declaredCap {
				t.Fatalf(
					"complemau: transaction size %d exceeded declared cap %d",
					len(events),
					declaredCap,
				)
			}
			observedCap = max(observedCap, len(events))
			reachedTerminal := false
			for _, got := range events {
				marker, _ := got.Content.Raw["complemau_case"].(string)
				if marker == terminalMarker {
					reachedTerminal = true
					continue
				}
				deviceID := string(got.ToDeviceID)
				content, expectedDevice := expected[deviceID]
				if !expectedDevice {
					t.Fatalf(
						"complemau: unexpected to-device recipient %s on device %s",
						got.ToUserID,
						got.ToDeviceID,
					)
				}
				assertComplemauToDeviceEvent(
					t,
					got,
					"m.room_key_request",
					wantSender,
					wantUserID,
					deviceID,
					content,
				)
				if firstTransaction, duplicate := seen[deviceID]; duplicate {
					t.Fatalf(
						"complemau: to-device message for %s appeared in transactions %s and %s",
						deviceID,
						firstTransaction,
						transaction.ID,
					)
				}
				seen[deviceID] = transaction.ID
			}
			if reachedTerminal {
				if len(seen) != len(expected) {
					t.Fatalf(
						"complemau: received %d of %d to-device messages before terminal marker",
						len(seen),
						len(expected),
					)
				}
				return observedCap
			}
		case <-timer.C:
			t.Fatalf(
				"complemau: received %d of %d to-device messages before the deadline",
				len(seen),
				len(expected),
			)
		}
	}
}

func mustReceiveComplemauToDeviceTransactionMarker(
	t *testing.T,
	transactions <-chan *complemauTransaction,
	marker string,
) {
	t.Helper()
	timer := time.NewTimer(complemauToDeviceTimeout)
	defer timer.Stop()
	for {
		select {
		case transaction, ok := <-transactions:
			if !ok {
				t.Fatalf("complemau: transaction drain closed before marker %q", marker)
			}
			if len(transaction.Body.ToDeviceEvents) != 0 {
				t.Fatalf("complemau: transaction %s used the stable to-device field", transaction.ID)
			}
			for _, got := range transaction.Body.MSC2409ToDeviceEvents {
				gotMarker, _ := got.Content.Raw["complemau_case"].(string)
				if gotMarker == marker {
					return
				}
			}
		case <-timer.C:
			t.Fatalf("complemau: timed out waiting for transaction marker %q", marker)
		}
	}
}

func complemauToDeviceRecipientKey(userID string, deviceID string) string {
	return userID + "\x00" + deviceID
}
