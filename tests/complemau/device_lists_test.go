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
)

const complemauDeviceListTimeout = 20 * time.Second

var complemauDeviceListFederationBlueprint = func() b.Blueprint {
	blueprint := newComplemauInterestBlueprint(
		"hs_with_complemau_device_list_federation",
		true,
	)
	blueprint.Homeservers[0].ApplicationServices[0].EnableEncryption = true
	return blueprint
}()

func TestComplemauDeviceListsRegistrationGateAndDeviceChanges(t *testing.T) {
	// tuwunel and Synapse read the Complement appservice registration file;
	// Dendrite does not yet. https://github.com/matrix-org/complement/issues/514
	runtime.SkipIf(t, runtime.Dendrite)

	deployment := complement.OldDeploy(t, b.BlueprintHSWithTwoComplemauBridges)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	bridgeClient := deployment.AppServiceUser(t, "hs1", b.ComplemauSenderID)
	enabled, disabled := startComplemauBridges(t, bridgeClient.BaseURL)
	defer enabled.stop()
	defer disabled.stop()

	const ghostID = "@complemau_device_lists:hs1"
	const deviceID = "COMPLEMAU_DEVICE_LISTS"

	enabled.mustCreateGhostDevice(t, ghostID, deviceID, "Complemau device lists")
	uploadComplemauOTKMaterial(t, bridgeClient, ghostID, deviceID, 0, false)
	markerID := sendComplemauDeviceListMarker(t, alice, ghostID)
	mustReachComplemauDeviceListMarkerWithUpdate(t, enabled, markerID, ghostID)
	mustReachComplemauDeviceListMarkerWithoutUpdate(t, disabled, markerID)

	bridgeClient.MustDo(t, "DELETE", []string{"_matrix", "client", "v3", "devices", deviceID},
		asUserDevice(ghostID, deviceID),
		client.WithJSONBody(t, map[string]interface{}{}),
	)
	markerID = sendComplemauDeviceListMarker(t, alice, ghostID)
	mustReachComplemauDeviceListMarkerWithUpdate(t, enabled, markerID, ghostID)
	mustReachComplemauDeviceListMarkerWithoutUpdate(t, disabled, markerID)
}

func TestComplemauDeviceListsRemoteRoomSharer(t *testing.T) {
	// tuwunel and Synapse read the Complement appservice registration file;
	// Dendrite does not yet. https://github.com/matrix-org/complement/issues/514
	runtime.SkipIf(t, runtime.Dendrite)

	deployment := complement.OldDeploy(t, complemauDeviceListFederationBlueprint)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	remote := deployment.Register(t, "hs2", helpers.RegistrationOpts{})
	bridgeClient := deployment.AppServiceUser(t, "hs1", b.ComplemauSenderID)
	registration := complemauDeviceListFederationBlueprint.Homeservers[0].ApplicationServices[0]
	bridge := startComplemauBridgeWithRegistration(
		t,
		bridgeClient.BaseURL,
		registration,
		b.ComplemauASPort,
	)
	defer bridge.stop()

	const ghostLocalpart = "as_device_list_federated"
	const ghostID = "@as_device_list_federated:hs1"
	registerAppserviceGhost(t, bridgeClient, ghostLocalpart)

	roomID := alice.MustCreateRoom(t, map[string]interface{}{
		"preset": "public_chat",
		"initial_state": []map[string]interface{}{
			{
				"type":      "m.room.encryption",
				"state_key": "",
				"content": map[string]interface{}{
					"algorithm": "m.megolm.v1.aes-sha2",
				},
			},
		},
	})
	alice.MustInviteRoom(t, roomID, ghostID)
	joinComplemauInterestUser(t, bridgeClient, roomID, ghostID)
	remote.MustJoinRoom(t, roomID, []spec.ServerName{
		deployment.GetFullyQualifiedHomeserverName(t, "hs1"),
	})

	setupMarkerID := sendComplemauInterestMessage(
		t,
		bridgeClient,
		roomID,
		ghostID,
		"device-list federation setup",
		"device_list_federation_setup",
	)
	mustReachComplemauDeviceListMarker(t, bridge, setupMarkerID)

	deviceKeys, oneTimeKeys := remote.MustGenerateOneTimeKeys(t, 1)
	remote.MustUploadKeys(t, deviceKeys, oneTimeKeys)
	markerID := remote.SendEventSynced(t, roomID, b.Event{
		Type: "m.room.message",
		Content: map[string]interface{}{
			"msgtype": "m.text",
			"body":    "remote device-list rekey marker",
		},
	})
	mustReachComplemauDeviceListMarkerWithUpdate(t, bridge, markerID, remote.UserID)
}

func mustReachComplemauDeviceListMarkerWithUpdate(
	t *testing.T,
	bridge *complemauBridge,
	markerID string,
	userID string,
) {
	t.Helper()
	deadline := time.Now().Add(complemauDeviceListTimeout)
	foundChange := false
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("complemau: appservice did not receive marker PDU %s", markerID)
		}
		transaction := bridge.mustReceiveTransaction(t, remaining)
		if transaction.Body.DeviceLists != nil {
			t.Fatalf("complemau: transaction %s used the stable device-list field", transaction.ID)
		}
		if deviceLists := transaction.Body.MSC3202DeviceLists; deviceLists != nil {
			if deviceLists.Left != nil {
				t.Fatalf("complemau: transaction %s included device_lists.left", transaction.ID)
			}
			if len(deviceLists.Changed) != 1 || string(deviceLists.Changed[0]) != userID {
				t.Fatalf(
					"complemau: transaction %s changed users = %v, want only %s",
					transaction.ID,
					deviceLists.Changed,
					userID,
				)
			}
			foundChange = true
		}
		if complemauTxnHasEvent(transaction, markerID) {
			if !foundChange {
				t.Fatalf("complemau: appservice reached marker %s without a device-list change for %s", markerID, userID)
			}
			return
		}
	}
}

func mustReachComplemauDeviceListMarker(
	t *testing.T,
	bridge *complemauBridge,
	markerID string,
) {
	t.Helper()
	deadline := time.Now().Add(complemauDeviceListTimeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("complemau: appservice did not receive marker PDU %s", markerID)
		}
		if complemauTxnHasEvent(bridge.mustReceiveTransaction(t, remaining), markerID) {
			return
		}
	}
}

func sendComplemauDeviceListMarker(t *testing.T, alice *client.CSAPI, ghostID string) string {
	t.Helper()
	roomID := alice.MustCreateRoom(t, map[string]interface{}{"preset": "public_chat"})
	return alice.SendEventSynced(t, roomID, b.Event{
		Type:     "m.room.member",
		StateKey: &ghostID,
		Content: map[string]interface{}{
			"membership": "invite",
		},
	})
}

func mustReachComplemauDeviceListMarkerWithoutUpdate(
	t *testing.T,
	bridge *complemauBridge,
	markerID string,
) {
	t.Helper()
	deadline := time.Now().Add(complemauDeviceListTimeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("complemau: appservice did not receive marker PDU %s", markerID)
		}
		transaction := bridge.mustReceiveTransaction(t, remaining)
		if transaction.Body.DeviceLists != nil || transaction.Body.MSC3202DeviceLists != nil {
			t.Fatalf(
				"complemau: flag-off appservice received device lists before marker %s",
				markerID,
			)
		}
		if complemauTxnHasEvent(transaction, markerID) {
			return
		}
	}
}
