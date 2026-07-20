//go:build complemau

package complemau_tests

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/matrix-org/complement"
	"github.com/matrix-org/complement/b"
	"github.com/matrix-org/complement/client"
	"github.com/matrix-org/complement/helpers"
	"github.com/matrix-org/complement/runtime"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/tidwall/gjson"
	"maunium.net/go/mautrix/id"
)

func TestComplemauAppserviceReceivesOTKCountsAndFallbackKeys(t *testing.T) {
	runtime.SkipIf(t, runtime.Dendrite)

	deployment := complement.OldDeploy(t, b.BlueprintHSWithTwoComplemauBridges)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	bridgeUser := deployment.AppServiceUser(t, "hs1", b.ComplemauSenderID)
	optedIn, optedOut := startComplemauBridges(t, bridgeUser.BaseURL)
	defer optedIn.stop()
	defer optedOut.stop()

	const (
		ghostUserID  = "@complemau_otk:hs1"
		unusedDevice = "UNUSED"
		usedDevice   = "USED"
		initialCount = 3
	)
	optedIn.mustCreateGhostDevice(t, ghostUserID, unusedDevice, "Unused fallback")
	optedIn.mustCreateGhostDevice(t, ghostUserID, usedDevice, "Used fallback")
	uploadComplemauOTKMaterial(t, bridgeUser, ghostUserID, unusedDevice, initialCount, true)
	uploadComplemauOTKMaterial(t, bridgeUser, ghostUserID, usedDevice, 0, true)

	deviceKeys, unrelatedOTKs := alice.MustGenerateOneTimeKeys(t, 5)
	alice.MustUploadKeys(t, deviceKeys, unrelatedOTKs)
	claimComplemauOTK(t, alice, ghostUserID, usedDevice)

	roomID := alice.MustCreateRoom(t, map[string]interface{}{"preset": "public_chat"})
	alice.MustSyncUntil(t, client.SyncReq{}, client.SyncJoinedTo(alice.UserID, roomID))
	joinComplemauInterestUser(t, bridgeUser, roomID, ghostUserID)

	markerEventID := sendComplemauInterestMessage(
		t,
		bridgeUser,
		roomID,
		ghostUserID,
		"MSC3202 count marker",
		"msc3202_count_marker",
	)
	initial := mustReceiveComplemauTxnWithEvent(t, optedIn, markerEventID, complemauTxnTimeout)
	assertComplemauMSC3202Transaction(
		t,
		initial,
		ghostUserID,
		unusedDevice,
		usedDevice,
		alice.UserID,
		initialCount,
	)

	// The second service shares the user namespace but did not opt in to MSC3202.
	// Receiving this marker transaction proves the extension fields were absent.
	unflagged := mustReceiveComplemauTxnWithEvent(t, optedOut, markerEventID, complemauTxnTimeout)
	assertComplemauMSC3202Absent(t, unflagged)

	claimComplemauOTK(t, alice, ghostUserID, unusedDevice)
	decrementMarkerID := sendComplemauInterestMessage(
		t,
		bridgeUser,
		roomID,
		ghostUserID,
		"MSC3202 decrement marker",
		"msc3202_decrement_marker",
	)
	decremented := mustReceiveComplemauTxnWithEvent(t, optedIn, decrementMarkerID, complemauTxnTimeout)
	assertComplemauMSC3202Transaction(
		t,
		decremented,
		ghostUserID,
		unusedDevice,
		usedDevice,
		alice.UserID,
		initialCount-1,
	)
}

func uploadComplemauOTKMaterial(
	t *testing.T,
	bridge *client.CSAPI,
	userID string,
	deviceID string,
	count int,
	includeFallback bool,
) {
	t.Helper()
	generatedCount := count
	if includeFallback {
		generatedCount++
	}
	device := *bridge
	device.UserID = userID
	device.DeviceID = deviceID
	deviceKeys, oneTimeKeys := device.MustGenerateOneTimeKeys(t, uint(generatedCount))
	ed25519Public, ed25519Private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("complemau: generate signing key: %s", err)
	}
	ed25519KeyID := fmt.Sprintf("ed25519:%s", deviceID)
	deviceKeyMap, ok := deviceKeys["keys"].(map[string]interface{})
	if !ok {
		t.Fatal("complemau: generated device keys had an unexpected shape")
	}
	deviceKeyMap[ed25519KeyID] = base64.RawStdEncoding.EncodeToString(ed25519Public)
	signComplemauKey(t, deviceKeys, userID, ed25519KeyID, ed25519Private)
	body := map[string]interface{}{
		"device_keys":   deviceKeys,
		"one_time_keys": oneTimeKeys,
	}
	if includeFallback {
		generatedKeyID := fmt.Sprintf("signed_curve25519:%d", count)
		fallbackKey, ok := oneTimeKeys[generatedKeyID].(map[string]interface{})
		if !ok {
			t.Fatalf("complemau: generated fallback key %s had an unexpected shape", generatedKeyID)
		}
		delete(oneTimeKeys, generatedKeyID)
		fallbackKey["fallback"] = true
		signComplemauKey(t, fallbackKey, userID, ed25519KeyID, ed25519Private)
		body["fallback_keys"] = map[string]interface{}{
			"signed_curve25519:fallback": fallbackKey,
		}
	}
	for keyID, value := range oneTimeKeys {
		key, ok := value.(map[string]interface{})
		if !ok {
			t.Fatalf("complemau: generated one-time key %s had an unexpected shape", keyID)
		}
		signComplemauKey(t, key, userID, ed25519KeyID, ed25519Private)
	}
	bridge.MustDo(
		t,
		http.MethodPost,
		[]string{"_matrix", "client", "v3", "keys", "upload"},
		client.WithJSONBody(t, body),
		asUserDevice(userID, deviceID),
	)
}

func signComplemauKey(
	t *testing.T,
	key map[string]interface{},
	userID string,
	keyID string,
	privateKey ed25519.PrivateKey,
) {
	t.Helper()
	delete(key, "signatures")
	encoded, err := json.Marshal(key)
	if err != nil {
		t.Fatalf("complemau: marshal key for signing: %s", err)
	}
	canonical, err := gomatrixserverlib.CanonicalJSON(encoded)
	if err != nil {
		t.Fatalf("complemau: canonicalize key for signing: %s", err)
	}
	signature := ed25519.Sign(privateKey, canonical)
	key["signatures"] = map[string]interface{}{
		userID: map[string]interface{}{
			keyID: base64.RawStdEncoding.EncodeToString(signature),
		},
	}
}

func claimComplemauOTK(t *testing.T, claimer *client.CSAPI, userID, deviceID string) {
	t.Helper()
	response := claimer.MustDo(
		t,
		http.MethodPost,
		[]string{"_matrix", "client", "v3", "keys", "claim"},
		client.WithJSONBody(t, map[string]interface{}{
			"one_time_keys": map[string]interface{}{
				userID: map[string]string{deviceID: "signed_curve25519"},
			},
		}),
	)
	keys := gjson.GetBytes(client.ParseJSON(t, response),
		"one_time_keys."+client.GjsonEscape(userID)+"."+client.GjsonEscape(deviceID),
	)
	if !keys.IsObject() || len(keys.Map()) != 1 {
		t.Fatalf("complemau: expected one claimed key for %s %s, got %s", userID, deviceID, keys.Raw)
	}
}

func assertComplemauMSC3202Transaction(
	t *testing.T,
	transaction *complemauTransaction,
	ghostUserID string,
	unusedDevice string,
	usedDevice string,
	unrelatedUserID string,
	wantCount int,
) {
	t.Helper()
	if transaction.Body.DeviceOTKCount != nil || transaction.Body.FallbackKeys != nil {
		t.Fatal("complemau: MSC3202 data appeared under stable transaction fields")
	}
	counts := transaction.Body.MSC3202DeviceOTKCount
	if counts == nil {
		t.Fatal("complemau: missing org.matrix.msc3202.device_one_time_keys_count")
	}
	if _, leaked := counts[id.UserID(unrelatedUserID)]; leaked {
		t.Fatalf("complemau: OTK counts leaked for unrelated user %s", unrelatedUserID)
	}
	ghostCounts, ok := counts[id.UserID(ghostUserID)]
	if !ok {
		t.Fatalf("complemau: missing OTK counts for %s", ghostUserID)
	}
	count, ok := ghostCounts[id.DeviceID(unusedDevice)]
	if !ok || count.SignedCurve25519 != wantCount {
		t.Fatalf(
			"complemau: OTK count for %s %s was %d, want %d",
			ghostUserID,
			unusedDevice,
			count.SignedCurve25519,
			wantCount,
		)
	}

	fallbackKeys := transaction.Body.MSC3202FallbackKeys
	if fallbackKeys == nil {
		t.Fatal("complemau: missing org.matrix.msc3202.device_unused_fallback_key_types")
	}
	if _, leaked := fallbackKeys[id.UserID(unrelatedUserID)]; leaked {
		t.Fatalf("complemau: fallback-key types leaked for unrelated user %s", unrelatedUserID)
	}
	ghostFallbacks, ok := fallbackKeys[id.UserID(ghostUserID)]
	if !ok {
		t.Fatalf("complemau: missing fallback-key types for %s", ghostUserID)
	}
	unused, ok := ghostFallbacks[id.DeviceID(unusedDevice)]
	if !ok || len(unused) != 1 || unused[0] != id.KeyAlgorithmSignedCurve25519 {
		t.Fatalf("complemau: unused fallback-key types for %s were %v", unusedDevice, unused)
	}
	used, ok := ghostFallbacks[id.DeviceID(usedDevice)]
	if !ok || len(used) != 0 {
		t.Fatalf("complemau: used fallback key for %s was still reported as unused: %v", usedDevice, used)
	}
}

func assertComplemauMSC3202Absent(t *testing.T, transaction *complemauTransaction) {
	t.Helper()
	if transaction.Body.DeviceOTKCount != nil ||
		transaction.Body.FallbackKeys != nil ||
		transaction.Body.MSC3202DeviceOTKCount != nil ||
		transaction.Body.MSC3202FallbackKeys != nil {
		t.Fatal("complemau: MSC3202 OTK data leaked to an appservice that did not opt in")
	}
}
