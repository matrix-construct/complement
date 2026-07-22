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
	"time"

	"github.com/matrix-org/complement"
	"github.com/matrix-org/complement/b"
	"github.com/matrix-org/complement/client"
	"github.com/matrix-org/complement/helpers"
	"github.com/matrix-org/complement/runtime"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/tidwall/gjson"
	"maunium.net/go/mautrix/id"
)

const complemauE2EKeyTimeout = 10 * time.Second

var complemauE2EKeyFederationBlueprint = func() b.Blueprint {
	return newComplemauInterestBlueprint("hs_with_complemau_e2e_key_federation", true)
}()

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

func TestComplemauAppserviceServesOneTimeKeys(t *testing.T) {
	runtime.SkipIf(t, runtime.Dendrite)

	deployment := complement.OldDeploy(t, b.BlueprintHSWithComplemauBridge)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	bridgeUser := deployment.AppServiceUser(t, "hs1", b.ComplemauSenderID)
	bridge := startComplemauBridge(t, bridgeUser.BaseURL)
	defer bridge.stop()

	const (
		ghostUserID      = "@complemau_claim:hs1"
		fallbackDevice   = "LOCAL_FALLBACK"
		appserviceDevice = "AS_CLAIM"
	)
	bridge.mustCreateGhostDevice(t, ghostUserID, fallbackDevice, "Local fallback")
	bridge.mustCreateGhostDevice(t, ghostUserID, appserviceDevice, "Appservice claim")
	_, fallbackKey := uploadComplemauOTKMaterial(t, bridgeUser, ghostUserID, fallbackDevice, 0, true)
	uploadComplemauOTKMaterial(t, bridgeUser, ghostUserID, appserviceDevice, 0, true)

	asKeyID, asKey := generateComplemauOneTimeKey(t, bridgeUser, ghostUserID, appserviceDevice)
	asResponse := map[string]interface{}{
		ghostUserID: map[string]interface{}{
			appserviceDevice: map[string]interface{}{asKeyID: asKey},
		},
	}
	bridge.queueEndpointResponse(t, complemauKeyClaim, http.StatusOK, asResponse)
	response := alice.MustDo(
		t,
		http.MethodPost,
		[]string{"_matrix", "client", "v3", "keys", "claim"},
		client.WithJSONBody(t, map[string]interface{}{
			"one_time_keys": map[string]interface{}{
				ghostUserID: map[string]string{
					fallbackDevice:   "signed_curve25519",
					appserviceDevice: "signed_curve25519",
				},
			},
		}),
	)

	// Tuwunel currently never makes this MSC3983 request. Keep the receive
	// strict so the known failure turns green only when OTK proxying lands.
	request := bridge.mustReceiveEndpointRequest(t, complemauKeyClaim, complemauE2EKeyTimeout)
	assertComplemauE2EEndpointRequest(
		t,
		request,
		"/_matrix/app/unstable/org.matrix.msc3983/keys/claim",
		map[string]interface{}{
			ghostUserID: map[string]interface{}{
				fallbackDevice:   []string{"signed_curve25519"},
				appserviceDevice: []string{"signed_curve25519"},
			},
		},
		b.BlueprintHSWithComplemauBridge.Homeservers[0].ApplicationServices[0].HSToken,
	)

	body := client.ParseJSON(t, response)
	appserviceClaim := gjson.GetBytes(
		body,
		"one_time_keys."+client.GjsonEscape(ghostUserID)+"."+client.GjsonEscape(appserviceDevice),
	)
	assertComplemauJSONEqual(
		t,
		json.RawMessage(appserviceClaim.Raw),
		map[string]interface{}{asKeyID: asKey},
	)
	fallbackClaim := gjson.GetBytes(
		body,
		"one_time_keys."+client.GjsonEscape(ghostUserID)+"."+client.GjsonEscape(fallbackDevice),
	)
	assertComplemauJSONEqual(
		t,
		json.RawMessage(fallbackClaim.Raw),
		map[string]interface{}{"signed_curve25519:fallback": fallbackKey},
	)
	assertComplemauEmptyFailures(t, body)
}

func TestComplemauAppserviceServesDeviceKeys(t *testing.T) {
	runtime.SkipIf(t, runtime.Dendrite)

	deployment := complement.OldDeploy(t, b.BlueprintHSWithComplemauBridge)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	bridgeUser := deployment.AppServiceUser(t, "hs1", b.ComplemauSenderID)
	bridge := startComplemauBridge(t, bridgeUser.BaseURL)
	defer bridge.stop()

	const (
		ghostUserID      = "@complemau_key_query:hs1"
		localDevice      = "LOCAL_ONLY"
		overriddenDevice = "AS_OVERRIDE"
		appserviceDevice = "AS_ONLY"
	)
	bridge.mustCreateGhostDevice(t, ghostUserID, localDevice, "Local device")
	bridge.mustCreateGhostDevice(t, ghostUserID, overriddenDevice, "Overridden device")
	localKeys, _ := uploadComplemauOTKMaterial(t, bridgeUser, ghostUserID, localDevice, 0, false)
	uploadComplemauOTKMaterial(t, bridgeUser, ghostUserID, overriddenDevice, 0, false)

	overriddenKeys := generateComplemauDeviceKeys(t, bridgeUser, ghostUserID, overriddenDevice)
	appserviceKeys := generateComplemauDeviceKeys(t, bridgeUser, ghostUserID, appserviceDevice)
	asResponse := map[string]interface{}{
		"device_keys": map[string]interface{}{
			ghostUserID: map[string]interface{}{
				overriddenDevice: overriddenKeys,
				appserviceDevice: appserviceKeys,
			},
		},
	}
	bridge.queueEndpointResponse(t, complemauKeyQuery, http.StatusOK, asResponse)
	response := alice.MustDo(
		t,
		http.MethodPost,
		[]string{"_matrix", "client", "v3", "keys", "query"},
		client.WithJSONBody(t, map[string]interface{}{
			"device_keys": map[string]interface{}{ghostUserID: []string{}},
		}),
	)

	// Tuwunel currently never makes this MSC3984 request. This receive is the
	// deliberate known failure until appservice device-key queries are wired.
	request := bridge.mustReceiveEndpointRequest(t, complemauKeyQuery, complemauE2EKeyTimeout)
	assertComplemauE2EEndpointRequest(
		t,
		request,
		"/_matrix/app/unstable/org.matrix.msc3984/keys/query",
		map[string]interface{}{ghostUserID: []string{}},
		b.BlueprintHSWithComplemauBridge.Homeservers[0].ApplicationServices[0].HSToken,
	)

	body := client.ParseJSON(t, response)
	devices := gjson.GetBytes(body, "device_keys."+client.GjsonEscape(ghostUserID))
	if !devices.IsObject() || len(devices.Map()) != 3 {
		t.Fatalf("complemau: merged device keys were %s, want exactly three devices", devices.Raw)
	}
	assertComplemauDeviceKeyEqual(
		t,
		json.RawMessage(devices.Get(client.GjsonEscape(localDevice)).Raw),
		localKeys,
	)
	assertComplemauJSONEqual(
		t,
		json.RawMessage(devices.Get(client.GjsonEscape(overriddenDevice)).Raw),
		overriddenKeys,
	)
	assertComplemauJSONEqual(
		t,
		json.RawMessage(devices.Get(client.GjsonEscape(appserviceDevice)).Raw),
		appserviceKeys,
	)
	assertComplemauEmptyFailures(t, body)
}

func TestComplemauAppserviceServesDeviceKeysOverFederation(t *testing.T) {
	runtime.SkipIf(t, runtime.Dendrite)

	deployment := complement.OldDeploy(t, complemauE2EKeyFederationBlueprint)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	remote := deployment.Register(t, "hs2", helpers.RegistrationOpts{})
	bridgeUser := deployment.AppServiceUser(t, "hs1", b.ComplemauSenderID)
	registration := complemauE2EKeyFederationBlueprint.Homeservers[0].ApplicationServices[0]
	bridge := startComplemauBridgeWithRegistration(
		t,
		bridgeUser.BaseURL,
		registration,
		b.ComplemauASPort,
	)
	defer bridge.stop()

	const (
		ghostLocalpart   = "as_e2e_federated"
		ghostUserID      = "@as_e2e_federated:hs1"
		localDevice      = "FED_LOCAL"
		appserviceDevice = "FED_AS_ONLY"
	)
	registerAppserviceGhost(t, bridgeUser, ghostLocalpart)
	roomID := alice.MustCreateRoom(t, map[string]interface{}{
		"preset": "public_chat",
		"initial_state": []map[string]interface{}{
			{
				"type":      "m.room.encryption",
				"state_key": "",
				"content":   map[string]interface{}{"algorithm": "m.megolm.v1.aes-sha2"},
			},
		},
	})
	alice.MustInviteRoom(t, roomID, ghostUserID)
	joinComplemauInterestUser(t, bridgeUser, roomID, ghostUserID)
	remote.MustJoinRoom(t, roomID, []spec.ServerName{
		deployment.GetFullyQualifiedHomeserverName(t, "hs1"),
	})
	_, since := remote.MustSync(t, client.SyncReq{})

	overriddenKeys := generateComplemauDeviceKeys(t, bridgeUser, ghostUserID, localDevice)
	appserviceKeys := generateComplemauDeviceKeys(t, bridgeUser, ghostUserID, appserviceDevice)
	asResponse := map[string]interface{}{
		"device_keys": map[string]interface{}{
			ghostUserID: map[string]interface{}{
				localDevice:      overriddenKeys,
				appserviceDevice: appserviceKeys,
			},
		},
	}
	for range 4 {
		bridge.queueEndpointResponse(t, complemauKeyQuery, http.StatusOK, asResponse)
	}
	bridge.mustCreateGhostDevice(t, ghostUserID, localDevice, "Federated local device")
	uploadComplemauOTKMaterial(t, bridgeUser, ghostUserID, localDevice, 0, false)

	remote.MustSyncUntil(t, client.SyncReq{Since: since}, func(_ string, sync gjson.Result) error {
		for _, userID := range sync.Get("device_lists.changed").Array() {
			if userID.Str == ghostUserID {
				return nil
			}
		}
		return fmt.Errorf("device list for %s did not change", ghostUserID)
	})

	response := remote.MustDo(
		t,
		http.MethodPost,
		[]string{"_matrix", "client", "v3", "keys", "query"},
		client.WithJSONBody(t, map[string]interface{}{
			"device_keys": map[string]interface{}{ghostUserID: []string{}},
		}),
	)
	// The remote client query reaches hs1 over federation before hs1 proxies to
	// the appservice.
	request := bridge.mustReceiveEndpointRequest(t, complemauKeyQuery, complemauE2EKeyTimeout)
	assertComplemauE2EEndpointRequest(
		t,
		request,
		"/_matrix/app/unstable/org.matrix.msc3984/keys/query",
		map[string]interface{}{ghostUserID: []string{}},
		registration.HSToken,
	)
	body := client.ParseJSON(t, response)
	devices := gjson.GetBytes(body, "device_keys."+client.GjsonEscape(ghostUserID))
	if !devices.IsObject() || len(devices.Map()) != 2 {
		t.Fatalf("complemau: federated device keys were %s, want exactly two devices", devices.Raw)
	}
	assertComplemauJSONEqual(
		t,
		json.RawMessage(devices.Get(client.GjsonEscape(localDevice)+".keys").Raw),
		overriddenKeys["keys"],
	)
	assertComplemauJSONEqual(
		t,
		json.RawMessage(devices.Get(client.GjsonEscape(appserviceDevice)+".keys").Raw),
		appserviceKeys["keys"],
	)
	assertComplemauEmptyFailures(t, body)
}

func uploadComplemauOTKMaterial(
	t *testing.T,
	bridge *client.CSAPI,
	userID string,
	deviceID string,
	count int,
	includeFallback bool,
) (map[string]interface{}, map[string]interface{}) {
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
	var fallbackKey map[string]interface{}
	if includeFallback {
		generatedKeyID := fmt.Sprintf("signed_curve25519:%d", count)
		fallbackKey, ok = oneTimeKeys[generatedKeyID].(map[string]interface{})
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
	return deviceKeys, fallbackKey
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

func generateComplemauOneTimeKey(
	t *testing.T,
	base *client.CSAPI,
	userID string,
	deviceID string,
) (string, map[string]interface{}) {
	t.Helper()
	device := *base
	device.UserID = userID
	device.DeviceID = deviceID
	_, oneTimeKeys := device.MustGenerateOneTimeKeys(t, 1)
	for keyID, value := range oneTimeKeys {
		key, ok := value.(map[string]interface{})
		if !ok {
			t.Fatalf("complemau: generated one-time key %s had an unexpected shape", keyID)
		}
		return keyID, key
	}
	t.Fatal("complemau: one-time-key generator returned no keys")
	return "", nil
}

func generateComplemauDeviceKeys(
	t *testing.T,
	base *client.CSAPI,
	userID string,
	deviceID string,
) map[string]interface{} {
	t.Helper()
	device := *base
	device.UserID = userID
	device.DeviceID = deviceID
	deviceKeys, _ := device.MustGenerateOneTimeKeys(t, 0)
	return deviceKeys
}

func assertComplemauE2EEndpointRequest(
	t *testing.T,
	request *complemauEndpointRequest,
	wantPath string,
	wantBody any,
	hsToken string,
) {
	t.Helper()
	if request.Method != http.MethodPost {
		t.Errorf("complemau: appservice E2E request method = %s, want POST", request.Method)
	}
	if request.Path != wantPath {
		t.Errorf("complemau: appservice E2E request path = %s, want %s", request.Path, wantPath)
	}
	// Tuwunel sends the legacy query token as well as the Bearer header.
	accessTokens, hasAccessToken := request.Query["access_token"]
	if len(request.Query) != 1 || !hasAccessToken || len(accessTokens) != 1 || accessTokens[0] != hsToken {
		t.Errorf("complemau: appservice E2E query = %v, want only access_token with the HS token", request.Query)
	}
	wantAuthorization := "Bearer " + hsToken
	authorization := request.Header.Values("Authorization")
	if len(authorization) != 1 || authorization[0] != wantAuthorization {
		t.Errorf("complemau: appservice E2E authorization = %v, want [%s]", authorization, wantAuthorization)
	}
	assertComplemauJSONEqual(t, request.Body, wantBody)
}

func assertComplemauJSONEqual(t *testing.T, got json.RawMessage, want any) {
	t.Helper()
	wantJSON, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("complemau: marshal expected JSON: %s", err)
	}
	gotCanonical, err := gomatrixserverlib.CanonicalJSON(got)
	if err != nil {
		t.Fatalf("complemau: canonicalize actual JSON %q: %s", got, err)
	}
	wantCanonical, err := gomatrixserverlib.CanonicalJSON(wantJSON)
	if err != nil {
		t.Fatalf("complemau: canonicalize expected JSON %q: %s", wantJSON, err)
	}
	if string(gotCanonical) != string(wantCanonical) {
		t.Errorf("complemau: JSON = %s, want %s", gotCanonical, wantCanonical)
	}
}

func assertComplemauDeviceKeyEqual(
	t *testing.T,
	got json.RawMessage,
	want map[string]interface{},
) {
	t.Helper()
	var gotDeviceKey map[string]interface{}
	if err := json.Unmarshal(got, &gotDeviceKey); err != nil {
		t.Fatalf("complemau: decode returned device key %q: %s", got, err)
	}
	delete(gotDeviceKey, "unsigned")
	gotWithoutUnsigned, err := json.Marshal(gotDeviceKey)
	if err != nil {
		t.Fatalf("complemau: encode returned device key: %s", err)
	}
	assertComplemauJSONEqual(t, gotWithoutUnsigned, want)
}

func assertComplemauEmptyFailures(t *testing.T, body []byte) {
	t.Helper()
	failures := gjson.GetBytes(body, "failures")
	if !failures.IsObject() || len(failures.Map()) != 0 {
		t.Errorf("complemau: key response failures = %s, want an empty object", failures.Raw)
	}
}
