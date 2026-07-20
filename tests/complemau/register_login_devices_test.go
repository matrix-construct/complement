//go:build complemau

package complemau_tests

import (
	"crypto/ed25519"
	"encoding/base64"
	"net/http"
	"testing"

	"github.com/matrix-org/complement"
	"github.com/matrix-org/complement/b"
	"github.com/matrix-org/complement/client"
	"github.com/matrix-org/complement/match"
	"github.com/matrix-org/complement/must"
	"github.com/matrix-org/complement/runtime"
	"github.com/tidwall/gjson"
)

const (
	complemauAppserviceLoginType        = "m.login.application_service"
	complemauAppserviceLoginUnsupported = "M_APPSERVICE_LOGIN_UNSUPPORTED"
)

var complemauRegisterLoginDevicesBlueprint = b.MustValidate(b.Blueprint{
	Name: "hs_with_complemau_register_login_devices",
	Homeservers: []b.Homeserver{
		{
			Name: "hs1",
			Users: []b.User{
				{
					Localpart:   "@alice",
					DisplayName: "Alice",
				},
			},
			ApplicationServices: []b.ApplicationService{
				{
					ID:              b.ComplemauASID,
					URL:             b.ComplemauASURL,
					SenderLocalpart: b.ComplemauSender,
					Namespaces: &b.ApplicationServiceNamespaces{
						Users: []b.ApplicationServiceNamespace{
							{Regex: "^@complemau_normal_.*:hs1$", Exclusive: true},
						},
					},
				},
				{
					ID:              b.ComplemauSecondASID,
					URL:             b.ComplemauSecondASURL,
					SenderLocalpart: b.ComplemauSecondSender,
					EnableMSC4190:   true,
					Namespaces: &b.ApplicationServiceNamespaces{
						Users: []b.ApplicationServiceNamespace{
							{Regex: "^@complemau_msc_.*:hs1$", Exclusive: true},
						},
					},
				},
			},
		},
	},
})

func TestComplemauAppserviceRegistrationLoginAndMSC4190Devices(t *testing.T) {
	// tuwunel and Synapse read the Complement appservice registration file;
	// Dendrite does not yet. https://github.com/matrix-org/complement/issues/514
	runtime.SkipIf(t, runtime.Dendrite)

	deployment := complement.OldDeploy(t, complemauRegisterLoginDevicesBlueprint)
	defer deployment.Destroy(t)

	normalClient := deployment.AppServiceUser(t, "hs1", b.ComplemauSenderID)
	mscClient := deployment.AppServiceUser(t, "hs1", b.ComplemauSecondSenderID)
	unauthenticatedClient := deployment.UnauthenticatedClient(t, "hs1")
	registrations := complemauRegisterLoginDevicesBlueprint.Homeservers[0].ApplicationServices
	normalBridge := startComplemauBridgeWithRegistration(
		t, normalClient.BaseURL, registrations[0], b.ComplemauASPort,
	)
	defer normalBridge.stop()
	mscBridge := startComplemauBridgeWithRegistration(
		t, mscClient.BaseURL, registrations[1], b.ComplemauSecondASPort,
	)
	defer mscBridge.stop()

	t.Run("register namespaced user without password", func(t *testing.T) {
		const localpart = "complemau_normal_registration"
		res := doComplemauAppserviceRegister(t, normalClient, localpart, false)
		must.MatchResponse(t, res, match.HTTPResponse{
			StatusCode: http.StatusOK,
			JSON: []match.JSON{
				match.JSONKeyEqual("user_id", "@"+localpart+":hs1"),
			},
		})
	})

	t.Run("missing registration type reaches normal registration exclusivity", func(t *testing.T) {
		const typedLocalpart = "complemau_normal_registration_typed"
		mustRegisterComplemauAppserviceUser(t, normalClient, typedLocalpart, false)

		// Synapse rejects every appservice-token registration without type. Tuwunel
		// treats this as normal registration, then rejects the reserved namespace.
		res := normalClient.Do(t, http.MethodPost,
			[]string{"_matrix", "client", "v3", "register"},
			client.WithJSONBody(t, map[string]interface{}{
				"username": "complemau_normal_registration_untyped",
			}),
		)
		must.MatchResponse(t, res, match.HTTPResponse{
			StatusCode: http.StatusBadRequest,
			JSON: []match.JSON{
				match.JSONKeyEqual("errcode", "M_EXCLUSIVE"),
			},
		})
	})

	t.Run("registration rejects an unknown appservice token", func(t *testing.T) {
		invalidTokenClient := deployment.UnauthenticatedClient(t, "hs1")
		invalidTokenClient.AccessToken = "complemau-invalid-appservice-token"
		res := invalidTokenClient.Do(t, http.MethodPost,
			[]string{"_matrix", "client", "v3", "register"},
			client.WithJSONBody(t, map[string]interface{}{
				"type":     complemauAppserviceLoginType,
				"username": "complemau_normal_invalid_token",
			}),
		)
		must.MatchResponse(t, res, match.HTTPResponse{
			StatusCode: http.StatusUnauthorized,
			JSON: []match.JSON{
				match.JSONKeyEqual("errcode", "M_UNKNOWN_TOKEN"),
			},
		})
	})

	t.Run("MSC4190 registration inhibits login", func(t *testing.T) {
		const localpart = "complemau_msc_registration"
		res := doComplemauAppserviceRegister(t, mscClient, localpart, true)
		must.MatchResponse(t, res, match.HTTPResponse{
			StatusCode: http.StatusOK,
			JSON: []match.JSON{
				match.JSONKeyEqual("user_id", "@"+localpart+":hs1"),
				match.JSONKeyMissing("access_token"),
			},
		})
	})

	t.Run("MSC4190 registration requires inhibit_login", func(t *testing.T) {
		res := doComplemauAppserviceRegister(
			t, mscClient, "complemau_msc_registration_without_inhibit", false,
		)
		// Synapse uses the longer IO.ELEMENT.MSC4190 error literal. Tuwunel
		// exposes the stable-form M_APPSERVICE_LOGIN_UNSUPPORTED spelling.
		must.MatchResponse(t, res, match.HTTPResponse{
			StatusCode: http.StatusBadRequest,
			JSON: []match.JSON{
				match.JSONKeyEqual("errcode", complemauAppserviceLoginUnsupported),
			},
		})
	})

	t.Run("login namespaced ghost", func(t *testing.T) {
		const localpart = "complemau_normal_login"
		mustRegisterComplemauAppserviceUser(t, normalClient, localpart, false)
		res := doComplemauAppserviceLogin(t, normalClient, localpart)
		must.MatchResponse(t, res, match.HTTPResponse{
			StatusCode: http.StatusOK,
			JSON: []match.JSON{
				match.JSONKeyEqual("user_id", "@"+localpart+":hs1"),
				match.JSONKeyTypeEqual("access_token", gjson.String),
				match.JSONKeyTypeEqual("device_id", gjson.String),
			},
		})
	})

	t.Run("MSC4190 appservice cannot use login", func(t *testing.T) {
		const localpart = "complemau_msc_login_rejected"
		mustRegisterComplemauAppserviceUser(t, mscClient, localpart, true)
		res := doComplemauAppserviceLogin(t, mscClient, localpart)
		must.MatchResponse(t, res, match.HTTPResponse{
			StatusCode: http.StatusBadRequest,
			JSON: []match.JSON{
				match.JSONKeyEqual("errcode", complemauAppserviceLoginUnsupported),
			},
		})
	})

	t.Run("login as appservice sender", func(t *testing.T) {
		res := doComplemauAppserviceLogin(t, normalClient, b.ComplemauSenderID)
		must.MatchResponse(t, res, match.HTTPResponse{
			StatusCode: http.StatusOK,
			JSON: []match.JSON{
				match.JSONKeyEqual("user_id", b.ComplemauSenderID),
				match.JSONKeyTypeEqual("access_token", gjson.String),
			},
		})
	})

	t.Run("login rejects never registered namespaced user", func(t *testing.T) {
		const localpart = "complemau_normal_login_orphan"
		// Synapse rejects this with 403. Tuwunel reaches device creation, which
		// rejects the nonexistent user with M_INVALID_PARAM and a 400 response.
		res := doComplemauAppserviceLogin(t, normalClient, localpart)
		must.MatchResponse(t, res, match.HTTPResponse{
			StatusCode: http.StatusBadRequest,
			JSON: []match.JSON{
				match.JSONKeyEqual("errcode", "M_INVALID_PARAM"),
			},
		})
	})

	t.Run("login rejects another appservice token", func(t *testing.T) {
		const localpart = "complemau_msc_wrong_appservice"
		mustRegisterComplemauAppserviceUser(t, mscClient, localpart, true)
		// Synapse returns 403. Tuwunel reports the target namespace mismatch as
		// M_EXCLUSIVE with a 400 response.
		res := doComplemauAppserviceLogin(t, normalClient, localpart)
		must.MatchResponse(t, res, match.HTTPResponse{
			StatusCode: http.StatusBadRequest,
			JSON: []match.JSON{
				match.JSONKeyEqual("errcode", "M_EXCLUSIVE"),
			},
		})
	})

	t.Run("appservice login requires token", func(t *testing.T) {
		const localpart = "complemau_normal_login_missing_token"
		mustRegisterComplemauAppserviceUser(t, normalClient, localpart, false)
		res := doComplemauAppserviceLogin(t, unauthenticatedClient, localpart)
		must.MatchResponse(t, res, match.HTTPResponse{
			StatusCode: http.StatusUnauthorized,
			JSON: []match.JSON{
				match.JSONKeyEqual("errcode", "M_MISSING_TOKEN"),
			},
		})
	})

	t.Run("MSC4190 creates and updates ghost device", func(t *testing.T) {
		const localpart = "complemau_msc_device_put"
		const userID = "@" + localpart + ":hs1"
		const deviceID = "AABBCCDD"
		mustRegisterComplemauAppserviceUser(t, mscClient, localpart, true)

		mustMatchComplemauDevices(t, mscClient, userID, 0, "")
		res := doComplemauPutGhostDevice(t, mscClient, userID, deviceID, "Alice's device")
		must.MatchResponse(t, res, match.HTTPResponse{StatusCode: http.StatusCreated})
		mustMatchComplemauDevices(t, mscClient, userID, 1, deviceID)

		res = doComplemauPutGhostDevice(t, mscClient, userID, deviceID, "Alice's device")
		must.MatchResponse(t, res, match.HTTPResponse{StatusCode: http.StatusOK})
	})

	t.Run("MSC4190 deletes ghost device without UIA", func(t *testing.T) {
		const localpart = "complemau_msc_device_delete"
		const userID = "@" + localpart + ":hs1"
		const deviceID = "AABBCCDD"
		mustRegisterComplemauAppserviceUser(t, mscClient, localpart, true)

		res := doComplemauPutGhostDevice(t, mscClient, userID, deviceID, "")
		must.MatchResponse(t, res, match.HTTPResponse{StatusCode: http.StatusCreated})
		mustMatchComplemauDevices(t, mscClient, userID, 1, deviceID)

		res = mscClient.Do(t, http.MethodDelete,
			[]string{"_matrix", "client", "v3", "devices", deviceID}, asUser(userID),
		)
		must.MatchResponse(t, res, match.HTTPResponse{StatusCode: http.StatusOK})
		mustMatchComplemauDevices(t, mscClient, userID, 0, "")
	})

	t.Run("MSC4190 bulk deletes ghost devices without UIA", func(t *testing.T) {
		const localpart = "complemau_msc_device_bulk_delete"
		const userID = "@" + localpart + ":hs1"
		const deviceID = "AABBCCDD"
		mustRegisterComplemauAppserviceUser(t, mscClient, localpart, true)

		res := doComplemauPutGhostDevice(t, mscClient, userID, deviceID, "")
		must.MatchResponse(t, res, match.HTTPResponse{StatusCode: http.StatusCreated})
		mustMatchComplemauDevices(t, mscClient, userID, 1, deviceID)

		res = mscClient.Do(t, http.MethodPost,
			[]string{"_matrix", "client", "v3", "delete_devices"},
			client.WithJSONBody(t, map[string]interface{}{
				"devices": []string{deviceID},
			}),
			asUser(userID),
		)
		must.MatchResponse(t, res, match.HTTPResponse{StatusCode: http.StatusOK})
		mustMatchComplemauDevices(t, mscClient, userID, 0, "")
	})

	t.Run("MSC4190 uploads cross-signing master key without UIA", func(t *testing.T) {
		const localpart = "complemau_msc_cross_signing"
		const userID = "@" + localpart + ":hs1"
		mustRegisterComplemauAppserviceUser(t, mscClient, localpart, true)

		privateKey := ed25519.NewKeyFromSeed([]byte("complemau-cross-signing-seed-001"))
		publicKey := base64.RawStdEncoding.EncodeToString(privateKey.Public().(ed25519.PublicKey))
		res := mscClient.Do(t, http.MethodPost,
			[]string{"_matrix", "client", "v3", "keys", "device_signing", "upload"},
			client.WithJSONBody(t, map[string]interface{}{
				"master_key": map[string]interface{}{
					"user_id": userID,
					"usage":   []string{"master"},
					"keys": map[string]string{
						"ed25519:" + publicKey: publicKey,
					},
				},
			}),
			asUser(userID),
		)
		must.MatchResponse(t, res, match.HTTPResponse{StatusCode: http.StatusOK})
	})
}

func doComplemauAppserviceRegister(
	t *testing.T,
	appserviceClient *client.CSAPI,
	localpart string,
	inhibitLogin bool,
) *http.Response {
	t.Helper()
	body := map[string]interface{}{
		"type":     complemauAppserviceLoginType,
		"username": localpart,
	}
	if inhibitLogin {
		body["inhibit_login"] = true
	}
	return appserviceClient.Do(t, http.MethodPost,
		[]string{"_matrix", "client", "v3", "register"},
		client.WithJSONBody(t, body),
	)
}

func mustRegisterComplemauAppserviceUser(
	t *testing.T,
	appserviceClient *client.CSAPI,
	localpart string,
	inhibitLogin bool,
) {
	t.Helper()
	res := doComplemauAppserviceRegister(t, appserviceClient, localpart, inhibitLogin)
	must.MatchResponse(t, res, match.HTTPResponse{
		StatusCode: http.StatusOK,
		JSON: []match.JSON{
			match.JSONKeyEqual("user_id", "@"+localpart+":hs1"),
		},
	})
}

func doComplemauAppserviceLogin(
	t *testing.T,
	appserviceClient *client.CSAPI,
	user string,
) *http.Response {
	t.Helper()
	return appserviceClient.Do(t, http.MethodPost,
		[]string{"_matrix", "client", "v3", "login"},
		client.WithJSONBody(t, map[string]interface{}{
			"type": complemauAppserviceLoginType,
			"identifier": map[string]interface{}{
				"type": "m.id.user",
				"user": user,
			},
		}),
	)
}

func doComplemauPutGhostDevice(
	t *testing.T,
	appserviceClient *client.CSAPI,
	userID string,
	deviceID string,
	displayName string,
) *http.Response {
	t.Helper()
	body := map[string]interface{}{}
	if displayName != "" {
		body["display_name"] = displayName
	}
	return appserviceClient.Do(t, http.MethodPut,
		[]string{"_matrix", "client", "v3", "devices", deviceID},
		client.WithJSONBody(t, body),
		asUser(userID),
	)
}

func mustMatchComplemauDevices(
	t *testing.T,
	appserviceClient *client.CSAPI,
	userID string,
	wantCount int,
	wantDeviceID string,
) {
	t.Helper()
	res := appserviceClient.Do(t, http.MethodGet,
		[]string{"_matrix", "client", "v3", "devices"}, asUser(userID),
	)
	matchers := []match.JSON{match.JSONKeyArrayOfSize("devices", wantCount)}
	if wantDeviceID != "" {
		matchers = append(matchers, match.JSONKeyEqual("devices.0.device_id", wantDeviceID))
	}
	must.MatchResponse(t, res, match.HTTPResponse{
		StatusCode: http.StatusOK,
		JSON:       matchers,
	})
}
