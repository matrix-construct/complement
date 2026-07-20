//go:build complemau

package complemau_tests

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/matrix-org/complement"
	"github.com/matrix-org/complement/b"
	"github.com/matrix-org/complement/client"
	"github.com/matrix-org/complement/match"
	"github.com/matrix-org/complement/must"
	"github.com/matrix-org/complement/runtime"
)

var complemauMasqueradeBlueprint = b.MustValidate(b.Blueprint{
	Name: "hs_with_complemau_masquerade_auth",
	Homeservers: []b.Homeserver{
		{
			Name: "hs1",
			Users: []b.User{
				{Localpart: "@alice", DisplayName: "Alice"},
			},
			ApplicationServices: []b.ApplicationService{
				{
					ID:              b.ComplemauASID,
					URL:             b.ComplemauASURL,
					SenderLocalpart: b.ComplemauSender,
					RateLimited:     false,
					EnableMSC4190:   true,
					Namespaces: &b.ApplicationServiceNamespaces{
						Users: []b.ApplicationServiceNamespace{
							{Regex: `^@as_.*:hs1$`, Exclusive: false},
						},
					},
				},
			},
		},
	},
})

func TestComplemauMasqueradeAuthentication(t *testing.T) {
	// tuwunel and Synapse read the Complement appservice registration file;
	// Dendrite does not yet. https://github.com/matrix-org/complement/issues/514
	runtime.SkipIf(t, runtime.Dendrite)

	deployment := complement.OldDeploy(t, complemauMasqueradeBlueprint)
	defer deployment.Destroy(t)

	bridgeClient := deployment.AppServiceUser(t, "hs1", b.ComplemauSenderID)
	registration := complemauMasqueradeBlueprint.Homeservers[0].ApplicationServices[0]
	bridge := startComplemauBridgeWithRegistration(
		t,
		bridgeClient.BaseURL,
		registration,
		b.ComplemauASPort,
	)
	defer bridge.stop()
	bridge.ensureReady(t)

	t.Run("bare token identifies the sender", func(t *testing.T) {
		mustMatchComplemauMasqueradeResponse(
			t,
			bridgeClient,
			http.StatusOK,
			[]match.JSON{
				match.JSONKeyEqual("user_id", b.ComplemauSenderID),
				match.JSONKeyMissing("device_id"),
			},
		)
	})

	t.Run("unknown token is rejected", func(t *testing.T) {
		unknownTokenClient := *bridgeClient
		unknownTokenClient.AccessToken = "not-a-valid-appservice-token"
		mustMatchComplemauMasqueradeResponse(
			t,
			&unknownTokenClient,
			http.StatusUnauthorized,
			[]match.JSON{match.JSONKeyEqual("errcode", "M_UNKNOWN_TOKEN")},
		)
	})

	t.Run("missing token is rejected", func(t *testing.T) {
		missingTokenClient := *bridgeClient
		missingTokenClient.AccessToken = ""
		mustMatchComplemauMasqueradeResponse(
			t,
			&missingTokenClient,
			http.StatusUnauthorized,
			[]match.JSON{match.JSONKeyEqual("errcode", "M_MISSING_TOKEN")},
		)
	})

	t.Run("out of namespace user is rejected", func(t *testing.T) {
		// Synapse returns 403 M_FORBIDDEN. tuwunel reports the namespace
		// mismatch as 400 M_EXCLUSIVE.
		mustMatchComplemauMasqueradeResponse(
			t,
			bridgeClient,
			http.StatusBadRequest,
			[]match.JSON{match.JSONKeyEqual("errcode", "M_EXCLUSIVE")},
			asUser("@not_in_appservice_namespace:hs1"),
		)
	})

	t.Run("unregistered in namespace user is accepted", func(t *testing.T) {
		const ghostID = "@as_never_registered:hs1"
		// Synapse rejects this because the ghost does not exist. tuwunel
		// authorizes an in-namespace local user without that precondition.
		mustMatchComplemauMasqueradeResponse(
			t,
			bridgeClient,
			http.StatusOK,
			[]match.JSON{
				match.JSONKeyEqual("user_id", ghostID),
				match.JSONKeyMissing("device_id"),
			},
			asUser(ghostID),
		)
	})

	const (
		ghostLocalpart = "as_masquerade"
		ghostID        = "@as_masquerade:hs1"
	)
	registerAppserviceGhost(t, bridgeClient, ghostLocalpart)
	roomID := mustCreateComplemauMasqueradeRoom(t, bridgeClient, ghostID)

	t.Run("registered in namespace user sends as the ghost", func(t *testing.T) {
		eventID := mustSendComplemauMasqueradeEvent(
			t,
			bridgeClient,
			[]string{"_matrix", "client", "v3", "rooms", roomID, "send", "m.room.message", "masquerade_sender"},
			map[string]interface{}{"msgtype": "m.text", "body": "masqueraded message"},
			url.Values{"user_id": {ghostID}},
		)
		mustMatchComplemauStoredEvent(
			t,
			bridgeClient,
			roomID,
			eventID,
			ghostID,
			match.JSONKeyEqual("sender", ghostID),
		)
	})

	t.Run("device ID assertion", func(t *testing.T) {
		const (
			deviceGhostID = "@as_device_masquerade:hs1"
			deviceID      = "MASQUERADE_DEVICE"
		)
		bridge.mustCreateGhostDevice(t, deviceGhostID, deviceID, "Masquerade device")

		t.Run("stable spelling", func(t *testing.T) {
			mustMatchComplemauMasqueradeResponse(
				t,
				bridgeClient,
				http.StatusOK,
				[]match.JSON{
					match.JSONKeyEqual("user_id", deviceGhostID),
					match.JSONKeyEqual("device_id", deviceID),
				},
				asUserDevice(deviceGhostID, deviceID),
			)
		})

		t.Run("MSC3202 spelling", func(t *testing.T) {
			mustMatchComplemauMasqueradeResponse(
				t,
				bridgeClient,
				http.StatusOK,
				[]match.JSON{
					match.JSONKeyEqual("user_id", deviceGhostID),
					match.JSONKeyEqual("device_id", deviceID),
				},
				client.WithQueries(url.Values{
					"user_id":                      {deviceGhostID},
					"org.matrix.msc3202.device_id": {deviceID},
				}),
			)
		})

		t.Run("unknown device", func(t *testing.T) {
			// Synapse uses UNKNOWN_DEVICE. tuwunel exposes the Matrix
			// M_INVALID_PARAM errcode for the same condition.
			mustMatchComplemauMasqueradeResponse(
				t,
				bridgeClient,
				http.StatusBadRequest,
				[]match.JSON{match.JSONKeyEqual("errcode", "M_INVALID_PARAM")},
				asUserDevice(deviceGhostID, "NOT_A_REAL_DEVICE"),
			)
		})
	})

	timestampCases := []struct {
		name    string
		path    []string
		content map[string]interface{}
	}{
		{
			name: "message event",
			path: []string{"_matrix", "client", "v3", "rooms", roomID, "send", "m.room.message", "timestamp_message"},
			content: map[string]interface{}{
				"msgtype": "m.text",
				"body":    "timestamped message",
			},
		},
		{
			name:    "state event",
			path:    []string{"_matrix", "client", "v3", "rooms", roomID, "state", "m.room.name"},
			content: map[string]interface{}{"name": "timestamped room name"},
		},
		{
			name: "membership event",
			path: []string{"_matrix", "client", "v3", "rooms", roomID, "state", "m.room.member", ghostID},
			content: map[string]interface{}{
				"membership":   "join",
				"display_name": "Timestamped ghost",
			},
		},
	}
	for _, testCase := range timestampCases {
		t.Run("timestamp massaging "+testCase.name, func(t *testing.T) {
			eventID := mustSendComplemauMasqueradeEvent(
				t,
				bridgeClient,
				testCase.path,
				testCase.content,
				url.Values{"user_id": {ghostID}, "ts": {"1"}},
			)
			mustMatchComplemauStoredEvent(
				t,
				bridgeClient,
				roomID,
				eventID,
				ghostID,
				match.JSONKeyEqual("origin_server_ts", 1),
			)
		})
	}
}

func mustMatchComplemauMasqueradeResponse(
	t *testing.T,
	c *client.CSAPI,
	status int,
	jsonChecks []match.JSON,
	opts ...client.RequestOpt,
) {
	t.Helper()
	res := c.Do(t, http.MethodGet, []string{"_matrix", "client", "v3", "account", "whoami"}, opts...)
	must.MatchResponse(t, res, match.HTTPResponse{StatusCode: status, JSON: jsonChecks})
}

func mustCreateComplemauMasqueradeRoom(t *testing.T, c *client.CSAPI, userID string) string {
	t.Helper()
	res := c.MustDo(
		t,
		http.MethodPost,
		[]string{"_matrix", "client", "v3", "createRoom"},
		client.WithJSONBody(t, map[string]interface{}{"visibility": "public"}),
		asUser(userID),
	)
	return client.GetJSONFieldStr(t, client.ParseJSON(t, res), "room_id")
}

func mustSendComplemauMasqueradeEvent(
	t *testing.T,
	c *client.CSAPI,
	path []string,
	content map[string]interface{},
	query url.Values,
) string {
	t.Helper()
	res := c.MustDo(
		t,
		http.MethodPut,
		path,
		client.WithJSONBody(t, content),
		client.WithQueries(query),
	)
	return client.GetJSONFieldStr(t, client.ParseJSON(t, res), "event_id")
}

func mustMatchComplemauStoredEvent(
	t *testing.T,
	c *client.CSAPI,
	roomID string,
	eventID string,
	userID string,
	jsonChecks ...match.JSON,
) {
	t.Helper()
	res := c.Do(
		t,
		http.MethodGet,
		[]string{"_matrix", "client", "v3", "rooms", roomID, "event", eventID},
		asUser(userID),
	)
	must.MatchResponse(t, res, match.HTTPResponse{StatusCode: http.StatusOK, JSON: jsonChecks})
}
