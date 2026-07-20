//go:build complemau

package complemau_tests

import (
	"net/http"
	"testing"

	"github.com/matrix-org/complement"
	"github.com/matrix-org/complement/b"
	"github.com/matrix-org/complement/client"
	"github.com/matrix-org/complement/helpers"
	"github.com/matrix-org/complement/match"
	"github.com/matrix-org/complement/must"
	"github.com/matrix-org/complement/runtime"
)

var complemauNamespacesExclusiveBlueprint = b.MustValidate(b.Blueprint{
	Name: "hs_with_complemau_exclusive_namespaces",
	Homeservers: []b.Homeserver{
		{
			Name: "hs1",
			ApplicationServices: []b.ApplicationService{
				{
					ID:              b.ComplemauASID,
					URL:             b.ComplemauASURL,
					SenderLocalpart: b.ComplemauSender,
					RateLimited:     false,
					Namespaces: &b.ApplicationServiceNamespaces{
						Users: []b.ApplicationServiceNamespace{
							{Regex: `^@asns_.*:hs1$`, Exclusive: true},
							{Regex: `^@shared_.*:hs1$`, Exclusive: false},
						},
						Aliases: []b.ApplicationServiceNamespace{
							{Regex: `^#asns_.*:hs1$`, Exclusive: true},
							{Regex: `^#shared_.*:hs1$`, Exclusive: false},
						},
					},
				},
			},
		},
	},
})

func TestComplemauExclusiveNamespaces(t *testing.T) {
	// tuwunel and Synapse read the Complement appservice registration file;
	// Dendrite does not yet. https://github.com/matrix-org/complement/issues/514
	runtime.SkipIf(t, runtime.Dendrite)

	deployment := complement.OldDeploy(t, complemauNamespacesExclusiveBlueprint)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	bridgeClient := deployment.AppServiceUser(t, "hs1", b.ComplemauSenderID)
	registration := complemauNamespacesExclusiveBlueprint.Homeservers[0].ApplicationServices[0]
	bridge := startComplemauBridgeWithRegistration(
		t,
		bridgeClient.BaseURL,
		registration,
		b.ComplemauASPort,
	)
	defer bridge.stop()

	roomID := alice.MustCreateRoom(t, map[string]interface{}{"preset": "public_chat"})

	t.Run("appservice creates and deletes an exclusive alias", func(t *testing.T) {
		alias := "#asns_owned:hs1"
		res := putComplemauRoomAlias(t, bridgeClient, alias, roomID)
		must.MatchResponse(t, res, match.HTTPResponse{StatusCode: http.StatusOK})

		res = bridgeClient.Do(t, "DELETE", []string{
			"_matrix", "client", "v3", "directory", "room", alias,
		})
		must.MatchResponse(t, res, match.HTTPResponse{StatusCode: http.StatusOK})
	})

	t.Run("regular user cannot claim an exclusive alias", func(t *testing.T) {
		res := putComplemauRoomAlias(t, alice, "#asns_reserved:hs1", roomID)
		must.MatchResponse(t, res, match.HTTPResponse{
			StatusCode: http.StatusBadRequest,
			JSON: []match.JSON{
				match.JSONKeyEqual("errcode", "M_EXCLUSIVE"),
			},
		})
	})

	t.Run("regular user cannot register an exclusive username", func(t *testing.T) {
		res := registerComplemauUsername(t, alice, "asns_carol")
		must.MatchResponse(t, res, match.HTTPResponse{
			StatusCode: http.StatusBadRequest,
			JSON: []match.JSON{
				match.JSONKeyEqual("errcode", "M_EXCLUSIVE"),
			},
		})
	})

	t.Run("appservice cannot claim an alias outside its namespaces", func(t *testing.T) {
		res := putComplemauRoomAlias(t, bridgeClient, "#outside_owned:hs1", roomID)
		// Synapse returns 403 here. tuwunel reports the namespace collision as
		// M_EXCLUSIVE with status 400.
		must.MatchResponse(t, res, match.HTTPResponse{
			StatusCode: http.StatusBadRequest,
			JSON: []match.JSON{
				match.JSONKeyEqual("errcode", "M_EXCLUSIVE"),
			},
		})
	})

	t.Run("nonexclusive namespaces allow regular claims", func(t *testing.T) {
		alias := "#shared_room:hs1"
		res := putComplemauRoomAlias(t, alice, alias, roomID)
		must.MatchResponse(t, res, match.HTTPResponse{StatusCode: http.StatusOK})

		res = registerComplemauUsername(t, alice, "shared_carol")
		must.MatchResponse(t, res, match.HTTPResponse{
			StatusCode: http.StatusOK,
			JSON: []match.JSON{
				match.JSONKeyEqual("user_id", "@shared_carol:hs1"),
			},
		})
	})
}

func putComplemauRoomAlias(
	t *testing.T,
	c *client.CSAPI,
	alias string,
	roomID string,
) *http.Response {
	t.Helper()
	return c.Do(
		t,
		"PUT",
		[]string{"_matrix", "client", "v3", "directory", "room", alias},
		client.WithJSONBody(t, map[string]string{"room_id": roomID}),
	)
}

func registerComplemauUsername(t *testing.T, c *client.CSAPI, username string) *http.Response {
	t.Helper()
	return c.Do(
		t,
		"POST",
		[]string{"_matrix", "client", "v3", "register"},
		client.WithJSONBody(t, map[string]interface{}{
			"auth": map[string]string{
				"type": "m.login.dummy",
			},
			"username": username,
			"password": "complement_meets_min_password_req",
		}),
		func(req *http.Request) {
			req.Header.Del("Authorization")
		},
	)
}
