//go:build complemau

package complemau_tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/matrix-org/complement"
	"github.com/matrix-org/complement/b"
	"github.com/matrix-org/complement/client"
	"github.com/matrix-org/complement/helpers"
	"github.com/matrix-org/complement/match"
	"github.com/matrix-org/complement/must"
	"github.com/matrix-org/complement/runtime"
	"maunium.net/go/mautrix/id"
)

const complemauQueryProtocol = "complemau-query"

var (
	complemauQueryEndpointsBlueprint = b.MustValidate(b.Blueprint{
		Name: "hs_with_complemau_query_endpoints",
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
								{Regex: `^@query_.*:hs1$`, Exclusive: false},
							},
							Aliases: []b.ApplicationServiceNamespace{
								{Regex: `^#query_.*:hs1$`, Exclusive: false},
							},
						},
						Protocols: []string{complemauQueryProtocol},
					},
				},
			},
		},
	})
	complemauQueryNoProtocolsBlueprint = b.MustValidate(b.Blueprint{
		Name: "hs_with_complemau_query_no_protocols",
		Homeservers: []b.Homeserver{
			{
				Name: "hs1",
				ApplicationServices: []b.ApplicationService{
					{
						ID:              b.ComplemauASID,
						URL:             b.ComplemauASURL,
						SenderLocalpart: b.ComplemauSender,
						RateLimited:     false,
					},
				},
			},
		},
	})
	complemauQueryNoAppservicesBlueprint = b.MustValidate(b.Blueprint{
		Name: "hs_with_complemau_query_no_appservices",
		Homeservers: []b.Homeserver{
			{Name: "hs1"},
		},
	})
)

func TestComplemauAppserviceRoomAliasQuery(t *testing.T) {
	alice, bridgeClient, bridge, _ := startComplemauQueryTest(t, complemauQueryEndpointsBlueprint)

	const alias = "#query_alias:hs1"
	roomID := alice.MustCreateRoom(t, map[string]interface{}{"preset": "public_chat"})
	mappingResult := make(chan error, 1)
	bridge.setAliasQueryResult(func(got id.RoomAlias) bool {
		var err error
		if string(got) != alias {
			err = fmt.Errorf("alias query = %s, want %s", got, alias)
		} else {
			err = putComplemauQueryAlias(bridgeClient, alias, roomID)
		}
		select {
		case mappingResult <- err:
		default:
		}
		return err == nil
	})

	res := alice.Do(t, http.MethodGet, []string{
		"_matrix", "client", "v3", "directory", "room", alias,
	})
	got := bridge.mustReceiveAliasQuery(t, 5*time.Second)
	if string(got) != alias {
		t.Errorf("complemau: alias query = %s, want %s", got, alias)
	}
	select {
	case err := <-mappingResult:
		if err != nil {
			t.Fatalf("complemau: failed to create alias mapping during query: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("complemau: timed out waiting for alias mapping result")
	}
	must.MatchResponse(t, res, match.HTTPResponse{
		StatusCode: http.StatusOK,
		JSON: []match.JSON{
			match.JSONKeyEqual("room_id", roomID),
			match.JSONKeyEqual("servers", []string{"hs1"}),
		},
	})
}

func TestComplemauAppserviceUserQueries(t *testing.T) {
	t.Run("unknown namespace user", func(t *testing.T) {
		alice, _, bridge, _ := startComplemauQueryTest(t, complemauQueryEndpointsBlueprint)

		const unknownUserID = "@query_unknown:hs1"
		bridge.setUserQueryResult(func(got id.UserID) bool {
			return string(got) == unknownUserID
		})
		roomID := alice.MustCreateRoom(t, map[string]interface{}{"preset": "public_chat"})
		res := alice.InviteRoom(t, roomID, unknownUserID)

		// Known tuwunel gap: no call site invokes the appservice user query.
		// Keep this strict so the test turns green when the endpoint is wired in.
		got := bridge.mustReceiveUserQuery(t, 5*time.Second)
		if string(got) != unknownUserID {
			t.Errorf("complemau: user query = %s, want %s", got, unknownUserID)
		}
		must.MatchResponse(t, res, match.HTTPResponse{StatusCode: http.StatusOK})
	})

	t.Run("known user is skipped before marker query", func(t *testing.T) {
		alice, bridgeClient, bridge, _ := startComplemauQueryTest(t, complemauQueryEndpointsBlueprint)

		const knownLocalpart = "query_known"
		const knownUserID = "@query_known:hs1"
		const markerUserID = "@query_marker:hs1"
		registerAppserviceGhost(t, bridgeClient, knownLocalpart)
		bridge.setUserQueryResult(func(id.UserID) bool { return true })

		roomID := alice.MustCreateRoom(t, map[string]interface{}{"preset": "public_chat"})
		alice.MustInviteRoom(t, roomID, knownUserID)
		markerRes := alice.InviteRoom(t, roomID, markerUserID)

		// The later unknown user is the marker proving the known user did not
		// cause a query. This remains a known gap until user queries are wired in.
		got := bridge.mustReceiveUserQuery(t, 5*time.Second)
		if string(got) != markerUserID {
			t.Errorf("complemau: first user query = %s, want marker %s", got, markerUserID)
		}
		must.MatchResponse(t, markerRes, match.HTTPResponse{StatusCode: http.StatusOK})
	})
}

func TestComplemauAppserviceThirdPartyProtocols(t *testing.T) {
	t.Run("all protocols", func(t *testing.T) {
		alice, _, bridge, registration := startComplemauQueryTest(t, complemauQueryEndpointsBlueprint)
		metadata := complemauQueryProtocolMetadata()
		bridge.queueEndpointResponse(t, complemauThirdPartyProtocol, http.StatusOK, metadata)

		res := alice.Do(t, http.MethodGet, []string{
			"_matrix", "client", "v3", "thirdparty", "protocols",
		})
		// Known tuwunel gap: the C-S endpoint currently returns an empty map
		// without consulting registered appservices.
		request := bridge.mustReceiveEndpointRequest(t, complemauThirdPartyProtocol, 5*time.Second)
		mustMatchComplemauThirdPartyRequest(
			t,
			request,
			http.MethodGet,
			"/_matrix/app/v1/thirdparty/protocol/"+complemauQueryProtocol,
			complemauQueryProtocol,
			registration.HSToken,
			nil,
		)
		must.MatchResponse(t, res, match.HTTPResponse{
			StatusCode: http.StatusOK,
			JSON: []match.JSON{
				match.JSONKeyEqual("", map[string]any{complemauQueryProtocol: metadata}),
			},
		})
	})

	t.Run("selected protocol", func(t *testing.T) {
		alice, _, bridge, registration := startComplemauQueryTest(t, complemauQueryEndpointsBlueprint)
		metadata := complemauQueryProtocolMetadata()
		bridge.queueEndpointResponse(t, complemauThirdPartyProtocol, http.StatusOK, metadata)

		res := alice.Do(t, http.MethodGet, []string{
			"_matrix", "client", "v3", "thirdparty", "protocol", complemauQueryProtocol,
		})
		// Known tuwunel gap: selected 3PE protocols are not forwarded.
		request := bridge.mustReceiveEndpointRequest(t, complemauThirdPartyProtocol, 5*time.Second)
		mustMatchComplemauThirdPartyRequest(
			t,
			request,
			http.MethodGet,
			"/_matrix/app/v1/thirdparty/protocol/"+complemauQueryProtocol,
			complemauQueryProtocol,
			registration.HSToken,
			nil,
		)
		must.MatchResponse(t, res, match.HTTPResponse{
			StatusCode: http.StatusOK,
			JSON: []match.JSON{
				match.JSONKeyEqual("", metadata),
			},
		})
	})
}

func TestComplemauAppserviceThirdPartyLookups(t *testing.T) {
	t.Run("user", func(t *testing.T) {
		alice, _, bridge, registration := startComplemauQueryTest(t, complemauQueryEndpointsBlueprint)
		fields := url.Values{"query": {"alice"}}
		want := []map[string]any{
			{
				"protocol": complemauQueryProtocol,
				"userid":   "@query_alice:hs1",
				"fields":   map[string]any{"query": "alice"},
			},
		}
		bridge.queueEndpointResponse(t, complemauThirdPartyUser, http.StatusOK, want)

		res := alice.Do(
			t,
			http.MethodGet,
			[]string{"_matrix", "client", "v3", "thirdparty", "user", complemauQueryProtocol},
			client.WithQueries(fields),
		)
		// Known tuwunel gap: 3PE user lookups are not forwarded.
		request := bridge.mustReceiveEndpointRequest(t, complemauThirdPartyUser, 5*time.Second)
		mustMatchComplemauThirdPartyRequest(
			t,
			request,
			http.MethodGet,
			"/_matrix/app/v1/thirdparty/user/"+complemauQueryProtocol,
			complemauQueryProtocol,
			registration.HSToken,
			fields,
		)
		must.MatchResponse(t, res, match.HTTPResponse{
			StatusCode: http.StatusOK,
			JSON:       []match.JSON{match.JSONKeyEqual("", want)},
		})
	})

	t.Run("location", func(t *testing.T) {
		alice, _, bridge, registration := startComplemauQueryTest(t, complemauQueryEndpointsBlueprint)
		fields := url.Values{"query": {"lobby"}}
		want := []map[string]any{
			{
				"protocol": complemauQueryProtocol,
				"alias":    "#query_lobby:hs1",
				"fields":   map[string]any{"query": "lobby"},
			},
		}
		bridge.queueEndpointResponse(t, complemauThirdPartyLocation, http.StatusOK, want)

		res := alice.Do(
			t,
			http.MethodGet,
			[]string{"_matrix", "client", "v3", "thirdparty", "location", complemauQueryProtocol},
			client.WithQueries(fields),
		)
		// Known tuwunel gap: 3PE location lookups are not forwarded.
		request := bridge.mustReceiveEndpointRequest(t, complemauThirdPartyLocation, 5*time.Second)
		mustMatchComplemauThirdPartyRequest(
			t,
			request,
			http.MethodGet,
			"/_matrix/app/v1/thirdparty/location/"+complemauQueryProtocol,
			complemauQueryProtocol,
			registration.HSToken,
			fields,
		)
		must.MatchResponse(t, res, match.HTTPResponse{
			StatusCode: http.StatusOK,
			JSON:       []match.JSON{match.JSONKeyEqual("", want)},
		})
	})
}

func TestComplemauAppserviceThirdPartyEdges(t *testing.T) {
	t.Run("no appservices", func(t *testing.T) {
		runtime.SkipIf(t, runtime.Dendrite)
		deployment := complement.OldDeploy(t, complemauQueryNoAppservicesBlueprint)
		defer deployment.Destroy(t)
		alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})

		res := alice.Do(t, http.MethodGet, []string{
			"_matrix", "client", "v3", "thirdparty", "protocols",
		})
		must.MatchResponse(t, res, match.HTTPResponse{
			StatusCode: http.StatusOK,
			JSON:       []match.JSON{match.JSONKeyEqual("", map[string]any{})},
		})
	})

	t.Run("appservice declares no protocols", func(t *testing.T) {
		alice, _, bridge, _ := startComplemauQueryTest(t, complemauQueryNoProtocolsBlueprint)
		res := alice.Do(t, http.MethodGet, []string{
			"_matrix", "client", "v3", "thirdparty", "protocols",
		})
		must.MatchResponse(t, res, match.HTTPResponse{
			StatusCode: http.StatusOK,
			JSON:       []match.JSON{match.JSONKeyEqual("", map[string]any{})},
		})
		mustHaveNoComplemauEndpointRequest(t, bridge, complemauThirdPartyProtocol)
	})

	t.Run("missing appservice answer is dropped", func(t *testing.T) {
		alice, _, bridge, registration := startComplemauQueryTest(t, complemauQueryEndpointsBlueprint)
		bridge.queueEndpointResponse(t, complemauThirdPartyProtocol, http.StatusNotFound, nil)

		res := alice.Do(t, http.MethodGet, []string{
			"_matrix", "client", "v3", "thirdparty", "protocols",
		})
		// Known tuwunel gap: protocol registrations are ignored, so the AS
		// cannot yet return the missing answer which should be dropped.
		request := bridge.mustReceiveEndpointRequest(t, complemauThirdPartyProtocol, 5*time.Second)
		mustMatchComplemauThirdPartyRequest(
			t,
			request,
			http.MethodGet,
			"/_matrix/app/v1/thirdparty/protocol/"+complemauQueryProtocol,
			complemauQueryProtocol,
			registration.HSToken,
			nil,
		)
		must.MatchResponse(t, res, match.HTTPResponse{
			StatusCode: http.StatusOK,
			JSON:       []match.JSON{match.JSONKeyEqual("", map[string]any{})},
		})
	})
}

func startComplemauQueryTest(
	t *testing.T,
	blueprint b.Blueprint,
) (*client.CSAPI, *client.CSAPI, *complemauBridge, b.ApplicationService) {
	t.Helper()
	// tuwunel and Synapse read the Complement appservice registration file;
	// Dendrite does not yet. https://github.com/matrix-org/complement/issues/514
	runtime.SkipIf(t, runtime.Dendrite)

	deployment := complement.OldDeploy(t, blueprint)
	t.Cleanup(func() { deployment.Destroy(t) })
	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	bridgeClient := deployment.AppServiceUser(t, "hs1", b.ComplemauSenderID)
	registration := blueprint.Homeservers[0].ApplicationServices[0]
	bridge := startComplemauBridgeWithRegistration(
		t,
		bridgeClient.BaseURL,
		registration,
		b.ComplemauASPort,
	)
	return alice, bridgeClient, bridge, registration
}

func putComplemauQueryAlias(c *client.CSAPI, alias string, roomID string) error {
	body, err := json.Marshal(map[string]string{"room_id": roomID})
	if err != nil {
		return fmt.Errorf("marshal alias mapping: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPut,
		c.BaseURL+"/_matrix/client/v3/directory/room/"+url.PathEscape(alias),
		bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("create alias mapping request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.AccessToken)
	req.Header.Set("Content-Type", "application/json")
	res, err := c.Client.Do(req)
	if err != nil {
		return fmt.Errorf("send alias mapping request: %w", err)
	}
	responseBody, readErr := io.ReadAll(res.Body)
	closeErr := res.Body.Close()
	if readErr != nil {
		return fmt.Errorf("read alias mapping response: %w", readErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close alias mapping response: %w", closeErr)
	}
	if res.StatusCode < http.StatusOK || res.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("alias mapping status = %d, body = %s", res.StatusCode, responseBody)
	}
	return nil
}

func complemauQueryProtocolMetadata() map[string]any {
	return map[string]any{
		"x-protocol-data": 42,
		"instances":       []any{},
	}
}

func mustMatchComplemauThirdPartyRequest(
	t *testing.T,
	request *complemauEndpointRequest,
	method string,
	path string,
	protocol string,
	hsToken string,
	fields url.Values,
) {
	t.Helper()
	if request.Method != method {
		t.Errorf("complemau: third-party request method = %s, want %s", request.Method, method)
	}
	if request.Path != path {
		t.Errorf("complemau: third-party request path = %s, want %s", request.Path, path)
	}
	if request.Protocol != protocol {
		t.Errorf("complemau: third-party request protocol = %s, want %s", request.Protocol, protocol)
	}
	wantAuthorization := "Bearer " + hsToken
	if got := request.Header.Get("Authorization"); got != wantAuthorization {
		t.Errorf("complemau: third-party authorization = %q, want %q", got, wantAuthorization)
	}
	for key, want := range fields {
		got := request.Query[key]
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("complemau: third-party query %q = %v, want %v", key, got, want)
		}
	}
	// tuwunel also sends a legacy access_token query parameter. Bearer
	// authentication is the interop behavior this test requires.
}

func mustHaveNoComplemauEndpointRequest(
	t *testing.T,
	bridge *complemauBridge,
	endpoint complemauEndpoint,
) {
	t.Helper()
	route, ok := bridge.routes[endpoint]
	if !ok {
		t.Fatalf("complemau: unknown appservice endpoint %q", endpoint)
	}
	route.requests.mu.Lock()
	defer route.requests.mu.Unlock()
	if len(route.requests.items) != 0 {
		t.Errorf("complemau: received %d unexpected requests for %q", len(route.requests.items), endpoint)
	}
}
