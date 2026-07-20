//go:build complemau

package complemau_tests

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/matrix-org/complement"
	"github.com/matrix-org/complement/b"
	"github.com/matrix-org/complement/client"
	"github.com/matrix-org/complement/helpers"
	"github.com/matrix-org/complement/match"
	"github.com/matrix-org/complement/must"
	"github.com/matrix-org/complement/runtime"
)

const (
	complemauPingSuccessID           = "complemau_ping_success"
	complemauPingNoURLID             = "complemau_ping_no_url"
	complemauPingConnectionFailedID  = "complemau_ping_connection_failed"
	complemauPingBadStatusID         = "complemau_ping_bad_status"
	complemauPingTimeoutID           = "complemau_ping_timeout"
	complemauPingBadStatusBody       = "complemau ping failure"
	complemauPingClientTimeout       = 45 * time.Second
	complemauPingConnectionFailedURL = "http://127.0.0.1:1"
)

var complemauPingErrorsBlueprint = b.MustValidate(b.Blueprint{
	Name: "hs_with_complemau_ping_errors",
	Homeservers: []b.Homeserver{
		{
			Name: "hs1",
			ApplicationServices: []b.ApplicationService{
				newComplemauPingRegistration(
					complemauPingSuccessID,
					"complemau_ping_success",
					b.ComplemauASURL,
				),
				newComplemauPingRegistration(
					complemauPingNoURLID,
					"complemau_ping_no_url",
					"",
				),
				newComplemauPingRegistration(
					complemauPingConnectionFailedID,
					"complemau_ping_connection_failed",
					complemauPingConnectionFailedURL,
				),
				newComplemauPingRegistration(
					complemauPingBadStatusID,
					"complemau_ping_bad_status",
					b.ComplemauASURL,
				),
				newComplemauPingRegistration(
					complemauPingTimeoutID,
					"complemau_ping_timeout",
					b.ComplemauSecondASURL,
				),
			},
		},
	},
})

func TestComplemauPingRoundTripAndAuthorization(t *testing.T) {
	// tuwunel and Synapse read the Complement appservice registration file;
	// Dendrite does not yet. https://github.com/matrix-org/complement/issues/514
	runtime.SkipIf(t, runtime.Dendrite)

	deployment := complement.OldDeploy(t, complemauPingErrorsBlueprint)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	asClient := deployment.AppServiceUser(t, "hs1", "@complemau_ping_success:hs1")
	otherASClient := deployment.AppServiceUser(t, "hs1", "@complemau_ping_no_url:hs1")
	registration := complemauPingErrorsBlueprint.Homeservers[0].ApplicationServices[0]
	bridge := startComplemauBridgeWithRegistration(
		t,
		asClient.BaseURL,
		registration,
		b.ComplemauASPort,
	)
	defer bridge.stop()

	// startComplemauBridgeWithRegistration performs a readiness ping. Drain it
	// before observing the transaction chosen below.
	bridge.mustReceivePing(t, 5*time.Second)

	const transactionID = "complemau_explicit_self_ping"
	res := pingComplemauAppservice(t, asClient, complemauPingSuccessID, transactionID)
	must.MatchResponse(t, res, match.HTTPResponse{
		StatusCode: http.StatusOK,
		JSON: []match.JSON{
			match.JSONKeyPresent("duration_ms"),
		},
	})
	if got := bridge.mustReceivePing(t, 5*time.Second); got != transactionID {
		t.Errorf("complemau: inbound ping transaction ID = %q, want %q", got, transactionID)
	}

	t.Run("normal user is forbidden", func(t *testing.T) {
		// Tuwunel rejects ordinary access tokens as unauthorized. Appservice
		// tokens for a different appservice reach the self-only check below.
		res := pingComplemauAppservice(t, alice, complemauPingSuccessID, "normal_user_ping")
		must.MatchResponse(t, res, match.HTTPResponse{
			StatusCode: http.StatusUnauthorized,
			JSON:       []match.JSON{match.JSONKeyEqual("errcode", "M_UNAUTHORIZED")},
		})
	})

	t.Run("appservice cannot ping another appservice", func(t *testing.T) {
		res := pingComplemauAppservice(
			t,
			otherASClient,
			complemauPingSuccessID,
			"other_appservice_ping",
		)
		must.MatchResponse(t, res, match.HTTPResponse{StatusCode: http.StatusForbidden})
	})
}

func TestComplemauPingErrorSurfacing(t *testing.T) {
	// tuwunel and Synapse read the Complement appservice registration file;
	// Dendrite does not yet. https://github.com/matrix-org/complement/issues/514
	runtime.SkipIf(t, runtime.Dendrite)

	deployment := complement.OldDeploy(t, complemauPingErrorsBlueprint)
	defer deployment.Destroy(t)

	startComplemauPingErrorServer(t, b.ComplemauASPort, http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/_matrix/app/v1/ping" {
				http.NotFound(w, r)
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(complemauPingBadStatusBody))
		},
	))

	timeoutRelease := make(chan struct{})
	startComplemauPingErrorServer(t, b.ComplemauSecondASPort, http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/_matrix/app/v1/ping" {
				http.NotFound(w, r)
				return
			}
			select {
			case <-r.Context().Done():
			case <-timeoutRelease:
			}
		},
	))
	t.Cleanup(func() {
		close(timeoutRelease)
	})

	t.Run("registration without URL", func(t *testing.T) {
		asClient := deployment.AppServiceUser(t, "hs1", "@complemau_ping_no_url:hs1")
		res := pingComplemauAppservice(t, asClient, complemauPingNoURLID, "ping_no_url")
		must.MatchResponse(t, res, match.HTTPResponse{
			StatusCode: http.StatusBadRequest,
			JSON: []match.JSON{
				match.JSONKeyEqual("errcode", "M_URL_NOT_SET"),
			},
		})
	})

	t.Run("connection failure", func(t *testing.T) {
		asClient := deployment.AppServiceUser(t, "hs1", "@complemau_ping_connection_failed:hs1")
		res := pingComplemauAppservice(
			t,
			asClient,
			complemauPingConnectionFailedID,
			"ping_connection_failed",
		)
		must.MatchResponse(t, res, match.HTTPResponse{
			StatusCode: http.StatusBadGateway,
			JSON: []match.JSON{
				match.JSONKeyEqual("errcode", "M_CONNECTION_FAILED"),
			},
		})
	})

	t.Run("receiver error status", func(t *testing.T) {
		asClient := deployment.AppServiceUser(t, "hs1", "@complemau_ping_bad_status:hs1")
		res := pingComplemauAppservice(t, asClient, complemauPingBadStatusID, "ping_bad_status")
		must.MatchResponse(t, res, match.HTTPResponse{
			StatusCode: http.StatusBadGateway,
			JSON: []match.JSON{
				match.JSONKeyEqual("errcode", "M_BAD_STATUS"),
				match.JSONKeyEqual("status", http.StatusInternalServerError),
				match.JSONKeyEqual("body", complemauPingBadStatusBody),
			},
		})
	})

	t.Run("receiver timeout", func(t *testing.T) {
		asClient := deployment.AppServiceUser(t, "hs1", "@complemau_ping_timeout:hs1")
		res := pingComplemauAppservice(t, asClient, complemauPingTimeoutID, "ping_timeout")
		must.MatchResponse(t, res, match.HTTPResponse{
			StatusCode: http.StatusGatewayTimeout,
			JSON: []match.JSON{
				match.JSONKeyEqual("errcode", "M_CONNECTION_TIMEOUT"),
			},
		})
	})
}

func newComplemauPingRegistration(id, senderLocalpart, appserviceURL string) b.ApplicationService {
	return b.ApplicationService{
		ID:              id,
		URL:             appserviceURL,
		SenderLocalpart: senderLocalpart,
		RateLimited:     false,
		Namespaces: &b.ApplicationServiceNamespaces{
			Users: []b.ApplicationServiceNamespace{
				{
					Regex:     fmt.Sprintf(`^@%s:hs1$`, senderLocalpart),
					Exclusive: false,
				},
			},
		},
	}
}

func pingComplemauAppservice(
	t *testing.T,
	asClient *client.CSAPI,
	appserviceID string,
	transactionID string,
) *http.Response {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), complemauPingClientTimeout)
	defer cancel()
	return asClient.Do(
		t,
		"POST",
		[]string{"_matrix", "client", "v1", "appservice", appserviceID, "ping"},
		client.WithJSONBody(t, map[string]string{"transaction_id": transactionID}),
		func(req *http.Request) {
			*req = *req.WithContext(ctx)
		},
	)
}

func startComplemauPingErrorServer(t *testing.T, port uint16, handler http.Handler) {
	t.Helper()
	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		t.Fatalf("complemau: failed to listen for ping errors on port %d: %v", port, err)
	}
	server := &http.Server{Handler: handler}
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		_ = server.Close()
	})
}
