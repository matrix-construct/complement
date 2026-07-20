//go:build complemau

package complemau_tests

import (
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/matrix-org/complement/b"
	"github.com/matrix-org/complement/client"

	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/event"
)

// complemauBridge is a mautrix appservice standing in for a real bridge. It runs
// the mautrix receive path (PUT /_matrix/app/v1/transactions/{txnId}, hs_token
// auth, typed Transaction/ephemeral parsing) against the homeserver under test
// and exposes the parsed ephemeral events for assertions. Shared by the
// complemau tests; extend it with typing/device-list/OTK channels as more of the
// appservice surface gets covered.
type complemauBridge struct {
	as       *appservice.AppService
	receipts chan *event.Event
}

// startComplemauBridge creates and starts the mautrix receiver on the fixed port
// the baked blueprint registration points the homeserver at, then drains the
// appservice event channel, forwarding m.receipt EDUs and discarding the
// membership PDUs that also arrive there.
func startComplemauBridge(t *testing.T, homeserverURL string) *complemauBridge {
	t.Helper()

	blueprintReg := b.BlueprintHSWithComplemauBridge.Homeservers[0].ApplicationServices[0]
	reg := &appservice.Registration{
		ID:              blueprintReg.ID,
		AppToken:        blueprintReg.ASToken,
		ServerToken:     blueprintReg.HSToken,
		SenderLocalpart: blueprintReg.SenderLocalpart,
		URL:             blueprintReg.URL,
		// Process ephemeral data (receipts, typing) rather than dropping it.
		EphemeralEvents:     true,
		SoruEphemeralEvents: true,
	}

	as, err := appservice.CreateFull(appservice.CreateOpts{
		Registration:     reg,
		HomeserverDomain: "hs1",
		HomeserverURL:    homeserverURL,
		HostConfig: appservice.HostConfig{
			Hostname: "0.0.0.0",
			Port:     b.ComplemauASPort,
		},
	})
	if err != nil {
		t.Fatalf("complemau: failed to create mautrix appservice: %v", err)
	}

	br := &complemauBridge{as: as, receipts: make(chan *event.Event, 16)}

	go as.Start()
	t.Cleanup(as.Stop)
	waitForReceiver(t)

	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	go br.drain(done)

	return br
}

func (br *complemauBridge) drain(done <-chan struct{}) {
	for {
		select {
		case evt := <-br.as.Events:
			if evt.Type.Type == "m.receipt" {
				select {
				case br.receipts <- evt:
				default:
				}
			}
		case <-done:
			return
		}
	}
}

func (br *complemauBridge) stop() { br.as.Stop() }

func (br *complemauBridge) mustReceiveReceipt(t *testing.T, timeout time.Duration) *event.Event {
	t.Helper()
	select {
	case evt := <-br.receipts:
		return evt
	case <-time.After(timeout):
		t.Fatalf("complemau: timed out after %s waiting for an m.receipt transaction", timeout)
		return nil
	}
}

func (br *complemauBridge) mustNotReceiveReceipt(t *testing.T, window time.Duration) {
	t.Helper()
	select {
	case evt := <-br.receipts:
		t.Errorf("complemau: homeserver re-emitted an m.receipt for a non-advancing read position (tuwunel#516): %v", evt.Content.Raw)
	case <-time.After(window):
	}
}

// waitForReceiver blocks until the mautrix HTTP server answers its liveness
// probe, so the homeserver's first transaction push is not lost to a refused
// connection.
func waitForReceiver(t *testing.T) {
	t.Helper()

	probe := "http://127.0.0.1:" + strconv.Itoa(b.ComplemauASPort) + "/_matrix/mau/live"
	probeClient := http.Client{Timeout: 250 * time.Millisecond}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := probeClient.Get(probe)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("complemau: mautrix receiver did not become ready on port %d", b.ComplemauASPort)
}

// registerAppserviceGhost provisions a namespaced user through appservice login,
// the way a bridge creates a ghost before puppeting it.
func registerAppserviceGhost(t *testing.T, bridge *client.CSAPI, localpart string) {
	t.Helper()
	bridge.MustDo(t, "POST", []string{"_matrix", "client", "v3", "register"},
		client.WithJSONBody(t, map[string]interface{}{
			"type":     "m.login.application_service",
			"username": localpart,
		}),
	)
}

// asUser masquerades a bridge (as_token) request as userID via the ?user_id=
// appservice query parameter.
func asUser(userID string) client.RequestOpt {
	return client.WithQueries(url.Values{"user_id": {userID}})
}

// postReadReceipt sends an m.read receipt for eventID. Pass asUser(...) to
// masquerade as a puppeted user.
func postReadReceipt(t *testing.T, c *client.CSAPI, roomID, eventID string, opts ...client.RequestOpt) {
	t.Helper()
	opts = append([]client.RequestOpt{client.WithJSONBody(t, struct{}{})}, opts...)
	c.MustDo(t, "POST",
		[]string{"_matrix", "client", "v3", "rooms", roomID, "receipt", "m.read", eventID},
		opts...,
	)
}
