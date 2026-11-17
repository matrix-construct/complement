package tests

import (
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/fclient"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/tidwall/gjson"

	"github.com/matrix-org/complement"
	"github.com/matrix-org/complement/b"
	"github.com/matrix-org/complement/client"
	"github.com/matrix-org/complement/federation"
	"github.com/matrix-org/complement/helpers"
	"github.com/matrix-org/complement/match"
	"github.com/matrix-org/complement/must"
)

// The key ID a policy server signs events under.
const policyKeyID gomatrixserverlib.KeyID = "ed25519:policy_server"

// policyServer plays a room's policy server from the Complement federation
// server, which is also a member of the room.
//
// It signs every event it is asked about with the key the room publishes,
// except that it refuses any message whose body mentions spam and signs any
// whose body mentions the wrong key with a key the room does not know. It
// records each event it is asked about.
type policyServer struct {
	name     spec.ServerName
	key      ed25519.PrivateKey
	wrongKey ed25519.PrivateKey
	public   ed25519.PublicKey

	mu      sync.Mutex
	version gomatrixserverlib.RoomVersion
	asked   []askedEvent
}

type askedEvent struct {
	eventID string
	body    string
}

// TestMSC4284PolicyServer checks a room protected by a policy server that the
// Complement server runs, both for messages tuwunel sends and for messages it
// receives.
//
// Outbound, a message goes out only with the policy server's signature, and a
// refusal reaches the client. Inbound, a message with a valid signature is
// accepted as is, one without is signed on request or refused, and one whose
// signature fails is asked about again and refused when that fails too.
func TestMSC4284PolicyServer(t *testing.T) {
	deployment := complement.Deploy(t, 1)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	hs1 := deployment.GetFullyQualifiedHomeserverName(t, "hs1")
	policy := newPolicyServer(t)

	srv := federation.NewServer(t, deployment,
		federation.HandleKeyRequests(),
		federation.HandleMakeSendJoinRequests(),
		federation.HandleTransactionRequests(nil, nil),
	)

	// Beside the room's traffic, tuwunel may ask a new peer for profiles and
	// devices, which this test has no answer for.
	srv.UnexpectedRequestsAreErrors = false

	// ValidFederationRequest checks tuwunel's X-Matrix signature on each request.
	srv.Mux().Handle("/_matrix/policy/v1/sign", srv.ValidFederationRequest(t, policy.sign)).Methods("POST")

	cancel := srv.Listen()
	defer cancel()

	policy.name = srv.ServerName()
	bob := srv.UserID("bob")

	// Bob joins Alice's public room, so the policy server has a joined user.
	roomID := alice.MustCreateRoom(t, map[string]interface{}{"preset": "public_chat"})
	room := srv.MustJoinRoom(t, deployment, hs1, roomID, bob)
	policy.setVersion(room.Version)
	alice.MustSyncUntil(t, client.SyncReq{}, client.SyncJoinedTo(bob, roomID))

	policyEventID := alice.SendEventSynced(t, roomID, b.Event{
		Type:     "m.room.policy",
		StateKey: b.Ptr(""),
		Content: map[string]interface{}{
			"via": string(srv.ServerName()),
			"public_keys": map[string]interface{}{
				"ed25519": spec.Base64Bytes(policy.public).Encode(),
			},
		},
	})

	room.WaiterForEvent(policyEventID).Waitf(t, 10*time.Second, "the policy event did not reach the policy server")

	t.Run("outbound_signed", func(t *testing.T) {
		eventID := alice.SendEventSynced(t, roomID, text("hello"))

		room.WaiterForEvent(eventID).Waitf(t, 10*time.Second, "the message did not reach the policy server's room")
		pdu, ok := room.GetEventInTimeline(eventID)
		if !ok {
			t.Fatalf("the message %s is missing from the policy server's room", eventID)
		}

		if err := policy.verify(pdu); err != nil {
			t.Fatalf("the message went out without a valid policy signature: %s", err)
		}

		if n := policy.askedFor(eventID); n != 1 {
			t.Fatalf("the policy server was asked about the message %d times, want 1", n)
		}
	})

	t.Run("outbound_refused", func(t *testing.T) {
		const body = "this is spam"

		res := alice.Do(t, "PUT",
			[]string{"_matrix", "client", "v3", "rooms", roomID, "send", "m.room.message", "msc4284-refused"},
			client.WithJSONBody(t, text(body).Content),
		)

		must.MatchResponse(t, res, match.HTTPResponse{
			StatusCode: 400,
			JSON: []match.JSON{
				match.JSONKeyEqual("errcode", "M_FORBIDDEN"),
			},
		})

		// A later message reaches the room; the refused one must not be ahead of it.
		eventID := alice.SendEventSynced(t, roomID, text("after the refusal"))
		room.WaiterForEvent(eventID).Waitf(t, 10*time.Second, "the later message did not reach the policy server's room")

		refused := policy.askedAbout(body)
		if len(refused) != 1 {
			t.Fatalf("the policy server was asked about the refused message %d times, want 1", len(refused))
		}

		for _, pdu := range timeline(room) {
			if pdu.EventID() == refused[0] {
				t.Fatalf("the refused message %s reached the policy server's room", refused[0])
			}
		}
	})

	t.Run("inbound_valid", func(t *testing.T) {
		pdu := policy.message(t, srv, room, bob, "signed in advance", policy.key)
		srv.MustSendTransaction(t, deployment, hs1, []json.RawMessage{pdu.JSON()}, nil)
		room.AddEvent(pdu)

		alice.MustSyncUntil(t, client.SyncReq{}, client.SyncTimelineHasEventID(roomID, pdu.EventID()))

		if n := policy.askedFor(pdu.EventID()); n != 0 {
			t.Fatalf("the policy server was asked about a message it had signed, %d times", n)
		}
	})

	t.Run("inbound_missing_signed_on_fetch", func(t *testing.T) {
		pdu := policy.message(t, srv, room, bob, "signed on request", nil)
		srv.MustSendTransaction(t, deployment, hs1, []json.RawMessage{pdu.JSON()}, nil)
		room.AddEvent(pdu)

		alice.MustSyncUntil(t, client.SyncReq{}, client.SyncTimelineHasEventID(roomID, pdu.EventID()))

		if n := policy.askedFor(pdu.EventID()); n != 1 {
			t.Fatalf("the policy server was asked about the unsigned message %d times, want 1", n)
		}
	})

	t.Run("inbound_missing_refused_on_fetch", func(t *testing.T) {
		since := alice.MustSyncUntil(t, client.SyncReq{}, client.SyncJoinedTo(alice.UserID, roomID))
		pdu := policy.message(t, srv, room, bob, "unsigned spam", nil)
		srv.MustSendTransaction(t, deployment, hs1, []json.RawMessage{pdu.JSON()}, nil)

		mustNotSee(t, alice, policy, srv, deployment, room, since, pdu.EventID())

		if n := policy.askedFor(pdu.EventID()); n != 1 {
			t.Fatalf("the policy server was asked about the unsigned spam %d times, want 1", n)
		}
	})

	t.Run("inbound_bad_signature_refetched", func(t *testing.T) {
		since := alice.MustSyncUntil(t, client.SyncReq{}, client.SyncJoinedTo(alice.UserID, roomID))
		pdu := policy.message(t, srv, room, bob, "signed with the wrong key", policy.wrongKey)
		srv.MustSendTransaction(t, deployment, hs1, []json.RawMessage{pdu.JSON()}, nil)

		mustNotSee(t, alice, policy, srv, deployment, room, since, pdu.EventID())

		// The failed signature is asked about again, and fails again.
		if n := policy.askedFor(pdu.EventID()); n != 1 {
			t.Fatalf("the policy server was asked about the badly signed message %d times, want 1", n)
		}
	})
}

func newPolicyServer(t *testing.T) *policyServer {
	t.Helper()

	public, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("failed to generate the policy key: %s", err)
	}

	_, wrongKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("failed to generate the wrong key: %s", err)
	}

	return &policyServer{key: key, wrongKey: wrongKey, public: public}
}

func (p *policyServer) setVersion(version gomatrixserverlib.RoomVersion) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.version = version
}

// sign answers POST /_matrix/policy/v1/sign, whose body is the event as
// federated.
func (p *policyServer) sign(fr *fclient.FederationRequest, _ map[string]string) util.JSONResponse {
	p.mu.Lock()
	version := p.version
	p.mu.Unlock()

	verImpl, err := gomatrixserverlib.GetRoomVersion(version)
	if err != nil {
		return util.JSONResponse{Code: 400, JSON: spec.Unknown(err.Error())}
	}

	pdu, err := verImpl.NewEventFromUntrustedJSON(fr.Content())
	if err != nil {
		return util.JSONResponse{Code: 400, JSON: spec.BadJSON(err.Error())}
	}

	body := gjson.GetBytes(pdu.Content(), "body").Str
	p.record(pdu.EventID(), body)

	switch {
	case strings.Contains(body, "spam"):
		return util.JSONResponse{Code: 400, JSON: spec.Forbidden("The policy server refuses this message.")}

	case strings.Contains(body, "wrong key"):
		return p.signature(pdu, p.wrongKey)

	default:
		return p.signature(pdu, p.key)
	}
}

// signature answers with the event's policy signature under the given key.
func (p *policyServer) signature(pdu gomatrixserverlib.PDU, key ed25519.PrivateKey) util.JSONResponse {
	sig, err := p.signatureOf(pdu.Sign(string(p.name), policyKeyID, key))
	if err != nil {
		return util.JSONResponse{Code: 500, JSON: spec.Unknown(err.Error())}
	}

	return util.JSONResponse{
		Code: 200,
		JSON: map[string]map[string]string{
			string(p.name): {string(policyKeyID): sig},
		},
	}
}

// signatureOf reads the policy server's signature out of a signed event.
func (p *policyServer) signatureOf(pdu gomatrixserverlib.PDU) (string, error) {
	var fields struct {
		Signatures map[string]map[string]string `json:"signatures"`
	}

	if err := json.Unmarshal(pdu.JSON(), &fields); err != nil {
		return "", err
	}

	sig, ok := fields.Signatures[string(p.name)][string(policyKeyID)]
	if !ok {
		return "", fmt.Errorf("no %s signature from %s", policyKeyID, p.name)
	}

	return sig, nil
}

// verify checks the event carries a valid signature under the room's policy key.
func (p *policyServer) verify(pdu gomatrixserverlib.PDU) error {
	verImpl, err := gomatrixserverlib.GetRoomVersion(pdu.Version())
	if err != nil {
		return err
	}

	redacted, err := verImpl.RedactEventJSON(pdu.JSON())
	if err != nil {
		return err
	}

	return gomatrixserverlib.VerifyJSON(string(p.name), policyKeyID, p.public, redacted)
}

func (p *policyServer) record(eventID, body string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.asked = append(p.asked, askedEvent{eventID: eventID, body: body})
}

func (p *policyServer) askedFor(eventID string) (n int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, asked := range p.asked {
		if asked.eventID == eventID {
			n++
		}
	}

	return n
}

// askedAbout lists the IDs of the events with this body the policy server was
// asked about, one entry per request.
func (p *policyServer) askedAbout(body string) (eventIDs []string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, asked := range p.asked {
		if asked.body == body {
			eventIDs = append(eventIDs, asked.eventID)
		}
	}

	return eventIDs
}

// message builds Bob's next message in the room, signed with the given policy
// key, or unsigned by the policy server when there is none.
func (p *policyServer) message(
	t *testing.T,
	srv *federation.Server,
	room *federation.ServerRoom,
	sender, body string,
	key ed25519.PrivateKey,
) gomatrixserverlib.PDU {
	t.Helper()

	pdu := srv.MustCreateEvent(t, room, federation.Event{
		Type:    "m.room.message",
		Sender:  sender,
		Content: text(body).Content,
	})

	if key == nil {
		return pdu
	}

	return pdu.Sign(string(p.name), policyKeyID, key)
}

// mustNotSee bounds the wait for an event Alice must not see with a later
// message the policy server signed, which she does see.
func mustNotSee(
	t *testing.T,
	alice *client.CSAPI,
	policy *policyServer,
	srv *federation.Server,
	deployment complement.Deployment,
	room *federation.ServerRoom,
	since, hidden string,
) {
	t.Helper()

	sentinel := policy.message(t, srv, room, srv.UserID("bob"), "sentinel", policy.key)
	srv.MustSendTransaction(t, deployment, deployment.GetFullyQualifiedHomeserverName(t, "hs1"),
		[]json.RawMessage{sentinel.JSON()}, nil)
	room.AddEvent(sentinel)

	// Every event of every response is looked at, so the hidden event is caught
	// even if it arrives in the same response as the sentinel, or after it.
	seen := false
	events := "rooms.join." + client.GjsonEscape(room.RoomID) + ".timeline.events"
	alice.MustSyncUntil(t, client.SyncReq{Since: since}, func(_ string, sync gjson.Result) error {
		arrived := false
		sync.Get(events).ForEach(func(_, ev gjson.Result) bool {
			eventID := ev.Get("event_id").Str
			seen = seen || eventID == hidden
			arrived = arrived || eventID == sentinel.EventID()

			return true
		})

		if !arrived {
			return fmt.Errorf("the sentinel %s has not arrived", sentinel.EventID())
		}

		return nil
	})

	if seen {
		t.Fatalf("Alice saw the event %s, which the policy server did not sign", hidden)
	}
}

// timeline copies the room's timeline, which incoming transactions append to
// concurrently.
func timeline(room *federation.ServerRoom) []gomatrixserverlib.PDU {
	room.TimelineMutex.RLock()
	defer room.TimelineMutex.RUnlock()

	return append([]gomatrixserverlib.PDU(nil), room.Timeline...)
}

func text(body string) b.Event {
	return b.Event{
		Type: "m.room.message",
		Content: map[string]interface{}{
			"msgtype": "m.text",
			"body":    body,
		},
	}
}
