package tests

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
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
	"github.com/matrix-org/complement/must"
)

// The key ID the room's policy server signs under.
const policyKeyID gomatrixserverlib.KeyID = "ed25519:policy_server"

// TestMSC4354OldStickyEventsOverFederation checks how a server that has just
// joined a room treats events from before its join, pushed to it over /send
// with their predecessors missing.
//
// Such an event is dropped as old unless it is a sticky event that has not
// expired: that one is accepted, while the predecessors fetched to fill its
// gap stay out of the timeline. A second old sticky event is accepted the same
// way. An expired sticky event, a sticky event from a sender who is not in the
// room, and a sticky event the room's policy server refuses are not accepted.
// The events fetched to fill the gaps are counted in the log.
func TestMSC4354OldStickyEventsOverFederation(t *testing.T) {
	deployment := complement.Deploy(t, 1)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	remote := newResident(t, deployment)
	cancel := remote.srv.Listen()
	defer cancel()

	charlie := remote.srv.UserID("charlie")
	ver := alice.GetDefaultRoomVersion(t)
	via := []spec.ServerName{remote.srv.ServerName()}

	t.Run("gate", func(t *testing.T) {
		room := remote.makeRoom(t, ver, charlie)
		past := time.Now().Add(-time.Minute)

		// One line of history, all dated before Alice's join. Each event pushed
		// below follows plain messages the homeserver has never accepted, so
		// each arrives with its predecessors missing.
		early := remote.stickyEvent(t, room, charlie, "the first sticky event", stickyDuration, past)
		room.AddEvent(early)
		run := remote.messages(t, room, charlie, 5)
		late := remote.stickyEvent(t, room, charlie, "the second sticky event", stickyDuration, past.Add(time.Second))
		room.AddEvent(late)
		run = append(run, remote.messages(t, room, charlie, 3)...)
		expired := remote.stickyEvent(t, room, charlie, "an expired sticky event", time.Second, past.Add(2*time.Second))
		room.AddEvent(expired)
		run = append(run, remote.messages(t, room, charlie, 3)...)
		plain := remote.message(t, room, charlie, "a plain message from before the join")
		run = append(run, remote.messages(t, room, charlie, 3)...)
		mallory := remote.srv.UserID("mallory")
		stranger := remote.stickyEvent(t, room, mallory, "a stranger's sticky event", stickyDuration, past.Add(3*time.Second))
		room.AddEvent(stranger)
		run = append(run, remote.messages(t, room, charlie, 2)...)

		alice.MustJoinRoom(t, room.RoomID, via)
		view := newObserver(alice, room.RoomID, false)
		view.sync(t)

		// The later sticky event goes first, so filling its gap fetches the
		// earlier one as a predecessor before it is pushed in its own right.
		for _, pdu := range []gomatrixserverlib.PDU{late, early} {
			if refused := remote.push(t, deployment, pdu); refused != "" {
				t.Fatalf("the homeserver refused the sticky event %s: %s", pdu.EventID(), refused)
			}
		}

		for _, pdu := range []gomatrixserverlib.PDU{expired, plain, stranger} {
			refused := remote.push(t, deployment, pdu)
			t.Logf("pushed %s, answered %q", pdu.EventID(), refused)
		}

		stop := remote.message(t, room, charlie, "after the join")
		if refused := remote.push(t, deployment, stop); refused != "" {
			t.Fatalf("the homeserver refused the message %s: %s", stop.EventID(), refused)
		}

		view.waitFor(t, early.EventID(), late.EventID(), stop.EventID())
		view.mustBeSticky(t, early.EventID(), late.EventID())
		view.mustNotHaveSeen(t, "a predecessor fetched to fill a gap", eventIDsOf(run)...)
		view.mustNotHaveSeen(t, "a plain message from before the join", plain.EventID())
		view.mustNotHaveSeen(t, "an expired sticky event", expired.EventID())
		view.mustNotHaveSeen(t, "the sticky event of a sender not in the room", stranger.EventID())

		fetches, fetched := remote.gapFills()
		t.Logf("filling the gaps took %d /get_missing_events requests serving %d events", fetches, fetched)
	})

	t.Run("policy_refused", func(t *testing.T) {
		room := remote.makeRoom(t, ver, charlie, remote.policyEvent(charlie))
		past := time.Now().Add(-time.Minute)

		spam := remote.stickyEvent(t, room, charlie, "sticky spam", stickyDuration, past)
		room.AddEvent(spam)
		remote.messages(t, room, charlie, 2)

		alice.MustJoinRoom(t, room.RoomID, via)
		view := newObserver(alice, room.RoomID, false)
		view.sync(t)

		refused := remote.push(t, deployment, spam)
		t.Logf("pushed %s, answered %q", spam.EventID(), refused)

		stop := remote.policySigned(remote.message(t, room, charlie, "after the join"))
		if refused := remote.push(t, deployment, stop); refused != "" {
			t.Fatalf("the homeserver refused the message %s: %s", stop.EventID(), refused)
		}

		view.waitFor(t, stop.EventID())
		view.mustNotHaveSeen(t, "a sticky event the policy server refused", spam.EventID())

		// Asking proves the event passed the gate for old events, so the
		// refusal is what kept it out.
		if n := remote.askedFor(spam.EventID()); n == 0 {
			t.Fatalf("the policy server was never asked about the sticky spam %s", spam.EventID())
		}
	})
}

// TestMSC4354SoftFailedStickyEventPromoted checks that a sticky event soft
// failed against the room's current state is accepted once that state changes
// to allow it, unless it was redacted meanwhile.
//
// Bob's server builds two sticky events while Bob is joined, and delivers them
// only after Alice has kicked him, as though they had crossed the kick in
// flight: both pass against the state before them and fail against the
// current state. Alice redacts the second. When Bob joins again the first must
// reach Alice and the second must not.
func TestMSC4354SoftFailedStickyEventPromoted(t *testing.T) {
	deployment := complement.Deploy(t, 1)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	hs1 := deployment.GetFullyQualifiedHomeserverName(t, "hs1")
	remote := newResident(t, deployment)
	cancel := remote.srv.Listen()
	defer cancel()

	bob := remote.srv.UserID("bob")
	charlie := remote.srv.UserID("charlie")

	roomID := alice.MustCreateRoom(t, map[string]interface{}{"preset": "public_chat"})
	remote.srv.MustJoinRoom(t, deployment, hs1, roomID, bob)
	room := remote.srv.MustJoinRoom(t, deployment, hs1, roomID, charlie)
	alice.MustSyncUntil(t, client.SyncReq{}, client.SyncJoinedTo(charlie, roomID))

	// Siblings, both built on the state in which Bob is joined. The resident
	// does not record them, so its later events cite only what the homeserver
	// has accepted.
	now := time.Now()
	promoted := remote.stickyEvent(t, room, bob, "crossed the kick", stickyDuration, now)
	redacted := remote.stickyEvent(t, room, bob, "crossed the kick, then redacted", stickyDuration, now)

	alice.MustDo(t, "POST", []string{"_matrix", "client", "v3", "rooms", roomID, "kick"},
		client.WithJSONBody(t, map[string]interface{}{"user_id": bob}),
	)
	since := alice.MustSyncUntil(t, client.SyncReq{}, client.SyncLeftFrom(bob, roomID))

	for _, pdu := range []gomatrixserverlib.PDU{promoted, redacted} {
		refused := remote.push(t, deployment, pdu)
		t.Logf("pushed %s, answered %q", pdu.EventID(), refused)
	}

	view := newObserver(alice, roomID, false)
	view.since = since

	// A later message from Charlie bounds the wait: neither sticky event may
	// be visible while Bob is out of the room.
	sentinel := remote.message(t, room, charlie, "after the kick")
	if refused := remote.push(t, deployment, sentinel); refused != "" {
		t.Fatalf("the homeserver refused the message %s: %s", sentinel.EventID(), refused)
	}

	view.waitFor(t, sentinel.EventID())
	view.mustNotHaveSeen(t, "a sticky event soft failed against the current state", promoted.EventID(), redacted.EventID())

	alice.MustDo(t, "PUT",
		[]string{"_matrix", "client", "v3", "rooms", roomID, "redact", redacted.EventID(), "msc4354-redaction"},
		client.WithJSONBody(t, map[string]interface{}{}),
	)

	rejoined := remote.srv.MustJoinRoom(t, deployment, hs1, roomID, bob)
	view.waitFor(t, promoted.EventID())
	view.mustBeSticky(t, promoted.EventID())

	// The redacted candidate would have been promoted with the other one, so
	// a later message bounds the wait for it.
	stop := remote.message(t, rejoined, charlie, "after the rejoin")
	if refused := remote.push(t, deployment, stop); refused != "" {
		t.Fatalf("the homeserver refused the message %s: %s", stop.EventID(), refused)
	}

	view.waitFor(t, stop.EventID())
	view.mustNotHaveSeen(t, "a soft failed sticky event redacted before the state allowed it", redacted.EventID())
}

// resident plays the only server of the rooms it makes, from the Complement
// federation server: it fills gaps, answers state lookups and backfill from
// those rooms, and is the policy server of any room that names it.
type resident struct {
	srv    *federation.Server
	public ed25519.PublicKey
	key    ed25519.PrivateKey

	mu      sync.Mutex
	rooms   map[string]*federation.ServerRoom
	fetches int
	fetched int
	asked   []string
}

func newResident(t *testing.T, deployment complement.Deployment) *resident {
	t.Helper()

	public, key, err := ed25519.GenerateKey(nil)
	must.NotError(t, "failed to generate the policy key", err)

	r := &resident{
		public: public,
		key:    key,
		rooms:  map[string]*federation.ServerRoom{},
	}

	r.srv = federation.NewServer(t, deployment,
		federation.HandleKeyRequests(),
		federation.HandleMakeSendJoinRequests(),
		federation.HandleTransactionRequests(nil, nil),
		federation.HandleEventRequests(),
		federation.HandleEventAuthRequests(),
	)

	// Beside the room's traffic, the homeserver may ask a new peer for
	// profiles and devices, which these tests have no answer for.
	r.srv.UnexpectedRequestsAreErrors = false

	routes := r.srv.Mux()
	routes.HandleFunc("/_matrix/federation/v1/get_missing_events/{roomID}", r.missingEvents(t)).Methods("POST")
	routes.HandleFunc("/_matrix/federation/v1/state_ids/{roomID}", r.stateIDs(t)).Methods("GET")
	routes.HandleFunc("/_matrix/federation/v1/backfill/{roomID}", r.backfill(t)).Methods("GET")
	routes.Handle("/_matrix/policy/v1/sign", r.srv.ValidFederationRequest(t, r.sign)).Methods("POST")

	return r
}

// makeRoom makes a public room with the given state beside the initial events.
func (r *resident) makeRoom(t *testing.T, ver gomatrixserverlib.RoomVersion, creator string, state ...federation.Event) *federation.ServerRoom {
	t.Helper()

	room := r.srv.MustMakeRoom(t, ver, append(federation.InitialRoomEvents(ver, creator), state...))

	r.mu.Lock()
	defer r.mu.Unlock()

	r.rooms[room.RoomID] = room
	return room
}

func (r *resident) room(roomID string) *federation.ServerRoom {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.rooms[roomID]
}

// message builds and records the sender's next plain message in the room.
func (r *resident) message(t *testing.T, room *federation.ServerRoom, sender, body string) gomatrixserverlib.PDU {
	t.Helper()

	pdu := r.srv.MustCreateEvent(t, room, federation.Event{
		Type:    "m.room.message",
		Sender:  sender,
		Content: text(body).Content,
	})

	room.AddEvent(pdu)
	return pdu
}

// messages builds and records the sender's next n plain messages in the room.
func (r *resident) messages(t *testing.T, room *federation.ServerRoom, sender string, n int) []gomatrixserverlib.PDU {
	t.Helper()

	pdus := make([]gomatrixserverlib.PDU, n)
	for i := range pdus {
		pdus[i] = r.message(t, room, sender, fmt.Sprintf("plain message %d", i))
	}

	return pdus
}

// stickyEvent builds the sender's next message in the room, sticky for the
// duration and dated at ts, without recording it in the room. The sticky
// object sits at the top level of the PDU, out of the event builder's reach,
// so the content hash and the resident's signature are made again over the
// amended event.
func (r *resident) stickyEvent(
	t *testing.T,
	room *federation.ServerRoom,
	sender, body string,
	duration time.Duration,
	ts time.Time,
) gomatrixserverlib.PDU {
	t.Helper()

	built := r.srv.MustCreateEvent(t, room, federation.Event{
		Type:    "m.room.message",
		Sender:  sender,
		Content: text(body).Content,
	})

	var fields map[string]interface{}
	decoder := json.NewDecoder(bytes.NewReader(built.JSON()))
	decoder.UseNumber()
	must.NotError(t, "failed to read the built event", decoder.Decode(&fields))

	delete(fields, "hashes")
	delete(fields, "signatures")
	delete(fields, "unsigned")
	fields["origin_server_ts"] = spec.AsTimestamp(ts)
	fields[stickyKey] = map[string]interface{}{"duration_ms": duration.Milliseconds()}

	unhashed, err := json.Marshal(fields)
	must.NotError(t, "failed to write the sticky event", err)
	canonical, err := gomatrixserverlib.CanonicalJSON(unhashed)
	must.NotError(t, "failed to canonicalise the sticky event", err)
	sum := sha256.Sum256(canonical)
	fields["hashes"] = map[string]string{"sha256": spec.Base64Bytes(sum[:]).Encode()}

	verImpl := gomatrixserverlib.MustGetRoomVersion(room.Version)
	hashed, err := json.Marshal(fields)
	must.NotError(t, "failed to write the hashed sticky event", err)
	redacted, err := verImpl.RedactEventJSON(hashed)
	must.NotError(t, "failed to redact the sticky event", err)
	signed, err := gomatrixserverlib.SignJSON(string(r.srv.ServerName()), r.srv.KeyID, r.srv.Priv, redacted)
	must.NotError(t, "failed to sign the sticky event", err)
	fields["signatures"] = json.RawMessage(gjson.GetBytes(signed, "signatures").Raw)

	full, err := json.Marshal(fields)
	must.NotError(t, "failed to write the signed sticky event", err)
	pdu, err := verImpl.NewEventFromTrustedJSON(full, false)
	must.NotError(t, "failed to load the signed sticky event", err)

	return pdu
}

// push sends the event to the homeserver in a transaction of its own, and
// returns the error the homeserver answered for it, if any.
func (r *resident) push(t *testing.T, deployment complement.Deployment, pdu gomatrixserverlib.PDU) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	res, err := r.srv.FederationClient(deployment).SendTransaction(ctx, gomatrixserverlib.Transaction{
		TransactionID: gomatrixserverlib.TransactionID(fmt.Sprintf("msc4354-%d", txnCounter.Add(1))),
		Origin:        r.srv.ServerName(),
		Destination:   deployment.GetFullyQualifiedHomeserverName(t, "hs1"),
		PDUs:          []json.RawMessage{pdu.JSON()},
	})
	must.NotError(t, "failed to send a transaction", err)

	return res.PDUs[pdu.EventID()].Error
}

// missingEvents answers /get_missing_events by walking back from the latest
// events, short of the earliest ones, nearest first and up to the limit, and
// serves what it found oldest first.
func (r *resident) missingEvents(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		room := r.room(mux.Vars(req)["roomID"])
		if room == nil {
			w.WriteHeader(404)
			return
		}

		body := must.ParseJSON(t, req.Body)
		limit := int(body.Get("limit").Int())
		if limit <= 0 {
			limit = 10
		}

		seen := map[string]bool{}
		for _, id := range body.Get("earliest_events").Array() {
			seen[id.Str] = true
		}

		var queue []string
		for _, id := range body.Get("latest_events").Array() {
			seen[id.Str] = true
			if pdu, ok := room.GetEventInTimeline(id.Str); ok {
				queue = append(queue, pdu.PrevEventIDs()...)
			}
		}

		var found []gomatrixserverlib.PDU
		for len(queue) > 0 && len(found) < limit {
			id := queue[0]
			queue = queue[1:]
			if seen[id] {
				continue
			}

			seen[id] = true
			if pdu, ok := room.GetEventInTimeline(id); ok {
				found = append(found, pdu)
				queue = append(queue, pdu.PrevEventIDs()...)
			}
		}

		sort.Slice(found, func(i, j int) bool { return found[i].Depth() < found[j].Depth() })
		events := make([]json.RawMessage, len(found))
		for i, pdu := range found {
			events[i] = pdu.JSON()
		}

		r.mu.Lock()
		r.fetches++
		r.fetched += len(found)
		r.mu.Unlock()

		respondJSON(t, w, map[string]interface{}{"events": events})
	}
}

// stateIDs answers /state_ids with the room's state before the event. Every
// room the resident makes has a single line of history, so that is the state
// its timeline has built up to the event.
func (r *resident) stateIDs(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		room := r.room(mux.Vars(req)["roomID"])
		if room == nil {
			w.WriteHeader(404)
			return
		}

		eventID := req.URL.Query().Get("event_id")
		state := map[[2]string]gomatrixserverlib.PDU{}
		found := false
		for _, pdu := range timeline(room) {
			if pdu.EventID() == eventID {
				found = true
				break
			}

			if pdu.StateKey() != nil {
				state[[2]string{pdu.Type(), *pdu.StateKey()}] = pdu
			}
		}

		if !found {
			w.WriteHeader(404)
			return
		}

		events := make([]gomatrixserverlib.PDU, 0, len(state))
		for _, pdu := range state {
			events = append(events, pdu)
		}

		respondJSON(t, w, map[string]interface{}{
			"pdu_ids":        eventIDsOf(events),
			"auth_chain_ids": eventIDsOf(room.AuthChainForEvents(events)),
		})
	}
}

// backfill answers emptily: history before the join is not under test, and
// only the pushed events may reach the homeserver's timeline.
func (r *resident) backfill(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		respondJSON(t, w, map[string]interface{}{
			"origin":           r.srv.ServerName(),
			"origin_server_ts": spec.AsTimestamp(time.Now()),
			"pdus":             []json.RawMessage{},
		})
	}
}

// policyEvent names the resident as the room's policy server.
func (r *resident) policyEvent(sender string) federation.Event {
	return federation.Event{
		Type:     "m.room.policy",
		StateKey: b.Ptr(""),
		Sender:   sender,
		Content: map[string]interface{}{
			"via": string(r.srv.ServerName()),
			"public_keys": map[string]interface{}{
				"ed25519": spec.Base64Bytes(r.public).Encode(),
			},
		},
	}
}

// policySigned adds the policy server's signature to the room's latest event,
// which it replaces.
func (r *resident) policySigned(pdu gomatrixserverlib.PDU) gomatrixserverlib.PDU {
	signed := pdu.Sign(string(r.srv.ServerName()), policyKeyID, r.key)

	room := r.room(pdu.RoomID().String())
	room.TimelineMutex.Lock()
	defer room.TimelineMutex.Unlock()

	room.Timeline[len(room.Timeline)-1] = signed
	return signed
}

// sign answers POST /_matrix/policy/v1/sign: it refuses any message whose body
// mentions spam, and signs every other event.
func (r *resident) sign(fr *fclient.FederationRequest, _ map[string]string) util.JSONResponse {
	room := r.room(gjson.GetBytes(fr.Content(), "room_id").Str)
	if room == nil {
		return util.JSONResponse{Code: 404, JSON: spec.NotFound("not a room of this policy server")}
	}

	verImpl, err := gomatrixserverlib.GetRoomVersion(room.Version)
	if err != nil {
		return util.JSONResponse{Code: 400, JSON: spec.Unknown(err.Error())}
	}

	pdu, err := verImpl.NewEventFromUntrustedJSON(fr.Content())
	if err != nil {
		return util.JSONResponse{Code: 400, JSON: spec.BadJSON(err.Error())}
	}

	r.mu.Lock()
	r.asked = append(r.asked, pdu.EventID())
	r.mu.Unlock()

	if strings.Contains(gjson.GetBytes(pdu.Content(), "body").Str, "spam") {
		return util.JSONResponse{Code: 400, JSON: spec.Forbidden("The policy server refuses this message.")}
	}

	name := string(r.srv.ServerName())
	sig := gjson.GetBytes(pdu.Sign(name, policyKeyID, r.key).JSON(), "signatures."+gjson.Escape(name)+"."+gjson.Escape(string(policyKeyID)))
	if !sig.Exists() {
		return util.JSONResponse{Code: 500, JSON: spec.Unknown("the policy signature is missing")}
	}

	return util.JSONResponse{
		Code: 200,
		JSON: map[string]map[string]string{
			name: {string(policyKeyID): sig.Str},
		},
	}
}

func (r *resident) askedFor(eventID string) (n int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, asked := range r.asked {
		if asked == eventID {
			n++
		}
	}

	return n
}

func (r *resident) gapFills() (fetches, fetched int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.fetches, r.fetched
}

// timeline copies the room's timeline, which incoming transactions append to
// concurrently.
func timeline(room *federation.ServerRoom) []gomatrixserverlib.PDU {
	room.TimelineMutex.RLock()
	defer room.TimelineMutex.RUnlock()

	return append([]gomatrixserverlib.PDU(nil), room.Timeline...)
}

func eventIDsOf(pdus []gomatrixserverlib.PDU) []string {
	eventIDs := make([]string, len(pdus))
	for i, pdu := range pdus {
		eventIDs[i] = pdu.EventID()
	}

	return eventIDs
}

func respondJSON(t *testing.T, w http.ResponseWriter, body interface{}) {
	raw, err := json.Marshal(body)
	must.NotError(t, "failed to marshal response", err)
	w.WriteHeader(200)
	w.Write(raw)
}
