package tests

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/matrix-org/complement"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/tidwall/gjson"

	"github.com/matrix-org/complement/client"
	"github.com/matrix-org/complement/federation"
	"github.com/matrix-org/complement/helpers"
	"github.com/matrix-org/complement/match"
	"github.com/matrix-org/complement/must"
)

// These tests cover state resolution at incoming federation events whose
// prev_events are stored but not yet resolved. A homeserver that already
// holds the entire ancestry of an incoming event, through gap filling or
// earlier deliveries, must derive the state at that event locally: it must
// not need /state_ids or /state, and an ancestor that is valid against the
// auth_events it cites but unauthorized at its position in the DAG must not
// leak into the room state while events built on top of it integrate.

// gapFillServer wires the Complement federation server shared by the tests in
// this file. It serves keys, joins, single events and transactions, answers
// /get_missing_events per room from a table the test fills in, tolerates
// /backfill, and fails the test on any /state_ids or /state request while
// still serving a valid answer, so a server that does fetch keeps running and
// the state assertions afterwards expose what the fetched claim did.
type gapFillServer struct {
	srv *federation.Server

	// mu guards rooms and missingEvents. The handlers registered below run on
	// the federation server's goroutines, which overlap with the tests still
	// building rooms and installing responders, so both maps are only reached
	// through the accessors underneath.
	mu            sync.RWMutex
	rooms         map[string]*federation.ServerRoom
	missingEvents map[string]http.HandlerFunc
}

// setRoom records a fixture room so the handlers can answer for it; room looks
// one up, returning nil for a room the tests never registered.
func (gs *gapFillServer) setRoom(room *federation.ServerRoom) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	gs.rooms[room.RoomID] = room
}

func (gs *gapFillServer) room(roomID string) *federation.ServerRoom {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	return gs.rooms[roomID]
}

// setMissingEvents installs the /get_missing_events responder for a room;
// missingEventsHandler returns it, or nil when no gap was prepared.
func (gs *gapFillServer) setMissingEvents(roomID string, handler http.HandlerFunc) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	gs.missingEvents[roomID] = handler
}

func (gs *gapFillServer) missingEventsHandler(roomID string) http.HandlerFunc {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	return gs.missingEvents[roomID]
}

func newGapFillServer(t *testing.T, deployment complement.Deployment) (*gapFillServer, func()) {
	gs := &gapFillServer{
		rooms:         make(map[string]*federation.ServerRoom),
		missingEvents: make(map[string]http.HandlerFunc),
	}
	gs.srv = federation.NewServer(t, deployment,
		federation.HandleKeyRequests(),
		federation.HandleMakeSendJoinRequests(),
		federation.HandleTransactionRequests(nil, nil),
		federation.HandleEventRequests(),
		federation.HandleEventAuthRequests(),
	)
	gs.srv.Mux().HandleFunc("/_matrix/federation/v1/get_missing_events/{roomID}", func(w http.ResponseWriter, req *http.Request) {
		roomID := mux.Vars(req)["roomID"]
		if handler := gs.missingEventsHandler(roomID); handler != nil {
			handler(w, req)
			return
		}
		t.Errorf("received /get_missing_events for room %s without a prepared gap", roomID)
		w.WriteHeader(404)
		w.Write([]byte("complement: no missing events prepared for this room"))
	}).Methods("POST")
	gs.srv.Mux().HandleFunc("/_matrix/federation/v1/state_ids/{roomID}", func(w http.ResponseWriter, req *http.Request) {
		roomID := mux.Vars(req)["roomID"]
		t.Errorf("received /state_ids for room %s at event %q: the ancestry is fully known to the homeserver", roomID, req.URL.Query().Get("event_id"))
		room := gs.room(roomID)
		if room == nil {
			w.WriteHeader(404)
			w.Write([]byte("complement: unknown room"))
			return
		}
		state := room.AllCurrentState()
		respondJSON(t, w, map[string]interface{}{
			"auth_chain_ids": eventIDsOf(room.AuthChainForEvents(state)),
			"pdu_ids":        eventIDsOf(state),
		})
	}).Methods("GET")
	gs.srv.Mux().HandleFunc("/_matrix/federation/v1/state/{roomID}", func(w http.ResponseWriter, req *http.Request) {
		roomID := mux.Vars(req)["roomID"]
		t.Errorf("received /state for room %s at event %q: the ancestry is fully known to the homeserver", roomID, req.URL.Query().Get("event_id"))
		room := gs.room(roomID)
		if room == nil {
			w.WriteHeader(404)
			w.Write([]byte("complement: unknown room"))
			return
		}
		state := room.AllCurrentState()
		respondJSON(t, w, map[string]interface{}{
			"auth_chain": gomatrixserverlib.NewEventJSONsFromEvents(room.AuthChainForEvents(state)),
			"pdus":       gomatrixserverlib.NewEventJSONsFromEvents(state),
		})
	}).Methods("GET")
	// Backfill is not part of the assertion surface; answer emptily so a
	// /messages walk past the join boundary cannot trip the 404 guard.
	gs.srv.Mux().HandleFunc("/_matrix/federation/v1/backfill/{roomID}", func(w http.ResponseWriter, req *http.Request) {
		respondJSON(t, w, map[string]interface{}{
			"origin":           gs.srv.ServerName(),
			"origin_server_ts": spec.AsTimestamp(time.Now()),
			"pdus":             []json.RawMessage{},
		})
	}).Methods("GET")
	cancel := gs.srv.Listen()
	return gs, cancel
}

// makeGapFillRoom creates a room on the Complement server with charlie as its
// creator and mallory joined, then joins the given homeserver user to it over
// federation, so later events by either Complement user authenticate against
// state the homeserver received in the join response.
func (gs *gapFillServer) makeGapFillRoom(t *testing.T, joiner *client.CSAPI) (room *federation.ServerRoom, charlie, mallory string) {
	charlie = gs.srv.UserID("charlie")
	mallory = gs.srv.UserID("mallory")
	ver := joiner.GetDefaultRoomVersion(t)
	room = gs.srv.MustMakeRoom(t, ver, federation.InitialRoomEvents(ver, charlie))
	gs.setRoom(room)
	malloryJoin := gs.srv.MustCreateEvent(t, room, federation.Event{
		Type:     spec.MRoomMember,
		StateKey: &mallory,
		Sender:   mallory,
		Content:  map[string]interface{}{"membership": "join"},
	})
	room.AddEvent(malloryJoin)
	joiner.MustJoinRoom(t, room.RoomID, []spec.ServerName{gs.srv.ServerName()})
	joiner.MustSyncUntil(t, client.SyncReq{}, client.SyncJoinedTo(joiner.UserID, room.RoomID))
	return room, charlie, mallory
}

// missingEventsResponder answers /get_missing_events with exactly the given
// events. The tests control the gap shape themselves, so the requested window
// is deliberately ignored; anything withheld here is still available through
// the /event fallback.
func missingEventsResponder(t *testing.T, events ...gomatrixserverlib.PDU) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		raws := make([]json.RawMessage, 0, len(events))
		for _, ev := range events {
			raws = append(raws, ev.JSON())
		}
		t.Logf("/get_missing_events: serving %d events", len(raws))
		respondJSON(t, w, map[string]interface{}{"events": raws})
	}
}

func respondJSON(t *testing.T, w http.ResponseWriter, body interface{}) {
	raw, err := json.Marshal(body)
	must.NotError(t, "failed to marshal response", err)
	w.WriteHeader(200)
	w.Write(raw)
}

func eventIDsOf(events []gomatrixserverlib.PDU) []string {
	ids := make([]string, 0, len(events))
	for _, ev := range events {
		ids = append(ids, ev.EventID())
	}
	return ids
}

// waitedMissingEventsResponder is missingEventsResponder with a Waiter that
// finishes once the homeserver has actually asked for the gap. Tests that must
// positively prove gap filling happened wait on it, rather than infer it from
// the absence of a state fetch.
func waitedMissingEventsResponder(t *testing.T, events ...gomatrixserverlib.PDU) (http.HandlerFunc, *helpers.Waiter) {
	respond := missingEventsResponder(t, events...)
	asked := helpers.NewWaiter()
	return func(w http.ResponseWriter, req *http.Request) {
		respond(w, req)
		asked.Finish()
	}, asked
}

// restoreCurrentState overwrites the fixture's tracked current state with the
// given state events. The fixture applies every event handed to it in the
// order it is handed over, so once a test builds a fork whose losing side
// carries state events its view no longer models the resolved state. Restoring
// the winners makes the fixture usable as the oracle the homeserver must agree
// with.
func restoreCurrentState(t *testing.T, room *federation.ServerRoom, events ...gomatrixserverlib.PDU) {
	t.Helper()
	for _, ev := range events {
		if ev.StateKey() == nil {
			t.Fatalf("restoreCurrentState: %s is not a state event", ev.EventID())
		}
		room.ReplaceCurrentState(ev)
	}
}

// stateTupleKey renders a (type, state_key) pair for use as a map key and in
// assertion messages.
func stateTupleKey(evType, stateKey string) string {
	return fmt.Sprintf("%s[%s]", evType, stateKey)
}

// clientState returns the room's complete client-visible state as a map from
// (type, state_key) to event ID.
func clientState(t *testing.T, c *client.CSAPI, roomID string) map[string]string {
	t.Helper()
	res := c.MustDo(t, "GET", []string{"_matrix", "client", "v3", "rooms", roomID, "state"})
	state := make(map[string]string)
	must.ParseJSON(t, res.Body).ForEach(func(_, ev gjson.Result) bool {
		state[stateTupleKey(ev.Get("type").Str, ev.Get("state_key").Str)] = ev.Get("event_id").Str
		return true
	})
	return state
}

// fixtureState is the same map for the Complement server's own view of the
// room, i.e. the state the homeserver is expected to have derived.
func fixtureState(room *federation.ServerRoom) map[string]string {
	state := make(map[string]string)
	for _, ev := range room.AllCurrentState() {
		state[stateTupleKey(ev.Type(), *ev.StateKey())] = ev.EventID()
	}
	return state
}

// timelineEventIDs returns the set of event IDs in the room's client-visible
// timeline, walking backwards from the most recent event. It reads a single
// page of at most 100 events and never follows the pagination token, which is
// sound only because every fixture in this file builds a bounded DAG well
// under that limit. It is not a general-purpose helper: against a longer room
// it would report the newest page as if it were the whole timeline, turning a
// missing event into a silent pass.
func timelineEventIDs(t *testing.T, c *client.CSAPI, roomID string) map[string]bool {
	t.Helper()
	res := c.MustDo(t, "GET",
		[]string{"_matrix", "client", "v3", "rooms", roomID, "messages"},
		client.WithQueries(url.Values{"dir": {"b"}, "limit": {"100"}}),
	)
	seen := make(map[string]bool)
	must.ParseJSON(t, res.Body).Get("chunk").ForEach(func(_, ev gjson.Result) bool {
		seen[ev.Get("event_id").Str] = true
		return true
	})
	return seen
}

// TestGapFillingUnauthorizedStateEvent drives the ancestry shape behind
// federation state resets: a state event that is valid against the
// auth_events it cites but unauthorized at its position in the DAG. When such
// an event arrives through gap filling, the homeserver must accept the events
// built on top of it, must keep it out of the room state, and must not ask
// for /state_ids: it already holds the event's entire ancestry.
//
// DAG, in room order:
//
//	ban    charlie bans mallory
//	evil   mallory updates her own membership, citing the pre-ban room
//	       state in auth_events; served only via /get_missing_events
//	probe  message by charlie with prev_events [evil], delivered via /send
//
// In every arrival order the correct outcome is the same: probe integrates,
// mallory stays banned, and no state fetch happens.
func TestGapFillingUnauthorizedStateEvent(t *testing.T) {
	deployment := complement.Deploy(t, 1)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	alice.SyncUntilTimeout = 30 * time.Second
	hs1 := deployment.GetFullyQualifiedHomeserverName(t, "hs1")
	gs, cancel := newGapFillServer(t, deployment)
	defer cancel()

	buildDAG := func(t *testing.T) (room *federation.ServerRoom, mallory string, ban, evil, probe gomatrixserverlib.PDU) {
		room, charlie, mallory := gs.makeGapFillRoom(t, alice)
		ban = gs.srv.MustCreateEvent(t, room, federation.Event{
			Type:     spec.MRoomMember,
			StateKey: &mallory,
			Sender:   charlie,
			Content:  map[string]interface{}{"membership": "ban", "reason": "unauthorized fold test"},
		})
		// Built before the ban is applied to the tracked room state, so the
		// automatic auth_events selection cites mallory's join: valid against
		// its own auth_events, unauthorized after the ban it descends from.
		evil = gs.srv.MustCreateEvent(t, room, federation.Event{
			Type:       spec.MRoomMember,
			StateKey:   &mallory,
			Sender:     mallory,
			Content:    map[string]interface{}{"membership": "join", "displayname": "evil"},
			PrevEvents: []string{ban.EventID()},
		})
		room.AddEvent(ban)
		room.AddEvent(evil)
		probe = gs.srv.MustCreateEvent(t, room, federation.Event{
			Type:       "m.room.message",
			Sender:     charlie,
			Content:    map[string]interface{}{"msgtype": "m.text", "body": "built on the unauthorized event"},
			PrevEvents: []string{evil.EventID()},
		})
		room.AddEvent(probe)
		return room, mallory, ban, evil, probe
	}

	assertOutcome := func(t *testing.T, room *federation.ServerRoom, mallory string, probe gomatrixserverlib.PDU) {
		alice.MustSyncUntil(t, client.SyncReq{}, client.SyncTimelineHasEventID(room.RoomID, probe.EventID()))
		content := alice.MustGetStateEventContent(t, room.RoomID, spec.MRoomMember, mallory)
		must.MatchGJSON(t, content, match.JSONKeyEqual("membership", "ban"))
		must.Equal(t, content.Get("displayname").Str, "", "the unauthorized membership update reached the room state")
	}

	t.Run("ban delivered first", func(t *testing.T) {
		room, mallory, ban, evil, probe := buildDAG(t)
		gs.setMissingEvents(room.RoomID, missingEventsResponder(t, evil))

		srv := gs.srv
		srv.MustSendTransaction(t, deployment, hs1, []json.RawMessage{ban.JSON()}, nil)
		alice.MustSyncUntil(t, client.SyncReq{}, client.SyncTimelineHasEventID(room.RoomID, ban.EventID()))

		srv.MustSendTransaction(t, deployment, hs1, []json.RawMessage{probe.JSON()}, nil)
		assertOutcome(t, room, mallory, probe)
	})

	t.Run("ban backfilled after probe", func(t *testing.T) {
		room, mallory, ban, evil, probe := buildDAG(t)
		// Only the direct gap is served; the ban is left for the per-event
		// fallback, so it arrives as an outlier below the unauthorized event.
		gs.setMissingEvents(room.RoomID, missingEventsResponder(t, evil))

		srv := gs.srv
		srv.MustSendTransaction(t, deployment, hs1, []json.RawMessage{probe.JSON()}, nil)
		// Delivering the ban afterwards must be a no-op: it was already
		// integrated while filling the gap.
		srv.MustSendTransaction(t, deployment, hs1, []json.RawMessage{ban.JSON()}, nil)
		alice.MustSyncUntil(t, client.SyncReq{}, client.SyncTimelineHasEventID(room.RoomID, ban.EventID()))
		assertOutcome(t, room, mallory, probe)
	})
}

// TestGapFillingAuthorizedStateEvent is the control for
// TestGapFillingUnauthorizedStateEvent: the same DAG shape with an authorized
// membership update in the gap must integrate the update into both timeline
// and state, still without any state fetch. This distinguishes "unauthorized
// ancestors are kept out of state" from "gap-filled ancestors never make it
// into state".
func TestGapFillingAuthorizedStateEvent(t *testing.T) {
	deployment := complement.Deploy(t, 1)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	alice.SyncUntilTimeout = 30 * time.Second
	hs1 := deployment.GetFullyQualifiedHomeserverName(t, "hs1")
	gs, cancel := newGapFillServer(t, deployment)
	defer cancel()

	room, charlie, mallory := gs.makeGapFillRoom(t, alice)
	emptyKey := ""
	rename := gs.srv.MustCreateEvent(t, room, federation.Event{
		Type:     "m.room.name",
		StateKey: &emptyKey,
		Sender:   charlie,
		Content:  map[string]interface{}{"name": "authorized fold control"},
	})
	update := gs.srv.MustCreateEvent(t, room, federation.Event{
		Type:       spec.MRoomMember,
		StateKey:   &mallory,
		Sender:     mallory,
		Content:    map[string]interface{}{"membership": "join", "displayname": "friendly"},
		PrevEvents: []string{rename.EventID()},
	})
	room.AddEvent(rename)
	room.AddEvent(update)
	probe := gs.srv.MustCreateEvent(t, room, federation.Event{
		Type:       "m.room.message",
		Sender:     charlie,
		Content:    map[string]interface{}{"msgtype": "m.text", "body": "built on the authorized event"},
		PrevEvents: []string{update.EventID()},
	})
	room.AddEvent(probe)
	gs.setMissingEvents(room.RoomID, missingEventsResponder(t, update))

	gs.srv.MustSendTransaction(t, deployment, hs1, []json.RawMessage{rename.JSON()}, nil)
	alice.MustSyncUntil(t, client.SyncReq{}, client.SyncTimelineHasEventID(room.RoomID, rename.EventID()))

	gs.srv.MustSendTransaction(t, deployment, hs1, []json.RawMessage{probe.JSON()}, nil)
	alice.MustSyncUntil(t, client.SyncReq{}, client.SyncTimelineHasEventID(room.RoomID, probe.EventID()))
	alice.MustSyncUntil(t, client.SyncReq{}, client.SyncTimelineHasEventID(room.RoomID, update.EventID()))

	content := alice.MustGetStateEventContent(t, room.RoomID, spec.MRoomMember, mallory)
	must.MatchGJSON(t, content,
		match.JSONKeyEqual("membership", "join"),
		match.JSONKeyEqual("displayname", "friendly"),
	)
}

// TestGapFillingDeepChain delivers a chain of thirty events through a single
// /send of its tip: /get_missing_events serves only the ten newest ancestors
// and the rest arrive as per-event fetches, so children land as stored
// outliers before their parents integrate. Mid-chain sits a run of events by
// a banned user, each valid against the pre-ban auth_events it cites but
// unauthorized at its position. The homeserver must integrate every
// authorized event, keep the unauthorized run out of timeline and state, and
// never fetch /state_ids.
func TestGapFillingDeepChain(t *testing.T) {
	deployment := complement.Deploy(t, 1)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	alice.SyncUntilTimeout = 60 * time.Second
	hs1 := deployment.GetFullyQualifiedHomeserverName(t, "hs1")
	gs, cancel := newGapFillServer(t, deployment)
	defer cancel()

	room, charlie, mallory := gs.makeGapFillRoom(t, alice)
	daniel := gs.srv.UserID("daniel")

	var dag []gomatrixserverlib.PDU          // every chain event in room order, tip last
	var unauthorized []gomatrixserverlib.PDU // mallory's post-ban run
	var prev string
	appendMessage := func(sender, body string) gomatrixserverlib.PDU {
		ev := gs.srv.MustCreateEvent(t, room, federation.Event{
			Type:       "m.room.message",
			Sender:     sender,
			Content:    map[string]interface{}{"msgtype": "m.text", "body": body},
			PrevEvents: []string{prev},
		})
		dag = append(dag, ev)
		prev = ev.EventID()
		return ev
	}

	// The head of the chain is built before any of it is applied to the
	// tracked room state, so the automatic auth_events selection for
	// mallory's run cites the pre-ban room state: each event is valid
	// against its own auth_events yet unauthorized where it sits.
	ban := gs.srv.MustCreateEvent(t, room, federation.Event{
		Type:     spec.MRoomMember,
		StateKey: &mallory,
		Sender:   charlie,
		Content:  map[string]interface{}{"membership": "ban", "reason": "deep chain test"},
	})
	dag = append(dag, ban)
	prev = ban.EventID()
	for i := 1; i <= 7; i++ {
		appendMessage(charlie, fmt.Sprintf("before the unauthorized run %d", i))
	}
	rejoin := gs.srv.MustCreateEvent(t, room, federation.Event{
		Type:       spec.MRoomMember,
		StateKey:   &mallory,
		Sender:     mallory,
		Content:    map[string]interface{}{"membership": "join", "displayname": "evil"},
		PrevEvents: []string{prev},
	})
	dag = append(dag, rejoin)
	unauthorized = append(unauthorized, rejoin)
	prev = rejoin.EventID()
	for i := 1; i <= 3; i++ {
		unauthorized = append(unauthorized, appendMessage(mallory, fmt.Sprintf("unauthorized message %d", i)))
	}
	for _, ev := range dag {
		room.AddEvent(ev)
	}

	// The tail is applied as it is built; ordinary auth selection applies.
	danielJoin := gs.srv.MustCreateEvent(t, room, federation.Event{
		Type:       spec.MRoomMember,
		StateKey:   &daniel,
		Sender:     daniel,
		Content:    map[string]interface{}{"membership": "join"},
		PrevEvents: []string{prev},
	})
	room.AddEvent(danielJoin)
	dag = append(dag, danielJoin)
	prev = danielJoin.EventID()
	for i := 1; i <= 17; i++ {
		room.AddEvent(appendMessage(charlie, fmt.Sprintf("after the unauthorized run %d", i)))
	}
	tip := dag[len(dag)-1]

	// Serve only the ten newest ancestors below the tip; the deeper chain is
	// withheld from the batch and arrives through per-event fetches instead.
	gs.setMissingEvents(room.RoomID, missingEventsResponder(t, dag[len(dag)-11:len(dag)-1]...))

	gs.srv.MustSendTransaction(t, deployment, hs1, []json.RawMessage{ban.JSON()}, nil)
	alice.MustSyncUntil(t, client.SyncReq{}, client.SyncTimelineHasEventID(room.RoomID, ban.EventID()))

	gs.srv.MustSendTransaction(t, deployment, hs1, []json.RawMessage{tip.JSON()}, nil)
	alice.MustSyncUntil(t, client.SyncReq{}, client.SyncTimelineHasEventID(room.RoomID, tip.EventID()))

	// The timeline converged on every authorized event and nothing else.
	res := alice.MustDo(t, "GET",
		[]string{"_matrix", "client", "v3", "rooms", room.RoomID, "messages"},
		client.WithQueries(url.Values{"dir": {"b"}, "limit": {"100"}}),
	)
	body := must.ParseJSON(t, res.Body)
	seen := map[string]bool{}
	body.Get("chunk").ForEach(func(_, ev gjson.Result) bool {
		seen[ev.Get("event_id").Str] = true
		return true
	})
	isUnauthorized := map[string]bool{}
	for _, ev := range unauthorized {
		isUnauthorized[ev.EventID()] = true
	}
	for _, ev := range dag {
		switch {
		case isUnauthorized[ev.EventID()] && seen[ev.EventID()]:
			t.Errorf("unauthorized event %s reached the timeline", ev.EventID())
		case !isUnauthorized[ev.EventID()] && !seen[ev.EventID()]:
			t.Errorf("authorized event %s missing from the timeline", ev.EventID())
		}
	}

	content := alice.MustGetStateEventContent(t, room.RoomID, spec.MRoomMember, mallory)
	must.MatchGJSON(t, content, match.JSONKeyEqual("membership", "ban"))
	must.Equal(t, content.Get("displayname").Str, "", "the unauthorized membership update reached the room state")
	content = alice.MustGetStateEventContent(t, room.RoomID, spec.MRoomMember, daniel)
	must.MatchGJSON(t, content, match.JSONKeyEqual("membership", "join"))

	// The unauthorized run stays retrievable as stored events even though it
	// never integrated.
	for _, ev := range unauthorized {
		res := alice.MustDo(t, "GET",
			[]string{"_matrix", "client", "v3", "rooms", room.RoomID, "event", ev.EventID()},
		)
		body := must.ParseJSON(t, res.Body)
		must.Equal(t, body.Get("event_id").Str, ev.EventID(), "unexpected event returned")
	}
}

// TestGapFillingForkResolution fills a gap that contains a real fork. Two
// branches leave the same event, each contributing a state event the other
// branch does not have, and the only event delivered over /send is a merge
// with both branch tips in its prev_events. The homeserver must fill the gap
// through /get_missing_events - proven here by waiting for the request rather
// than inferring it - and must resolve the state at the merge from both
// parents: resolving from either parent alone loses that parent's sibling
// contribution, which the state assertions below would then miss.
//
// DAG, forking at alice's join:
//
//	join -+- name  - chat a -+
//	      +- topic - chat b -+- merge   (delivered via /send)
//
// The two contributions land on different state tuples, so nothing about the
// outcome depends on a timestamp or event ID tie-break.
func TestGapFillingForkResolution(t *testing.T) {
	deployment := complement.Deploy(t, 1)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	alice.SyncUntilTimeout = 30 * time.Second
	hs1 := deployment.GetFullyQualifiedHomeserverName(t, "hs1")
	gs, cancel := newGapFillServer(t, deployment)
	defer cancel()

	room, charlie, _ := gs.makeGapFillRoom(t, alice)
	fork := room.ForwardExtremities[0] // alice's join: the newest event hs1 holds
	emptyKey := ""

	// Branch A contributes the room name.
	name := gs.srv.MustCreateEvent(t, room, federation.Event{
		Type:       "m.room.name",
		StateKey:   &emptyKey,
		Sender:     charlie,
		Content:    map[string]interface{}{"name": "fork branch a"},
		PrevEvents: []string{fork},
	})
	room.AddEvent(name)
	chatA := gs.srv.MustCreateEvent(t, room, federation.Event{
		Type:       "m.room.message",
		Sender:     charlie,
		Content:    map[string]interface{}{"msgtype": "m.text", "body": "branch a"},
		PrevEvents: []string{name.EventID()},
	})
	room.AddEvent(chatA)

	// Branch B leaves the same fork point and contributes the topic.
	topic := gs.srv.MustCreateEvent(t, room, federation.Event{
		Type:       "m.room.topic",
		StateKey:   &emptyKey,
		Sender:     charlie,
		Content:    map[string]interface{}{"topic": "fork branch b"},
		PrevEvents: []string{fork},
	})
	room.AddEvent(topic)
	chatB := gs.srv.MustCreateEvent(t, room, federation.Event{
		Type:       "m.room.message",
		Sender:     charlie,
		Content:    map[string]interface{}{"msgtype": "m.text", "body": "branch b"},
		PrevEvents: []string{topic.EventID()},
	})
	room.AddEvent(chatB)

	merge := gs.srv.MustCreateEvent(t, room, federation.Event{
		Type:       "m.room.message",
		Sender:     charlie,
		Content:    map[string]interface{}{"msgtype": "m.text", "body": "merge of both branches"},
		PrevEvents: []string{chatA.EventID(), chatB.EventID()},
	})
	room.AddEvent(merge)
	must.Equal(t, int(gjson.GetBytes(merge.JSON(), "prev_events.#").Int()), 2,
		"the merge event does not have two parents")

	handler, gapFilled := waitedMissingEventsResponder(t, name, chatA, topic, chatB)
	gs.setMissingEvents(room.RoomID, handler)

	gs.srv.MustSendTransaction(t, deployment, hs1, []json.RawMessage{merge.JSON()}, nil)
	gapFilled.Waitf(t, 30*time.Second, "hs1 never asked for the missing events of room %s", room.RoomID)
	alice.MustSyncUntil(t, client.SyncReq{}, client.SyncTimelineHasEventID(room.RoomID, merge.EventID()))

	// Both branches integrated, so both parents of the merge were walked.
	seen := timelineEventIDs(t, alice, room.RoomID)
	for _, ev := range []gomatrixserverlib.PDU{name, chatA, topic, chatB, merge} {
		if !seen[ev.EventID()] {
			t.Errorf("event %s from the filled gap is missing from the timeline", ev.EventID())
		}
	}

	// Both contributions survived the merge. A server that took branch A alone
	// has no topic, one that took branch B alone has no name, and either way
	// one of these lookups fails.
	must.MatchGJSON(t, alice.MustGetStateEventContent(t, room.RoomID, "m.room.name", ""),
		match.JSONKeyEqual("name", "fork branch a"))
	must.MatchGJSON(t, alice.MustGetStateEventContent(t, room.RoomID, "m.room.topic", ""),
		match.JSONKeyEqual("topic", "fork branch b"))
	state := clientState(t, alice, room.RoomID)
	must.Equal(t, state[stateTupleKey("m.room.name", "")], name.EventID(),
		"branch a's state event is not the current room name")
	must.Equal(t, state[stateTupleKey("m.room.topic", "")], topic.EventID(),
		"branch b's state event is not the current room topic")
}

// TestGapFillingDroppedStateEventDependents forks the DAG into a trunk that
// bans mallory and a withheld branch on which mallory re-joins and charlie
// then sets a room name. The two membership events genuinely conflict at the
// merge: the branch one is not a descendant of the ban, and it cites the same
// pre-fork membership. Only the ban is a control event: charlie sent it and
// its state_key is mallory, so the sender differs from the state_key. It is
// therefore resolved alone in the power-event pass. Mallory's rejoin is
// self-sent, so it is an ordinary conflicted event and is resolved in the
// later mainline-ordered pass, where iterative auth checks it against a state
// that already carries the resolved ban: mallory is banned there, so the
// rejoin fails auth and is dropped. The ban wins on authorization; no
// timestamp or event ID tie-break is reached.
//
// Dropping that membership event must not drop what was built after it on the
// same branch. The name event is authorized regardless of mallory's
// membership, so it has to survive, and the whole client-visible state is
// compared with the fixture's tuple by tuple: this is the oracle for a false
// positive that cascades from a dropped state event into its dependents.
//
// DAG, forking at alice's join:
//
//	join -+- ban (mallory)                       -+   delivered first
//	      +- rejoin (mallory) - name (charlie)   -+- merge   (delivered via /send)
func TestGapFillingDroppedStateEventDependents(t *testing.T) {
	deployment := complement.Deploy(t, 1)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	alice.SyncUntilTimeout = 30 * time.Second
	hs1 := deployment.GetFullyQualifiedHomeserverName(t, "hs1")
	gs, cancel := newGapFillServer(t, deployment)
	defer cancel()

	room, charlie, mallory := gs.makeGapFillRoom(t, alice)
	fork := room.ForwardExtremities[0]
	emptyKey := ""

	ban := gs.srv.MustCreateEvent(t, room, federation.Event{
		Type:       spec.MRoomMember,
		StateKey:   &mallory,
		Sender:     charlie,
		Content:    map[string]interface{}{"membership": "ban", "reason": "dropped dependents test"},
		PrevEvents: []string{fork},
	})
	// Built before the ban is applied to the tracked state, so it cites
	// mallory's original join rather than the ban: a sibling of the ban, not a
	// descendant of it.
	rejoin := gs.srv.MustCreateEvent(t, room, federation.Event{
		Type:       spec.MRoomMember,
		StateKey:   &mallory,
		Sender:     mallory,
		Content:    map[string]interface{}{"membership": "join", "displayname": "evil twin"},
		PrevEvents: []string{fork},
	})
	room.AddEvent(ban)
	room.AddEvent(rejoin)
	// Independent of mallory's membership and authorized either way: charlie
	// created the room. It must survive the loss of its DAG parent.
	branchName := gs.srv.MustCreateEvent(t, room, federation.Event{
		Type:       "m.room.name",
		StateKey:   &emptyKey,
		Sender:     charlie,
		Content:    map[string]interface{}{"name": "set below the dropped membership"},
		PrevEvents: []string{rejoin.EventID()},
	})
	room.AddEvent(branchName)
	merge := gs.srv.MustCreateEvent(t, room, federation.Event{
		Type:       "m.room.message",
		Sender:     charlie,
		Content:    map[string]interface{}{"msgtype": "m.text", "body": "merge of the ban and the branch"},
		PrevEvents: []string{ban.EventID(), branchName.EventID()},
	})
	room.AddEvent(merge)
	// The fixture applied both membership events in the order they were built,
	// leaving mallory joined; state resolution at the merge keeps the ban, so
	// restore it before using the fixture as the oracle.
	restoreCurrentState(t, room, ban)

	handler, gapFilled := waitedMissingEventsResponder(t, rejoin, branchName)
	gs.setMissingEvents(room.RoomID, handler)

	gs.srv.MustSendTransaction(t, deployment, hs1, []json.RawMessage{ban.JSON()}, nil)
	alice.MustSyncUntil(t, client.SyncReq{}, client.SyncTimelineHasEventID(room.RoomID, ban.EventID()))

	gs.srv.MustSendTransaction(t, deployment, hs1, []json.RawMessage{merge.JSON()}, nil)
	gapFilled.Waitf(t, 30*time.Second, "hs1 never asked for the missing events of room %s", room.RoomID)
	alice.MustSyncUntil(t, client.SyncReq{}, client.SyncTimelineHasEventID(room.RoomID, merge.EventID()))

	// The branch's membership event lost the conflict ...
	content := alice.MustGetStateEventContent(t, room.RoomID, spec.MRoomMember, mallory)
	must.MatchGJSON(t, content, match.JSONKeyEqual("membership", "ban"))
	must.Equal(t, content.Get("displayname").Str, "", "the dropped membership event reached the room state")
	// ... and the state event built on top of it did not follow it out.
	must.MatchGJSON(t, alice.MustGetStateEventContent(t, room.RoomID, "m.room.name", ""),
		match.JSONKeyEqual("name", "set below the dropped membership"))

	// The complete state agrees with the fixture, tuple for tuple: nothing was
	// dropped as collateral and nothing stale was kept.
	want := fixtureState(room)
	got := clientState(t, alice, room.RoomID)
	for tuple, wantID := range want {
		gotID, ok := got[tuple]
		switch {
		case !ok:
			t.Errorf("client state is missing %s (expected %s)", tuple, wantID)
		case gotID != wantID:
			t.Errorf("client state for %s is %s, want %s", tuple, gotID, wantID)
		}
	}
	for tuple, gotID := range got {
		if _, ok := want[tuple]; !ok {
			t.Errorf("client state has an unexpected entry %s (%s)", tuple, gotID)
		}
	}
}

// manufacturedGap is the DAG built by TestGapFillingManufacturedGapSoftFail: a
// trunk event the homeserver integrates first, a withheld branch of three
// events whose middle one is sent by mallory, and the sentinel that reunites
// the two sides.
type manufacturedGap struct {
	room     *federation.ServerRoom
	mallory  string
	trunk    gomatrixserverlib.PDU
	opener   gomatrixserverlib.PDU
	message  gomatrixserverlib.PDU
	closer   gomatrixserverlib.PDU
	sentinel gomatrixserverlib.PDU
}

// TestGapFillingManufacturedGapSoftFail manufactures a withheld branch of
// several events around a message by mallory that is valid against the state
// its prev_events claim - mallory is still joined there - and reunites that
// branch with a trunk on which charlie has banned her. The ban is delivered
// and integrated first, so when the branch arrives through gap filling the
// message is authorized at its own position yet unauthorized against the
// current state. The homeserver must soft fail it: retain it as an event,
// keep it out of the room timeline, integrate the rest of the branch and the
// reunion sentinel anyway, and leave the ban as the current membership.
//
// The control arm runs the identical shape with a harmless topic as the trunk
// event. There the message integrates, which is what makes the claim "valid
// against its claimed predecessor state" a measurement rather than an
// assumption: the branch is only excluded when the trunk revokes mallory's
// membership. That authorized arm is also what separates soft failure from
// rejection here. Retrievability does not: a rejected event may be stored and
// served by ID just the same, so the /event lookup in the banned arm records
// only that the message survived, never why it was withheld.
//
// DAG in both arms, forking at alice's join:
//
//	join -+- trunk: ban, or a topic in the control        -+   delivered first
//	      +- opener - mallory's message - closer          -+- sentinel   (via /send)
func TestGapFillingManufacturedGapSoftFail(t *testing.T) {
	deployment := complement.Deploy(t, 1)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	alice.SyncUntilTimeout = 30 * time.Second
	hs1 := deployment.GetFullyQualifiedHomeserverName(t, "hs1")
	gs, cancel := newGapFillServer(t, deployment)
	defer cancel()

	// The /get_missing_events responder is installed on the federation server
	// for the rest of the run and may be called after the subtest that
	// installed it has returned, so it logs and fails against the parent
	// handle. Assertions and the Wait below stay on the subtest handle, where
	// a failure names the arm that produced it.
	parentT := t

	// deliver builds the DAG above in a fresh room and delivers it: the trunk
	// event on its own first, then the sentinel, whose gap is served in one
	// batch. banned selects whether the trunk event revokes mallory's
	// membership or is the harmless control.
	deliver := func(t *testing.T, banned bool) manufacturedGap {
		room, charlie, mallory := gs.makeGapFillRoom(t, alice)
		fork := room.ForwardExtremities[0]
		emptyKey := ""

		trunkEvent := federation.Event{
			Type:       "m.room.topic",
			StateKey:   &emptyKey,
			Sender:     charlie,
			Content:    map[string]interface{}{"topic": "control arm trunk"},
			PrevEvents: []string{fork},
		}
		if banned {
			trunkEvent = federation.Event{
				Type:       spec.MRoomMember,
				StateKey:   &mallory,
				Sender:     charlie,
				Content:    map[string]interface{}{"membership": "ban", "reason": "manufactured gap test"},
				PrevEvents: []string{fork},
			}
		}
		trunk := gs.srv.MustCreateEvent(t, room, trunkEvent)

		// The branch is built before the trunk is applied to the tracked
		// state, so mallory's message cites her join: valid against the state
		// its prev_events claim, unauthorized against the state the trunk
		// leaves behind.
		opener := gs.srv.MustCreateEvent(t, room, federation.Event{
			Type:       "m.room.message",
			Sender:     charlie,
			Content:    map[string]interface{}{"msgtype": "m.text", "body": "withheld branch opener"},
			PrevEvents: []string{fork},
		})
		room.AddEvent(opener)
		message := gs.srv.MustCreateEvent(t, room, federation.Event{
			Type:       "m.room.message",
			Sender:     mallory,
			Content:    map[string]interface{}{"msgtype": "m.text", "body": "manufactured gap message"},
			PrevEvents: []string{opener.EventID()},
		})
		room.AddEvent(message)
		closer := gs.srv.MustCreateEvent(t, room, federation.Event{
			Type:       "m.room.message",
			Sender:     charlie,
			Content:    map[string]interface{}{"msgtype": "m.text", "body": "withheld branch closer"},
			PrevEvents: []string{message.EventID()},
		})
		room.AddEvent(closer)
		room.AddEvent(trunk)

		sentinel := gs.srv.MustCreateEvent(t, room, federation.Event{
			Type:       "m.room.message",
			Sender:     charlie,
			Content:    map[string]interface{}{"msgtype": "m.text", "body": "reunion sentinel"},
			PrevEvents: []string{trunk.EventID(), closer.EventID()},
		})
		room.AddEvent(sentinel)

		handler, gapFilled := waitedMissingEventsResponder(parentT, opener, message, closer)
		gs.setMissingEvents(room.RoomID, handler)

		// The trunk integrates on its own, so the branch is judged against a
		// current state that already carries it.
		gs.srv.MustSendTransaction(t, deployment, hs1, []json.RawMessage{trunk.JSON()}, nil)
		alice.MustSyncUntil(t, client.SyncReq{}, client.SyncTimelineHasEventID(room.RoomID, trunk.EventID()))

		gs.srv.MustSendTransaction(t, deployment, hs1, []json.RawMessage{sentinel.JSON()}, nil)
		gapFilled.Waitf(t, 30*time.Second, "hs1 never asked for the missing events of room %s", room.RoomID)
		alice.MustSyncUntil(t, client.SyncReq{}, client.SyncTimelineHasEventID(room.RoomID, sentinel.EventID()))

		return manufacturedGap{
			room:     room,
			mallory:  mallory,
			trunk:    trunk,
			opener:   opener,
			message:  message,
			closer:   closer,
			sentinel: sentinel,
		}
	}

	t.Run("message soft fails against the integrated ban", func(t *testing.T) {
		gap := deliver(t, true)

		seen := timelineEventIDs(t, alice, gap.room.RoomID)
		if seen[gap.message.EventID()] {
			t.Errorf("the soft failed message %s reached the room timeline", gap.message.EventID())
		}
		// Soft failure is not rejection and it does not cascade: everything
		// else on the branch, and the sentinel below it, still integrates.
		for _, ev := range []gomatrixserverlib.PDU{gap.opener, gap.closer, gap.sentinel} {
			if !seen[ev.EventID()] {
				t.Errorf("authorized event %s is missing from the timeline: the soft failure cascaded", ev.EventID())
			}
		}
		// The message is still addressable by ID after being withheld. This
		// records what happened to the event, not which verdict produced it:
		// a rejected event may be retrievable this way too, so retrievability
		// is not what tells soft failure and rejection apart. That is the job
		// of the control arm below, where the identical branch integrates
		// once the trunk no longer revokes mallory's membership.
		must.Equal(t,
			alice.MustGetEvent(t, gap.room.RoomID, gap.message.EventID()).Get("event_id").Str,
			gap.message.EventID(),
			"the withheld message is no longer retrievable by event ID",
		)

		content := alice.MustGetStateEventContent(t, gap.room.RoomID, spec.MRoomMember, gap.mallory)
		must.MatchGJSON(t, content, match.JSONKeyEqual("membership", "ban"))
	})

	t.Run("control: the same branch without the ban", func(t *testing.T) {
		gap := deliver(t, false)

		seen := timelineEventIDs(t, alice, gap.room.RoomID)
		for _, ev := range []gomatrixserverlib.PDU{gap.opener, gap.message, gap.closer, gap.sentinel} {
			if !seen[ev.EventID()] {
				t.Errorf("event %s is missing from the timeline: the withheld branch is not valid where it claims to be", ev.EventID())
			}
		}
		must.MatchGJSON(t, alice.MustGetStateEventContent(t, gap.room.RoomID, spec.MRoomMember, gap.mallory),
			match.JSONKeyEqual("membership", "join"))
		must.MatchGJSON(t, alice.MustGetStateEventContent(t, gap.room.RoomID, "m.room.topic", ""),
			match.JSONKeyEqual("topic", "control arm trunk"))
		// Cross-check the delivered trunk event against the full /state
		// response rather than the single-tuple lookup above: the tuple
		// resolves to the trunk event itself, not some later topic, so the
		// branch merged around it without displacing it.
		state := clientState(t, alice, gap.room.RoomID)
		must.Equal(t, state[stateTupleKey("m.room.topic", "")], gap.trunk.EventID(),
			"the current m.room.topic is not the trunk event")
	})
}
