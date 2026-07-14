package tests

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
	"github.com/matrix-org/complement/must"
)

// maxForwardExtremities mirrors the homeserver's default cap on the number of
// forward extremities tracked per room (forward_extremities_max).
const maxForwardExtremities = 60

// TestForwardExtremityPrune covers pruning of a room's forward-extremity set
// on the federation receive path. Sibling events which all cite the same
// committed prev each leave one more extremity behind; a homeserver tracking
// that frontier without bound bloats every /get_missing_events boundary and
// every per-event extremity rewrite in the room. Past the cap the least
// useful leaves must be forgotten instead: message events before state
// events, oldest first. Forgetting a leaf is bookkeeping, not rejection; the
// pruned event must remain stored, readable and buildable-upon.
//
// The frontier is observed through the earliest_events boundary of the
// /get_missing_events request the homeserver issues while filling a gap: that
// boundary cites the full forward-extremity set.
func TestForwardExtremityPrune(t *testing.T) {
	deployment := complement.Deploy(t, 1)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	alice.SyncUntilTimeout = 30 * time.Second
	hs1 := deployment.GetFullyQualifiedHomeserverName(t, "hs1")

	srv := federation.NewServer(t, deployment,
		federation.HandleKeyRequests(),
		federation.HandleMakeSendJoinRequests(),
		federation.HandleTransactionRequests(nil, nil),
		federation.HandleEventRequests(),
	)

	// Captures each earliest_events boundary the homeserver cites, and holds
	// the withheld event served back once the gap probe below runs.
	var gapMu sync.Mutex
	var boundaries [][]string
	var withheld gomatrixserverlib.PDU
	srv.Mux().HandleFunc("/_matrix/federation/v1/get_missing_events/{roomID}", func(w http.ResponseWriter, req *http.Request) {
		body, err := io.ReadAll(req.Body)
		must.NotError(t, "failed to read /get_missing_events request", err)
		var earliest []string
		for _, id := range gjson.GetBytes(body, "earliest_events").Array() {
			earliest = append(earliest, id.Str)
		}
		gapMu.Lock()
		boundaries = append(boundaries, earliest)
		served := withheld
		gapMu.Unlock()
		if served == nil {
			t.Errorf("received /get_missing_events for room %s before the gap probe", mux.Vars(req)["roomID"])
			w.WriteHeader(404)
			w.Write([]byte("complement: no gap prepared for this room"))
			return
		}
		respondJSON(t, w, map[string]interface{}{
			"events": []json.RawMessage{served.JSON()},
		})
	}).Methods("POST")
	// Backfill is not part of the assertion surface; answer emptily so a
	// timeline walk past the join boundary cannot trip the 404 guard.
	srv.Mux().HandleFunc("/_matrix/federation/v1/backfill/{roomID}", func(w http.ResponseWriter, req *http.Request) {
		respondJSON(t, w, map[string]interface{}{
			"origin":           srv.ServerName(),
			"origin_server_ts": spec.AsTimestamp(time.Now()),
			"pdus":             []json.RawMessage{},
		})
	}).Methods("GET")
	cancel := srv.Listen()
	defer cancel()

	charlie := srv.UserID("charlie")
	ver := alice.GetDefaultRoomVersion(t)
	room := srv.MustMakeRoom(t, ver, federation.InitialRoomEvents(ver, charlie))
	alice.MustJoinRoom(t, room.RoomID, []spec.ServerName{srv.ServerName()})
	alice.MustSyncUntil(t, client.SyncReq{}, client.SyncJoinedTo(alice.UserID, room.RoomID))

	send := func(ev gomatrixserverlib.PDU) {
		srv.MustSendTransaction(t, deployment, hs1, []json.RawMessage{ev.JSON()}, nil)
	}
	message := func(body string, prevEvents []string) gomatrixserverlib.PDU {
		ev := federation.Event{
			Type:    "m.room.message",
			Sender:  charlie,
			Content: map[string]interface{}{"msgtype": "m.text", "body": body},
		}
		if prevEvents != nil {
			ev.PrevEvents = prevEvents
		}
		pdu := srv.MustCreateEvent(t, room, ev)
		room.AddEvent(pdu)
		return pdu
	}

	// The committed prev every sibling cites; nothing delivered later needs a
	// fetch, so each delivery is a degree-one append leaving one more
	// extremity behind.
	anchor := message("the shared prev", nil)
	send(anchor)
	alice.MustSyncUntil(t, client.SyncReq{}, client.SyncTimelineHasEventID(room.RoomID, anchor.EventID()))

	// The oldest leaf is a state event. Age alone would prune it first; it
	// must instead outlive every younger message leaf.
	emptyKey := ""
	stateLeaf := srv.MustCreateEvent(t, room, federation.Event{
		Type:       "m.room.topic",
		StateKey:   &emptyKey,
		Sender:     charlie,
		Content:    map[string]interface{}{"topic": "the oldest leaf is a state event"},
		PrevEvents: []string{anchor.EventID()},
	})
	room.AddEvent(stateLeaf)
	send(stateLeaf)

	// One message sibling over the cap: the last delivery leaves the frontier
	// oversized and the excess has to go.
	messages := make([]gomatrixserverlib.PDU, maxForwardExtremities)
	for i := range messages {
		messages[i] = message(fmt.Sprintf("sibling %d", i), []string{anchor.EventID()})
		send(messages[i])
	}
	alice.MustSyncUntil(t, client.SyncReq{}, client.SyncTimelineHasEventID(room.RoomID, messages[len(messages)-1].EventID()))
	fresh := message("first past the cap", []string{anchor.EventID()})
	send(fresh)

	// Pruning forgets a leaf; it must not delete the event.
	dropped := messages[0]
	res := alice.MustDo(t, "GET",
		[]string{"_matrix", "client", "v3", "rooms", room.RoomID, "event", dropped.EventID()},
	)
	body := must.ParseJSON(t, res.Body)
	must.Equal(t, body.Get("event_id").Str, dropped.EventID(), "unexpected event returned")

	// A pruned leaf also remains buildable-upon: an event citing it appends
	// like any other.
	onDropped := message("built on a pruned extremity", []string{dropped.EventID()})
	send(onDropped)
	alice.MustSyncUntil(t, client.SyncReq{}, client.SyncTimelineHasEventID(room.RoomID, onDropped.EventID()))

	// Withhold one event and deliver its child: filling the gap makes the
	// homeserver cite its forward extremities as the earliest_events
	// boundary.
	gap := message("withheld from delivery", []string{anchor.EventID()})
	gapMu.Lock()
	withheld = gap
	gapMu.Unlock()
	probe := message("delivered above the withheld event", []string{gap.EventID()})
	send(probe)
	alice.MustSyncUntil(t, client.SyncReq{}, client.SyncTimelineHasEventID(room.RoomID, gap.EventID()))
	alice.MustSyncUntil(t, client.SyncReq{}, client.SyncTimelineHasEventID(room.RoomID, probe.EventID()))

	gapMu.Lock()
	captured := boundaries
	gapMu.Unlock()
	if len(captured) == 0 {
		t.Fatalf("the homeserver never issued /get_missing_events for the gap probe")
	}

	// By the time of the gap, three deliveries had pushed the frontier past
	// the cap, so the three oldest message leaves are gone and the boundary
	// is exactly the cap: the state leaf, the surviving siblings, and the
	// two newest arrivals.
	expected := map[string]bool{
		stateLeaf.EventID(): true,
		fresh.EventID():     true,
		onDropped.EventID(): true,
	}
	for _, ev := range messages[3:] {
		expected[ev.EventID()] = true
	}
	boundary := captured[0]
	cited := make(map[string]bool, len(boundary))
	for _, id := range boundary {
		cited[id] = true
	}
	must.Equal(t, len(boundary), maxForwardExtremities, "forward extremities cited at the gap")
	if !cited[stateLeaf.EventID()] {
		t.Errorf("the state-event leaf was pruned: it must outlive every younger message leaf")
	}
	for i, ev := range messages[:3] {
		if cited[ev.EventID()] {
			t.Errorf("oldest message leaf %d survived: the excess was not pruned oldest-first", i)
		}
	}
	for id := range cited {
		if !expected[id] {
			t.Errorf("unexpected forward extremity %s", id)
		}
	}
	for id := range expected {
		if !cited[id] {
			t.Errorf("missing forward extremity %s", id)
		}
	}
}
