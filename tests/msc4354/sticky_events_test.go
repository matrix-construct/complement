package tests

import (
	"fmt"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/tidwall/gjson"

	"github.com/matrix-org/complement"
	"github.com/matrix-org/complement/b"
	"github.com/matrix-org/complement/client"
	"github.com/matrix-org/complement/helpers"
	"github.com/matrix-org/complement/must"
)

// The unstable names MSC4354 and MSC4480 are served under.
const (
	stickyKey       = "msc4354_sticky"
	stickyTTLKey    = "unsigned.msc4354_sticky_duration_ttl_ms"
	stickyParam     = "org.matrix.msc4354.sticky_duration_ms"
	stickyExtension = "org.matrix.msc4354.sticky_events"
)

// stickyDuration outlives every test, and is under the hour MSC4354 clamps to.
const stickyDuration = 10 * time.Minute

// syncTimeout bounds each wait for events to arrive over federation.
const syncTimeout = 30 * time.Second

// runLength is how many plain messages separate two sticky events: more than a
// joining server's timeline shows, and more than one gap fill fetches.
const runLength = 25

var txnCounter atomic.Int64

// TestMSC4354StickyEventsReachJoiningServer checks that the servers already in
// a room send their unexpired sticky events to a server joining it.
//
// Alice's server answers the join, so it learns of the join through
// /send_join; Bob's learns of it through /send. Each must send its own sticky
// events, from before and after a run of plain messages, to the joining
// server, which must accept them although they predate its join and their
// predecessors are missing. The plain messages must stay out of the joining
// user's timeline.
func TestMSC4354StickyEventsReachJoiningServer(t *testing.T) {
	deployment := complement.Deploy(t, 3)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	bob := deployment.Register(t, "hs2", helpers.RegistrationOpts{})
	joiner := deployment.Register(t, "hs3", helpers.RegistrationOpts{})
	via := []spec.ServerName{deployment.GetFullyQualifiedHomeserverName(t, "hs1")}

	forEachSync(t, func(t *testing.T, sliding bool) {
		roomID := alice.MustCreateRoom(t, map[string]interface{}{"preset": "public_chat"})
		bob.MustJoinRoom(t, roomID, via)
		alice.MustSyncUntil(t, client.SyncReq{}, client.SyncJoinedTo(bob.UserID, roomID))

		aliceEarly := sendSticky(t, alice, roomID, "Alice, before the run")
		bob.MustSyncUntil(t, client.SyncReq{}, client.SyncTimelineHasEventID(roomID, aliceEarly))
		bobEarly := sendSticky(t, bob, roomID, "Bob, before the run")
		alice.MustSyncUntil(t, client.SyncReq{}, client.SyncTimelineHasEventID(roomID, bobEarly))

		run := sendRun(t, alice, roomID, runLength)
		aliceLate := sendSticky(t, alice, roomID, "Alice, after the run")
		bob.MustSyncUntil(t, client.SyncReq{}, client.SyncTimelineHasEventID(roomID, aliceLate))
		bobLate := sendSticky(t, bob, roomID, "Bob, after the run")
		alice.MustSyncUntil(t, client.SyncReq{}, client.SyncTimelineHasEventID(roomID, bobLate))

		joiner.MustJoinRoom(t, roomID, via)

		// Nothing is sent after the join until the sticky events have arrived,
		// so only the residents' own push can deliver them.
		view := newObserver(joiner, roomID, sliding)
		view.waitFor(t, aliceEarly, bobEarly, aliceLate, bobLate)
		view.mustBeSticky(t, aliceEarly, bobEarly, aliceLate, bobLate)

		// A later message bounds the wait for the run, which must not appear.
		stop := alice.Unsafe_SendEventUnsynced(t, roomID, text("after the join"))
		view.waitFor(t, stop)
		view.mustNotHaveSeen(t, "a plain message from before the join", run...)
	})
}

// TestMSC4354ErasedSenderStickyEventsStayHome checks that a server sending its
// sticky events to a joining server leaves out those of an erased sender.
//
// Carol sends a sticky event and then deactivates her account with erasure.
// When Bob's server joins, it must receive Alice's sticky event, sent after
// the erasure, and never Carol's. A run of plain messages keeps Carol's event
// beyond the reach of the gap fill that Alice's event starts.
func TestMSC4354ErasedSenderStickyEventsStayHome(t *testing.T) {
	deployment := complement.Deploy(t, 2)
	defer deployment.Destroy(t)

	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	carol := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	bob := deployment.Register(t, "hs2", helpers.RegistrationOpts{})
	via := []spec.ServerName{deployment.GetFullyQualifiedHomeserverName(t, "hs1")}

	roomID := alice.MustCreateRoom(t, map[string]interface{}{"preset": "public_chat"})
	carol.MustJoinRoom(t, roomID, via)
	erased := sendSticky(t, carol, roomID, "Carol, before her erasure")

	carol.MustDo(t, "POST", []string{"_matrix", "client", "v3", "account", "deactivate"},
		client.WithJSONBody(t, map[string]interface{}{
			"erase": true,
			"auth": map[string]interface{}{
				"type": "m.login.password",
				"identifier": map[string]interface{}{
					"type": "m.id.user",
					"user": carol.UserID,
				},
				"password": carol.Password,
			},
		}),
	)

	alice.MustSyncUntil(t, client.SyncReq{}, client.SyncLeftFrom(carol.UserID, roomID))
	sendRun(t, alice, roomID, runLength)
	kept := sendSticky(t, alice, roomID, "Alice, after the erasure")

	bob.MustJoinRoom(t, roomID, via)

	view := newObserver(bob, roomID, false)
	view.waitFor(t, kept)
	view.mustBeSticky(t, kept)

	// A later message bounds the wait for Carol's event, which must not appear.
	stop := alice.Unsafe_SendEventUnsynced(t, roomID, text("after the join"))
	view.waitFor(t, stop)
	view.mustNotHaveSeen(t, "the sticky event of an erased sender", erased)
}

// forEachSync runs the check once over legacy sync and once over sliding sync.
func forEachSync(t *testing.T, check func(t *testing.T, sliding bool)) {
	t.Run("sync", func(t *testing.T) { check(t, false) })
	t.Run("sliding_sync", func(t *testing.T) { check(t, true) })
}

// sendSticky sends a message sticky for stickyDuration and returns its ID.
func sendSticky(t *testing.T, user *client.CSAPI, roomID, body string) string {
	t.Helper()

	txnID := "msc4354-" + strconv.FormatInt(txnCounter.Add(1), 10)
	duration := strconv.FormatInt(stickyDuration.Milliseconds(), 10)
	res := user.MustDo(t, "PUT",
		[]string{"_matrix", "client", "v3", "rooms", roomID, "send", "m.room.message", txnID},
		client.WithJSONBody(t, text(body).Content),
		client.WithQueries(url.Values{stickyParam: []string{duration}}),
	)

	return must.ParseJSON(t, res.Body).Get("event_id").Str
}

// sendRun sends plain messages without waiting for them, and returns their IDs.
func sendRun(t *testing.T, user *client.CSAPI, roomID string, n int) []string {
	t.Helper()

	eventIDs := make([]string, n)
	for i := range eventIDs {
		eventIDs[i] = user.Unsafe_SendEventUnsynced(t, roomID, text(fmt.Sprintf("plain message %d", i)))
	}

	return eventIDs
}

// slidingRequest asks for every room, with the sticky events extension.
var slidingRequest = map[string]interface{}{
	"lists": map[string]interface{}{
		"all": map[string]interface{}{
			"ranges":         [][]int{{0, 20}},
			"timeline_limit": 10,
			"required_state": [][]string{{"*", "*"}},
		},
	},
	"extensions": map[string]interface{}{
		stickyExtension: map[string]interface{}{"enabled": true},
	},
}

// observer follows one user's view of one room through successive syncs, and
// keeps every event that its timeline or its sticky section has returned.
type observer struct {
	user    *client.CSAPI
	roomID  string
	sliding bool
	since   string
	seen    map[string]gjson.Result
}

func newObserver(user *client.CSAPI, roomID string, sliding bool) *observer {
	return &observer{
		user:    user,
		roomID:  roomID,
		sliding: sliding,
		seen:    map[string]gjson.Result{},
	}
}

// sync makes one request, from where the previous one left off.
func (o *observer) sync(t *testing.T) {
	t.Helper()

	room := client.GjsonEscape(o.roomID)
	var events []gjson.Result
	if o.sliding {
		query := url.Values{"timeout": []string{"1000"}}
		if o.since != "" {
			query.Set("pos", o.since)
		}

		res := o.user.MustDo(t, "POST",
			[]string{"_matrix", "client", "unstable", "org.matrix.simplified_msc3575", "sync"},
			client.WithJSONBody(t, slidingRequest),
			client.WithQueries(query),
		)

		body := must.ParseJSON(t, res.Body)
		o.since = body.Get("pos").Str
		section := "extensions." + client.GjsonEscape(stickyExtension) + ".rooms." + room + ".events"
		events = append(body.Get("rooms."+room+".timeline").Array(), body.Get(section).Array()...)
	} else {
		body, since := o.user.MustSync(t, client.SyncReq{Since: o.since})
		o.since = since
		joined := "rooms.join." + room
		events = append(body.Get(joined+".timeline.events").Array(), body.Get(joined+"."+stickyKey+".events").Array()...)
	}

	for _, ev := range events {
		o.seen[ev.Get("event_id").Str] = ev
	}
}

// waitFor syncs until every one of the events has been seen.
func (o *observer) waitFor(t *testing.T, eventIDs ...string) {
	t.Helper()

	deadline := time.Now().Add(syncTimeout)
	for {
		var missing []string
		for _, eventID := range eventIDs {
			if _, ok := o.seen[eventID]; !ok {
				missing = append(missing, eventID)
			}
		}

		if len(missing) == 0 {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("%s did not see %v within %s", o.user.UserID, missing, syncTimeout)
		}

		o.sync(t)
	}
}

// mustBeSticky checks that each event was served with its sticky object and
// with a remaining time above zero and within its duration.
func (o *observer) mustBeSticky(t *testing.T, eventIDs ...string) {
	t.Helper()

	for _, eventID := range eventIDs {
		ev := o.seen[eventID]
		duration := ev.Get(stickyKey + ".duration_ms")
		if !duration.Exists() {
			t.Errorf("%s was served without its sticky object: %s", eventID, ev.Raw)
			continue
		}

		ttl := ev.Get(stickyTTLKey)
		if !ttl.Exists() || ttl.Int() <= 0 || ttl.Int() > duration.Int() {
			t.Errorf("%s was served with remaining time %q for a duration of %d ms", eventID, ttl.Raw, duration.Int())
		}
	}
}

// mustNotHaveSeen checks that none of the events has been seen.
func (o *observer) mustNotHaveSeen(t *testing.T, what string, eventIDs ...string) {
	t.Helper()

	for _, eventID := range eventIDs {
		if _, ok := o.seen[eventID]; ok {
			t.Errorf("%s saw %s, %s", o.user.UserID, what, eventID)
		}
	}
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
