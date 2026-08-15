package csapi_tests

import (
	"testing"

	"github.com/matrix-org/complement"
	"github.com/matrix-org/complement/b"
	"github.com/matrix-org/complement/client"
	"github.com/matrix-org/complement/helpers"
	"github.com/matrix-org/complement/match"
	"github.com/matrix-org/complement/must"
)

func TestStateDedupRequiresJoinedSender(t *testing.T) {
	deployment := complement.Deploy(t, 1)
	defer deployment.Destroy(t)

	creator := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	user := deployment.Register(t, "hs1", helpers.RegistrationOpts{})

	const eventType = "com.example.test"
	roomID := creator.MustCreateRoom(t, map[string]interface{}{
		"preset": "public_chat",
		"power_level_content_override": map[string]interface{}{
			"events": map[string]int{
				eventType: 0,
			},
		},
	})
	user.MustJoinRoom(t, roomID, nil)

	content := map[string]interface{}{"a": "b"}
	user.SendEventSynced(t, roomID, b.Event{
		Type:     eventType,
		StateKey: b.Ptr(""),
		Content:  content,
	})

	user.MustLeaveRoom(t, roomID)
	creator.MustSyncUntil(t, client.SyncReq{}, client.SyncLeftFrom(user.UserID, roomID))

	res := user.Do(
		t,
		"PUT",
		[]string{"_matrix", "client", "v3", "rooms", roomID, "state", eventType, ""},
		client.WithJSONBody(t, content),
	)
	must.MatchResponse(t, res, match.HTTPResponse{
		StatusCode: 403,
		JSON: []match.JSON{
			match.JSONKeyEqual("errcode", "M_FORBIDDEN"),
		},
	})
}
