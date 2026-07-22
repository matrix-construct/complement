//go:build complemau

package complemau_tests

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/matrix-org/complement"
	"github.com/matrix-org/complement/b"
	"github.com/matrix-org/complement/client"
	"github.com/matrix-org/complement/helpers"
	"github.com/matrix-org/complement/runtime"
	"github.com/tidwall/gjson"
	"maunium.net/go/mautrix/id"
)

// complemauDirectoryExclusionBlueprint mirrors BlueprintHSWithComplemauBridge but
// scopes its appservice to an exclusive users namespace covering only this test's
// ghosts (@as_directory*:hs1). Under the shared blueprint's non-exclusive `.*`
// namespace the ghost is indistinguishable from the marker user, so the directory
// filter cannot drop the ghost without also dropping the marker.
var complemauDirectoryExclusionBlueprint = b.MustValidate(b.Blueprint{
	Name: "hs_with_complemau_directory_bridge",
	Homeservers: []b.Homeserver{
		{
			Name: "hs1",
			Users: []b.User{
				{Localpart: "@alice", DisplayName: "Alice"},
			},
			ApplicationServices: []b.ApplicationService{
				{
					ID:              b.ComplemauASID,
					URL:             b.ComplemauASURL,
					SenderLocalpart: b.ComplemauSender,
					RateLimited:     false,
					SendEphemeral:   true,
					Namespaces: &b.ApplicationServiceNamespaces{
						Users: []b.ApplicationServiceNamespace{
							{Regex: `^@as_directory.*:hs1$`, Exclusive: true},
						},
					},
				},
			},
		},
	},
})

func TestComplemauDirectoryExcludesAppserviceUsers(t *testing.T) {
	runtime.SkipIf(t, runtime.Dendrite)

	testCases := []struct {
		name           string
		ghostLocalpart string
		userID         string
		displayName    string
		searchTerm     string
	}{
		{
			name:           "ghost membership",
			ghostLocalpart: "as_directoryghost",
			userID:         "@as_directoryghost:hs1",
			searchTerm:     "directoryghost",
		},
		{
			name:       "sender membership",
			userID:     b.ComplemauSenderID,
			searchTerm: "complemau",
		},
		{
			name:           "ghost profile change",
			ghostLocalpart: "as_directoryprofile",
			userID:         "@as_directoryprofile:hs1",
			displayName:    "ComplemauDirectoryGhostProfile",
			searchTerm:     "ComplemauDirectoryGhostProfile",
		},
		{
			name:        "sender profile change",
			userID:      b.ComplemauSenderID,
			displayName: "ComplemauDirectorySenderProfile",
			searchTerm:  "ComplemauDirectorySenderProfile",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			deployment := complement.OldDeploy(t, complemauDirectoryExclusionBlueprint)
			defer deployment.Destroy(t)

			searcher := deployment.Register(t, "hs1", helpers.RegistrationOpts{
				LocalpartSuffix: "directorysearcher",
			})
			bridgeClient := deployment.AppServiceUser(t, "hs1", b.ComplemauSenderID)
			registration := complemauDirectoryExclusionBlueprint.Homeservers[0].ApplicationServices[0]
			bridge := startComplemauBridgeWithRegistration(t, bridgeClient.BaseURL, registration, b.ComplemauASPort)
			defer bridge.stop()
			bridge.ensureReady(t)

			if testCase.ghostLocalpart != "" {
				registerAppserviceGhost(t, bridgeClient, testCase.ghostLocalpart)
			}

			roomID := searcher.MustCreateRoom(t, map[string]interface{}{
				"preset":     "public_chat",
				"visibility": "public",
			})
			mustJoinComplemauDirectoryUser(t, bridgeClient, roomID, testCase.userID)

			if testCase.userID == b.ComplemauSenderID && testCase.displayName != "" {
				bridgeClient.MustSetDisplayName(t, "ComplemauDirectorySenderSeed")
			}
			if testCase.displayName != "" {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				intent := bridge.as.BotIntent()
				if testCase.userID != b.ComplemauSenderID {
					intent = bridge.as.Intent(id.UserID(testCase.userID))
				}
				if err := intent.SetDisplayName(ctx, testCase.displayName); err != nil {
					t.Fatalf("complemau: failed to set appservice profile: %v", err)
				}
			}

			marker := deployment.Register(t, "hs1", helpers.RegistrationOpts{
				LocalpartSuffix: "directorymarker",
			})
			marker.MustSetDisplayName(t, testCase.searchTerm)
			marker.MustDo(
				t,
				http.MethodPost,
				[]string{"_matrix", "client", "v3", "rooms", roomID, "join"},
				client.WithJSONBody(t, struct{}{}),
			)

			resultIDs := mustSearchComplemauDirectoryAfterMarker(
				t,
				searcher,
				testCase.searchTerm,
				marker.UserID,
			)

			// Synapse excludes identities owned by an appservice from its user
			// directory. This is a policy choice, not a Matrix requirement.
			// Tuwunel currently has no appservice directory filter, so this
			// strict Synapse-shaped assertion is a known expected failure.
			for _, resultID := range resultIDs {
				if resultID == testCase.userID {
					t.Fatalf(
						"complemau: appservice user %s surfaced in directory results after marker %s: %v",
						testCase.userID,
						marker.UserID,
						resultIDs,
					)
				}
			}
			if len(resultIDs) != 1 || resultIDs[0] != marker.UserID {
				t.Fatalf(
					"complemau: expected only directory marker %s, got %v",
					marker.UserID,
					resultIDs,
				)
			}
		})
	}
}

func mustJoinComplemauDirectoryUser(t *testing.T, bridgeClient *client.CSAPI, roomID, userID string) {
	t.Helper()
	if userID == b.ComplemauSenderID {
		bridgeClient.MustDo(
			t,
			http.MethodPost,
			[]string{"_matrix", "client", "v3", "rooms", roomID, "join"},
			client.WithJSONBody(t, struct{}{}),
		)
		return
	}

	bridgeClient.MustDo(
		t,
		http.MethodPost,
		[]string{"_matrix", "client", "v3", "rooms", roomID, "join"},
		client.WithJSONBody(t, struct{}{}),
		asUser(userID),
	)
}

func mustSearchComplemauDirectoryAfterMarker(
	t *testing.T,
	searcher *client.CSAPI,
	searchTerm,
	markerUserID string,
) []string {
	t.Helper()
	var resultIDs []string
	searcher.MustDo(
		t,
		http.MethodPost,
		[]string{"_matrix", "client", "v3", "user_directory", "search"},
		client.WithJSONBody(t, map[string]interface{}{"search_term": searchTerm}),
		client.WithRetryUntil(10*time.Second, func(res *http.Response) bool {
			if res.StatusCode != http.StatusOK {
				t.Fatalf("complemau: user directory search returned %d", res.StatusCode)
			}
			body, err := io.ReadAll(res.Body)
			if err != nil {
				t.Fatalf("complemau: failed to read user directory response: %v", err)
			}

			results := gjson.GetBytes(body, "results")
			if !results.IsArray() {
				t.Fatalf("complemau: user directory response has no results array: %s", body)
			}

			resultIDs = resultIDs[:0]
			markerSeen := false
			for _, result := range results.Array() {
				userID := result.Get("user_id").String()
				resultIDs = append(resultIDs, userID)
				markerSeen = markerSeen || userID == markerUserID
			}
			return markerSeen
		}),
	)
	return resultIDs
}
