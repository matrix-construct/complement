// This file contains tests for "push rules of polls" as defined by MSC3930.
// The MSC that defines the design of the polls system is MSC3381.
//
// Note that implementation of MSC3381 is not required by the homeserver to
// pass the tests in this file.
//
// You can read either MSC using the links below.
// Polls: https://github.com/matrix-org/matrix-doc/pull/3381
// Push rules for polls: https://github.com/matrix-org/matrix-doc/pull/3930

package tests

import (
	"math"
	"testing"

	"github.com/tidwall/gjson"

	"github.com/matrix-org/complement"
	"github.com/matrix-org/complement/helpers"
	"github.com/matrix-org/complement/match"
	"github.com/matrix-org/complement/must"
)

const pollResponseRuleID = ".org.matrix.msc3930.rule.poll_response"
const pollStartOneToOneRuleID = ".org.matrix.msc3930.rule.poll_start_one_to_one"
const pollEndOneToOneRuleID = ".org.matrix.msc3930.rule.poll_end_one_to_one"
const pollStartRuleID = ".org.matrix.msc3930.rule.poll_start"
const pollEndRuleID = ".org.matrix.msc3930.rule.poll_end"

// assertPollTypeCondition verifies the event-type condition of a poll push rule.
// The condition may be serialized either as the legacy "event_match" kind (with
// a "pattern" key) or as the "event_property_is" kind (with a "value" key) named
// by MSC3930. For a literal event type both forms select the same events, so the
// test accepts whichever shape the homeserver presents.
func assertPollTypeCondition(t *testing.T, rule gjson.Result, wantType string) {
	t.Helper()
	for _, cond := range rule.Get("conditions").Array() {
		if cond.Get("key").Str != "type" {
			continue
		}
		switch cond.Get("kind").Str {
		case "event_match":
			must.Equal(t, cond.Get("pattern").Str, wantType, "event_match pattern on type")
			return
		case "event_property_is":
			must.Equal(t, cond.Get("value").Str, wantType, "event_property_is value on type")
			return
		}
	}
	t.Fatalf("poll rule %q has no event_match or event_property_is condition on \"type\" (want %q)", rule.Get("rule_id").Str, wantType)
}

func TestPollsLocalPushRules(t *testing.T) {
	deployment := complement.Deploy(t, 1)
	defer deployment.Destroy(t)

	// Create a user to poll the push rules of.
	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})

	// Test for the presence of the expected push rules. Clients are expected
	// to implement local matching of events based on the presented rules.
	t.Run("Polls push rules are correctly presented to the client", func(t *testing.T) {
		// Request each of the push rule IDs defined by MSC3930 and verify their structure.

		// This push rule silences all poll responses.
		pollResponseRule := alice.MustGetPushRule(t, "global", "override", pollResponseRuleID)
		must.MatchGJSON(
			t,
			pollResponseRule,
			match.JSONKeyEqual("actions", []string{}),
			match.JSONKeyEqual("default", true),
			match.JSONKeyEqual("enabled", true),
			// There should only be one condition defined for this type
			match.JSONKeyEqual("conditions.#", 1),
		)
		// Check the contents of the first (and only) condition
		assertPollTypeCondition(t, pollResponseRule, "org.matrix.msc3381.poll.response")

		// This push rule creates a sound and notifies the user when a poll is started in a one-to-one room.
		pollStartOneToOneRule := alice.MustGetPushRule(t, "global", "underride", pollStartOneToOneRuleID)
		must.MatchGJSON(
			t,
			pollStartOneToOneRule,
			// Check that the appropriate actions are set for this rule
			match.JSONKeyEqual("actions.#", 2),
			match.JSONKeyEqual("actions", []interface{}{
				"notify",
				map[string]interface{}{"set_tweak": "sound", "value": "default"},
			},
			),
			match.JSONKeyEqual("default", true),
			match.JSONKeyEqual("enabled", true),
			// There should two conditions defined for this type
			match.JSONKeyEqual("conditions.#", 2),
			// Check the condition that requires a room between two users
			match.JSONKeyEqual("conditions.#(kind==\"room_member_count\").is", "2"),
		)
		// Check the condition that requires a poll start event
		assertPollTypeCondition(t, pollStartOneToOneRule, "org.matrix.msc3381.poll.start")

		// This push rule creates a sound and notifies the user when a poll is ended in a one-to-one room.
		pollEndOneToOneRule := alice.MustGetPushRule(t, "global", "underride", pollEndOneToOneRuleID)
		must.MatchGJSON(
			t,
			pollEndOneToOneRule,
			// Check that the appropriate actions are set for this rule
			match.JSONKeyEqual("actions.#", 2),
			match.JSONKeyEqual("actions", []interface{}{
				"notify",
				map[string]interface{}{"set_tweak": "sound", "value": "default"},
			},
			),
			match.JSONKeyEqual("default", true),
			match.JSONKeyEqual("enabled", true),
			// There should two conditions defined for this type
			match.JSONKeyEqual("conditions.#", 2),
			// Check the condition that requires a room between two users
			match.JSONKeyEqual("conditions.#(kind==\"room_member_count\").is", "2"),
		)
		// Check the condition that requires a poll end event
		assertPollTypeCondition(t, pollEndOneToOneRule, "org.matrix.msc3381.poll.end")

		// This push rule notifies the user when a poll is started in any room.
		pollStartRule := alice.MustGetPushRule(t, "global", "underride", pollStartRuleID)
		must.MatchGJSON(
			t,
			pollStartRule,
			// Check that the appropriate actions are set for this rule
			match.JSONKeyEqual("actions", []string{"notify"}),
			match.JSONKeyEqual("default", true),
			match.JSONKeyEqual("enabled", true),
			// There should only be one condition defined for this type
			match.JSONKeyEqual("conditions.#", 1),
		)
		// Check the contents of the first (and only) condition
		assertPollTypeCondition(t, pollStartRule, "org.matrix.msc3381.poll.start")

		// This push rule notifies the user when a poll is ended in any room.
		pollEndRule := alice.MustGetPushRule(t, "global", "underride", pollEndRuleID)
		must.MatchGJSON(
			t,
			pollEndRule,
			// Check that the appropriate actions are set for this rule
			match.JSONKeyEqual("actions", []string{"notify"}),
			match.JSONKeyEqual("default", true),
			match.JSONKeyEqual("enabled", true),
			// There should only be one condition defined for this type
			match.JSONKeyEqual("conditions.#", 1),
		)
		// Check the contents of the first (and only) condition
		assertPollTypeCondition(t, pollEndRule, "org.matrix.msc3381.poll.end")

		// The DM-specific rules for poll start and poll end should come before the rules that
		// define behaviour for any room. We verify this by ensuring that the DM-specific rules
		// have a lower index when requesting all push rules.
		allPushRules := alice.GetAllPushRules(t)
		globalUnderridePushRules := allPushRules.Get("global").Get("underride").Array()

		pollStartOneToOneRuleIndex := math.MaxInt64
		pollEndOneToOneRuleIndex := math.MaxInt64
		for index, rule := range globalUnderridePushRules {
			// Iterate over the user's global underride rules as a client would.
			// If we come across a one-to-one room rule ID, we note down its index.
			// When we come across the rule ID of its generic counterpart, we ensure
			// its counterpart is at a higher index, and thus would be considered
			// lower priority.
			if rule.Get("rule_id").Str == pollStartOneToOneRuleID {
				pollStartOneToOneRuleIndex = index
			} else if rule.Get("rule_id").Str == pollEndOneToOneRuleID {
				pollEndOneToOneRuleIndex = index
			} else if rule.Get("rule_id").Str == pollStartRuleID {
				if pollStartOneToOneRuleIndex > index {
					t.Fatalf(
						"Expected rule '%s' to come after '%s' in '%s's global underride rules",
						pollStartRuleID,
						pollStartOneToOneRuleID,
						alice.UserID,
					)
				}
			} else if rule.Get("rule_id").Str == pollEndRuleID {
				if pollEndOneToOneRuleIndex > index {
					t.Fatalf(
						"Expected rule '%s' to come after '%s' in '%s's global underride rules",
						pollEndRuleID,
						pollEndOneToOneRuleID,
						alice.UserID,
					)
				}
			}
		}
	})

	// TODO: Test whether the homeserver correctly calls POST /_matrix/push/v1/notify on the push gateway
	// in accordance with the push rules. Blocked by Complement not having a push gateway implementation.
}
