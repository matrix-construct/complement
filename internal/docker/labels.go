package docker

import (
	"encoding/base64"
	"strings"

	"github.com/docker/docker/api/types/filters"

	"github.com/matrix-org/complement/b"
)

// label returns a filter for the presence of certain labels ("complement_context") or a match of
// labels ("complement_blueprint=foo").
func label(labelFilters ...string) filters.Args {
	f := filters.NewArgs()
	// label=<key> or label=<key>=<value>
	for _, in := range labelFilters {
		f.Add("label", in)
	}
	return f
}

// runIDLabel scopes a docker resource (container, network, or committed image) to a single
// COMPLEMENT_RUN_ID. Concurrent `go test` invocations sharing one docker daemon stamp distinct
// run IDs, so neither lists nor cleans up the other's resources.
const runIDLabel = "complement_run_id"

// runFilter returns the label match term that narrows a docker query to one run, for use as a
// label(...) argument alongside the other resource filters.
func runFilter(runID string) string {
	return runIDLabel + "=" + runID
}

func tokensFromLabels(labels map[string]string) map[string]string {
	userIDToToken := make(map[string]string)
	for k, v := range labels {
		if strings.HasPrefix(k, "access_token_") {
			userIDToToken[strings.TrimPrefix(k, "access_token_")] = v
		}
	}
	return userIDToToken
}

func asIDToRegistrationFromLabels(labels map[string]string) map[string]string {
	asMap := make(map[string]string)
	for k, v := range labels {
		if strings.HasPrefix(k, "application_service_") {
			asMap[strings.TrimPrefix(k, "application_service_")] = decodeASRegistrationLabel(v)
		}
	}
	return asMap
}

func labelsForApplicationServices(hs b.Homeserver) map[string]string {
	labels := make(map[string]string)
	// collect and store app service registrations as labels 'application_service_$as_id: $registration'
	// collect and store app service access tokens as labels 'access_token_$sender_localpart: $as_token'
	for _, as := range hs.ApplicationServices {
		labels["application_service_"+as.ID] = encodeASRegistrationLabel(generateASRegistrationYaml(as))

		labels["access_token_@"+as.SenderLocalpart+":"+hs.Name] = as.ASToken
	}
	return labels
}

const encodedASRegistrationPrefix = "base64:"

func encodeASRegistrationLabel(registration string) string {
	return encodedASRegistrationPrefix + base64.RawStdEncoding.EncodeToString([]byte(registration))
}

func decodeASRegistrationLabel(value string) string {
	if !strings.HasPrefix(value, encodedASRegistrationPrefix) {
		return strings.ReplaceAll(value, "\\n", "\n")
	}
	decoded, err := base64.RawStdEncoding.DecodeString(strings.TrimPrefix(value, encodedASRegistrationPrefix))
	if err != nil {
		return value
	}
	return string(decoded)
}

func deviceIDsFromLabels(labels map[string]string) map[string]string {
	userIDToToken := make(map[string]string)
	for k, v := range labels {
		if strings.HasPrefix(k, "device_id") {
			userIDToToken[strings.TrimPrefix(k, "device_id")] = v
		}
	}
	return userIDToToken
}
