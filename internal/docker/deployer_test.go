package docker

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/client"

	"github.com/matrix-org/complement/config"
)

func TestBuildContainerEnvAppendsDeploymentValues(t *testing.T) {
	const prefix = "COMPLEMENT_TEST_DEPLOY_ENV_"
	t.Setenv(prefix+"SHARED", "from-host")
	cfg := &config.Complement{EnvVarsPropagatePrefix: prefix}

	got := buildContainerEnv("hs1", cfg, []string{"SHARED=from-option", "LOCAL=value"})
	want := []string{"SERVER_NAME=hs1", "SHARED=from-host", "SHARED=from-option", "LOCAL=value"}
	if !slices.Equal(got, want) {
		t.Errorf("container environment = %v, want %v", got, want)
	}
}

// fakeDaemon answers each container action with the next of its queued statuses, and
// records the actions it was asked for.
type fakeDaemon struct {
	t        *testing.T
	statuses map[string][]int
	calls    []string
	body     json.RawMessage
}

func (f *fakeDaemon) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	action := path.Base(r.URL.Path)
	f.calls = append(f.calls, strings.TrimSuffix(action+"?"+r.URL.RawQuery, "?"))
	queued := f.statuses[action]
	if len(queued) == 0 {
		f.t.Errorf("unexpected request: %s %s", r.Method, r.URL)
		w.WriteHeader(http.StatusTeapot)
		return
	}
	f.statuses[action] = queued[1:]
	if action == "create" {
		if err := json.NewDecoder(r.Body).Decode(&f.body); err != nil {
			f.t.Errorf("decoding %s: %s", r.URL, err)
		}
	}
	w.WriteHeader(queued[0])
	if action == "wait" && queued[0] == http.StatusOK {
		w.Write([]byte(`{"StatusCode":137}`))
	}
}

func (f *fakeDaemon) client() *client.Client {
	f.t.Helper()
	server := httptest.NewServer(f)
	f.t.Cleanup(server.Close)
	docker, err := client.New(client.WithHost(server.URL), client.WithAPIVersion("1.55"))
	if err != nil {
		f.t.Fatalf("client.New: %s", err)
	}
	f.t.Cleanup(func() { docker.Close() })
	return docker
}

func TestStopServer(t *testing.T) {
	const stop, wait = "stop?t=10", "wait?condition=not-running"
	failed := http.StatusInternalServerError
	testCases := []struct {
		name      string
		statuses  map[string][]int
		wantCalls []string
		wantErr   bool
	}{
		{
			name:      "stopped",
			statuses:  map[string][]int{"stop": {http.StatusNoContent}},
			wantCalls: []string{stop},
		},
		{
			name:      "already stopped",
			statuses:  map[string][]int{"stop": {http.StatusNotModified}},
			wantCalls: []string{stop},
		},
		{
			name:      "exits after a failed stop",
			statuses:  map[string][]int{"stop": {failed}, "wait": {http.StatusOK}},
			wantCalls: []string{stop, wait},
		},
		{
			name:      "wait fails after a failed stop",
			statuses:  map[string][]int{"stop": {failed}, "wait": {http.StatusNotFound}},
			wantCalls: []string{stop, wait},
			wantErr:   true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			daemon := &fakeDaemon{t: t, statuses: tc.statuses}
			d := &Deployer{
				Docker: daemon.client(),
				config: &config.Complement{SpawnHSTimeout: 10 * time.Second},
			}

			err := d.StopServer(&HomeserverDeployment{ContainerID: "testee"})
			if (err != nil) != tc.wantErr {
				t.Errorf("StopServer() error = %v, wantErr %t", err, tc.wantErr)
			}
			if !slices.Equal(daemon.calls, tc.wantCalls) {
				t.Errorf("daemon calls = %v, want %v", daemon.calls, tc.wantCalls)
			}
		})
	}
}

func TestDestroyWaitsForExit(t *testing.T) {
	daemon := &fakeDaemon{t: t, statuses: map[string][]int{
		"stop":   {http.StatusInternalServerError},
		"wait":   {http.StatusOK},
		"testee": {http.StatusNoContent},
	}}
	d := &Deployer{
		Docker: daemon.client(),
		config: &config.Complement{DestroyHSTimeout: 10 * time.Second},
	}

	d.Destroy(&Deployment{HS: map[string]*HomeserverDeployment{
		"hs1": {ContainerID: "testee"},
	}}, false, "test", false)

	want := []string{"stop?t=10", "wait?condition=not-running", "testee?force=1"}
	if !slices.Equal(daemon.calls, want) {
		t.Errorf("daemon calls = %v, want %v", daemon.calls, want)
	}
}

func TestRemoveContainer(t *testing.T) {
	const remove, wait = "testee?force=1", "wait?condition=not-running"
	failed := http.StatusInternalServerError
	testCases := []struct {
		name      string
		statuses  map[string][]int
		wantCalls []string
		wantErr   bool
	}{
		{
			name:      "removed",
			statuses:  map[string][]int{"testee": {http.StatusNoContent}},
			wantCalls: []string{remove},
		},
		{
			name: "exits after a failed remove",
			statuses: map[string][]int{
				"testee": {failed, http.StatusNoContent},
				"wait":   {http.StatusOK},
			},
			wantCalls: []string{remove, wait, remove},
		},
		{
			name:      "wait fails after a failed remove",
			statuses:  map[string][]int{"testee": {failed}, "wait": {http.StatusNotFound}},
			wantCalls: []string{remove, wait},
			wantErr:   true,
		},
		{
			name:      "second remove fails",
			statuses:  map[string][]int{"testee": {failed, failed}, "wait": {http.StatusOK}},
			wantCalls: []string{remove, wait, remove},
			wantErr:   true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			daemon := &fakeDaemon{t: t, statuses: tc.statuses}

			err := removeContainer(daemon.client(), "testee")
			if (err != nil) != tc.wantErr {
				t.Errorf("removeContainer() error = %v, wantErr %t", err, tc.wantErr)
			}
			if !slices.Equal(daemon.calls, tc.wantCalls) {
				t.Errorf("daemon calls = %v, want %v", daemon.calls, tc.wantCalls)
			}
		})
	}
}

func TestDeployImageSetsStopTimeout(t *testing.T) {
	daemon := &fakeDaemon{
		t:        t,
		statuses: map[string][]int{"create": {http.StatusInternalServerError}},
	}

	_, err := deployImage(
		daemon.client(), "image", "testee", "pkg", "blueprint", "hs1",
		nil, "context", "network", &config.Complement{}, nil,
	)
	if err == nil {
		t.Fatal("deployImage() succeeded, want the failed create")
	}

	var created struct{ StopTimeout *int }
	if err := json.Unmarshal(daemon.body, &created); err != nil {
		t.Fatalf("decoding the create body: %s", err)
	}
	if created.StopTimeout == nil || *created.StopTimeout != containerExitGraceSecs {
		t.Errorf("StopTimeout = %v, want %d", created.StopTimeout, containerExitGraceSecs)
	}
}
