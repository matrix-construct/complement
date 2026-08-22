package docker

import (
	"slices"
	"testing"

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
