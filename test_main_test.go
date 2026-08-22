package complement

import (
	"slices"
	"testing"
)

func TestWithEnvIsolation(t *testing.T) {
	input := []string{"FIRST=value"}
	option := WithEnv(input...)
	first := resolveDeployOpts(option)
	input[0] = "MUTATED=value"
	reused := resolveDeployOpts(option)
	second := resolveDeployOpts(WithEnv("SECOND=value"))

	if !slices.Equal(first.extraEnv, []string{"FIRST=value"}) {
		t.Errorf("first deployment environment = %v", first.extraEnv)
	}
	if !slices.Equal(reused.extraEnv, []string{"FIRST=value"}) {
		t.Errorf("reused deployment environment = %v", reused.extraEnv)
	}
	if !slices.Equal(second.extraEnv, []string{"SECOND=value"}) {
		t.Errorf("second deployment environment = %v", second.extraEnv)
	}
}
