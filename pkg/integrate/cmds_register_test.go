package integrate

import (
	"testing"

	impl "tidbcloud-insight/pkg/integrate/cmds_impl"

	"github.com/innerr/ticat/pkg/core/model"
	"github.com/innerr/ticat/pkg/ticat"
)

func TestConfigEmptyArgsNotOverwriteEnv(t *testing.T) {
	tc := ticat.NewTiCatForTest()
	RegisterCmds(tc.Cmds)

	env := tc.Env.GetLayer(model.EnvLayerSession)

	env.Set(EnvKeyCacheDir, "/existing/cache")
	env.Set(EnvKeyFetchTimeout, "10m")
	env.Set(EnvKeyVerbose, "false")

	tc.RunCli("config")

	if env.GetRaw(EnvKeyCacheDir) != "/existing/cache" {
		t.Errorf("Expected cache-dir to remain '/existing/cache', got '%s'", env.GetRaw(EnvKeyCacheDir))
	}
	if env.GetRaw(EnvKeyFetchTimeout) != "10m" {
		t.Errorf("Expected fetch-timeout to remain '10m', got '%s'", env.GetRaw(EnvKeyFetchTimeout))
	}
	if env.GetRaw(EnvKeyVerbose) != "false" {
		t.Errorf("Expected verbose to remain 'false', got '%s'", env.GetRaw(EnvKeyVerbose))
	}
}

func TestConfigProvidedArgsOverwriteEnv(t *testing.T) {
	tc := ticat.NewTiCatForTest()
	RegisterCmds(tc.Cmds)

	env := tc.Env.GetLayer(model.EnvLayerSession)

	env.Set(EnvKeyCacheDir, "/existing/cache")
	env.Set(EnvKeyFetchTimeout, "10m")

	tc.RunCli("config", "cache-dir=/new/cache", "fetch-timeout=20m")

	if env.GetRaw(EnvKeyCacheDir) != "/new/cache" {
		t.Errorf("Expected cache-dir to be '/new/cache', got '%s'", env.GetRaw(EnvKeyCacheDir))
	}
	if env.GetRaw(EnvKeyFetchTimeout) != "20m" {
		t.Errorf("Expected fetch-timeout to be '20m', got '%s'", env.GetRaw(EnvKeyFetchTimeout))
	}
}

func TestConfigPartialArgsOnlyAffectsProvided(t *testing.T) {
	tc := ticat.NewTiCatForTest()
	RegisterCmds(tc.Cmds)

	env := tc.Env.GetLayer(model.EnvLayerSession)

	env.Set(EnvKeyCacheDir, "/existing/cache")
	env.Set(EnvKeyMetaDir, "/existing/meta")
	env.Set(EnvKeyVerbose, "false")

	tc.RunCli("config", "cache-dir=/new/cache")

	if env.GetRaw(EnvKeyCacheDir) != "/new/cache" {
		t.Errorf("Expected cache-dir to be '/new/cache', got '%s'", env.GetRaw(EnvKeyCacheDir))
	}
	if env.GetRaw(EnvKeyMetaDir) != "/existing/meta" {
		t.Errorf("Expected meta-dir to remain '/existing/meta', got '%s'", env.GetRaw(EnvKeyMetaDir))
	}
	if env.GetRaw(EnvKeyVerbose) != "false" {
		t.Errorf("Expected verbose to remain 'false', got '%s'", env.GetRaw(EnvKeyVerbose))
	}
}

func TestConfigDefaultSetsDefaultValues(t *testing.T) {
	tc := ticat.NewTiCatForTest()
	RegisterCmds(tc.Cmds)

	env := tc.Env.GetLayer(model.EnvLayerSession)

	tc.RunCli("config.default")

	if env.GetRaw(EnvKeyCacheDir) != "./cache" {
		t.Errorf("Expected cache-dir to be './cache', got '%s'", env.GetRaw(EnvKeyCacheDir))
	}
	if env.GetRaw(EnvKeyMetaDir) != "./meta" {
		t.Errorf("Expected meta-dir to be './meta', got '%s'", env.GetRaw(EnvKeyMetaDir))
	}
	if env.GetRaw(EnvKeyVerbose) != "true" {
		t.Errorf("Expected verbose to be 'true', got '%s'", env.GetRaw(EnvKeyVerbose))
	}
	if env.GetRaw(EnvKeyFetchTimeout) != "5m" {
		t.Errorf("Expected fetch-timeout to be '5m', got '%s'", env.GetRaw(EnvKeyFetchTimeout))
	}
	if env.GetRaw(EnvKeyIdleTimeout) != "1m" {
		t.Errorf("Expected idle-timeout to be '1m', got '%s'", env.GetRaw(EnvKeyIdleTimeout))
	}
	if env.GetRaw(impl.EnvKeyTargetChunkSizeMB) != "8" {
		t.Errorf("Expected fetch-target-chunk-size-mb to be '8', got '%s'", env.GetRaw(impl.EnvKeyTargetChunkSizeMB))
	}
	if env.GetRaw(EnvKeyTimeDuration) != "21d" {
		t.Errorf("Expected time-duration to be '21d', got '%s'", env.GetRaw(EnvKeyTimeDuration))
	}
	if env.GetRaw(impl.EnvKeyCacheMaxSizeMB) != "100000" {
		t.Errorf("Expected cache-max-size-mb to be '100000', got '%s'", env.GetRaw(impl.EnvKeyCacheMaxSizeMB))
	}
}

func TestConfigDefaultSetsEmptyEnv(t *testing.T) {
	tc := ticat.NewTiCatForTest()
	RegisterCmds(tc.Cmds)

	env := tc.Env.GetLayer(model.EnvLayerSession)

	tc.RunCli("config.default")

	if env.GetRaw(EnvKeyCacheDir) != "./cache" {
		t.Errorf("Expected cache-dir to be './cache', got '%s'", env.GetRaw(EnvKeyCacheDir))
	}
	if env.GetRaw(EnvKeyFetchTimeout) != "5m" {
		t.Errorf("Expected fetch-timeout to be '5m', got '%s'", env.GetRaw(EnvKeyFetchTimeout))
	}
}

func TestConfigDefaultNoAuthClientSecret(t *testing.T) {
	tc := ticat.NewTiCatForTest()
	RegisterCmds(tc.Cmds)

	env := tc.Env.GetLayer(model.EnvLayerSession)

	tc.RunCli("config.default")

	if env.GetRaw(EnvKeyAuthClientID) != "" {
		t.Errorf("Expected auth-client-id to be empty (not set by config.default), got '%s'", env.GetRaw(EnvKeyAuthClientID))
	}
	if env.GetRaw(EnvKeyAuthClientSecret) != "" {
		t.Errorf("Expected auth-client-secret to be empty (not set by config.default), got '%s'", env.GetRaw(EnvKeyAuthClientSecret))
	}
}
