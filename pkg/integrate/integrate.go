package integrate

import (
	"fmt"
	"os"
	"path/filepath"

	impl "tidbcloud-insight/pkg/integrate/cmds_impl"
	"tidbcloud-insight/pkg/logger"

	"github.com/innerr/ticat/pkg/core/model"
	"github.com/innerr/ticat/pkg/ticat"
)

func Integrate(tc *ticat.TiCat) error {
	execPath, err := os.Executable()
	if err != nil {
		execPath = "."
	}
	envPath := filepath.Join(filepath.Dir(execPath), "tidbcloud-insight.env")

	if err := tc.LoadEnvFile(envPath); err != nil {
		return fmt.Errorf("Error loading env file: %v\n", err)
	}

	tc.AddIntegratedModVersion("tidbcloud-insight 1.0")

	defEnv := tc.Env.GetLayer(model.EnvLayer3RdDefault)

	defEnv.Set("sys.hub.init-repo", "")
	defEnv.SetBool("display.utf8", false)
	defEnv.SetBool("display.meow", false)
	defEnv.SetBool("display.color", true)
	defEnv.SetBool(EnvKeyVerbose, true)
	defEnv.SetInt(impl.EnvKeyTargetChunkSizeMB, 8)

	defEnv.Set(EnvKeyCacheDir, "./cache")
	defEnv.Set(EnvKeyMetaDir, "./meta")
	defEnv.Set(EnvKeyFetchTimeout, "5m")
	defEnv.Set(EnvKeyIdleTimeout, "1m")
	defEnv.Set(EnvKeyRateLimitMaxBackoff, "5m")
	defEnv.SetInt(impl.EnvKeyRateLimitDesiredConcurrency, 5)
	defEnv.Set(EnvKeyRateLimitRecoveryInterval, "30s")
	defEnv.Set(EnvKeyRateLimitMinRecovery, "10s")

	defEnv.Set(EnvKeyAuthTokenURL, "https://tidb-soc2.us.auth0.com/oauth/token")
	defEnv.Set(EnvKeyAuthAudience, "https://tidb-soc2.us.auth0.com/api/v2/")

	RegisterCmds(tc.Cmds)
	RegisterHelp(tc)

	if tc.Env.GetBool(EnvKeyVerbose) {
		logger.SetVerbose(true)
	}
	logger.SetColorGetter(func() bool {
		return tc.Env.GetBool("display.color")
	})

	return nil
}
