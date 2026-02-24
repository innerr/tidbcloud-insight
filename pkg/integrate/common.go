package integrate

import (
	"fmt"
	"time"

	"tidbcloud-insight/pkg/auth"
	impl "tidbcloud-insight/pkg/integrate/cmds_impl"

	"github.com/innerr/ticat/pkg/core/model"
)

type AuthParams struct {
	ClientID     string
	ClientSecret string
	TokenURL     string
	Audience     string
	CachePath    string
}

type ClientParams = impl.ClientParams

func getEnvString(env *model.Env, key, def string) string {
	v := env.GetRaw(key)
	if v == "" {
		return def
	}
	return v
}

func getEnvInt(env *model.Env, key string, def int) int {
	v := env.GetInt(key)
	if v == 0 {
		return def
	}
	return v
}

func getEnvDuration(env *model.Env, key string, def time.Duration) time.Duration {
	s := env.GetRaw(key)
	if s == "" {
		return def
	}
	if d, err := time.ParseDuration(s); err == nil {
		return d
	}
	return def
}

func getEnvBool(env *model.Env, key string, def bool) bool {
	return env.GetBool(key)
}

func getCacheDir(env *model.Env) string {
	return getEnvString(env, EnvKeyCacheDir, "./cache")
}

func getMetaDir(env *model.Env) string {
	return getEnvString(env, EnvKeyMetaDir, "./meta")
}

func getAuthParams(env *model.Env, cacheDir string) (AuthParams, error) {
	tokenURL := env.GetRaw(EnvKeyAuthTokenURL)
	if tokenURL == "" {
		return AuthParams{}, fmt.Errorf(EnvKeyAuthTokenURL + " is not set")
	}
	audience := env.GetRaw(EnvKeyAuthAudience)
	if audience == "" {
		return AuthParams{}, fmt.Errorf(EnvKeyAuthAudience + " is not set")
	}
	return AuthParams{
		ClientID:     env.GetRaw(EnvKeyAuthClientID),
		ClientSecret: env.GetRaw(EnvKeyAuthClientSecret),
		TokenURL:     tokenURL,
		Audience:     audience,
		CachePath:    cacheDir + "/auth.json",
	}, nil
}

func getClientParams(env *model.Env) ClientParams {
	return ClientParams{
		FetchTimeout: getEnvDuration(env, EnvKeyFetchTimeout, 5*time.Minute),
		IdleTimeout:  getEnvDuration(env, EnvKeyIdleTimeout, 3*time.Minute),
		DisplayVerb:  getEnvBool(env, EnvKeyVerbose, true),
		Concurrency: impl.ConcurrencyConfig{
			DesiredConcurrency:  env.GetInt(impl.EnvKeyRateLimitDesiredConcurrency),
			RecoveryInterval:    getEnvDuration(env, EnvKeyRateLimitRecoveryInterval, 30*time.Second),
			MinRecoveryInterval: getEnvDuration(env, EnvKeyRateLimitMinRecovery, 10*time.Second),
		},
	}
}

func getMaxBackoff(env *model.Env) time.Duration {
	return getEnvDuration(env, EnvKeyRateLimitMaxBackoff, 5*time.Minute)
}

func (p AuthParams) NewManager() *auth.Manager {
	return auth.NewManager(p.ClientID, p.ClientSecret, p.TokenURL, p.Audience, p.CachePath)
}

func getTimeRangeFromEnv(env *model.Env) (*TimeRange, error) {
	return ParseTimeRange(
		env.GetRaw(EnvKeyTimeStart),
		env.GetRaw(EnvKeyTimeEnd),
		env.GetRaw(EnvKeyTimeDurationAgoAsEnd),
		env.GetRaw(EnvKeyTimeDuration),
		time.Now(),
	)
}
