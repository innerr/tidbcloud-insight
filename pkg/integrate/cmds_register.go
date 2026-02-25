package integrate

import (
	"github.com/innerr/ticat/pkg/core/model"
	"github.com/innerr/ticat/pkg/ticat"

	impl "tidbcloud-insight/pkg/integrate/cmds_impl"
)

func noOpCmd(argv model.ArgVals, cc *model.Cli, env *model.Env, flow []model.ParsedCmd) error {
	return nil
}

const (
	EnvPrefix = "tidbcloud-insight."

	EnvKeyConfigFile      = EnvPrefix + "config.file"
	EnvKeyCacheDir        = EnvPrefix + "cache.dir"
	EnvKeyMetaDir         = EnvPrefix + "meta.dir"
	EnvKeyVerbose         = EnvPrefix + "log.verbose"
	EnvKeyLogAllHTTPCodes = EnvPrefix + "log.all-http-codes"

	EnvKeyAuthClientID     = EnvPrefix + "auth.client-id"
	EnvKeyAuthClientSecret = EnvPrefix + "auth.client-secret"
	EnvKeyAuthTokenURL     = EnvPrefix + "auth.token-url"
	EnvKeyAuthAudience     = EnvPrefix + "auth.audience"

	EnvKeyRateLimitMaxBackoff       = EnvPrefix + "rate-limit.max-backoff"
	EnvKeyRateLimitRecoveryInterval = EnvPrefix + "rate-limit.recovery-interval"
	EnvKeyRateLimitMinRecovery      = EnvPrefix + "rate-limit.min-recovery-interval"

	EnvKeyFetchTimeout = EnvPrefix + "fetch.timeout"
	EnvKeyIdleTimeout  = EnvPrefix + "fetch.idle-timeout"

	EnvKeyTimeStart            = EnvPrefix + "time.start"
	EnvKeyTimeEnd              = EnvPrefix + "time.end"
	EnvKeyTimeDurationAgoAsEnd = EnvPrefix + "time.duration-ago-as-end"
	EnvKeyTimeDuration         = EnvPrefix + "time.duration"

	EnvKeyBizType              = EnvPrefix + "biz.type"
	EnvKeyLocal                = EnvPrefix + "local"
	EnvKeyCacheID              = EnvPrefix + "cache.id"
	EnvKeyConcurrency          = EnvPrefix + "dig.concurrency"
	EnvKeyClusterID            = EnvPrefix + "cluster.id"
	EnvKeyMetrics              = EnvPrefix + "query.metrics"
	EnvKeyClustersListFile     = EnvPrefix + "clusters.list-file"
	EnvKeyClustersFetchTimeout = EnvPrefix + "clusters.fetch-timeout"
	EnvKeyClustersPageSize     = EnvPrefix + "clusters.page-size"
)

func RegisterCmds(cmds *model.CmdTree) {
	configCmd := cmds.AddSub("config", "cfg").RegCmd(noOpCmd, "set config values", "")

	configCmd.AddArg("cache-dir", "", "cache", "cd").
		AddArg2Env(EnvKeyCacheDir, "cache-dir")
	configCmd.AddArg("meta-dir", "", "meta", "md").
		AddArg2Env(EnvKeyMetaDir, "meta-dir")
	configCmd.AddArg("verbose", "", "v").
		AddArg2Env(EnvKeyVerbose, "verbose")
	configCmd.AddArg("log-all-http-codes", "", "http-codes").
		AddArg2Env(EnvKeyLogAllHTTPCodes, "log-all-http-codes")

	configCmd.AddArg("auth-client-id", "", "client-id", "acid").
		AddArg2Env(EnvKeyAuthClientID, "auth-client-id")
	configCmd.AddArg("auth-client-secret", "", "client-secret", "secret", "acs").
		AddArg2Env(EnvKeyAuthClientSecret, "auth-client-secret")
	configCmd.AddArg("auth-token-url", "", "token-url", "atu").
		AddArg2Env(EnvKeyAuthTokenURL, "auth-token-url")
	configCmd.AddArg("auth-audience", "", "audience", "aud").
		AddArg2Env(EnvKeyAuthAudience, "auth-audience")

	configCmd.AddArg("rate-limit-max-backoff", "", "max-backoff", "rlmb").
		AddArg2Env(EnvKeyRateLimitMaxBackoff, "rate-limit-max-backoff")
	configCmd.AddArg("rate-limit-recovery-interval", "", "recovery-interval", "rlri").
		AddArg2Env(EnvKeyRateLimitRecoveryInterval, "rate-limit-recovery-interval")
	configCmd.AddArg("rate-limit-min-recovery", "", "min-recovery", "rlmr").
		AddArg2Env(EnvKeyRateLimitMinRecovery, "rate-limit-min-recovery")
	configCmd.AddArg("rate-limit-desired-concurrency", "", "concurrency", "rldc").
		AddArg2Env(impl.EnvKeyRateLimitDesiredConcurrency, "rate-limit-desired-concurrency")

	configCmd.AddArg("fetch-timeout", "", "timeout", "ft").
		AddArg2Env(EnvKeyFetchTimeout, "fetch-timeout")
	configCmd.AddArg("idle-timeout", "", "idle", "it").
		AddArg2Env(EnvKeyIdleTimeout, "idle-timeout")
	configCmd.AddArg("fetch-target-chunk-size-mb", "", "chunk-size", "ftcs").
		AddArg2Env(impl.EnvKeyTargetChunkSizeMB, "fetch-target-chunk-size-mb")

	configCmd.AddArg("time-start", "", "start", "ts").
		AddArg2Env(EnvKeyTimeStart, "time-start")
	configCmd.AddArg("time-end", "", "end", "te").
		AddArg2Env(EnvKeyTimeEnd, "time-end")
	configCmd.AddArg("time-duration-ago-as-end", "", "ago-as-end", "aae").
		AddArg2Env(EnvKeyTimeDurationAgoAsEnd, "time-duration-ago-as-end")
	configCmd.AddArg("time-duration", "", "duration", "td").
		AddArg2Env(EnvKeyTimeDuration, "time-duration")

	configCmd.AddArg("cluster-id", "", "cluster", "id", "cid").
		AddArg2Env(EnvKeyClusterID, "cluster-id")
	configCmd.AddArg("metrics", "", "m").
		AddArg2Env(EnvKeyMetrics, "metrics")
	configCmd.AddArg("metrics-fetch-step", "", "step", "mfs").
		AddArg2Env(impl.EnvKeyMetricsFetchStep, "metrics-fetch-step")

	configCmd.AddArg("clusters-fetch-timeout", "", "cft").
		AddArg2Env(EnvKeyClustersFetchTimeout, "clusters-fetch-timeout")
	configCmd.AddArg("clusters-page-size", "", "page-size", "cps").
		AddArg2Env(EnvKeyClustersPageSize, "clusters-page-size")
	configCmd.AddArg("clusters-list-file", "", "list-file", "clf").
		AddArg2Env(EnvKeyClustersListFile, "clusters-list-file")

	configCmd.AddArg("cache-max-size-mb", "", "max-size", "cms").
		AddArg2Env(impl.EnvKeyCacheMaxSizeMB, "cache-max-size-mb").Owner()

	configDefault := configCmd.AddSub("default", "def").RegCmd(noOpCmd, "set config to default values", "")

	configDefault.AddArg("cache-dir", "./cache", "cache", "cd").
		AddArg2Env(EnvKeyCacheDir, "cache-dir")
	configDefault.AddArg("meta-dir", "./meta", "meta", "md").
		AddArg2Env(EnvKeyMetaDir, "meta-dir")
	configDefault.AddArg("verbose", "true", "v").
		AddArg2Env(EnvKeyVerbose, "verbose")
	configDefault.AddArg("log-all-http-codes", "", "http-codes").
		AddArg2Env(EnvKeyLogAllHTTPCodes, "log-all-http-codes")

	configDefault.AddArg("auth-token-url", "https://tidb-soc2.us.auth0.com/oauth/token", "token-url", "atu").
		AddArg2Env(EnvKeyAuthTokenURL, "auth-token-url")
	configDefault.AddArg("auth-audience", "https://tidb-soc2.us.auth0.com/api/v2/", "audience", "aud").
		AddArg2Env(EnvKeyAuthAudience, "auth-audience")

	configDefault.AddArg("rate-limit-max-backoff", "5m", "max-backoff", "rlmb").
		AddArg2Env(EnvKeyRateLimitMaxBackoff, "rate-limit-max-backoff")
	configDefault.AddArg("rate-limit-recovery-interval", "30s", "recovery-interval", "rlri").
		AddArg2Env(EnvKeyRateLimitRecoveryInterval, "rate-limit-recovery-interval")
	configDefault.AddArg("rate-limit-min-recovery", "10s", "min-recovery", "rlmr").
		AddArg2Env(EnvKeyRateLimitMinRecovery, "rate-limit-min-recovery")
	configDefault.AddArg("rate-limit-desired-concurrency", "5", "concurrency", "rldc").
		AddArg2Env(impl.EnvKeyRateLimitDesiredConcurrency, "rate-limit-desired-concurrency")

	configDefault.AddArg("fetch-timeout", "5m", "timeout", "ft").
		AddArg2Env(EnvKeyFetchTimeout, "fetch-timeout")
	configDefault.AddArg("idle-timeout", "1m", "idle", "it").
		AddArg2Env(EnvKeyIdleTimeout, "idle-timeout")
	configDefault.AddArg("fetch-target-chunk-size-mb", "8", "chunk-size", "ftcs").
		AddArg2Env(impl.EnvKeyTargetChunkSizeMB, "fetch-target-chunk-size-mb")

	configDefault.AddArg("time-start", "", "start", "ts").
		AddArg2Env(EnvKeyTimeStart, "time-start")
	configDefault.AddArg("time-end", "", "end", "te").
		AddArg2Env(EnvKeyTimeEnd, "time-end")
	configDefault.AddArg("time-duration-ago-as-end", "", "ago-as-end", "aae").
		AddArg2Env(EnvKeyTimeDurationAgoAsEnd, "time-duration-ago-as-end")
	configDefault.AddArg("time-duration", "21d", "duration", "td").
		AddArg2Env(EnvKeyTimeDuration, "time-duration")

	configDefault.AddArg("cluster-id", "", "cluster", "id", "cid").
		AddArg2Env(EnvKeyClusterID, "cluster-id")
	configDefault.AddArg("metrics", "", "m").
		AddArg2Env(EnvKeyMetrics, "metrics")
	configDefault.AddArg("metrics-fetch-step", "2m", "step", "mfs").
		AddArg2Env(impl.EnvKeyMetricsFetchStep, "metrics-fetch-step")

	configDefault.AddArg("clusters-fetch-timeout", "60s", "cft").
		AddArg2Env(EnvKeyClustersFetchTimeout, "clusters-fetch-timeout")
	configDefault.AddArg("clusters-page-size", "500", "page-size", "cps").
		AddArg2Env(EnvKeyClustersPageSize, "clusters-page-size")
	configDefault.AddArg("clusters-list-file", "", "list-file", "clf").
		AddArg2Env(EnvKeyClustersListFile, "clusters-list-file")

	configDefault.AddArg("cache-max-size-mb", "100000", "max-size", "cms").
		AddArg2Env(impl.EnvKeyCacheMaxSizeMB, "cache-max-size-mb")

	dig := cmds.AddSub("dig", "d").RegEmptyCmd("dig operations").Owner()

	dig.AddSub("profile", "profiling", "prof", "p").RegPowerCmd(DigProfileCmd,
		"analyze cluster load profile and characteristics").
		AddArg("cluster-id", "", "cluster", "id", "c").
		AddArg2Env(EnvKeyClusterID, "cluster-id").
		AddEnvOp(EnvKeyClusterID, model.EnvOpTypeRead).
		AddArg("start", "", "s").
		AddArg2Env(EnvKeyTimeStart, "start").
		AddEnvOp(EnvKeyTimeStart, model.EnvOpTypeMayRead).
		AddArg("end", "", "e").
		AddArg2Env(EnvKeyTimeEnd, "end").
		AddEnvOp(EnvKeyTimeEnd, model.EnvOpTypeMayRead).
		AddArg("duration-ago-as-end", "", "ago-as-end", "aae", "a").
		AddArg2Env(EnvKeyTimeDurationAgoAsEnd, "duration-ago-as-end").
		AddEnvOp(EnvKeyTimeDurationAgoAsEnd, model.EnvOpTypeMayRead).
		AddArg("duration", "7d", "d").
		AddArg2Env(EnvKeyTimeDuration, "duration").
		AddEnvOp(EnvKeyTimeDuration, model.EnvOpTypeMayRead)

	dig.AddSub("abnormal", "a").RegPowerCmd(DigAbnormalCmd,
		"detect anomalies using all detection algorithms").
		AddArg("cluster-id", "", "cluster", "id", "c").
		AddArg2Env(EnvKeyClusterID, "cluster-id").
		AddEnvOp(EnvKeyClusterID, model.EnvOpTypeRead).
		AddArg("start", "", "s").
		AddArg2Env(EnvKeyTimeStart, "start").
		AddEnvOp(EnvKeyTimeStart, model.EnvOpTypeMayRead).
		AddArg("end", "", "e").
		AddArg2Env(EnvKeyTimeEnd, "end").
		AddEnvOp(EnvKeyTimeEnd, model.EnvOpTypeMayRead).
		AddArg("duration-ago-as-end", "", "ago-as-end", "aae", "a").
		AddArg2Env(EnvKeyTimeDurationAgoAsEnd, "duration-ago-as-end").
		AddEnvOp(EnvKeyTimeDurationAgoAsEnd, model.EnvOpTypeMayRead).
		AddArg("duration", "7d", "d").
		AddArg2Env(EnvKeyTimeDuration, "duration").
		AddEnvOp(EnvKeyTimeDuration, model.EnvOpTypeMayRead)

	digRandom := dig.AddSub("random", "r").RegEmptyCmd("random cluster operations").Owner()

	digRandom.AddSub("profile", "profiling", "prof", "p").RegPowerCmd(DigRandomProfileCmd,
		"analyze load profile for a random cluster").
		AddArg("start", "", "s").
		AddArg2Env(EnvKeyTimeStart, "start").
		AddEnvOp(EnvKeyTimeStart, model.EnvOpTypeMayRead).
		AddArg("end", "", "e").
		AddArg2Env(EnvKeyTimeEnd, "end").
		AddEnvOp(EnvKeyTimeEnd, model.EnvOpTypeMayRead).
		AddArg("duration-ago-as-end", "", "ago-as-end", "aae", "a").
		AddArg2Env(EnvKeyTimeDurationAgoAsEnd, "duration-ago-as-end").
		AddEnvOp(EnvKeyTimeDurationAgoAsEnd, model.EnvOpTypeMayRead).
		AddArg("duration", "7d", "d").
		AddArg2Env(EnvKeyTimeDuration, "duration").
		AddEnvOp(EnvKeyTimeDuration, model.EnvOpTypeMayRead)

	digRandom.AddSub("abnormal", "a").RegPowerCmd(DigRandomAbnormalCmd,
		"detect anomalies for a random cluster").
		AddArg("start", "", "s").
		AddArg2Env(EnvKeyTimeStart, "start").
		AddEnvOp(EnvKeyTimeStart, model.EnvOpTypeMayRead).
		AddArg("end", "", "e").
		AddArg2Env(EnvKeyTimeEnd, "end").
		AddEnvOp(EnvKeyTimeEnd, model.EnvOpTypeMayRead).
		AddArg("duration-ago-as-end", "", "ago-as-end", "aae", "a").
		AddArg2Env(EnvKeyTimeDurationAgoAsEnd, "duration-ago-as-end").
		AddEnvOp(EnvKeyTimeDurationAgoAsEnd, model.EnvOpTypeMayRead).
		AddArg("duration", "7d", "d").
		AddArg2Env(EnvKeyTimeDuration, "duration").
		AddEnvOp(EnvKeyTimeDuration, model.EnvOpTypeMayRead)

	digWalk := dig.AddSub("walk", "w").RegEmptyCmd("walk through all clusters").Owner()

	digWalk.AddSub("profile", "profiling", "prof", "p").RegPowerCmd(DigWalkProfileCmd,
		"analyze load profile for all clusters sequentially").
		AddArg("start", "", "s").
		AddArg2Env(EnvKeyTimeStart, "start").
		AddEnvOp(EnvKeyTimeStart, model.EnvOpTypeMayRead).
		AddArg("end", "", "e").
		AddArg2Env(EnvKeyTimeEnd, "end").
		AddEnvOp(EnvKeyTimeEnd, model.EnvOpTypeMayRead).
		AddArg("duration-ago-as-end", "", "ago-as-end", "aae", "a").
		AddArg2Env(EnvKeyTimeDurationAgoAsEnd, "duration-ago-as-end").
		AddEnvOp(EnvKeyTimeDurationAgoAsEnd, model.EnvOpTypeMayRead).
		AddArg("duration", "7d", "d").
		AddArg2Env(EnvKeyTimeDuration, "duration").
		AddEnvOp(EnvKeyTimeDuration, model.EnvOpTypeMayRead)

	digWalk.AddSub("abnormal", "a").RegPowerCmd(DigWalkAbnormalCmd,
		"detect anomalies for all clusters sequentially").
		AddArg("start", "", "s").
		AddArg2Env(EnvKeyTimeStart, "start").
		AddEnvOp(EnvKeyTimeStart, model.EnvOpTypeMayRead).
		AddArg("end", "", "e").
		AddArg2Env(EnvKeyTimeEnd, "end").
		AddEnvOp(EnvKeyTimeEnd, model.EnvOpTypeMayRead).
		AddArg("duration-ago-as-end", "", "ago-as-end", "aae", "a").
		AddArg2Env(EnvKeyTimeDurationAgoAsEnd, "duration-ago-as-end").
		AddEnvOp(EnvKeyTimeDurationAgoAsEnd, model.EnvOpTypeMayRead).
		AddArg("duration", "7d", "d").
		AddArg2Env(EnvKeyTimeDuration, "duration").
		AddEnvOp(EnvKeyTimeDuration, model.EnvOpTypeMayRead)

	clusters := cmds.AddSub("clusters", "cluster", "c").RegEmptyCmd("cluster operations").Owner()

	clusters.AddSub("fetch-all", "fa").RegFlowBuiltinCmd(
		[]string{"clusters.dedicated.fetch", "clusters.premium.fetch", "metrics.fetch.all"},
		"fetch all important metrics of dedicated and premium clusters")

	dedicated := clusters.AddSub("dedicated", "ded").RegEmptyCmd("dedicated cluster operations").Owner()
	dedicated.AddSub("fetch", "f").RegPowerCmd(ClustersDedicatedFetch,
		"fetch and list dedicated clusters").
		AddArg("timeout", "60s", "t").
		AddArg2Env(EnvKeyClustersFetchTimeout, "timeout").
		AddEnvOp(EnvKeyClustersFetchTimeout, model.EnvOpTypeRead).
		AddArg("page-size", "500", "ps").
		AddArg2Env(EnvKeyClustersPageSize, "page-size").
		AddEnvOp(EnvKeyClustersPageSize, model.EnvOpTypeRead).
		AddEnvOp(EnvKeyClustersListFile, model.EnvOpTypeWrite)

	premium := clusters.AddSub("premium", "prem", "p").RegEmptyCmd("premium cluster operations").Owner()
	premium.AddSub("fetch", "f").RegPowerCmd(ClustersPremiumFetch,
		"fetch and list premium clusters").
		AddArg("timeout", "60s", "t").
		AddArg2Env(EnvKeyClustersFetchTimeout, "timeout").
		AddEnvOp(EnvKeyClustersFetchTimeout, model.EnvOpTypeRead).
		AddArg("page-size", "500", "ps").
		AddArg2Env(EnvKeyClustersPageSize, "page-size").
		AddEnvOp(EnvKeyClustersPageSize, model.EnvOpTypeRead).
		AddEnvOp(EnvKeyClustersListFile, model.EnvOpTypeWrite)

	metrics := cmds.AddSub("metrics", "m").RegEmptyCmd("metrics operations").Owner()

	metricsFetch := metrics.AddSub("fetch", "f").RegPowerCmd(MetricsFetchCmd,
		"fetch dig metrics from cluster and save to cache").
		AddArg("cluster-id", "", "cluster", "id").
		AddArg2Env(EnvKeyClusterID, "cluster-id").
		AddEnvOp(EnvKeyClusterID, model.EnvOpTypeRead).
		AddArg("metric", "", "m").
		AddArg2Env(EnvKeyMetrics, "metric").
		AddEnvOp(EnvKeyMetrics, model.EnvOpTypeMayRead).
		AddArg("start", "", "s").
		AddArg2Env(EnvKeyTimeStart, "start").
		AddEnvOp(EnvKeyTimeStart, model.EnvOpTypeMayRead).
		AddArg("end", "", "e").
		AddArg2Env(EnvKeyTimeEnd, "end").
		AddEnvOp(EnvKeyTimeEnd, model.EnvOpTypeMayRead).
		AddArg("duration-ago-as-end", "", "ago-as-end", "aae", "a").
		AddArg2Env(EnvKeyTimeDurationAgoAsEnd, "duration-ago-as-end").
		AddEnvOp(EnvKeyTimeDurationAgoAsEnd, model.EnvOpTypeMayRead).
		AddArg("duration", "7d", "d").
		AddArg2Env(EnvKeyTimeDuration, "duration").
		AddEnvOp(EnvKeyTimeDuration, model.EnvOpTypeMayRead).
		AddArg("metrics-fetch-step", "2m", "step").
		AddArg2Env(impl.EnvKeyMetricsFetchStep, "metrics-fetch-step").
		AddEnvOp(impl.EnvKeyMetricsFetchStep, model.EnvOpTypeRead).
		AddEnvOp(impl.EnvKeyTargetChunkSizeMB, model.EnvOpTypeRead).
		AddEnvOp(impl.EnvKeyRateLimitDesiredConcurrency, model.EnvOpTypeRead).
		AddEnvOp(impl.EnvKeyCacheMaxSizeMB, model.EnvOpTypeRead).Owner()

	metricsFetch.AddSub("random", "r").RegPowerCmd(MetricsFetchRandom,
		"fetch metrics from a random cluster, writes cluster-id to env").
		AddArg("metric", "", "m").
		AddArg2Env(EnvKeyMetrics, "metric").
		AddEnvOp(EnvKeyMetrics, model.EnvOpTypeMayRead).
		AddArg("duration", "1h", "d").
		AddArg2Env(EnvKeyTimeDuration, "duration").
		AddEnvOp(EnvKeyTimeDuration, model.EnvOpTypeRead).
		AddArg("start", "", "s").
		AddArg2Env(EnvKeyTimeStart, "start").
		AddEnvOp(EnvKeyTimeStart, model.EnvOpTypeMayRead).
		AddArg("end", "", "e").
		AddArg2Env(EnvKeyTimeEnd, "end").
		AddEnvOp(EnvKeyTimeEnd, model.EnvOpTypeMayRead).
		AddArg("duration-ago-as-end", "", "ago-as-end", "aae", "a").
		AddArg2Env(EnvKeyTimeDurationAgoAsEnd, "duration-ago-as-end").
		AddEnvOp(EnvKeyTimeDurationAgoAsEnd, model.EnvOpTypeMayRead).
		AddArg("metrics-fetch-step", "2m", "step").
		AddArg2Env(impl.EnvKeyMetricsFetchStep, "metrics-fetch-step").
		AddEnvOp(impl.EnvKeyMetricsFetchStep, model.EnvOpTypeRead).
		AddEnvOp(impl.EnvKeyTargetChunkSizeMB, model.EnvOpTypeRead).
		AddEnvOp(impl.EnvKeyRateLimitDesiredConcurrency, model.EnvOpTypeRead).
		AddEnvOp(impl.EnvKeyCacheMaxSizeMB, model.EnvOpTypeRead).
		AddEnvOp(EnvKeyClusterID, model.EnvOpTypeWrite)

	metricsFetch.AddSub("all", "a").RegPowerCmd(MetricsFetchAll,
		"fetch metrics from all clusters (excluding inactive)").
		AddArg("start", "", "s").
		AddArg2Env(EnvKeyTimeStart, "start").
		AddEnvOp(EnvKeyTimeStart, model.EnvOpTypeMayRead).
		AddArg("end", "", "e").
		AddArg2Env(EnvKeyTimeEnd, "end").
		AddEnvOp(EnvKeyTimeEnd, model.EnvOpTypeMayRead).
		AddArg("duration-ago-as-end", "", "ago-as-end", "aae", "a").
		AddArg2Env(EnvKeyTimeDurationAgoAsEnd, "duration-ago-as-end").
		AddEnvOp(EnvKeyTimeDurationAgoAsEnd, model.EnvOpTypeMayRead).
		AddArg("duration", "7d", "d").
		AddArg2Env(EnvKeyTimeDuration, "duration").
		AddEnvOp(EnvKeyTimeDuration, model.EnvOpTypeRead).
		AddArg("metrics-fetch-step", "2m", "step").
		AddArg2Env(impl.EnvKeyMetricsFetchStep, "metrics-fetch-step").
		AddEnvOp(impl.EnvKeyMetricsFetchStep, model.EnvOpTypeRead).
		AddEnvOp(impl.EnvKeyTargetChunkSizeMB, model.EnvOpTypeRead).
		AddEnvOp(impl.EnvKeyRateLimitDesiredConcurrency, model.EnvOpTypeRead).
		AddEnvOp(impl.EnvKeyCacheMaxSizeMB, model.EnvOpTypeRead)

	metricsCache := metrics.AddSub("cache", "ca").RegEmptyCmd("metrics cache operations").Owner()
	metricsCache.AddSub("list", "l", "ls").RegPowerCmd(MetricsCacheListCmd,
		"list cached metrics for a cluster").
		AddArg("cluster-id", "", "cluster", "id").
		AddArg2Env(EnvKeyClusterID, "cluster-id").
		AddEnvOp(EnvKeyClusterID, model.EnvOpTypeRead).
		AddArg("metric", "", "m").
		AddArg2Env(EnvKeyMetrics, "metric").
		AddEnvOp(EnvKeyMetrics, model.EnvOpTypeMayRead)
	metricsCache.AddSub("clear", "c").RegPowerCmd(MetricsCacheClearClusterCmd,
		"clear metrics cache for a specific cluster").
		AddArg("cluster-id", "", "cluster", "id").
		AddArg2Env(EnvKeyClusterID, "cluster-id").
		AddEnvOp(EnvKeyClusterID, model.EnvOpTypeRead).
		AddArg("metric", "", "m").
		AddArg2Env(EnvKeyMetrics, "metric").
		AddEnvOp(EnvKeyMetrics, model.EnvOpTypeMayRead).Owner().
		AddSub("all", "a").RegPowerCmd(MetricsCacheClearCmd,
		"clear all metrics cache")
}

func RegisterHelp(tc *ticat.TiCat) {
	tc.SetHelpCmds(
		"clusters.fetch-all",
		"clusters.dedicated.fetch",
		"clusters.premium.fetch",
		"metrics.fetch",
		"metrics.fetch.random",
		"metrics.fetch.all",
		"metrics.cache.list",
		"metrics.cache.clear",
		"metrics.cache.clear.all",
		"dig.profile",
		"dig.abnormal",
		"dig.random.profile",
		"dig.random.abnormal",
		"dig.walk.profile",
		"dig.walk.abnormal",
	)
}
