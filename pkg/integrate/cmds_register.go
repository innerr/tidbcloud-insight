package integrate

import (
	"github.com/innerr/ticat/pkg/core/model"
	"github.com/innerr/ticat/pkg/ticat"
)

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

	EnvKeyRateLimitMaxBackoff         = EnvPrefix + "rate-limit.max-backoff"
	EnvKeyRateLimitDesiredConcurrency = EnvPrefix + "rate-limit.desired-concurrency"
	EnvKeyRateLimitRecoveryInterval   = EnvPrefix + "rate-limit.recovery-interval"
	EnvKeyRateLimitMinRecovery        = EnvPrefix + "rate-limit.min-recovery-interval"

	EnvKeyFetchTimeout    = EnvPrefix + "fetch.timeout"
	EnvKeyIdleTimeout     = EnvPrefix + "fetch.idle-timeout"
	EnvKeyTargetChunkSize = EnvPrefix + "fetch.target-chunk-size"

	EnvKeyTimeStart            = EnvPrefix + "time.start"
	EnvKeyTimeEnd              = EnvPrefix + "time.end"
	EnvKeyTimeDurationAgoAsEnd = EnvPrefix + "time.duration-ago-as-end"
	EnvKeyTimeDuration         = EnvPrefix + "time.duration"

	EnvKeyBizType              = EnvPrefix + "biz.type"
	EnvKeyJSON                 = EnvPrefix + "output.json"
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
	dig := cmds.AddSub("dig", "d").RegEmptyCmd("analyze cluster for anomalies and load characteristics").Owner()

	dig.AddSub("random", "r").RegPowerCmd(DigRandomCmd,
		"analyze a random cluster").
		AddArg("biz-type", "", "biz", "b").
		AddArg2Env(EnvKeyBizType, "biz-type").
		AddEnvOp(EnvKeyBizType, model.EnvOpTypeRead).
		AddArg("json", "false", "j").
		AddArg2Env(EnvKeyJSON, "json").
		AddEnvOp(EnvKeyJSON, model.EnvOpTypeRead).
		AddArg("local", "false", "l").
		AddArg2Env(EnvKeyLocal, "local").
		AddEnvOp(EnvKeyLocal, model.EnvOpTypeRead).
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

	dig.AddSub("walk", "w").RegPowerCmd(DigWalkCmd,
		"analyze all clusters sequentially").
		AddArg("concurrency", "1", "c").
		AddArg2Env(EnvKeyConcurrency, "concurrency").
		AddEnvOp(EnvKeyConcurrency, model.EnvOpTypeRead).
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

	dig.AddSub("local", "l").RegPowerCmd(DigLocalCmd,
		"re-analyze cached data by id").
		AddArg("cache-id", "", "id", "c").
		AddArg2Env(EnvKeyCacheID, "cache-id").
		AddEnvOp(EnvKeyCacheID, model.EnvOpTypeRead).
		AddArg("json", "false", "j").
		AddArg2Env(EnvKeyJSON, "json").
		AddEnvOp(EnvKeyJSON, model.EnvOpTypeRead).
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
		AddEnvOp(EnvKeyTimeDuration, model.EnvOpTypeMayRead).Owner()

	metricsFetch.AddSub("random", "r").RegPowerCmd(MetricsFetchRandom,
		"fetch metrics from a random cluster").
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
		AddArg("duration", "1h", "d").
		AddArg2Env(EnvKeyTimeDuration, "duration").
		AddEnvOp(EnvKeyTimeDuration, model.EnvOpTypeRead)

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
		AddEnvOp(EnvKeyTimeDuration, model.EnvOpTypeRead)

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
		"clusters.dedicated.fetch",
		"clusters.premium.fetch",
		"metrics.fetch",
		"metrics.fetch.random",
		"metrics.fetch.all",
		"metrics.cache.list",
		"metrics.cache.clear",
		"metrics.cache.clear.all",
		"dig.random",
		"dig.walk",
		"dig.local",
	)
}
