# TiDB Cloud Insight

[![CI](https://github.com/innerr/tidbcloud-insight/actions/workflows/ci.yml/badge.svg)](https://github.com/innerr/tidbcloud-insight/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/innerr/tidbcloud-insight)](https://goreportcard.com/report/github.com/innerr/tidbcloud-insight)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

O11Y Cluster Management Tool for TiDB Cloud.

## Installation

```bash
make build
```

Or install to GOPATH:

```bash
make install
```

## Configuration

Copy the example config and edit:

```bash
cp tidbcloud-insight.yaml.example tidbcloud-insight.yaml
```
Then edit `client_id` and `client_secret`.

## Usage

Need to be inside PingCAP's internal network:
```bash
# Fetch all dedicated clusters and cache them to local cache
tidbcloud-insight raw clusters dedicated [dedicated|premium]

# Analyze cluster for anomalies and patterns
tidbcloud-insight dig 1234567890 [--duration 7d]

# Profile cluster, and analyze cluster for a random cluster from cached list of command `clusters`
tidbcloud-insight dig 1234567890 random [--duration 7d]

# Do `dig` for local cached (did dig before) cluster
tidbcloud-insight dig 1234567890 --local

# Do `dig` for a local random cached (did dig before) cluster
tidbcloud-insight dig 1234567890 random --local
```

Typical output:
```
============================================================
Cluster: 0000000000000000000 (dedicated/aws/ap-northeast-1)
Progress: 0 done, 0 failed, 137 remaining
Time: 2026-02-20 22:32:10
============================================================
Auth: from cache
Querying cluster info for: 0000000000000000000
Looking up cluster at aws/ap-northeast-1 (dedicated)...
Provider:        aws
Region:          ap-northeast-1
BizType:         dedicated

Querying metrics (last 7d)...
  Metrics: qps, latency, sqlType, tikvOp, tikvLatency

  * (1/5) tikvOp: OK (301056 points)
  * (2/5) sqlType: OK (161280 points)
  * (3/5) qps: OK (279552 points)
  * (4/5) latency: OK (4838400 points)
  * (5/5) tikvLatency: OK (6924288 points)
============================================================
ANALYSIS REPORT - 0000000000000000000
============================================================

CLUSTER TOPOLOGY
------------------------------------------------------------
  TiDB:     6 instances
  TiKV:     3 instances

  Version:  v7.5.6-20260120-b12bb34

Time Range: 2026-02-13 22:34 ~ 2026-02-20 22:32
Duration: 168.0 hours (7.0 days)
Samples: 5040

------------------------------------------------------------
QPS Summary
------------------------------------------------------------
  Mean: 4007
  Min:  3085
  Max:  9074

------------------------------------------------------------
Anomalies Summary (total: 912, per day: 130.3)
------------------------------------------------------------
  QPS_DROP: 26
  QPS_SPIKE: 884
  LATENCY_SPIKE: 1
  INSTANCE_IMBALANCE: 1

------------------------------------------------------------
[QPS_DROP] 26 events
------------------------------------------------------------
  [MEDIUM] 2026-02-14 01:38: Dynamic threshold breach: 3282 
         (bounds=[3284, 4338])
  [MEDIUM] 2026-02-15 01:44: Dynamic threshold breach: 3454 
         (bounds=[3457, 4402])
  [MEDIUM] 2026-02-15 02:14: Dynamic threshold breach: 3211 
         (bounds=[3301, 3934])
  [MEDIUM] 2026-02-15 14:32: Dynamic threshold breach: 3821 
         (bounds=[3879, 4541])
  [MEDIUM] 2026-02-15 14:34: Dynamic threshold breach: 3793 
         (bounds=[3837, 4503])
  [MEDIUM] 2026-02-15 22:28: Dynamic threshold breach: 3895 
         (bounds=[3966, 4629])
  [MEDIUM] 2026-02-16 00:34: Dynamic threshold breach: 3740 
         (bounds=[3785, 4430])
  [MEDIUM] 2026-02-16 00:54: Dynamic threshold breach: 3657 
         (bounds=[3667, 4596])
  [MEDIUM] 2026-02-16 03:14: Dynamic threshold breach: 3203 
         (bounds=[3211, 3808])
  [MEDIUM] 2026-02-16 03:18: Dynamic threshold breach: 3312 
         (bounds=[3313, 3910])
  ... and 16 more

------------------------------------------------------------
[QPS_SPIKE] 884 events
------------------------------------------------------------
  [MEDIUM] 2026-02-13 22:42: Peak detected: 4189 
         (prominence=313, width=3)
  [MEDIUM] 2026-02-13 22:52: Peak detected: 4200 
         (prominence=330, width=4)
  [CRITICAL] 2026-02-13 23:02: Dynamic threshold breach: 5181 
         (bounds=[3561, 4595])
  [MEDIUM] 2026-02-13 23:04: Dynamic threshold breach: 4769 
         (bounds=[3578, 4618])
  [MEDIUM] 2026-02-13 23:06: Dynamic threshold breach: 5056 
         (bounds=[3437, 4763])
  [MEDIUM] 2026-02-13 23:12: Peak detected: 4619 
         (prominence=258, width=3)
  [MEDIUM] 2026-02-13 23:22: Peak detected: 4751 
         (prominence=516, width=6)
  [MEDIUM] 2026-02-13 23:32: Peak detected: 4673 
         (prominence=555, width=3)
  [MEDIUM] 2026-02-13 23:42: Peak detected: 4378 
         (prominence=441, width=6)
  [MEDIUM] 2026-02-14 00:02: Peak detected: 4358 
         (prominence=455, width=5)
  ... and 874 more

------------------------------------------------------------
[LATENCY_SPIKE] 1 events
------------------------------------------------------------
  [CRITICAL] 2026-02-20 22:32: P99 latency spiked to 1019.1ms 
         (baseline=1.6ms)

------------------------------------------------------------
[INSTANCE_IMBALANCE] 1 events
------------------------------------------------------------
  [CRITICAL] tikv db-tikv-2 latency on kv_batch_get_command is 2.6x cluster avg 
         (inst=1579.9ms, cluster=617.5ms)

------------------------------------------------------------
WORKLOAD COMPOSITION
------------------------------------------------------------

  SQL Statement Distribution:
  Type          Percent     Rate
  -----------------------------------
  Select          34.6%    791.7
  Set             18.8%    430.2
  Commit          17.0%    388.3
  Update          13.3%    304.9
  Begin           12.4%    284.7
  Delete           0.5%     10.5

  Read Ratio:   67.4%
  Write Ratio:  32.6%

  Workload Type: READ-HEAVY
  Dominant SQL:  Select

  Workload Analysis:
    Pattern:     high_write_amplification
    Hotspot Risk: unknown

------------------------------------------------------------
WEEKLY PATTERN
------------------------------------------------------------

  Daily QPS Comparison:

    2026-02-13 (Fri) |█████████████████████████| 4.3K
  W 2026-02-14 (Sat) |███████████████████████░░| 4.1K
  W 2026-02-15 (Sun) |███████████████████████░░| 4.0K
    2026-02-16 (Mon) |███████████████████████░░| 4.0K
    2026-02-17 (Tue) |███████████████████████░░| 4.0K
    2026-02-18 (Wed) |███████████████████████░░| 4.0K
    2026-02-19 (Thu) |███████████████████████░░| 4.0K
    2026-02-20 (Fri) |███████████████████████░░| 4.0K

  Legend: W = Weekend

  Weekday Average:  4.0K
  Weekend Average:  4.0K
  Weekend Increase: 1.1%
  Pattern:          Weekend-heavy traffic

------------------------------------------------------------
ADVANCED PROFILING ANALYSIS
------------------------------------------------------------

--- Frequency Domain Analysis
  (Detects periodic patterns using FFT spectral analysis)
  Dominant Periods (sorted by strength):
    (no significant periods detected)
  Spectral Entropy: 0.678 [0-1, lower=more regular] => somewhat random

  Detected Patterns:
    (no standard calendar patterns detected)

--- Multimodal Analysis
  (Checks if data has multiple distinct workload levels)
  Multimodal: NO (single workload distribution)
  => Traffic follows a single continuous pattern
  Dip Test p-value: 0.4775 [0-1, <0.05=multimodal] => likely unimodal

--- SQL Type Multimodal Analysis
  (Checks if each SQL type has distinct workload levels)
  ExplainSQL, CreateTable, AlterTable: multimodal (1 modes)
  Show: multimodal (8 modes)
  Update, Begin: unimodal (single workload level)
  Commit: unimodal (single workload level)
  other: unimodal (single workload level)
  Select, Set: unimodal (single workload level)

--- Tail Distribution
  (Analyzes extreme values and spike behavior)
  Tail Type: LIGHT
  => Rare extreme values, traffic mostly predictable
  Tail Index: 0.191 [>1=heavy, <0.5=light] => light tail (bounded extremes)
  GPD Parameters: shape=-0.134 [affects tail weight], scale=718.163
  Predicted Extreme (P99.9): 5040

--- SQL Type Tail Distribution
  (Analyzes extreme values for each SQL type)
  Delete, Insert, Use: HEAVY tail => heavy (frequent extreme spikes)
    Predicted Extreme (P99.9): avg=78
  Set: MODERATE tail => moderate (occasional spikes)
    Predicted Extreme (P99.9): avg=633
  Select, Commit, other, Begin, CreateTable, ExplainSQL, AlterTable, Update, Rollback, Show: LIGHT tail => light
    Predicted Extreme (P99.9): avg=313

--- Volatility Metrics
  (Measures traffic stability)
  Realized Volatility: 5.3568 [higher=more variable] => very high variability
  Parkinson HL: 0.1095 [range-based] => stable
  Vol-of-Vol: 0.4658 [0-1+, lower=more stable] => consistent volatility
  Volatility Ratio: 1.05 [current vs avg] => NORMAL
  Leverage Effect: -1.0000 [-1 to 1] => volatility spikes when load drops (unusual)

--- SQL Type Volatility Metrics
  (Measures traffic stability for each SQL type)
  Delete, Insert, AlterTable, Use, Set, Begin, Select, Commit, Update: very high variability
  Show, Rollback, other: very high variability (LOW - recently calmer)
  ExplainSQL, CreateTable: high variability (LOW - recently calmer)

--- Regime Detection
  (Identifies distinct operational states)
  Regimes Detected: 1 (stable operational mode)
  => Traffic follows a consistent pattern without distinct states

--- SQL Type Regime Detection
  (Identifies distinct operational states for each SQL type)
  ALL types: stable
```

## Development

```bash
# Run
make run

# Test
make test

# Lint
make lint

# Format
make fmt
```

## Project Structure

```
.
├── cmd/                    # Application entry points
│   └── tidbcloud-insight/
│       └── main.go
├── internal/               # Private application code
│   ├── analysis/
│   ├── auth/
│   ├── cache/
│   ├── client/
│   ├── commands/
│   └── config/
├── meta/                   # Metadata files
├── go.mod
├── go.sum
├── Makefile
└── README.md
```
