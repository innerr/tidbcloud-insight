# Anomaly Detection Capabilities

This document describes the anomaly detection and workload profiling capabilities provided by TiDB Cloud Insight.

## Table of Contents

1. [Core Statistical Detection](#1-core-statistical-detection)
2. [Machine Learning Detection](#2-machine-learning-detection)
3. [Spectral/Signal Detection](#3-spectralsignal-detection)
4. [Enhanced Detection](#4-enhanced-detection)
5. [Change Point Detection & Forecasting](#5-change-point-detection--forecasting)
6. [Advanced Statistical Detection](#6-advanced-statistical-detection)
7. [Workload Anomaly Detection](#7-workload-anomaly-detection)
8. [Workload Profiling](#8-workload-profiling) *(Not for anomaly detection)*

---

## 1. Core Statistical Detection

### 1.1 QPS Drop Detection

| Attribute | Value |
|-----------|-------|
| **Method** | `detectDrops()` |
| **Detects** | Sudden QPS decreases (>30% drop from baseline) |
| **Algorithm** | Statistical threshold with median/std deviation |
| **Parameters** | `MinBaselineSamples: 10`, `adjustFactor` (traffic-class based) |
| **Metrics** | QPS time series |
| **Severity** | MEDIUM (30-50%), HIGH (50-70%), CRITICAL (>70% or zero with baseline>100) |

### 1.2 QPS Spike Detection

| Attribute | Value |
|-----------|-------|
| **Method** | `detectSpikes()` |
| **Detects** | Sudden QPS increases (>1.5-1.8x baseline) |
| **Algorithm** | Statistical threshold with median/std deviation |
| **Parameters** | `MinBaselineSamples: 10`, `minSpikeRatio: 1.5 + adjustFactor*0.3` |
| **Metrics** | QPS time series |
| **Severity** | MEDIUM (1.5-3x), HIGH (3-5x), CRITICAL (>5x) |

### 1.3 Sustained Anomaly Detection

| Attribute | Value |
|-----------|-------|
| **Method** | `detectSustained()` |
| **Detects** | Prolonged high/low QPS over multiple samples |
| **Algorithm** | Sliding window (6 samples) mean comparison |
| **Parameters** | `windowSize: 6`, `lowThreshold: 0.4*baseline`, `highThreshold: 2.0*baseline` |
| **Metrics** | QPS time series |
| **Severity** | MEDIUM, HIGH (extreme deviations) |

### 1.4 Latency Anomaly Detection

| Attribute | Value |
|-----------|-------|
| **Method** | `DetectLatencyAnomalies()` |
| **Detects** | P99 latency spikes |
| **Algorithm** | Median and P90 percentile comparison |
| **Parameters** | `threshold: p99Median * 2.5` |
| **Metrics** | P50/P99 latency |
| **Severity** | MEDIUM (2.5-4x), HIGH (4-10x), CRITICAL (>10x) |

### 1.5 Instance Imbalance Detection

| Attribute | Value |
|-----------|-------|
| **Method** | `DetectInstanceImbalance()` |
| **Detects** | Load imbalance across instances |
| **Algorithm** | Per-instance mean vs cluster mean deviation |
| **Parameters** | `deviation threshold: 50%` |
| **Metrics** | Per-instance QPS, latency, SQL types, TiKV ops |
| **Severity** | MEDIUM (50-70%), HIGH (>70%) |

---

## 2. Machine Learning Detection

### 2.1 Isolation Forest

| Attribute | Value |
|-----------|-------|
| **Method** | `IsolationForest.Detect()` |
| **Detects** | General outliers (spikes/drops) |
| **Algorithm** | Tree-based anomaly isolation with random feature splits |
| **Parameters** | `NumTrees: 100`, `SamplingSize: 256`, `MaxDepth: log2(samplingSize)` |
| **Metrics** | Any time series (uses value, hour, day features) |
| **Severity** | MEDIUM (score>0.6), HIGH (>0.75), CRITICAL (>0.85) |

### 2.2 S-H-ESD (Seasonal Hybrid ESD)

| Attribute | Value |
|-----------|-------|
| **Method** | `SHESDDetector.Detect()` |
| **Detects** | Seasonal anomalies with statistical significance |
| **Algorithm** | STL decomposition + ESD test with Grubbs' statistic |
| **Parameters** | `MaxAnomalies: 2%`, `Alpha: 0.05`, `Period: 24`, `Longterm: true` |
| **Metrics** | Any time series |
| **Severity** | Based on z-score vs critical value |

### 2.3 Random Cut Forest (RCF)

| Attribute | Value |
|-----------|-------|
| **Method** | `RandomCutForest.Detect()` |
| **Detects** | Streaming anomalies with shingle-based scoring |
| **Algorithm** | Online tree ensemble with random dimension cuts |
| **Parameters** | `NumTrees: 50`, `TreeSize: 256`, `TimeDecay: 0.01`, `ShingleSize: 4` |
| **Metrics** | Any time series |
| **Severity** | MEDIUM (score>1.0), HIGH (>1.5), CRITICAL (>2.0) |

### 2.4 Ensemble Anomaly Detector

| Attribute | Value |
|-----------|-------|
| **Method** | `EnsembleAnomalyDetector.DetectAll()` |
| **Detects** | Consensus anomalies from multiple methods |
| **Algorithm** | Voting ensemble (MAD, ESD, IsolationForest, SHESD, RCF) |
| **Parameters** | `VotingThreshold: 2`, `MinSamples: 24` |
| **Metrics** | Any time series |
| **Severity** | Based on number of agreeing methods |

---

## 3. Spectral/Signal Detection

### 3.1 Spectral Residual Detector

| Attribute | Value |
|-----------|-------|
| **Method** | `SpectralResidualDetector.Detect()` |
| **Detects** | Visual saliency-based anomalies |
| **Algorithm** | FFT → Log spectrum → Residual → Inverse FFT → Saliency map |
| **Parameters** | `WindowSize: 64`, `LocalWindowSize: 21`, `Threshold: 3.0` |
| **Metrics** | Any time series |
| **Severity** | MEDIUM (zScore>1), HIGH (>2), CRITICAL (>3) |

### 3.2 Multi-Scale Anomaly Detector

| Attribute | Value |
|-----------|-------|
| **Method** | `MultiScaleAnomalyDetector.Detect()` |
| **Detects** | Anomalies at multiple time scales |
| **Algorithm** | Agreement across sliding windows of different sizes |
| **Parameters** | `Scales: [6, 12, 24, 48]`, `Threshold: 3.0`, `MinAgreement: 2` |
| **Metrics** | Any time series |
| **Severity** | Based on number of agreeing scales |

### 3.3 DBSCAN Anomaly Detector

| Attribute | Value |
|-----------|-------|
| **Method** | `DBSCANAnomalyDetector.Detect()` |
| **Detects** | Density-based outliers |
| **Algorithm** | DBSCAN clustering, labels -1 = anomaly |
| **Parameters** | `Epsilon: 2.0`, `MinSamples: 5`, `WindowSize: 24` |
| **Metrics** | Any normalized time series |
| **Severity** | MEDIUM (zScore>3), HIGH (>4), CRITICAL (>5) |

### 3.4 Holt-Winters Detector

| Attribute | Value |
|-----------|-------|
| **Method** | `HoltWintersDetector.Detect()` |
| **Detects** | Seasonal forecast deviations |
| **Algorithm** | Triple exponential smoothing with trend/seasonal components |
| **Parameters** | `Alpha: 0.3`, `Beta: 0.1`, `Gamma: 0.1`, `SeasonLength: 24` |
| **Metrics** | Any time series |
| **Severity** | Based on z-score of residual |

---

## 4. Enhanced Detection

### 4.1 Dynamic Threshold Detector

| Attribute | Value |
|-----------|-------|
| **Method** | `DynamicThresholdDetector.ComputeThresholds()` |
| **Detects** | Adaptive bound violations |
| **Algorithm** | Rolling window with MAD/median, seasonal adjustment |
| **Parameters** | `WindowSize: 24`, `LowerBoundFactor: 3.0`, `UpperBoundFactor: 3.0`, `Period: 24` |
| **Metrics** | Any time series |
| **Severity** | MEDIUM (deviation>1), HIGH (>2), CRITICAL (>3) |

### 4.2 Contextual Anomaly Detector

| Attribute | Value |
|-----------|-------|
| **Method** | `ContextualAnomalyDetector.Detect()` |
| **Detects** | Context-aware anomalies (hourly/daily/weekly patterns) |
| **Algorithm** | Separate baselines per context (hour, day, week) |
| **Parameters** | `MinSamplesPerCtx: 5`, `Threshold: 3.0` |
| **Metrics** | Any time series |
| **Severity** | Based on average z-score across contexts |

### 4.3 Peak Detector

| Attribute | Value |
|-----------|-------|
| **Method** | `PeakDetector.DetectPeakAnomalies()` |
| **Detects** | Significant peaks with prominence |
| **Algorithm** | Local maxima detection with prominence/width calculation |
| **Parameters** | `MinPeakDistance: 5`, `ProminenceFactor: 0.5` |
| **Metrics** | Any time series |
| **Severity** | MEDIUM, HIGH (>3std prominence), CRITICAL (>5std) |

---

## 5. Change Point Detection & Forecasting

### 5.1 CUSUM Change Point Detection

| Attribute | Value |
|-----------|-------|
| **Method** | `ChangePointDetector.DetectCUSUM()` |
| **Detects** | Cumulative sum-based regime changes |
| **Algorithm** | Cumulative sum control chart |
| **Parameters** | `MinSegmentSize: 10`, `Threshold: 2.0` |
| **Metrics** | Any time series |

### 5.2 PELT Change Point Detection

| Attribute | Value |
|-----------|-------|
| **Method** | `ChangePointDetector.DetectPELT()` |
| **Detects** | Optimal change points via dynamic programming |
| **Algorithm** | Pruned Exact Linear Time |
| **Parameters** | `MinSegmentSize: 10`, `Penalty: 3.0` |
| **Metrics** | Any time series |

### 5.3 Sliding Window Change Point

| Attribute | Value |
|-----------|-------|
| **Method** | `ChangePointDetector.DetectSlidingWindow()` |
| **Detects** | Local mean shifts via t-statistic |
| **Algorithm** | Two-window t-test comparison |
| **Parameters** | `MinSegmentSize: 10`, `Threshold: 2.0` |
| **Metrics** | Any time series |

### 5.4 E-Divisive Change Point

| Attribute | Value |
|-----------|-------|
| **Method** | `ChangePointDetector.DetectEDivisive()` |
| **Detects** | Non-parametric distribution changes |
| **Algorithm** | Energy statistic based divisive clustering |
| **Parameters** | `MinSegmentSize: 10`, `SignificanceLevel: 0.05`, `MaxChangePoints: 10` |
| **Metrics** | Any time series |

### 5.5 Forecasting Methods

| Method | Algorithm | Use Case |
|--------|-----------|----------|
| `ForecastSimpleMovingAverage()` | Windowed mean | Stable series |
| `ForecastExponentialSmoothing()` | Single EMA | Moderate variability |
| `ForecastDoubleExponentialSmoothing()` | Holt's method | Trending data |
| `ForecastLinearRegression()` | Linear fit | Strong trends |
| `ForecastSeasonalNaive()` | Seasonal lag | Seasonal patterns |
| `ForecastAuto()` | Auto-select | Automatic selection |

---

## 6. Advanced Statistical Detection

### 6.1 MAD (Median Absolute Deviation)

| Attribute | Value |
|-----------|-------|
| **Method** | `AdvancedAnomalyDetector.DetectMAD()` |
| **Detects** | Robust outliers |
| **Algorithm** | Median-based deviation with 1.4826 scaling |
| **Parameters** | `MinSamples: 10`, `MADThreshold: 3.5` |
| **Metrics** | Any time series |
| **Severity** | MEDIUM (>3.5), HIGH (>5.25), CRITICAL (>7) |

### 6.2 ESD (Extreme Studentized Deviate)

| Attribute | Value |
|-----------|-------|
| **Method** | `AdvancedAnomalyDetector.DetectESD()` |
| **Detects** | Multiple outliers with Grubbs' test |
| **Algorithm** | Iterative Grubbs' test |
| **Parameters** | `ESDAlpha: 0.05`, `ESDMaxAnomalies: 2%` |
| **Metrics** | Any time series |

### 6.3 STL Decomposition Anomaly

| Attribute | Value |
|-----------|-------|
| **Method** | `AdvancedAnomalyDetector.DetectWithDecomposition()` |
| **Detects** | Residual anomalies after trend/seasonal removal |
| **Algorithm** | STL + MAD on residuals |
| **Parameters** | `SeasonalPeriod: 24`, `TrendWindow: 7`, `RobustDecomposition: true` |
| **Metrics** | Any time series |

### 6.4 EMA Deviation Anomaly

| Attribute | Value |
|-----------|-------|
| **Method** | `AdvancedAnomalyDetector.DetectEMAAnomalies()` |
| **Detects** | Deviations from exponential moving average |
| **Algorithm** | EMA with dynamic standard deviation |
| **Parameters** | `EMAAlpha: 0.2`, `EMAThreshold: 0.3` |
| **Metrics** | Any time series |

### 6.5 Hourly Baseline Anomaly

| Attribute | Value |
|-----------|-------|
| **Method** | `AdvancedAnomalyDetector.DetectHourlyBaselineAnomalies()` |
| **Detects** | Hour-of-day specific anomalies |
| **Algorithm** | Per-hour z-score |
| **Parameters** | `ZScoreThreshold: 3.0` |
| **Metrics** | Any time series |

### 6.6 Volatility Anomaly Detection

| Attribute | Value |
|-----------|-------|
| **Method** | `AdvancedAnomalyDetector.DetectVolatilityAnomalies()` |
| **Detects** | Sudden volatility changes |
| **Algorithm** | Rolling coefficient of variation with MAD |
| **Parameters** | `windowSize: 6`, `threshold: 3` |
| **Metrics** | Any time series |

---

## 7. Workload Anomaly Detection

### 7.1 Workload Type Anomaly

| Attribute | Value |
|-----------|-------|
| **Method** | `DetectWorkloadAnomalies()` |
| **Detects** | SQL type and TiKV operation anomalies |
| **Algorithm** | Per-category statistical thresholds |
| **Parameters** | `MinBaselineSamples: 10`, drop threshold: 30%, spike ratio: 1.5x |
| **Metrics** | SQL types (Select, Insert, Update, Delete), TiKV ops |
| **Severity** | Based on drop/spike ratio |

---

## 8. Workload Profiling

> **Note:** The following methods are used for workload characterization and profiling, **not** for anomaly detection. They help understand traffic patterns and system behavior.

### 8.1 Frequency Domain Analysis

| Attribute | Value |
|-----------|-------|
| **Method** | `analyzeFrequencyDomain()` |
| **Purpose** | Identify periodic patterns (daily, weekly, monthly) |
| **Algorithm** | FFT power spectrum peak detection |
| **Parameters** | `FFTMinSamples: 64` |
| **Output** | Dominant periods, spectral entropy, pattern flags |

### 8.2 Multimodal Analysis

| Attribute | Value |
|-----------|-------|
| **Method** | `analyzeMultimodal()` |
| **Purpose** | Detect multiple distinct workload levels |
| **Algorithm** | Hartigan's Dip Test + Kernel Density Estimation |
| **Parameters** | `EnableMultimodal: true` |
| **Output** | Number of modes, mode locations/weights, p-value |

### 8.3 Tail Distribution Analysis

| Attribute | Value |
|-----------|-------|
| **Method** | `analyzeTailDistribution()` |
| **Purpose** | Characterize heavy vs light tails, extreme value behavior |
| **Algorithm** | Generalized Pareto Distribution fitting |
| **Parameters** | `EnableTailFit: true` |
| **Output** | Tail type (heavy/moderate/light), tail index, GPD parameters |

### 8.4 Volatility Metrics

| Attribute | Value |
|-----------|-------|
| **Method** | `analyzeVolatility()` |
| **Purpose** | Measure traffic stability patterns |
| **Algorithm** | Realized volatility, Parkinson HL, volatility-of-volatility |
| **Output** | RealizedVolatility, ParkinsonHL, VolOfVol, LeverageEffect |

### 8.5 Regime Detection

| Attribute | Value |
|-----------|-------|
| **Method** | `analyzeRegimes()` |
| **Purpose** | Identify distinct operational states |
| **Algorithm** | Change point detection on rolling statistics |
| **Output** | Number of regimes, regime means/stds, transitions |

---

## Summary Table

### Anomaly Detection Methods

| Category | Detection Methods | Primary Use |
|----------|------------------|-------------|
| **Statistical** | Drop, Spike, Sustained, MAD, ESD, Z-Score | General time series |
| **ML-Based** | Isolation Forest, RCF, Ensemble | Complex patterns |
| **Spectral** | Spectral Residual, Multi-Scale, DBSCAN, Holt-Winters | Signal processing |
| **Contextual** | Hourly Baseline, Daily/Weekly Patterns | Time-aware detection |
| **Change Point** | CUSUM, PELT, Sliding Window, E-Divisive | Regime changes |
| **Forecasting** | SMA, EMA, DES, Linear, Seasonal Naive | Prediction-based |
| **Workload** | SQL/TiKV anomalies, Imbalance | Database-specific |

### Profiling Methods

| Category | Methods | Primary Use |
|----------|---------|-------------|
| **Frequency** | FFT Analysis | Periodic pattern detection |
| **Distribution** | Multimodal, Tail | Distribution characterization |
| **Stability** | Volatility, Regime | Traffic stability analysis |
