# 异常检测能力文档

本文档描述 TiDB Cloud Insight 工具提供的异常检测和工作负载分析能力。

## 目录

1. [核心统计检测](#1-核心统计检测)
2. [机器学习检测](#2-机器学习检测)
3. [频谱/信号检测](#3-频谱信号检测)
4. [增强检测](#4-增强检测)
5. [变点检测与预测](#5-变点检测与预测)
6. [高级统计检测](#6-高级统计检测)
7. [工作负载异常检测](#7-工作负载异常检测)
8. [工作负载分析](#8-工作负载分析) *（非异常检测）*

---

## 1. 核心统计检测

### 1.1 QPS 下跌检测

| 属性 | 值 |
|------|-----|
| **方法** | `detectDrops()` |
| **检测目标** | 突然的 QPS 下降（相对基线下降 >30%） |
| **算法** | 基于中位数和标准差的统计阈值 |
| **参数** | `MinBaselineSamples: 10`, `adjustFactor`（基于流量类型） |
| **指标** | QPS 时间序列 |
| **严重级别** | 中等 (30-50%), 高 (50-70%), 严重 (>70% 或基线>100时归零) |

### 1.2 QPS 尖峰检测

| 属性 | 值 |
|------|-----|
| **方法** | `detectSpikes()` |
| **检测目标** | 突然的 QPS 上升（基线的 1.5-1.8 倍） |
| **算法** | 基于中位数和标准差的统计阈值 |
| **参数** | `MinBaselineSamples: 10`, `minSpikeRatio: 1.5 + adjustFactor*0.3` |
| **指标** | QPS 时间序列 |
| **严重级别** | 中等 (1.5-3x), 高 (3-5x), 严重 (>5x) |

### 1.3 持续异常检测

| 属性 | 值 |
|------|-----|
| **方法** | `detectSustained()` |
| **检测目标** | 多个采样点持续高/低 QPS |
| **算法** | 滑动窗口（6个样本）均值比较 |
| **参数** | `windowSize: 6`, `lowThreshold: 0.4*baseline`, `highThreshold: 2.0*baseline` |
| **指标** | QPS 时间序列 |
| **严重级别** | 中等, 高（极端偏差） |

### 1.4 延迟异常检测

| 属性 | 值 |
|------|-----|
| **方法** | `DetectLatencyAnomalies()` |
| **检测目标** | P99 延迟尖峰 |
| **算法** | 中位数与 P90 百分位比较 |
| **参数** | `threshold: p99Median * 2.5` |
| **指标** | P50/P99 延迟 |
| **严重级别** | 中等 (2.5-4x), 高 (4-10x), 严重 (>10x) |

### 1.5 实例不均衡检测

| 属性 | 值 |
|------|-----|
| **方法** | `DetectInstanceImbalance()` |
| **检测目标** | 实例间负载不均衡 |
| **算法** | 每实例均值与集群均值偏差 |
| **参数** | `deviation threshold: 50%` |
| **指标** | 每实例 QPS、延迟、SQL 类型、TiKV 操作 |
| **严重级别** | 中等 (50-70%), 高 (>70%) |

---

## 2. 机器学习检测

### 2.1 孤立森林 (Isolation Forest)

| 属性 | 值 |
|------|-----|
| **方法** | `IsolationForest.Detect()` |
| **检测目标** | 通用异常值（尖峰/下跌） |
| **算法** | 基于树的异常隔离，随机特征分割 |
| **参数** | `NumTrees: 100`, `SamplingSize: 256`, `MaxDepth: log2(samplingSize)` |
| **指标** | 任意时间序列（使用值、小时、天特征） |
| **严重级别** | 中等 (score>0.6), 高 (>0.75), 严重 (>0.85) |

### 2.2 S-H-ESD (季节性混合 ESD)

| 属性 | 值 |
|------|-----|
| **方法** | `SHESDDetector.Detect()` |
| **检测目标** | 具有统计显著性的季节性异常 |
| **算法** | STL 分解 + ESD 检验与 Grubbs 统计量 |
| **参数** | `MaxAnomalies: 2%`, `Alpha: 0.05`, `Period: 24`, `Longterm: true` |
| **指标** | 任意时间序列 |
| **严重级别** | 基于 z-score 与临界值 |

### 2.3 随机切割森林 (Random Cut Forest)

| 属性 | 值 |
|------|-----|
| **方法** | `RandomCutForest.Detect()` |
| **检测目标** | 基于 shingle 的流式异常 |
| **算法** | 在线树集成，随机维度切割 |
| **参数** | `NumTrees: 50`, `TreeSize: 256`, `TimeDecay: 0.01`, `ShingleSize: 4` |
| **指标** | 任意时间序列 |
| **严重级别** | 中等 (score>1.0), 高 (>1.5), 严重 (>2.0) |

### 2.4 集成异常检测器

| 属性 | 值 |
|------|-----|
| **方法** | `EnsembleAnomalyDetector.DetectAll()` |
| **检测目标** | 多方法共识异常 |
| **算法** | 投票集成 (MAD, ESD, IsolationForest, SHESD, RCF) |
| **参数** | `VotingThreshold: 2`, `MinSamples: 24` |
| **指标** | 任意时间序列 |
| **严重级别** | 基于同意的方法数量 |

---

## 3. 频谱/信号检测

### 3.1 频谱残差检测器

| 属性 | 值 |
|------|-----|
| **方法** | `SpectralResidualDetector.Detect()` |
| **检测目标** | 基于视觉显著性的异常 |
| **算法** | FFT → 对数频谱 → 残差 → 逆 FFT → 显著性图 |
| **参数** | `WindowSize: 64`, `LocalWindowSize: 21`, `Threshold: 3.0` |
| **指标** | 任意时间序列 |
| **严重级别** | 中等 (zScore>1), 高 (>2), 严重 (>3) |

### 3.2 多尺度异常检测器

| 属性 | 值 |
|------|-----|
| **方法** | `MultiScaleAnomalyDetector.Detect()` |
| **检测目标** | 多时间尺度的异常 |
| **算法** | 不同大小滑动窗口的一致性检测 |
| **参数** | `Scales: [6, 12, 24, 48]`, `Threshold: 3.0`, `MinAgreement: 2` |
| **指标** | 任意时间序列 |
| **严重级别** | 基于同意的尺度数量 |

### 3.3 DBSCAN 异常检测器

| 属性 | 值 |
|------|-----|
| **方法** | `DBSCANAnomalyDetector.Detect()` |
| **检测目标** | 基于密度的离群点 |
| **算法** | DBSCAN 聚类，标签 -1 = 异常 |
| **参数** | `Epsilon: 2.0`, `MinSamples: 5`, `WindowSize: 24` |
| **指标** | 任意归一化时间序列 |
| **严重级别** | 中等 (zScore>3), 高 (>4), 严重 (>5) |

### 3.4 Holt-Winters 检测器

| 属性 | 值 |
|------|-----|
| **方法** | `HoltWintersDetector.Detect()` |
| **检测目标** | 季节性预测偏差 |
| **算法** | 三重指数平滑，包含趋势/季节分量 |
| **参数** | `Alpha: 0.3`, `Beta: 0.1`, `Gamma: 0.1`, `SeasonLength: 24` |
| **指标** | 任意时间序列 |
| **严重级别** | 基于残差 z-score |

---

## 4. 增强检测

### 4.1 动态阈值检测器

| 属性 | 值 |
|------|-----|
| **方法** | `DynamicThresholdDetector.ComputeThresholds()` |
| **检测目标** | 自适应边界违反 |
| **算法** | 滚动窗口 + MAD/中位数 + 季节性调整 |
| **参数** | `WindowSize: 24`, `LowerBoundFactor: 3.0`, `UpperBoundFactor: 3.0`, `Period: 24` |
| **指标** | 任意时间序列 |
| **严重级别** | 中等 (deviation>1), 高 (>2), 严重 (>3) |

### 4.2 上下文异常检测器

| 属性 | 值 |
|------|-----|
| **方法** | `ContextualAnomalyDetector.Detect()` |
| **检测目标** | 上下文感知异常（小时/天/周模式） |
| **算法** | 每个上下文独立基线（小时、天、周） |
| **参数** | `MinSamplesPerCtx: 5`, `Threshold: 3.0` |
| **指标** | 任意时间序列 |
| **严重级别** | 基于跨上下文平均 z-score |

### 4.3 峰值检测器

| 属性 | 值 |
|------|-----|
| **方法** | `PeakDetector.DetectPeakAnomalies()` |
| **检测目标** | 具有显著性的峰值 |
| **算法** | 局部极大值检测，计算显著性/宽度 |
| **参数** | `MinPeakDistance: 5`, `ProminenceFactor: 0.5` |
| **指标** | 任意时间序列 |
| **严重级别** | 中等, 高 (>3std 显著性), 严重 (>5std) |

---

## 5. 变点检测与预测

### 5.1 CUSUM 变点检测

| 属性 | 值 |
|------|-----|
| **方法** | `ChangePointDetector.DetectCUSUM()` |
| **检测目标** | 累积和变化点检测 |
| **算法** | 累积和控制图 |
| **参数** | `MinSegmentSize: 10`, `Threshold: 2.0` |
| **指标** | 任意时间序列 |

### 5.2 PELT 变点检测

| 属性 | 值 |
|------|-----|
| **方法** | `ChangePointDetector.DetectPELT()` |
| **检测目标** | 动态规划最优变点 |
| **算法** | 剪枝精确线性时间 |
| **参数** | `MinSegmentSize: 10`, `Penalty: 3.0` |
| **指标** | 任意时间序列 |

### 5.3 滑动窗口变点检测

| 属性 | 值 |
|------|-----|
| **方法** | `ChangePointDetector.DetectSlidingWindow()` |
| **检测目标** | t 统计量局部均值偏移 |
| **算法** | 双窗口 t 检验比较 |
| **参数** | `MinSegmentSize: 10`, `Threshold: 2.0` |
| **指标** | 任意时间序列 |

### 5.4 E-Divisive 变点检测

| 属性 | 值 |
|------|-----|
| **方法** | `ChangePointDetector.DetectEDivisive()` |
| **检测目标** | 非参数分布变化 |
| **算法** | 基于能量统计的分裂聚类 |
| **参数** | `MinSegmentSize: 10`, `SignificanceLevel: 0.05`, `MaxChangePoints: 10` |
| **指标** | 任意时间序列 |

### 5.5 预测方法

| 方法 | 算法 | 用途 |
|------|------|------|
| `ForecastSimpleMovingAverage()` | 窗口均值 | 稳定序列 |
| `ForecastExponentialSmoothing()` | 单指数平滑 | 中等变化 |
| `ForecastDoubleExponentialSmoothing()` | Holt 方法 | 趋势数据 |
| `ForecastLinearRegression()` | 线性拟合 | 强趋势 |
| `ForecastSeasonalNaive()` | 季节性滞后 | 季节性模式 |
| `ForecastAuto()` | 自动选择 | 自动选择 |

---

## 6. 高级统计检测

### 6.1 MAD (中位数绝对偏差)

| 属性 | 值 |
|------|-----|
| **方法** | `AdvancedAnomalyDetector.DetectMAD()` |
| **检测目标** | 稳健离群点 |
| **算法** | 基于中位数的偏差，1.4826 缩放 |
| **参数** | `MinSamples: 10`, `MADThreshold: 3.5` |
| **指标** | 任意时间序列 |
| **严重级别** | 中等 (>3.5), 高 (>5.25), 严重 (>7) |

### 6.2 ESD (极端学生化偏差)

| 属性 | 值 |
|------|-----|
| **方法** | `AdvancedAnomalyDetector.DetectESD()` |
| **检测目标** | Grubbs 检验多重离群点 |
| **算法** | 迭代 Grubbs 检验 |
| **参数** | `ESDAlpha: 0.05`, `ESDMaxAnomalies: 2%` |
| **指标** | 任意时间序列 |

### 6.3 STL 分解异常

| 属性 | 值 |
|------|-----|
| **方法** | `AdvancedAnomalyDetector.DetectWithDecomposition()` |
| **检测目标** | 趋势/季节性移除后的残差异常 |
| **算法** | STL + 残差 MAD |
| **参数** | `SeasonalPeriod: 24`, `TrendWindow: 7`, `RobustDecomposition: true` |
| **指标** | 任意时间序列 |

### 6.4 EMA 偏差异常

| 属性 | 值 |
|------|-----|
| **方法** | `AdvancedAnomalyDetector.DetectEMAAnomalies()` |
| **检测目标** | 指数移动平均偏差 |
| **算法** | EMA + 动态标准差 |
| **参数** | `EMAAlpha: 0.2`, `EMAThreshold: 0.3` |
| **指标** | 任意时间序列 |

### 6.5 小时基线异常

| 属性 | 值 |
|------|-----|
| **方法** | `AdvancedAnomalyDetector.DetectHourlyBaselineAnomalies()` |
| **检测目标** | 特定小时的异常 |
| **算法** | 每小时 z-score |
| **参数** | `ZScoreThreshold: 3.0` |
| **指标** | 任意时间序列 |

### 6.6 波动率异常检测

| 属性 | 值 |
|------|-----|
| **方法** | `AdvancedAnomalyDetector.DetectVolatilityAnomalies()` |
| **检测目标** | 突然的波动率变化 |
| **算法** | 滚动变异系数 + MAD |
| **参数** | `windowSize: 6`, `threshold: 3` |
| **指标** | 任意时间序列 |

---

## 7. 工作负载异常检测

### 7.1 工作负载类型异常

| 属性 | 值 |
|------|-----|
| **方法** | `DetectWorkloadAnomalies()` |
| **检测目标** | SQL 类型和 TiKV 操作异常 |
| **算法** | 每类别统计阈值 |
| **参数** | `MinBaselineSamples: 10`, 下跌阈值: 30%, 尖峰比率: 1.5x |
| **指标** | SQL 类型 (Select, Insert, Update, Delete), TiKV 操作 |
| **严重级别** | 基于下跌/尖峰比率 |

---

## 8. 工作负载分析

> **注意：** 以下方法用于工作负载特征刻画和分析，**不是**用于异常检测。它们帮助理解流量模式和系统行为。

### 8.1 频域分析

| 属性 | 值 |
|------|-----|
| **方法** | `analyzeFrequencyDomain()` |
| **用途** | 识别周期性模式（日、周、月） |
| **算法** | FFT 功率谱峰值检测 |
| **参数** | `FFTMinSamples: 64` |
| **输出** | 主导周期、频谱熵、模式标志 |

### 8.2 多模态分析

| 属性 | 值 |
|------|-----|
| **方法** | `analyzeMultimodal()` |
| **用途** | 检测多个不同的工作负载级别 |
| **算法** | Hartigan's Dip Test + 核密度估计 |
| **参数** | `EnableMultimodal: true` |
| **输出** | 模态数量、模态位置/权重、p-value |

### 8.3 尾分布分析

| 属性 | 值 |
|------|-----|
| **方法** | `analyzeTailDistribution()` |
| **用途** | 刻画重尾 vs 轻尾，极值行为 |
| **算法** | 广义帕累托分布拟合 |
| **参数** | `EnableTailFit: true` |
| **输出** | 尾类型（重/中/轻）、尾指数、GPD 参数 |

### 8.4 波动率指标

| 属性 | 值 |
|------|-----|
| **方法** | `analyzeVolatility()` |
| **用途** | 衡量流量稳定性模式 |
| **算法** | 实现波动率、Parkinson HL、波动率的波动率 |
| **输出** | RealizedVolatility, ParkinsonHL, VolOfVol, LeverageEffect |

### 8.5 状态检测

| 属性 | 值 |
|------|-----|
| **方法** | `analyzeRegimes()` |
| **用途** | 识别不同的运行状态 |
| **算法** | 滚动统计变点检测 |
| **输出** | 状态数量、状态均值/标准差、转换 |

---

## 汇总表

### 异常检测方法

| 类别 | 检测方法 | 主要用途 |
|------|----------|----------|
| **统计** | 下跌、尖峰、持续、MAD、ESD、Z-Score | 通用时间序列 |
| **机器学习** | 孤立森林、RCF、集成 | 复杂模式 |
| **频谱** | 频谱残差、多尺度、DBSCAN、Holt-Winters | 信号处理 |
| **上下文** | 小时基线、日/周模式 | 时间感知检测 |
| **变点** | CUSUM、PELT、滑动窗口、E-Divisive | 状态变化 |
| **预测** | SMA、EMA、DES、线性、季节性朴素 | 基于预测 |
| **工作负载** | SQL/TiKV 异常、不均衡 | 数据库特定 |

### 工作负载分析方法

| 类别 | 方法 | 主要用途 |
|------|------|----------|
| **频率** | FFT 分析 | 周期性模式检测 |
| **分布** | 多模态、尾分布 | 分布特征刻画 |
| **稳定性** | 波动率、状态 | 流量稳定性分析 |
