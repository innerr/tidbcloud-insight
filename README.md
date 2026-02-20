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
