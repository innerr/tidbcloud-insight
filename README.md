# TiDB Cloud Insight

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

## Usage

```bash
# List all dedicated clusters
tidbcloud-insight list-clusters dedicated

# Query cluster metrics
tidbcloud-insight query --clusterID 1234567890 --metrics tidb_server_query_total

# Analyze cluster for anomalies
tidbcloud-insight dig 1234567890 --duration 7d

# Run deep analysis on cached data
tidbcloud-insight digdeep 1

# Manage cache
tidbcloud-insight cache --list
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
