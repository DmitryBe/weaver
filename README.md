# Weaver

Weaver is a small Go recommendation-pipeline runtime with a declarative DSL for building retrieval, enrichment, scoring, and ranking flows.

It can be used in two ways:

- as an HTTP service via `cmd/server`
- as a local pipeline runner via `cmd/prun`

The repository currently includes a sandbox pipeline set backed by:

- a file-based feature store in [`testdata/featurestore`](/Users/dmitry/git/github.com/dmitryBe/weaver/testdata/featurestore)
- optional Neo4j-backed knowledge graph retrieval
- a mock model API in [`mock_apis/mock_inference_api.py`](/Users/dmitry/git/github.com/dmitryBe/weaver/mock_apis/mock_inference_api.py)

## What It Does

Weaver composes pipeline steps such as:

- validating request input
- fetching context features
- retrieving candidates from KV or KG sources
- enriching candidates with feature-store lookups
- calling model endpoints for scoring or embedding
- sorting, merging, taking top-K, and caching results

The pipeline definitions in [`pipelines/sandbox`](/Users/dmitry/git/github.com/dmitryBe/weaver/pipelines/sandbox) show the intended authoring model.

## Included Example Pipelines

- `sandbox.simple_retrieve`: retrieve candidates from the file-backed feature store
- `sandbox.simple_score`: retrieve, enrich, and score candidates with the mock `/score` model
- `sandbox.simple_score_candidates`: score candidates provided directly in the request
- `sandbox.simple_embed`: generate an embedding from `user_id` and `text`
- `sandbox.simple_e2e`: compact end-to-end example with context enrichment, retrieval, enrichment, scoring, and ranking
- `sandbox.simple_retrieve_kg`: retrieve candidates from a Cypher query against a configured knowledge graph
- `sandbox.trending`: larger example combining context fetches, embedding, KV + KG retrieval, enrichment, scoring, and caching

## Quickstart

### Requirements

- Go `1.25+`
- Python with [`uv`](https://docs.astral.sh/uv/) if you want to run the mock inference API
- optional: Neo4j credentials if you want to run KG pipelines

### 1. Start the mock model API

```bash
uv run mock_apis/mock_inference_api.py --port 8001
```

Health check:

```bash
curl -s http://127.0.0.1:8001/health
```

### 2. Point Weaver at a local config file

Weaver does not currently have a working default remote config path, so local development should always set `CONFIG_FILE`.

```bash
export CONFIG_FILE=configs/dev.yaml
```

The default local config:

- serves HTTP on port `8080`
- uses the file-backed feature store at `testdata/featurestore/playground`
- enables an in-memory LRU cache

### 3. Start the API server

```bash
go run ./cmd/server
```

Sanity checks:

```bash
curl -s http://127.0.0.1:8080/v1/health
curl -s http://127.0.0.1:8080/v1/registry
```

## CLI Usage

`prun` runs a pipeline locally and prints JSON to stdout.

Basic form:

```bash
go run ./cmd/prun --pipeline <pipeline> --context key=value [key=value ...]
```

Example:

```bash
go run ./cmd/prun \
  --pipeline sandbox.simple_score \
  --context city=almaty vertical=food user_id=42
```

Embedding example:

```bash
go run ./cmd/prun \
  --pipeline sandbox.simple_embed \
  --context user_id=42 text="best coffee near me"
```

Debug mode adds per-step snapshots:

```bash
go run ./cmd/prun \
  --pipeline sandbox.simple_e2e \
  --context city=almaty vertical=food user_id=42 \
  --debug
```

Run `go run ./cmd/prun --help` for the full flag list.

## HTTP API

### Endpoints

- `GET /v1/health`
- `GET /v1/registry`
- `POST /v1/surface/{pipeline}`
- `GET /metrics` when metrics are enabled with a handler backend

### Execute a pipeline

```bash
curl -s -X POST http://127.0.0.1:8080/v1/surface/sandbox.simple_e2e \
  -H 'Content-Type: application/json' \
  -d '{
    "context": {
      "city": "almaty",
      "vertical": "food",
      "user_id": "42"
    }
  }'
```

Score caller-supplied candidates:

```bash
curl -s -X POST http://127.0.0.1:8080/v1/surface/sandbox.simple_score_candidates \
  -H 'Content-Type: application/json' \
  -d '{
    "context": {
      "user_id": "42"
    },
    "candidates": [
      { "brand_id": 101, "rating": 4.8 },
      { "brand_id": 102, "rating": 4.5 }
    ]
  }'
```

Debug mode:

```bash
curl -s -X POST 'http://127.0.0.1:8080/v1/surface/sandbox.simple_e2e?debug=true' \
  -H 'Content-Type: application/json' \
  -d '{
    "context": {
      "city": "almaty",
      "vertical": "food",
      "user_id": "42"
    }
  }'
```

## Configuration

Weaver loads config from:

1. the path in `CONFIG_FILE`, or
2. a remote Galileo source

At the moment, the Galileo loader is not implemented, so `CONFIG_FILE` is the practical development path.

Useful configs:

- [`configs/dev.yaml`](/Users/dmitry/git/github.com/dmitryBe/weaver/configs/dev.yaml): local file-store setup
- [`configs/dev-kg-neo4j.yaml`](/Users/dmitry/git/github.com/dmitryBe/weaver/configs/dev-kg-neo4j.yaml): local file-store setup plus Neo4j KG connection

Main config areas:

- `port`: HTTP port for `cmd/server`
- `feature_store`: feature source, currently `file`
- `cache`: cache backend, currently in-memory LRU
- `knowledge_graphs`: optional named Neo4j/Bolt connections
- `metrics`: metrics backend selection and handler settings

## Project Layout

```text
cmd/
  prun/        CLI entrypoint
  server/      HTTP server entrypoint
configs/       Local development configs
docs/          Supporting docs and test notes
internal/
  api/         HTTP transport layer
  appconfig/   Config loading and dependency wiring
  dsl/         Pipeline DSL building blocks
  nodes/       Runtime node implementations
  retriever/   Retrieval backends
  runner/      Pipeline loading and execution orchestration
  runtime/     Execution engine and types
mock_apis/     Local mock model service
pipelines/     Registered pipeline definitions
queries/       Knowledge graph queries
testdata/      Feature-store fixtures used by local configs
```

## How Pipelines Are Defined

Pipelines are registered under a domain and name and built from a fluent DSL.

Common building blocks include:

- `Input(...)` for required request fields
- `Context(...)` for context-level fetches and mutations
- `Retrieve(...)` for candidate retrieval
- `Candidates(...)` for candidate-level fetches, model calls, and post-processing
- `Cache(...)` for result caching
- `Resilience(...)` for retry/timeout behavior around dependency calls

See these examples:

- [`pipelines/sandbox/simple_e2e.go`](/Users/dmitry/git/github.com/dmitryBe/weaver/pipelines/sandbox/simple_e2e.go)
- [`pipelines/sandbox/trending.go`](/Users/dmitry/git/github.com/dmitryBe/weaver/pipelines/sandbox/trending.go)
- [`pipelines/sandbox/simple_retrieve_kg.go`](/Users/dmitry/git/github.com/dmitryBe/weaver/pipelines/sandbox/simple_retrieve_kg.go)

## Testing

Run the Go test suite:

```bash
go test ./...
```

For manual API and CLI validation, see [`docs/api_cli_test_matrix.md`](/Users/dmitry/git/github.com/dmitryBe/weaver/docs/api_cli_test_matrix.md).

## Notes

- `prun` currently accepts `context` fields, but not caller-supplied candidates, so `sandbox.simple_score_candidates` is primarily exercised through the HTTP API.
- KG pipelines require a `knowledge_graphs` config entry and any required credentials such as `NEO4J_DEFAULT_PASSWORD`.
- The local model endpoints used by the sandbox pipelines are hard-coded to `http://localhost:8001`.
