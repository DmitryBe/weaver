# API + CLI Test Matrix

This matrix is the manual checklist for the comprehensive API and CLI test pass.

## Scope
- Primary sandbox fixtures:
  - `sandbox.simple_retrieve`
  - `sandbox.simple_score`
  - `sandbox.simple_score_candidates`
  - `sandbox.simple_embed`
  - `sandbox.simple_e2e`
- API surface: `POST /v1/surface/{pipeline}`
- CLI surface: `go run ./cmd/prun --pipeline <pipeline> --context ...`

## Environment Prep
1. Start `dev_services` so the local model endpoints are available.
2. Confirm `http://localhost:8001/health` returns `{"status":"ok"}`.
3. Start Weaver API server.
4. Confirm `GET /v1/health` and `GET /v1/registry` succeed.
5. Run one happy-path CLI call and one happy-path API call before continuing.

## Baseline Inputs
### `sandbox.simple_score`
- `city=almaty`
- `vertical=food`
- `user_id=42`

### `sandbox.simple_score_candidates`
- `user_id=42`
- candidates:
  - `{ "brand_id": 101, "rating": 4.8 }`
  - `{ "brand_id": 102, "rating": 4.5 }`

### `sandbox.simple_retrieve`
- `city=almaty`
- `vertical=food`

### `sandbox.simple_embed`
- `user_id=42`
- `text=best coffee near me`

### `sandbox.simple_e2e`
- `city=almaty`
- `vertical=food`
- `user_id=42`

## Scenario Matrix
| ID | Scenario | Pipeline | Surface | Setup | Expected behavior to validate |
| --- | --- | --- | --- | --- | --- |
| 1 | Happy path | `sandbox.simple_retrieve` | API + CLI | Normal services | Success, pipeline name present, request id present, retrieved candidates returned |
| 2 | Happy path | `sandbox.simple_score` | API + CLI | Normal services | Success, pipeline name present, request id present, scored candidates returned |
| 3 | Happy path | `sandbox.simple_score_candidates` | API | Normal services with request candidates | Success, request candidates are scored and sorted without retrieval |
| 4 | Happy path | `sandbox.simple_embed` | API + CLI | Normal services | Success, pipeline name present, request id present, embedding returned in context/output |
| 5 | Happy path | `sandbox.simple_e2e` | API + CLI | Normal services | Success, pipeline name present, request id present, enriched and scored candidates returned |
| 6 | Missing one required input | Any simple pipeline | API + CLI | Omit one required field | API returns `400 INVALID_ARGUMENT`; CLI exits non-zero with missing input error |
| 7 | Missing multiple required inputs | Any simple pipeline | API + CLI | Provide only part of required context | Same class of validation failure, confirm message quality |
| 8 | Malformed API body | Any | API | Invalid JSON | API returns `400 INVALID_ARGUMENT` with malformed body message |
| 9 | Empty CLI context block | Any CLI-supported pipeline | CLI | `--context` with no pairs | CLI exits with parse error |
| 10 | Unknown pipeline | Any | API + CLI | Misspelled pipeline name | API returns `404 NOT_FOUND`; CLI exits non-zero with available pipeline list |
| 11 | Missing context feature definition | `sandbox.simple_e2e` | API + CLI | Change pipeline fetch to a non-existent feature name such as `user/not_a_feature` | Confirm whether execution defaults, fails, or hides the cause |
| 12 | Context feature exists but key has no value | `sandbox.simple_e2e` | API + CLI | Use unknown `user_id` for `user/segment` | Confirm defaults are applied and whether the response is clear |
| 13 | Missing retrieve feature definition | `sandbox.simple_retrieve`, `sandbox.simple_score`, `sandbox.simple_e2e` | API + CLI | Change retrieve source to a non-existent feature name | Confirm retrieve-stage failure is understandable |
| 14 | Retrieve feature exists but key has no value | `sandbox.simple_retrieve`, `sandbox.simple_score`, `sandbox.simple_e2e` | API + CLI | Use unknown `city` or `vertical` | Confirm empty candidate list behavior and readability |
| 15 | Missing candidate feature definition | `sandbox.simple_score`, `sandbox.simple_e2e` | API + CLI | Change candidate fetch to a non-existent feature name such as `brand/not_a_feature` | Confirm defaulting or failure behavior is explicit |
| 16 | Candidate feature exists but key has no value | `sandbox.simple_score`, `sandbox.simple_e2e` | API + CLI | Retrieve candidates with missing brand or brand-user rows | Confirm defaults are applied rather than hard failure |
| 17 | Missing provided candidate fields | `sandbox.simple_score_candidates` | API | Omit `brand_id` or `rating` in request candidates | Confirm request reaches scorer and surfaced error is understandable |
| 18 | Endpoint unavailable | `sandbox.simple_score` | API + CLI | Stop model service or change port/path | CLI should include upstream error detail; API likely returns generic `500 INTERNAL` |
| 19 | Endpoint unavailable | `sandbox.simple_score_candidates` | API | Stop model service or change port/path | Same scorer failure path without retrieval noise |
| 20 | Endpoint unavailable | `sandbox.simple_embed` | API + CLI | Stop model service or change port/path | Same comparison for embedding path |
| 21 | Endpoint returns non-2xx | `sandbox.simple_score` | API + CLI | Force `/score` to return 4xx/5xx | Validate exact surfaced error text |
| 22 | Endpoint returns non-2xx | `sandbox.simple_score_candidates` | API | Force `/score` to return 4xx/5xx | Validate scorer-only surfaced error text |
| 23 | Endpoint returns non-2xx | `sandbox.simple_embed` | API + CLI | Force `/embed` to return 4xx/5xx | Validate exact surfaced error text |
| 24 | Invalid JSON from endpoint | `sandbox.simple_score` | API + CLI | Force malformed `/score` response | Confirm parse failure visibility |
| 25 | Invalid JSON from endpoint | `sandbox.simple_score_candidates` | API | Force malformed `/score` response | Confirm scorer-only parse failure visibility |
| 26 | Invalid JSON from endpoint | `sandbox.simple_embed` | API + CLI | Force malformed `/embed` response | Confirm parse failure visibility |
| 27 | Missing expected field | `sandbox.simple_score` | API + CLI | `/score` omits `scores` | Confirm parse failure visibility |
| 28 | Missing expected field | `sandbox.simple_score_candidates` | API | `/score` omits `scores` | Confirm scorer-only parse failure visibility |
| 29 | Missing expected field | `sandbox.simple_embed` | API + CLI | `/embed` omits `embedding` | Confirm parse failure visibility |
| 30 | Wrong field type | `sandbox.simple_score` | API + CLI | `/score` returns wrong `scores` type | Confirm parse failure visibility |
| 31 | Wrong field type | `sandbox.simple_score_candidates` | API | `/score` returns wrong `scores` type | Confirm scorer-only parse failure visibility |
| 32 | Wrong field type | `sandbox.simple_embed` | API + CLI | `/embed` returns wrong `embedding` type | Confirm parse failure visibility |
| 33 | Mismatched score count | `sandbox.simple_score` | API + CLI | `/score` returns wrong length | Confirm executor error is understandable |
| 34 | Mismatched score count | `sandbox.simple_score_candidates` | API | `/score` returns wrong length | Confirm executor error is understandable |
| 35 | Latency below threshold | `sandbox.simple_score`, `sandbox.simple_score_candidates`, `sandbox.simple_embed`, `sandbox.simple_e2e` | API + CLI where supported | Use per-request latency below `100ms` | Success, no timeout |
| 36 | Latency near threshold | `sandbox.simple_score`, `sandbox.simple_score_candidates`, `sandbox.simple_embed`, `sandbox.simple_e2e` | API + CLI where supported | Use per-request latency near `100ms` | Confirm whether behavior remains successful |
| 37 | Latency above threshold | `sandbox.simple_score`, `sandbox.simple_score_candidates`, `sandbox.simple_embed`, `sandbox.simple_e2e` | API + CLI where supported | Use per-request latency above `100ms` | Confirm timeout/retry behavior and surfaced error |
| 38 | Latency far above threshold | `sandbox.simple_score`, `sandbox.simple_score_candidates`, `sandbox.simple_embed`, `sandbox.simple_e2e` | API + CLI where supported | Use much larger latency | Confirm stable failure mode and message consistency |
| 39 | Empty retrieval result | `sandbox.simple_retrieve`, `sandbox.simple_score`, `sandbox.simple_e2e` | API + CLI | Unknown city/vertical combination | Success with zero candidates, readable response |
| 40 | Small result set | `sandbox.simple_retrieve`, `sandbox.simple_score`, `sandbox.simple_e2e` | API + CLI | Only one matching candidate | Success with one candidate, no shape regression |

## Notes
- `sandbox.simple_score_candidates` is API-first today because `prun` does not yet accept request candidates as input.
- Feature-store cases should be tested separately for:
  - feature definition missing entirely
  - feature exists but there is no row/value for the requested key
  - feature exists and row exists, but the value is `null`

## Recording Template
For each scenario, capture:
- Exact command or request body
- Expected result before execution
- Actual HTTP status or CLI exit code
- Actual response body / stdout / stderr
- Assessment: `clear`, `acceptable`, or `needs improvement`
- Notes on whether API and CLI are aligned enough

## Output Review Focus
- Is the failure category obvious from what the user sees?
- Does the message name the missing input, missing pipeline, or upstream endpoint clearly enough?
- Does API hide too much detail compared with CLI?
- Should timeouts and upstream failures map to more specific API error codes?
- Are successful responses informative enough for debugging manual tests?
