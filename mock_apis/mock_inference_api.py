#!/usr/bin/env python3
# /// script
# dependencies = ["fastapi", "uvicorn"]
# ///
"""
Generic mock ML service for local testing.

Endpoints:
    POST /score
    POST /embed
    GET  /health

Start:
    uv run mock_apis/mock_inference_api.py [--port 8001] [--latency-ms 150]

Examples:
    curl -s -X POST http://localhost:8001/score \
      -H 'Content-Type: application/json' \
      -d '{"candidates":[{"brand_id":"1","rating":4.5},{"brand_id":"2","score":0.7}]}' | jq .

    curl -s -X POST http://localhost:8001/embed \
      -H 'Content-Type: application/json' \
      -d '{"user_id":1, "history": [1,2,3]}' | jq .
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import random
from typing import Any

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

app = FastAPI(title="mock-ml")
app.state.default_latency_ms = 0.0
app.state.score_latency_ms = 0.0
app.state.embed_latency_ms = 0.0


class ScoreRequest(BaseModel):
    candidates: list[dict[str, Any]]
    debug_latency_ms: float | None = None
    # any additional context fields (user_id, etc.) are accepted and ignored
    model_config = {"extra": "allow"}


class ScoreResponse(BaseModel):
    scores: list[float]


class EmbedRequest(BaseModel):
    debug_latency_ms: float | None = None
    model_config = {"extra": "allow"}


class EmbedResponse(BaseModel):
    response: list[list[float]]


@app.post("/score", response_model=ScoreResponse)
async def score(req: ScoreRequest) -> ScoreResponse:
    log.info("REQUEST  /score  candidates=%d  body=%s", len(req.candidates), req.model_dump_json())
    scores = [_score_candidate(candidate) for candidate in req.candidates]
    resp = ScoreResponse(scores=scores)
    log.info("RESPONSE /score  scores=%s", resp.scores)
    await _maybe_sleep(_resolve_latency_ms(req.debug_latency_ms, app.state.score_latency_ms))
    return resp


@app.post("/embed", response_model=EmbedResponse)
async def embed(req: EmbedRequest) -> EmbedResponse:
    payload = req.model_dump(exclude_none=True)
    payload.pop("debug_latency_ms", None)
    items = _embed_items(payload)
    log.info("REQUEST  /embed  items=%d  body=%s", len(items), json.dumps(payload, sort_keys=True))
    resp = EmbedResponse(response=[_embed_vector(item) for item in items])
    log.info("RESPONSE /embed  items=%d", len(resp.response))
    await _maybe_sleep(_resolve_latency_ms(req.debug_latency_ms, app.state.embed_latency_ms))
    return resp


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


def _score_candidate(candidate: dict[str, Any]) -> float:
    if "rating" in candidate:
        return min(float(candidate["rating"]) / 5.0, 1.0)
    if "_score" in candidate:
        return float(candidate["_score"])
    if "score" in candidate:
        return float(candidate["score"])
    return round(random.random(), 4)


def _embed_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    items = payload.get("request")
    if isinstance(items, list) and items:
        return [item for item in items if isinstance(item, dict)]

    return [dict(payload)]


def _embed_vector(item: dict[str, Any], dims: int = 8) -> list[float]:
    seed = _embed_seed(item)
    digest = hashlib.sha256(seed.encode("utf-8")).digest()
    values: list[float] = []

    for index in range(dims):
        start = index * 4
        chunk = int.from_bytes(digest[start : start + 4], "big", signed=False)
        values.append(round((chunk / 0xFFFFFFFF) * 2.0 - 1.0, 6))

    return values


def _embed_seed(item: dict[str, Any]) -> str:
    for key in ("query", "text", "input", "prompt"):
        value = item.get(key)
        if value is not None:
            return f"{key}:{value}"
    return json.dumps(item, sort_keys=True, separators=(",", ":"), default=str)


async def _maybe_sleep(latency_ms: float) -> None:
    if latency_ms > 0:
        await asyncio.sleep(latency_ms / 1000.0)


def _resolve_latency_ms(request_latency_ms: float | None, endpoint_latency_ms: float) -> float:
    if request_latency_ms is not None:
        return max(request_latency_ms, 0.0)
    if endpoint_latency_ms > 0:
        return endpoint_latency_ms
    return max(app.state.default_latency_ms, 0.0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument(
        "--latency-ms",
        type=float,
        default=0.0,
        help="Optional default latency to simulate for every request.",
    )
    parser.add_argument(
        "--score-latency-ms",
        type=float,
        default=0.0,
        help="Optional default latency for /score requests.",
    )
    parser.add_argument(
        "--embed-latency-ms",
        type=float,
        default=0.0,
        help="Optional default latency for /embed requests.",
    )
    args = parser.parse_args()

    app.state.default_latency_ms = max(args.latency_ms, 0.0)
    app.state.score_latency_ms = max(args.score_latency_ms, 0.0)
    app.state.embed_latency_ms = max(args.embed_latency_ms, 0.0)
    uvicorn.run(app, host=args.host, port=args.port)
