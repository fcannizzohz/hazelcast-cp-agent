"""
Hazelcast CP Subsystem AI Analysis Agent
FastAPI backend — serves the UI and the analysis SSE endpoint.

Environment variables:
  PROMETHEUS_URL    default http://localhost:9090
  ANTHROPIC_API_KEY (required when using Claude models)
  OPENAI_API_KEY    (required when using OpenAI models)
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Annotated

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from analysis import build_context, summarise_instant, summarise_range
from chat import chat_stream
from cluster import derive_context
from llm import AVAILABLE_MODELS, analyse
from prom import PrometheusClient, choose_step
from queries import INSTANT_QUERIES, RANGE_QUERIES

PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL", "http://localhost:9090")

app = FastAPI(title="CP Subsystem AI Agent")
app.mount("/static", StaticFiles(directory="static"), name="static")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _duration_label(seconds: float) -> str:
    if seconds < 3600:
        return f"{int(seconds // 60)} min"
    if seconds < 86400:
        h = seconds / 3600
        return f"{h:.1f} h"
    d = seconds / 86400
    return f"{d:.1f} d"


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index():
    with open("static/index.html") as f:
        content = f.read()
    return HTMLResponse(
        content=content,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/api/models")
async def models():
    return AVAILABLE_MODELS


@app.get("/api/config")
async def config():
    return {"prometheus_url": PROMETHEUS_URL}


@app.get("/api/health")
async def health(
    prometheus_url: Annotated[str, Query()] = PROMETHEUS_URL,
):
    prom = PrometheusClient(prometheus_url)
    ok = await prom.health()
    return {"prometheus": ok, "prometheus_url": prometheus_url}


@app.get("/api/analyse")
async def analyse_endpoint(
    start: Annotated[float, Query(description="Unix timestamp — window start")],
    end: Annotated[float, Query(description="Unix timestamp — window end")],
    model: Annotated[str, Query(description="LLM model id")] = "claude-sonnet-4-6",
    prometheus_url: Annotated[str, Query(description="Prometheus base URL")] = PROMETHEUS_URL,
    user_context: Annotated[str, Query(description="JSON array of operator context paragraphs")] = "[]",
):
    """
    Stream the analysis as Server-Sent Events.
    Each event is a plain text chunk.  The final event is 'data: [DONE]\\n\\n'.
    """

    async def event_stream():
        prom = PrometheusClient(prometheus_url)
        duration = end - start
        step = choose_step(duration)

        start_iso = datetime.fromtimestamp(start, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        end_iso = datetime.fromtimestamp(end, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        def sse(event: str, data: str) -> str:
            return f"event: {event}\ndata: {json.dumps(data)}\n\n"

        # ── Status: fetching ──────────────────────────────────────────────
        yield sse("status", "Querying Prometheus…")

        # ── Instant queries (current state) ───────────────────────────────
        instant_results: dict = {}
        query_meta: dict = {}

        instant_tasks = {
            q.name: prom.query(q.query, timestamp=end) for q in INSTANT_QUERIES
        }
        for name, coro in instant_tasks.items():
            try:
                raw = await coro
                instant_results[name] = summarise_instant(raw)
            except Exception as exc:
                instant_results[name] = []
                yield sse("warning", f"instant/{name} failed: {exc}")

        for q in INSTANT_QUERIES:
            query_meta[q.name] = {
                "description": q.description,
                "healthy_hint": q.healthy_hint,
            }

        # ── Range queries (time-series summaries) ─────────────────────────
        range_results: dict = {}

        range_tasks = {
            q.name: prom.query_range(q.query, start=start, end=end, step=step)
            for q in RANGE_QUERIES
        }
        for name, coro in range_tasks.items():
            try:
                raw = await coro
                range_results[name] = summarise_range(raw)
            except Exception as exc:
                range_results[name] = []
                yield sse("warning", f"range/{name} failed: {exc}")

        for q in RANGE_QUERIES:
            query_meta[q.name] = {
                "description": q.description,
                "healthy_hint": q.healthy_hint,
                "unit": getattr(q, "unit", ""),
            }

        # ── Build LLM context ─────────────────────────────────────────────
        context = build_context(
            start_iso=start_iso,
            end_iso=end_iso,
            duration_label=_duration_label(duration),
            instant_results=instant_results,
            range_results=range_results,
            query_meta=query_meta,
        )

        cluster_context = await derive_context(prom)

        try:
            ctx_list: list[str] = json.loads(user_context) if user_context else []
        except Exception:
            ctx_list = []

        yield sse("status", "Analysing with LLM…")

        # ── Stream LLM response ───────────────────────────────────────────
        try:
            async for chunk in analyse(context, cluster_context, model, ctx_list):
                yield f"data: {json.dumps(chunk)}\n\n"
        except Exception as exc:
            yield sse("agent_error", str(exc))

        yield sse("done", "[DONE]")

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/api/chat")
async def chat_endpoint(request: Request):
    """
    Agentic follow-up chat with live Prometheus tool access.
    Accepts JSON body:
      messages        — chat history [{role, content}, ...]
      analysis        — completed analysis text
      prometheus_url  — Prometheus base URL
      start           — original analysis window start (Unix timestamp)
      end             — original analysis window end   (Unix timestamp)
      model           — LLM model id (must be a Claude model)
    Streams SSE: text chunks (default event) + tool_call / done / error events.
    """
    body = await request.json()
    messages       = body.get("messages", [])
    analysis       = body.get("analysis", "")
    prometheus_url = body.get("prometheus_url", PROMETHEUS_URL)
    start          = float(body.get("start", 0))
    end            = float(body.get("end", 0))
    model          = body.get("model", "claude-sonnet-4-6")

    return StreamingResponse(
        chat_stream(messages, analysis, prometheus_url, start, end, model),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
