"""
Prometheus MCP server — HTTP/SSE transport.

Exposes three tools:
  prometheus_query        — instant PromQL query
  prometheus_query_range  — range PromQL query
  prometheus_list_metrics — list metric names (optionally filtered by prefix)

Environment:
  PROMETHEUS_URL   default http://localhost:9090
  PORT             default 8001
"""

import json
import os

import httpx
from dotenv import load_dotenv
import uvicorn
from mcp.server import Server
from mcp.server.sse import SseServerTransport
from mcp.types import TextContent, Tool
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Mount, Route

load_dotenv()

PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL", "http://localhost:9090").rstrip("/")
PORT = int(os.environ.get("PORT", "8001"))

app = Server("prometheus-mcp")


# ---------------------------------------------------------------------------
# Tool definitions
# ---------------------------------------------------------------------------

@app.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="prometheus_query",
            description=(
                "Execute an instant PromQL query against Prometheus. "
                "Returns a list of {metric, value} objects."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "PromQL expression to evaluate",
                    },
                    "time": {
                        "type": "number",
                        "description": "Unix timestamp for evaluation (optional, defaults to now)",
                    },
                },
                "required": ["query"],
            },
        ),
        Tool(
            name="prometheus_query_range",
            description=(
                "Execute a range PromQL query against Prometheus. "
                "Returns time-series data as a list of {metric, values} objects."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "PromQL expression to evaluate",
                    },
                    "start": {
                        "type": "number",
                        "description": "Unix timestamp — range start",
                    },
                    "end": {
                        "type": "number",
                        "description": "Unix timestamp — range end",
                    },
                    "step": {
                        "type": "string",
                        "description": "Step duration, e.g. '30s', '1m', '5m'",
                    },
                },
                "required": ["query", "start", "end", "step"],
            },
        ),
        Tool(
            name="prometheus_list_metrics",
            description=(
                "List all metric names available in Prometheus, optionally filtered by a prefix. "
                "Use this to discover exact metric names before writing PromQL queries."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "prefix": {
                        "type": "string",
                        "description": "Only return metrics whose name starts with this prefix (optional)",
                    },
                },
                "required": [],
            },
        ),
    ]


# ---------------------------------------------------------------------------
# Tool execution
# ---------------------------------------------------------------------------

@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    try:
        if name == "prometheus_query":
            result = await _query(arguments["query"], arguments.get("time"))
        elif name == "prometheus_query_range":
            result = await _query_range(
                arguments["query"],
                arguments["start"],
                arguments["end"],
                arguments["step"],
            )
        elif name == "prometheus_list_metrics":
            result = await _list_metrics(arguments.get("prefix", ""))
        else:
            result = {"error": f"Unknown tool: {name}"}
    except Exception as exc:
        result = {"error": str(exc)}

    return [TextContent(type="text", text=json.dumps(result, indent=2))]


# ---------------------------------------------------------------------------
# Prometheus helpers
# ---------------------------------------------------------------------------

async def _query(promql: str, timestamp=None) -> list:
    params: dict = {"query": promql}
    if timestamp is not None:
        params["time"] = timestamp
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(f"{PROMETHEUS_URL}/api/v1/query", params=params)
        r.raise_for_status()
    data = r.json()
    if data["status"] != "success":
        raise RuntimeError(f"Prometheus error: {data}")
    return data["data"]["result"]


async def _query_range(promql: str, start: float, end: float, step: str) -> list:
    params = {"query": promql, "start": start, "end": end, "step": step}
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(f"{PROMETHEUS_URL}/api/v1/query_range", params=params)
        r.raise_for_status()
    data = r.json()
    if data["status"] != "success":
        raise RuntimeError(f"Prometheus error: {data}")
    return data["data"]["result"]


async def _list_metrics(prefix: str) -> list[str]:
    # Fetch all metric names; filter in Python.
    # Note: the match[] selector is intentionally omitted — constructing
    # {__name__=~"..."} in an f-string is error-prone and the full name list
    # is small enough (~hundreds of metrics) that server-side filtering adds
    # no meaningful benefit.
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(f"{PROMETHEUS_URL}/api/v1/label/__name__/values")
        r.raise_for_status()
    data = r.json()
    if data["status"] != "success":
        raise RuntimeError(f"Prometheus error: {data}")
    names = sorted(data.get("data", []))
    if prefix:
        names = [n for n in names if n.startswith(prefix)]
    return names


# ---------------------------------------------------------------------------
# Starlette / SSE wiring
# ---------------------------------------------------------------------------

sse_transport = SseServerTransport("/messages")


class _NullResponse:
    """Returned by handle_sse so Starlette's request_response wrapper has a
    callable to invoke instead of None, avoiding TypeError on SSE disconnect."""
    async def __call__(self, *_) -> None:
        pass


async def handle_sse(request: Request) -> _NullResponse:
    async with sse_transport.connect_sse(
        request.scope, request.receive, request._send
    ) as streams:
        await app.run(streams[0], streams[1], app.create_initialization_options())
    return _NullResponse()


starlette_app = Starlette(
    routes=[
        Route("/sse", endpoint=handle_sse),
        Mount("/messages", app=sse_transport.handle_post_message),
        Route("/health", endpoint=lambda r: Response("ok")),
    ]
)

if __name__ == "__main__":
    uvicorn.run(starlette_app, host="0.0.0.0", port=PORT)
