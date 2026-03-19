"""
Hazelcast MCP server — HTTP/SSE transport.

Exposes three tools for log access, ordered by token cost (cheapest first):

  hz_log_summary          — per-member WARN/ERROR counts for a time window
                            (~50 tokens; use this first to identify affected members)
  hz_get_logs             — structured, filtered log entries from one or more members
                            (bounded by max_lines; stack traces folded to save tokens)
  hz_get_diagnostic_logs  — keyword-filtered diagnostic log snippets from container
                            filesystem (only useful if diagnostics are enabled in HZ config)

Log access is via the Docker daemon socket (/var/run/docker.sock), which must be
mounted read-only into this container.

Environment:
  HZ_MEMBERS   comma-separated container names  default: hz1,hz2,hz3,hz4,hz5
  MC_URL       Management Center base URL        default: http://management-center:8080
  MC_CLUSTER   Hazelcast cluster name            default: dev
  PORT         HTTP port                         default: 8002
"""

from __future__ import annotations

import io
import json
import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from urllib.parse import quote

import httpx
import uvicorn
from mcp.server import Server
from mcp.server.sse import SseServerTransport
from mcp.types import TextContent, Tool
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Mount, Route

PORT       = int(os.environ.get("PORT", "8002"))
MEMBERS    = [m.strip() for m in os.environ.get("HZ_MEMBERS", "hz1,hz2,hz3,hz4,hz5").split(",")]
MC_URL     = os.environ.get("MC_URL",     "http://management-center:8080").rstrip("/")
MC_CLUSTER = os.environ.get("MC_CLUSTER", "dev")

# Hazelcast log4j2 line pattern:  "HH:mm:ss.SSS [thread] LEVEL  logger - message"
_LEVEL_RE   = re.compile(r"\]\s+(TRACE|DEBUG|INFO|WARN|ERROR|FATAL)\s+")
_LOGGER_RE  = re.compile(r"\]\s+(?:TRACE|DEBUG|INFO|WARN|ERROR|FATAL)\s+(\S+)\s+-\s+(.*)")
# Docker prepends RFC3339Nano timestamp when timestamps=True
_DOCKER_TS  = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.\d+Z\s+(.*)$")

# Lines that are part of Java stack traces (not separate log events)
_STACKTRACE_LINE = re.compile(r"^\s+at |^Caused by:|^\s+\.\.\. \d+ more")

app = Server("hz-mcp")


# ---------------------------------------------------------------------------
# Tool definitions
# ---------------------------------------------------------------------------

@app.list_tools()
async def list_tools() -> list[Tool]:
    member_list = ", ".join(MEMBERS)
    return [
        Tool(
            name="hz_log_summary",
            description=(
                "Return per-member WARN and ERROR log line counts for a time window. "
                "Very cheap (~50 tokens). Call this FIRST to identify which members have "
                "issues before fetching full log lines with hz_get_logs."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "start": {"type": "number", "description": "Window start — Unix timestamp"},
                    "end":   {"type": "number", "description": "Window end   — Unix timestamp"},
                },
                "required": ["start", "end"],
            },
        ),
        Tool(
            name="hz_get_logs",
            description=(
                "Fetch structured log entries from Hazelcast members for a time window. "
                f"Available members: {member_list}. "
                "Returns compact JSON entries {ts, member, level, logger, msg}. "
                "Stack traces are folded to the exception class + first cause line to save tokens. "
                "Tips: use hz_log_summary first; specify only affected members; "
                "pass keywords from Prometheus findings to filter further."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "start":    {"type": "number",  "description": "Window start — Unix timestamp"},
                    "end":      {"type": "number",  "description": "Window end   — Unix timestamp"},
                    "members":  {
                        "type": "array", "items": {"type": "string"},
                        "description": f"Members to query (default: all). Available: {member_list}",
                    },
                    "level":    {
                        "type": "string",
                        "enum": ["WARN", "ERROR", "INFO", "DEBUG"],
                        "description": "Minimum log level to include (default: WARN)",
                    },
                    "keywords": {
                        "type": "array", "items": {"type": "string"},
                        "description": "Optional keywords — only lines containing at least one are returned",
                    },
                    "max_lines": {
                        "type": "integer",
                        "description": "Maximum log entries to return (default: 100, max: 300)",
                    },
                },
                "required": ["start", "end"],
            },
        ),
        Tool(
            name="hz_get_member_config",
            description=(
                "Fetch the live Hazelcast configuration for a specific member from "
                "Management Center and return it as JSON. "
                "Use this to verify CP subsystem settings: session TTL, "
                "group size, missing-member auto-removal timeout, CPMap size limits, etc."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "member": {
                        "type": "string",
                        "description": (
                            f"Member to query (e.g. 'hz1' or 'hz1:5701'). "
                            f"Available: {member_list}"
                        ),
                    },
                },
                "required": ["member"],
            },
        ),
        Tool(
            name="hz_get_diagnostic_logs",
            description=(
                "Fetch Hazelcast diagnostic log snippets from the container filesystem. "
                "Only available if diagnostics are enabled in hazelcast.xml "
                "(hazelcast.diagnostics.enabled=true). Returns 'not available' otherwise. "
                "Always keyword-filter to avoid returning large files."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "member":   {"type": "string",  "description": f"Target member. One of: {member_list}"},
                    "start":    {"type": "number",  "description": "Window start — Unix timestamp"},
                    "end":      {"type": "number",  "description": "Window end   — Unix timestamp"},
                    "keywords": {
                        "type": "array", "items": {"type": "string"},
                        "description": "Keywords to filter diagnostic lines (strongly recommended)",
                    },
                    "max_lines": {
                        "type": "integer",
                        "description": "Maximum lines to return (default: 100)",
                    },
                },
                "required": ["member", "start", "end"],
            },
        ),
    ]


# ---------------------------------------------------------------------------
# Tool execution
# ---------------------------------------------------------------------------

@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    try:
        if name == "hz_log_summary":
            result = await _log_summary(
                float(arguments["start"]),
                float(arguments["end"]),
            )
        elif name == "hz_get_logs":
            result = await _get_logs(
                start    = float(arguments["start"]),
                end      = float(arguments["end"]),
                members  = arguments.get("members") or MEMBERS,
                level    = arguments.get("level", "WARN"),
                keywords = [k.lower() for k in (arguments.get("keywords") or [])],
                max_lines= min(int(arguments.get("max_lines") or 100), 300),
            )
        elif name == "hz_get_member_config":
            result = await _get_member_config(arguments["member"])
        elif name == "hz_get_diagnostic_logs":
            result = await _get_diagnostic_logs(
                member   = arguments["member"],
                start    = float(arguments["start"]),
                end      = float(arguments["end"]),
                keywords = [k.lower() for k in (arguments.get("keywords") or [])],
                max_lines= int(arguments.get("max_lines") or 100),
            )
        else:
            result = {"error": f"Unknown tool: {name}"}
    except Exception as exc:
        result = {"error": str(exc)}

    return [TextContent(type="text", text=json.dumps(result, indent=2))]


# ---------------------------------------------------------------------------
# Member config helper
# ---------------------------------------------------------------------------

def _xml_to_dict(element: ET.Element) -> dict | str | None:
    """Recursively convert an XML element to a plain Python dict / str."""
    tag = element.tag.split("}", 1)[1] if "}" in element.tag else element.tag
    attrib = dict(element.attrib)
    children = list(element)
    text = (element.text or "").strip()

    if not children:
        # Leaf node — return a simple string if no attributes, else a dict
        if attrib:
            node = dict(attrib)
            if text:
                node["_value"] = text
            return node
        return text or None

    child_map: dict = {}
    for child in children:
        child_tag = child.tag.split("}", 1)[1] if "}" in child.tag else child.tag
        child_val = _xml_to_dict(child)
        if child_tag in child_map:
            if not isinstance(child_map[child_tag], list):
                child_map[child_tag] = [child_map[child_tag]]
            child_map[child_tag].append(child_val)
        else:
            child_map[child_tag] = child_val

    if attrib:
        child_map.update(attrib)
    return child_map


async def _get_member_config(member: str) -> dict:
    member_addr = member if ":" in member else f"{member}:5701"
    url = f"{MC_URL}/api/clusters/{MC_CLUSTER}/members/{quote(member_addr, safe='')}/memberConfig"
    print(f"[hz_get_member_config] GET {url}", flush=True)
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(url)
    print(f"[hz_get_member_config] status={r.status_code} content-type={r.headers.get('content-type')} body={r.text[:500]!r}", flush=True)
    r.raise_for_status()
    # MC wraps the XML in a JSON string: the response body is `"<hazelcast ...>"`.
    # json.loads() unwraps it; encode back to bytes so ET handles the XML
    # encoding declaration (<?xml version='1.0' encoding='UTF-8'?>) correctly.
    xml_str = json.loads(r.text)
    root = ET.parse(io.BytesIO(xml_str.encode("utf-8"))).getroot()
    return {"member": member_addr, "config": _xml_to_dict(root)}


# ---------------------------------------------------------------------------
# Log helpers
# ---------------------------------------------------------------------------

def _docker_client():
    import docker
    return docker.from_env()


def _parse_line(raw: str, member: str) -> dict | None:
    """
    Parse a single Docker-timestamped Hazelcast log line into a compact dict.
    Returns None for blank lines and pure stack-trace continuation lines.
    """
    m = _DOCKER_TS.match(raw)
    if not m:
        return None
    ts, content = m.group(1), m.group(2)
    content = content.rstrip()
    if not content:
        return None

    # Level
    lm = _LEVEL_RE.search(content)
    level = lm.group(1) if lm else "INFO"

    # Logger + message
    lm2 = _LOGGER_RE.search(content)
    if lm2:
        logger  = lm2.group(1)
        # Shorten logger: com.hazelcast.cp.internal.raft.impl.RaftNodeImpl → c.h.c.i.r.i.RaftNodeImpl
        parts   = logger.split(".")
        short   = ".".join(p[0] for p in parts[:-1]) + "." + parts[-1] if len(parts) > 1 else logger
        message = lm2.group(2)
    else:
        short   = ""
        message = content

    return {"ts": ts, "member": member, "level": level, "logger": short, "msg": message}


def _fetch_raw_logs(container_name: str, since: int, until: int) -> bytes:
    client = _docker_client()
    container = client.containers.get(container_name)
    return container.logs(since=since, until=until, timestamps=True, stream=False)


async def _log_summary(start: float, end: float) -> dict:
    """
    Scan raw log bytes for WARN/ERROR occurrences per member.
    Never materialises parsed entries — just byte counting.
    """
    since, until = int(start), int(end)
    summary = {}
    errors = {}
    for member in MEMBERS:
        try:
            raw = _fetch_raw_logs(member, since, until)
            lines = raw.split(b"\n")
            warn  = sum(1 for l in lines if b"] WARN" in l or b"] WARNING" in l)
            error = sum(1 for l in lines if b"] ERROR" in l)
            total = len([l for l in lines if l.strip()])
            summary[member] = {"total_lines": total, "WARN": warn, "ERROR": error}
        except Exception as exc:
            errors[member] = str(exc)

    result: dict = {"window": {"start": _fmt_ts(start), "end": _fmt_ts(end)}, "members": summary}
    if errors:
        result["errors"] = errors
    return result


_LEVEL_ORDER = {"TRACE": 0, "DEBUG": 1, "INFO": 2, "WARN": 3, "ERROR": 4, "FATAL": 5}


async def _get_logs(
    start: float,
    end: float,
    members: list[str],
    level: str,
    keywords: list[str],
    max_lines: int,
) -> dict:
    since, until = int(start), int(end)
    min_level = _LEVEL_ORDER.get(level.upper(), 3)
    entries: list[dict] = []
    errors: dict = {}

    for member in members:
        if member not in MEMBERS:
            errors[member] = "unknown member"
            continue
        try:
            raw = _fetch_raw_logs(member, since, until)
            lines = raw.decode("utf-8", errors="replace").splitlines()
            # Track consecutive stack-trace lines to fold them
            last_entry: dict | None = None
            stack_count = 0

            for line in lines:
                if len(entries) >= max_lines:
                    break

                # Fold stack-trace continuation lines into the preceding entry
                if _STACKTRACE_LINE.match(line):
                    if last_entry is not None and stack_count == 0:
                        # First continuation line: append the exception cause
                        last_entry["msg"] += f" | {line.strip()}"
                    stack_count += 1
                    continue

                if stack_count > 1 and last_entry is not None:
                    last_entry["msg"] += f" [+{stack_count - 1} more lines]"
                stack_count = 0

                parsed = _parse_line(line, member)
                if parsed is None:
                    continue
                if _LEVEL_ORDER.get(parsed["level"], 2) < min_level:
                    continue
                if keywords and not any(kw in line.lower() for kw in keywords):
                    continue

                entries.append(parsed)
                last_entry = parsed

            if stack_count > 1 and last_entry is not None:
                last_entry["msg"] += f" [+{stack_count - 1} more lines]"

        except Exception as exc:
            errors[member] = str(exc)

    result: dict = {
        "window": {"start": _fmt_ts(start), "end": _fmt_ts(end)},
        "filters": {"level": level, "keywords": keywords or None, "max_lines": max_lines},
        "returned": len(entries),
        "entries": entries,
    }
    if errors:
        result["errors"] = errors
    return result


async def _get_diagnostic_logs(
    member: str,
    start: float,
    end: float,
    keywords: list[str],
    max_lines: int,
) -> dict:
    if member not in MEMBERS:
        return {"error": f"Unknown member: {member}. Available: {MEMBERS}"}

    client = _docker_client()
    try:
        container = client.containers.get(member)
    except Exception as exc:
        return {"error": str(exc)}

    # Locate diagnostic files — default path is working dir or /opt/hazelcast/
    find_result = container.exec_run(
        "find /opt/hazelcast /data -name 'diagnostics*.log' -type f 2>/dev/null",
        demux=True,
    )
    stdout = (find_result.output[0] or b"").decode("utf-8", errors="replace").strip()
    if not stdout:
        return {
            "available": False,
            "message": "No diagnostic log files found. Enable diagnostics with "
                       "-Dhazelcast.diagnostics.enabled=true in JAVA_OPTS.",
        }

    diag_files = stdout.splitlines()
    start_str = _fmt_ts(start)
    end_str   = _fmt_ts(end)
    collected: list[str] = []
    files_read: list[str] = []

    for path in diag_files[:3]:  # cap at 3 files
        cat_result = container.exec_run(f"cat {path}", demux=True)
        content = (cat_result.output[0] or b"").decode("utf-8", errors="replace")
        files_read.append(path)
        for line in content.splitlines():
            if len(collected) >= max_lines:
                break
            # Rough timestamp filter: diagnostic lines typically start with timestamp
            if start_str[:10] not in line and end_str[:10] not in line:
                # Try to include lines without timestamps (continuation) only if recent context
                pass
            if keywords and not any(kw in line.lower() for kw in keywords):
                continue
            collected.append(line)

    return {
        "available": True,
        "files": files_read,
        "lines_returned": len(collected),
        "content": collected,
    }


def _fmt_ts(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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
        Route("/sse",      endpoint=handle_sse),
        Mount("/messages", app=sse_transport.handle_post_message),
        Route("/health",   endpoint=lambda r: Response("ok")),
    ]
)

if __name__ == "__main__":
    uvicorn.run(starlette_app, host="0.0.0.0", port=PORT)
