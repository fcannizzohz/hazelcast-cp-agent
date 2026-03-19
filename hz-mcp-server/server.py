"""
Hazelcast MCP server — HTTP/SSE transport.

Tools (ordered by token cost, cheapest first):

  hz_get_member_config    — CP config read directly from the member (container or file)
  hz_log_summary          — per-member WARN/ERROR counts for a time window
                            (~50 tokens; use this first to identify affected members)
  hz_get_logs             — structured, filtered log entries from one or more members
                            (bounded by max_lines; stack traces folded to save tokens)
  hz_get_diagnostic_logs  — keyword-filtered diagnostic log snippets

All tools require FILE_ACCESS_BACKEND != "none".

File access backend is selected via FILE_ACCESS_BACKEND:
  docker  (default) — reads logs and files via the Docker daemon socket
                      (/var/run/docker.sock); the socket must be mounted read-only.
  files             — reads from FILE_DIR/{member}/ on the local filesystem;
                      mount your log/config directory at FILE_DIR.
  none              — file access disabled; no tools are exposed.

Environment:
  HZ_MEMBERS             comma-separated member names  default: hz1,hz2,hz3,hz4,hz5
  MC_URL                 Management Center base URL    default: http://host.docker.internal:8080
  MC_CLUSTER             Hazelcast cluster name        default: dev
  PORT                   HTTP port                     default: 8002
  FILE_ACCESS_BACKEND    docker | files | none         default: docker
  FILE_DIR               base directory (files backend) default: /logs
"""

from __future__ import annotations

import glob
import io
import json
import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import uvicorn
from dotenv import load_dotenv
from mcp.server import Server
from mcp.server.sse import SseServerTransport
from mcp.types import TextContent, Tool
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Mount, Route

load_dotenv()

PORT                 = int(os.environ.get("PORT", "8002"))
MEMBERS              = [m.strip() for m in os.environ.get("HZ_MEMBERS", "hz1,hz2,hz3,hz4,hz5").split(",")]
MC_URL               = os.environ.get("MC_URL",               "http://host.docker.internal:8080").rstrip("/")
MC_CLUSTER           = os.environ.get("MC_CLUSTER",           "dev")
FILE_ACCESS_BACKEND  = os.environ.get("FILE_ACCESS_BACKEND",  "docker").lower()   # docker | files | none
FILE_DIR             = os.environ.get("FILE_DIR",             "/logs")

# Hazelcast log4j2 line pattern:  "HH:mm:ss.SSS [thread] LEVEL  logger - message"
_LEVEL_RE   = re.compile(r"\]\s+(TRACE|DEBUG|INFO|WARN|ERROR|FATAL)\s+")
_LOGGER_RE  = re.compile(r"\]\s+(?:TRACE|DEBUG|INFO|WARN|ERROR|FATAL)\s+(\S+)\s+-\s+(.*)")
# Docker prepends RFC3339Nano timestamp when timestamps=True
_DOCKER_TS  = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.\d+Z\s+(.*)$")

# Lines that are part of Java stack traces (not separate log events)
_STACKTRACE_LINE = re.compile(r"^\s+at |^Caused by:|^\s+\.\.\. \d+ more")

_LEVEL_ORDER = {"TRACE": 0, "DEBUG": 1, "INFO": 2, "WARN": 3, "ERROR": 4, "FATAL": 5}

app = Server("hz-mcp")


# ---------------------------------------------------------------------------
# File access abstraction
# ---------------------------------------------------------------------------

def _member_name(member: str) -> str:
    """Strip port suffix: 'hz1:5701' → 'hz1'."""
    return member.split(":")[0]


def member_find_files(
    member: str,
    filenames: list[str],
    docker_search_dirs: list[str] | None = None,
) -> list[str]:
    """
    Locate files on/in a member by filename pattern.

    For docker: runs `find` inside the container; docker_search_dirs specifies
    where to look (defaults to common Hazelcast paths).
    For files:  globs under FILE_DIR/{member}/ recursively.

    Returns a list of absolute paths (inside container for docker, on host for files).
    """
    name = _member_name(member)

    if FILE_ACCESS_BACKEND == "docker":
        dirs = " ".join(docker_search_dirs or ["/opt/hazelcast", "/data", "/opt/hazelcast/config"])
        name_expr = " -o ".join(f"-name '{f}'" for f in filenames)
        cmd = f"find {dirs} \\( {name_expr} \\) -type f 2>/dev/null"
        import docker as _docker
        print(f"[member_find_files] container={name!r} cmd={cmd!r}", flush=True)
        container = _docker.from_env().containers.get(name)
        result = container.exec_run(cmd, demux=True)
        stdout = (result.output[0] or b"").decode("utf-8", errors="replace").strip()
        print(f"[member_find_files] exit_code={result.exit_code} stdout={stdout!r}", flush=True)
        return stdout.splitlines() if stdout else []

    if FILE_ACCESS_BACKEND == "files":
        found: list[str] = []
        base = os.path.join(FILE_DIR, name)
        for fn in filenames:
            found.extend(glob.glob(os.path.join(base, "**", fn), recursive=True))
            found.extend(glob.glob(os.path.join(FILE_DIR, fn.replace("*", f"{name}*"))))
        # deduplicate while preserving order
        seen: set[str] = set()
        return [p for p in found if not (p in seen or seen.add(p))]  # type: ignore[func-returns-value]

    return []


def member_read_file(member: str, path: str) -> bytes:
    """
    Read a file from/on a member.
    For docker: path is inside the container (absolute).
    For files:  path is a local filesystem path returned by member_find_files.
    """
    name = _member_name(member)

    if FILE_ACCESS_BACKEND == "docker":
        import docker as _docker
        print(f"[member_read_file] container={name!r} path={path!r}", flush=True)
        container = _docker.from_env().containers.get(name)
        result = container.exec_run(f"cat {path}", demux=True)
        nbytes = len(result.output[0] or b"")
        print(f"[member_read_file] exit_code={result.exit_code} bytes={nbytes}", flush=True)
        return result.output[0] or b""

    if FILE_ACCESS_BACKEND == "files":
        with open(path, "rb") as f:
            return f.read()

    return b""


def member_read_logs(member: str, since: int, until: int) -> bytes:
    """Read timestamped log bytes from a member for the given time window."""
    name = _member_name(member)

    if FILE_ACCESS_BACKEND == "docker":
        import docker as _docker
        container = _docker.from_env().containers.get(name)
        return container.logs(since=since, until=until, timestamps=True, stream=False)

    if FILE_ACCESS_BACKEND == "files":
        return _read_logs_files(name, since, until)

    return b""


def _read_logs_files(member: str, since: int, until: int) -> bytes:
    """
    Read log content from files under FILE_DIR.

    Searched paths (first match wins, all matching files are merged):
      {FILE_DIR}/{member}.log
      {FILE_DIR}/{member}/*.log
      {FILE_DIR}/{member}*.log

    Lines with Docker-format timestamps (YYYY-MM-DDTHH:MM:SS) are filtered to
    the [since, until] window.  Lines without timestamps are included as-is.
    """
    patterns = [
        os.path.join(FILE_DIR, f"{member}.log"),
        os.path.join(FILE_DIR, member, "*.log"),
        os.path.join(FILE_DIR, f"{member}*.log"),
    ]
    found: list[str] = []
    for pattern in patterns:
        found.extend(glob.glob(pattern))
    seen: set[str] = set()
    paths = [p for p in found if not (p in seen or seen.add(p))]  # type: ignore[func-returns-value]

    lines: list[bytes] = []
    for path in paths:
        try:
            with open(path, "rb") as f:
                for raw in f:
                    text = raw.decode("utf-8", errors="replace")
                    m = _DOCKER_TS.match(text)
                    if m:
                        try:
                            ts = datetime.fromisoformat(m.group(1)).replace(tzinfo=timezone.utc)
                            if not (since <= ts.timestamp() <= until):
                                continue
                        except Exception:
                            pass
                    lines.append(raw)
        except Exception:
            pass
    return b"".join(lines)


# ---------------------------------------------------------------------------
# XML helper
# ---------------------------------------------------------------------------

def _xml_to_dict(element: ET.Element) -> dict | str | None:
    """Recursively convert an XML element to a plain Python dict / str."""
    tag = element.tag.split("}", 1)[1] if "}" in element.tag else element.tag
    attrib = dict(element.attrib)
    children = list(element)
    text = (element.text or "").strip()

    if not children:
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


# ---------------------------------------------------------------------------
# Tool definitions
# ---------------------------------------------------------------------------

@app.list_tools()
async def list_tools() -> list[Tool]:
    member_list = ", ".join(MEMBERS)

    if FILE_ACCESS_BACKEND == "none":
        return []

    backend_note = f"({FILE_ACCESS_BACKEND} backend)"

    return [
        Tool(
            name="hz_get_member_config",
            description=(
                "Read the Hazelcast configuration file directly from a member "
                f"{backend_note} and return it as JSON. "
                "Use this to verify CP subsystem settings: session TTL, "
                "group size, missing-member auto-removal timeout, CPMap size limits, etc."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "member": {
                        "type": "string",
                        "description": (
                            f"Member to query (e.g. 'hz1'). "
                            f"Available: {member_list}"
                        ),
                    },
                },
                "required": ["member"],
            },
        ),
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
            name="hz_get_diagnostic_logs",
            description=(
                "Fetch Hazelcast diagnostic log snippets from a member "
                f"{backend_note}. "
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
    print(f"[tool:{name}] called  args={json.dumps(arguments)}", flush=True)
    try:
        if name == "hz_get_member_config":
            result = await _get_member_config(arguments["member"])
        elif name == "hz_log_summary":
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
        elif name == "hz_get_diagnostic_logs":
            result = await _get_diagnostic_logs(
                member   = arguments["member"],
                keywords = [k.lower() for k in (arguments.get("keywords") or [])],
                max_lines= int(arguments.get("max_lines") or 100),
            )
        else:
            result = {"error": f"Unknown tool: {name}"}
    except Exception as exc:
        result = {"error": str(exc)}

    has_error = "error" in result
    summary = result.get("error") or result.get("source") or list(result.keys())
    print(f"[tool:{name}] {'ERROR' if has_error else 'ok'}  result={summary}", flush=True)
    return [TextContent(type="text", text=json.dumps(result, indent=2))]


# ---------------------------------------------------------------------------
# Tool implementations
# ---------------------------------------------------------------------------

_CONFIG_CANDIDATES = [
    "/opt/hazelcast/config/hazelcast.xml",
    "/opt/hazelcast/config/hazelcast.yaml",
    "/opt/hazelcast/config/hazelcast.yml",
    "/opt/hazelcast/hazelcast.xml",
    "/data/hazelcast.xml",
]


async def _get_member_config(member: str) -> dict:
    """
    Read hazelcast.xml (or hazelcast.yaml) directly from the member using
    the configured FILE_ACCESS_BACKEND.
    Tries well-known paths first, then falls back to find.
    """
    name = _member_name(member)
    print(f"[member_config] member={name!r} backend={FILE_ACCESS_BACKEND} known_members={MEMBERS}", flush=True)
    if name not in MEMBERS:
        return {"error": f"Unknown member: {name}. Available: {MEMBERS}"}

    # Try well-known paths before falling back to find
    config_path: str | None = None
    raw_bytes: bytes = b""
    for candidate in _CONFIG_CANDIDATES:
        try:
            data = member_read_file(name, candidate)
            if data:
                config_path = candidate
                raw_bytes = data
                print(f"[member_config] found at {candidate!r} ({len(data)} bytes)", flush=True)
                break
            else:
                print(f"[member_config] {candidate!r} → empty (file missing or unreadable)", flush=True)
        except Exception as exc:
            print(f"[member_config] {candidate!r} → exception: {exc}", flush=True)

    if config_path is None:
        print(f"[member_config] candidates exhausted, falling back to find", flush=True)
        try:
            paths = member_find_files(name, ["hazelcast.xml", "hazelcast.yaml", "hazelcast.yml"])
            print(f"[member_config] find result: {paths}", flush=True)
        except Exception as exc:
            print(f"[member_config] find error: {exc}", flush=True)
            return {"error": f"Could not locate config file: {exc}"}
        if not paths:
            return {
                "error": "No hazelcast.xml / hazelcast.yaml found. "
                         "Check the container/file layout or mount a config volume.",
            }
        config_path = paths[0]
        try:
            raw_bytes = member_read_file(name, config_path)
            print(f"[member_config] read {len(raw_bytes)} bytes from find result", flush=True)
        except Exception as exc:
            print(f"[member_config] read error: {exc}", flush=True)
            return {"error": f"Could not read {config_path}: {exc}"}

    content = raw_bytes.decode("utf-8", errors="replace")

    if config_path.endswith(".xml"):
        try:
            root = ET.parse(io.BytesIO(content.encode("utf-8"))).getroot()
            result = {
                "member": name,
                "source": config_path,
                "config": _xml_to_dict(root),
            }
            cp_present = "cp-subsystem" in (result["config"] or {})
            print(f"[member_config] parsed XML ok  cp-subsystem_present={cp_present}", flush=True)
            return result
        except ET.ParseError as exc:
            print(f"[member_config] XML parse error: {exc}", flush=True)
            return {"member": name, "source": config_path,
                    "error": f"XML parse error: {exc}", "raw": content[:2000]}

    # YAML — return as raw text; LLM can read it directly
    print(f"[member_config] returning YAML ({len(content)} chars)", flush=True)
    return {"member": name, "source": config_path, "config_yaml": content}


async def _log_summary(start: float, end: float) -> dict:
    since, until = int(start), int(end)
    summary: dict = {}
    errors: dict = {}
    for member in MEMBERS:
        try:
            raw = member_read_logs(member, since, until)
            lines = raw.split(b"\n")
            warn  = sum(1 for l in lines if b"] WARN" in l or b"] WARNING" in l)
            error = sum(1 for l in lines if b"] ERROR" in l)
            total = len([l for l in lines if l.strip()])
            summary[member] = {"total_lines": total, "WARN": warn, "ERROR": error}
        except Exception as exc:
            errors[member] = str(exc)

    result: dict = {"backend": FILE_ACCESS_BACKEND, "window": {"start": _fmt_ts(start), "end": _fmt_ts(end)}, "members": summary}
    if errors:
        result["errors"] = errors
    return result


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
            raw = member_read_logs(member, since, until)
            lines = raw.decode("utf-8", errors="replace").splitlines()
            last_entry: dict | None = None
            stack_count = 0

            for line in lines:
                if len(entries) >= max_lines:
                    break

                if _STACKTRACE_LINE.match(line):
                    if last_entry is not None and stack_count == 0:
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
        "backend": FILE_ACCESS_BACKEND,
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
    keywords: list[str],
    max_lines: int,
) -> dict:
    name = _member_name(member)
    if name not in MEMBERS:
        return {"error": f"Unknown member: {name}. Available: {MEMBERS}"}

    try:
        diag_files = member_find_files(name, ["diagnostics*.log"])
    except Exception as exc:
        return {"error": str(exc)}

    if not diag_files:
        return {
            "available": False,
            "message": "No diagnostic log files found. Enable diagnostics with "
                       "-Dhazelcast.diagnostics.enabled=true in JAVA_OPTS.",
        }

    collected: list[str] = []
    files_read: list[str] = []

    for path in diag_files[:3]:
        try:
            content = member_read_file(name, path).decode("utf-8", errors="replace")
        except Exception:
            continue
        files_read.append(path)
        for line in content.splitlines():
            if len(collected) >= max_lines:
                break
            if keywords and not any(kw in line.lower() for kw in keywords):
                continue
            collected.append(line)

    return {
        "available": True,
        "files": files_read,
        "lines_returned": len(collected),
        "content": collected,
    }


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

    lm = _LEVEL_RE.search(content)
    level = lm.group(1) if lm else "INFO"

    lm2 = _LOGGER_RE.search(content)
    if lm2:
        logger  = lm2.group(1)
        parts   = logger.split(".")
        short   = ".".join(p[0] for p in parts[:-1]) + "." + parts[-1] if len(parts) > 1 else logger
        message = lm2.group(2)
    else:
        short   = ""
        message = content

    return {"ts": ts, "member": member, "level": level, "logger": short, "msg": message}


def _fmt_ts(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Backend health check
# ---------------------------------------------------------------------------

def _check_file_access_backend() -> dict:
    """
    Verify the configured file access backend is actually accessible.
    Returns a dict with ok, message, members_found, members_missing.
    """
    if FILE_ACCESS_BACKEND == "none":
        return {
            "ok": False,
            "message": "File access disabled (FILE_ACCESS_BACKEND=none)",
            "members_found": [],
            "members_missing": [],
        }

    if FILE_ACCESS_BACKEND == "docker":
        try:
            import docker as _docker
            client = _docker.from_env()
            found, missing = [], []
            for m in MEMBERS:
                try:
                    client.containers.get(m)
                    found.append(m)
                except Exception:
                    missing.append(m)
            msg = f"{len(found)}/{len(MEMBERS)} containers accessible"
            if missing:
                msg += f"; missing: {', '.join(missing)}"
            return {"ok": len(found) > 0, "message": msg,
                    "members_found": found, "members_missing": missing}
        except Exception as exc:
            return {"ok": False, "message": f"Docker socket unavailable: {exc}",
                    "members_found": [], "members_missing": MEMBERS}

    if FILE_ACCESS_BACKEND == "files":
        if not os.path.isdir(FILE_DIR):
            return {"ok": False, "message": f"File directory not found: {FILE_DIR}",
                    "members_found": [], "members_missing": MEMBERS}
        found, missing = [], []
        for m in MEMBERS:
            patterns = [
                os.path.join(FILE_DIR, f"{m}.log"),
                os.path.join(FILE_DIR, m, "*.log"),
                os.path.join(FILE_DIR, f"{m}*.log"),
            ]
            (found if any(glob.glob(p) for p in patterns) else missing).append(m)
        msg = f"{len(found)}/{len(MEMBERS)} members accessible"
        if missing:
            msg += f"; missing: {', '.join(missing)}"
        return {"ok": len(found) > 0, "message": msg,
                "members_found": found, "members_missing": missing}

    return {"ok": False, "message": f"Unknown backend: {FILE_ACCESS_BACKEND}",
            "members_found": [], "members_missing": MEMBERS}


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


async def handle_config_update(request: Request):
    global MC_URL, MC_CLUSTER, MEMBERS, FILE_ACCESS_BACKEND, FILE_DIR
    body = await request.json()
    if "mc_url"               in body and body["mc_url"]:
        MC_URL               = body["mc_url"].rstrip("/")
    if "mc_cluster"           in body and body["mc_cluster"]:
        MC_CLUSTER           = body["mc_cluster"]
    if "members"              in body and body["members"]:
        MEMBERS              = [m.strip() for m in body["members"].split(",") if m.strip()]
    if "file_access_backend"  in body and body["file_access_backend"]:
        FILE_ACCESS_BACKEND  = body["file_access_backend"].lower()
    if "file_dir"             in body and body["file_dir"]:
        FILE_DIR             = body["file_dir"]
    return JSONResponse({"ok": True})


async def handle_status(_: Request):
    backend_status = _check_file_access_backend()
    return JSONResponse({
        "file_access_backend": FILE_ACCESS_BACKEND,
        "members":             MEMBERS,
        "file_dir":            FILE_DIR if FILE_ACCESS_BACKEND == "files" else None,
        **backend_status,
    })


async def handle_member_config(request: Request):
    """REST shortcut so analysis-agent can populate cp_subsystem_config without MCP."""
    member = request.path_params["member"]
    print(f"[REST /member-config] member={member!r}", flush=True)
    result = await _get_member_config(member)
    has_error = "error" in result
    print(f"[REST /member-config] {'ERROR: ' + result['error'] if has_error else 'ok  source=' + str(result.get('source'))}", flush=True)
    return JSONResponse(result)


starlette_app = Starlette(
    routes=[
        Route("/sse",                       endpoint=handle_sse),
        Mount("/messages",                  app=sse_transport.handle_post_message),
        Route("/health",                    endpoint=lambda _: Response("ok")),
        Route("/status",                    endpoint=handle_status),
        Route("/config",                    endpoint=handle_config_update, methods=["POST"]),
        Route("/member-config/{member}",    endpoint=handle_member_config),
    ]
)

if __name__ == "__main__":
    uvicorn.run(starlette_app, host="0.0.0.0", port=PORT)
