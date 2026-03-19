# Hazelcast CP Agent

An AI-powered SRE analysis stack for Hazelcast CP subsystem health monitoring. It combines curated Prometheus queries with LLM-powered interpretation and an agentic follow-up chat, all served through a dark-theme web UI.

---

## What it does

1. **Automated analysis** — Queries ~50 Prometheus metrics (Raft consensus, member resources, CP Maps, sessions, data structures) over a configurable time window, summarizes them, and streams an AI-generated health report.
2. **Agentic chat** — After analysis, a follow-up chat interface lets you ask questions. The agent calls live Prometheus and Hazelcast tools (via MCP) to fetch data on demand.
3. **Multi-LLM** — Supports Claude (Sonnet, Opus, Haiku) and OpenAI (GPT-4o, GPT-4o mini). Requires either `ANTHROPIC_API_KEY` or `OPENAI_API_KEY`.

---

## Architecture

```
Browser
  └── analysis-agent :8000  (FastAPI + web UI)
        ├── prom-mcp-server :8001  (Prometheus MCP tools + REST query API)
        └── hz-mcp-server   :8002  (Hazelcast config + log MCP tools)

analysis-agent ──REST──► prom-mcp-server ──HTTP──► Prometheus       (host.docker.internal:9090)
analysis-agent ──REST──► hz-mcp-server   ──HTTP──► Management Center (host.docker.internal:8080)
LLM (chat)     ──MCP───► prom-mcp-server
LLM (chat)     ──MCP───► hz-mcp-server   ──socket─► Docker daemon   (for log and config file access)
```

All three services share an internal Docker bridge network (`agent-net`). The agent reaches your cluster over `host.docker.internal` — no shared network with the cluster containers is required.

All Prometheus query logic lives in `prom-mcp-server` and is shared between both access paths:
- **Analysis pipeline** calls the MCP server's REST endpoints directly (`/query`, `/query_range`) — no LLM involved.
- **Chat agent** calls the same logic as MCP tools via the SSE transport, invoked by the LLM.

---

## Prerequisites

- Docker + Docker Compose
- A running Hazelcast cluster with Prometheus metrics scraped by a Prometheus instance
- `ANTHROPIC_API_KEY` **or** `OPENAI_API_KEY`

---

## Quickstart

```bash
# 1. Clone the repo
git clone <repo-url>
cd hazelcast-cp-agent

# 2. Copy and fill in the config
cp .env.example .env
# edit .env — set ANTHROPIC_API_KEY or OPENAI_API_KEY at minimum

# 3. Start the stack
docker compose up --build

# 4. Open the UI
open http://localhost:8000
```

Pick a time window, select a model, and click **Analyze**.

---

## Configuration

All configuration is via environment variables. Copy `.env.example` to `.env` and fill in your values — Docker Compose picks it up automatically. All settings except `AGENT_PORT` and API keys can also be changed at runtime from the ⚙ config menu in the UI without restarting.

### Core variables

| Variable | Default | Description |
|---|---|---|
| `ANTHROPIC_API_KEY` | — | Anthropic API key (required unless using OpenAI) |
| `OPENAI_API_KEY` | — | OpenAI API key (required unless using Anthropic) |
| `PROMETHEUS_URL` | `http://host.docker.internal:9090` | Prometheus base URL |
| `MC_URL` | `http://host.docker.internal:8080` | Management Center base URL |
| `MC_CLUSTER` | `dev` | Cluster name as configured in Management Center |
| `HZ_MEMBERS` | `hz1,hz2,hz3,hz4,hz5` | Comma-separated Hazelcast member container/host names |
| `HZ_MEMBER_ADDRS` | — | Optional explicit `host:port` pairs for the Hazelcast REST health check; required when the agent runs outside the cluster network |
| `AGENT_PORT` | `8000` | Host port for the web UI |

### File access backend

Controls how `hz-mcp-server` reads member logs and configuration files during chat. The same backend is used for all file operations — logs, diagnostic files, and `hazelcast.xml`.

| Variable | Default | Description |
|---|---|---|
| `FILE_ACCESS_BACKEND` | `docker` | `docker` — read via Docker socket (requires `/var/run/docker.sock` mount)<br>`files` — read from a mounted directory<br>`none` — disable all file access tools |
| `FILE_DIR` | `/logs` | Base path inside the container when using `files` backend |

#### `docker` backend (default)

Accesses member containers directly via the Docker socket. The socket is bind-mounted read-only into `hz-mcp-server` by default. Supports:
- Streaming container logs
- Reading `hazelcast.xml` / `hazelcast.yaml` from inside the container
- Reading Hazelcast diagnostic log files

#### `files` backend

Use this when member logs and config files are available on a shared volume or directory. Mount the directory into `hz-mcp-server` and organize it as:

```
/logs/
  hz1/
    hz1.log          (or hz1.log at /logs/hz1.log)
    hazelcast.xml    (optional, for config access)
  hz2/
    ...
```

Example override:

```yaml
# docker-compose.override.yml
services:
  mcp-hazelcast:
    environment:
      FILE_ACCESS_BACKEND: files
      FILE_DIR: /data
    volumes:
      - /path/to/member/data:/data:ro
```

#### `none`

Disables all file access tools. Only the Prometheus tools remain available during chat.

### Example: remote cluster

```env
ANTHROPIC_API_KEY=sk-ant-...
PROMETHEUS_URL=http://prom.example.internal:9090
MC_URL=http://mc.example.internal:8080
MC_CLUSTER=production
HZ_MEMBERS=hz-0,hz-1,hz-2
```

---

## Using the UI

### Analysis

1. Choose a **time window** using the preset buttons (15m, 30m, 1h, 3h, 6h, 12h, 24h) or enter custom start/end datetimes.
2. Select a **model** from the dropdown.
3. Optionally add **operator context** — free-text notes about recent changes, known issues, or ongoing incidents. The LLM incorporates these when generating findings.
4. Click **Analyze**. The analysis streams in real time.

The report includes a health summary per CP group and member, findings with severity, recommendations, and analysis confidence.

### Chat

Once analysis completes, a chat input appears. You can ask follow-up questions like:

- *"Which member is holding the most locks?"*
- *"Show me the leader election count for group1 over the last hour"*
- *"Fetch the hazelcast config for hz2"*
- *"Are there any WARN or ERROR entries in hz3 logs?"*

The agent has access to live Prometheus queries and Hazelcast file access tools (see below). Tool calls are surfaced inline in the chat.

### Config menu (⚙)

Click the gear icon in the header to configure endpoints at runtime:

- **Prometheus URL** / **MC URL** / **MC Cluster** — backend service addresses
- **HZ Member Addrs** — explicit `host:port` pairs for the Hazelcast REST health check
- **HZ Members** — member names for log/config access
- **File access backend** — switch between `docker`, `files`, or `none`
- **File Directory** — shown when using `files` backend

The header shows five status dots (Prometheus, MC, Prom MCP, HZ MCP, Files). Hover for a status message. Settings are persisted in `localStorage`, scoped per MC URL so different clusters don't interfere in the same browser.

---

## Services

### analysis-agent (`:8000`)

FastAPI application. Serves the web UI and two streaming SSE endpoints:

- `GET /api/analyse` — runs the full analysis pipeline
- `POST /api/chat` — agentic follow-up with MCP tool use

### prom-mcp-server (`:8001`)

Prometheus query server. Exposes the same query logic over two transports:

**MCP tools** (used by the LLM during chat):

| Tool | Description |
|---|---|
| `prometheus_query` | Instant PromQL query |
| `prometheus_query_range` | Range PromQL query |
| `prometheus_list_metrics` | List available metrics (optional prefix filter) |

**REST endpoints** (used by the analysis pipeline directly):

| Endpoint | Description |
|---|---|
| `GET /query?query=&time=&url=` | Instant PromQL query |
| `GET /query_range?query=&start=&end=&step=&url=` | Range PromQL query |
| `GET /metrics?prefix=&url=` | List available metrics |
| `POST /config` | Update runtime `prometheus_url` |

The optional `url=` parameter overrides the server's configured Prometheus URL for that request, allowing the UI to target a different Prometheus instance without restarting.

### hz-mcp-server (`:8002`)

MCP server exposing Hazelcast tools. All tools require `FILE_ACCESS_BACKEND != none`.

| Tool | Description |
|---|---|
| `hz_get_member_config` | Read `hazelcast.xml` / `hazelcast.yaml` from a member and return it as JSON |
| `hz_log_summary` | WARN/ERROR line counts per member for a time window (~50 tokens) |
| `hz_get_logs` | Filtered structured log entries with stack traces folded |
| `hz_get_diagnostic_logs` | Keyword-filtered diagnostic log snippets |

---

## Cluster requirements

The agent is read-only and makes no changes to the cluster. It requires:

- **Prometheus** — scraping Hazelcast JVM metrics (the `hz_*` and `mc_*` metric families)
- **Management Center** — for health checks and cluster context (no auth required for these endpoints)
- **Docker socket** or **mounted file directory** — for log and config access (configurable via `FILE_ACCESS_BACKEND`)

---

## Companion monitoring stack

For a ready-made Hazelcast cluster with Prometheus, Grafana, and Management Center, see [hazelcast-cp-monitoring](https://github.com/fcannizzohz/hazelcast-cp-monitoring). That stack exposes Management Center on `:8080`, Prometheus on `:9090`, and Grafana on `:3000` — matching the agent defaults out of the box.
