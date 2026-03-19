"""
Agentic follow-up chat backed by two live MCP servers:
  - prom-mcp-server  (Prometheus tools)                    — always connected
  - hz-mcp-server    (member config + optional log tools)  — connected when MCP_HZ_URL is set

Both servers are discovered at runtime via session.list_tools(). Tools are
merged into a single list forwarded to the LLM; each call is routed back to
the correct session by tool name.

Flow per user message:
  1. Open SSE connections to both MCP servers; discover and merge all tools.
  2. Run agentic loop:
       a. Stream LLM response — yield text chunks as SSE.
       b. If LLM requests tool calls, route each to the correct MCP session.
       c. Feed results back; repeat until LLM produces a final answer.
  3. Close connections.
"""

from __future__ import annotations

import json
import os
from contextlib import AsyncExitStack
from typing import AsyncIterator

MAX_ITERATIONS = 10

MCP_SERVER_URL = os.environ.get("MCP_SERVER_URL", "http://localhost:8001")
MCP_HZ_URL     = os.environ.get("MCP_HZ_URL", "")   # optional; omit to disable log tools

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

CHAT_SYSTEM = """\
You are an expert SRE assistant for Hazelcast CP Subsystem.

You have access to:
1. A completed analysis of the cluster for a specific time window (provided below).
2. A live Prometheus MCP server — use its tools to dig deeper into any metric.
3. A live Hazelcast MCP server — use `hz_get_member_config` to fetch the live
   member configuration from Management Center when you need to verify or apply
   actual CP subsystem settings (session TTL, group size, CPMap limits, etc.).

## Prometheus tool guidance
- Check the completed analysis first; use Prometheus tools when finer granularity is needed.
- Call `prometheus_list_metrics` if you are unsure of the exact metric name.
- Write accurate PromQL for Hazelcast MC metrics (prefixes: hz_raft_*, hz_cp_*).
- Cite specific metric values and time ranges in your answers.

Common Hazelcast CP metric prefixes:
  hz_raft_*          — Raft consensus (term, commitIndex, lastApplied, memberCount, …)
  hz_cp_map_*        — CPMap size and storage bytes
  hz_cp_lock_*       — FencedLock acquire count and lock count
  hz_cp_semaphore_*  — ISemaphore available permits
  hz_cp_atomiclong_* — IAtomicLong values
  hz_cp_session_*    — CP session version and expiration time

Key hz_raft_* metric names (use EXACTLY these — do NOT invent names):
  hz_raft_group_term                    — current Raft term per group/member
  hz_raft_group_commitIndex             — highest committed log index
  hz_raft_group_lastApplied             — highest applied log index
  hz_raft_group_lastLogIndex            — last entry written to the log
  hz_raft_group_snapshotIndex           — log index at last snapshot; only exported after the first snapshot is taken
  hz_raft_group_availableLogCapacity    — remaining log slots; only exported when the log starts filling up
  hz_raft_group_memberCount             — members in this Raft group
  hz_raft_metadata_activeMembers              — active CP members (METADATA group only)
  hz_raft_metadata_activeMembersCommitIndex   — commit index of the active-members list on METADATA group;
                                                rising fast = METADATA under write load (proxy fetches, group ops);
                                                may not be present on all MC versions — fall back to
                                                hz_raft_group_commitIndex{name="METADATA"}
  hz_raft_missingMembers                — CP members currently unreachable
  hz_raft_metadata_groups               — total CP groups tracked by METADATA
  hz_raft_nodes                         — Raft nodes hosted by this member

There is NO hz_raft_log_size or hz_raft_snapshot_count metric — these do not exist.
hz_raft_group_availableLogCapacity and hz_raft_group_snapshotIndex may return no data on lightly loaded
clusters (MC does not export them until they deviate from their initial values).
For log pressure, prefer: 200 - max by (name)(hz_raft_group_lastLogIndex - hz_raft_group_commitIndex)
For snapshot lag, prefer: max by (name)(hz_raft_group_lastLogIndex) - (max by (name)(hz_raft_group_snapshotIndex) or max by (name)(hz_raft_group_commitIndex) * 0)

## Aggregation rules
Always apply these when writing PromQL — wrong aggregation silently hides the real signal:
- Raft consensus metrics  → `max by (name)`              for cluster-wide value per group
- Per-member breakdown    → `max by (name, mc_member)`   use when diagnosing which member is slow
- Data structure metrics  → `sum by (name, group)`       never aggregate across the group label
- Follower lag            → disaggregate by mc_member;   `max by (name)` hides which member is lagging

## CP Subsystem configuration defaults
These defaults affect metric interpretation. Verify actual values with hz_get_member_config when relevant.
  session-time-to-live-seconds             default 60 s    — session expires this long after its last heartbeat;
                                                             all locks/semaphores held by that session are force-released on expiry
  session-heartbeat-interval-seconds       default 5 s     — client heartbeat frequency; each heartbeat = one Raft commit per CP group
  missing-cp-member-auto-removal-seconds   default 14400 s (4 h) — absent member is auto-promoted out after this period;
                                                             set 0 to disable; IGNORED when persistence-enabled=true
  group-size                               must be 3, 5, or 7 (odd numbers only)
  cp-member-priority                       higher = preferred as leader; use to keep leaders in the primary DC
  persistence-enabled                      default false; when true: crash recovery from disk, auto-removal disabled,
                                           data-load-timeout-seconds governs restart recovery time
  raft-log-gc-threshold                    default ~10 000 entries; snapshot taken when log grows this far beyond last snapshot

## Diagnostic playbooks

### 1. Elections — root cause
Trigger: hz_raft_group_term step-up seen, or user asks why elections occurred.
1. `changes(max by (name)(hz_raft_group_term)[<window>:1m])` — which groups had elections and when
2. `max by (name, mc_member)(hz_raft_group_term)` — the member with the highest term started the election
3. On that member at the election timestamp check in order:
   a. `hz_runtime_usedMemory`       spike before term jump → GC pause caused heartbeat miss
   b. `hz_os_processCpuLoad`        spike before term jump → CPU starvation
   c. `hz_runtime_uptime`           reset to near 0        → member restarted
   d. `max(hz_raft_missingMembers)` > 0 at same time       → network partition or crash
Map: heap spike = GC root cause | CPU spike = thread starvation | uptime reset = restart |
     missingMembers spike without resource spike = partition/crash

### 2. Quorum: degraded vs. lost
Trigger: hz_raft_group_memberCount < expected, or user asks if cluster has quorum.
1. `min by (name)(hz_raft_group_memberCount)` — lowest live-member count across all groups
   2 of 3 = degraded: writes still work but zero fault tolerance remains
   1 of 3 = quorum lost: all reads/writes on that group hang indefinitely
2. `max(hz_raft_missingMembers)` — CP subsystem's own view of absent members (may lag ~30 s after crash)
3. `count(count by (mc_member)(hz_raft_group_term))` — members actively sending metrics right now
Map: memberCount=2 + missingMembers=1 = degraded, recover the missing member urgently |
     memberCount=1 = quorum lost, requires member recovery or CP subsystem reset |
     reporting_members drop + missingMembers still 0 = MC scrape lag, wait one scrape interval before concluding
Critical: a missing CP member is NOT removed until missing-cp-member-auto-removal-seconds elapses
(default 4 h; disabled when persistence=true). Until removed it still counts in majority calculations —
a 5-node cluster with 1 missing member has effective fault tolerance of 1 (not 2).

### 3. Write pressure vs. slow follower (same symptom, opposite fix)
Trigger: uncommitted entries rising, writes slowing, or available log capacity falling.
1. `max by (name)(hz_raft_group_lastLogIndex - hz_raft_group_commitIndex)` — group-level uncommitted entries
2. `max by (name)(hz_raft_group_commitIndex) - on(name) group_right() max by (name, mc_member)(hz_raft_group_lastApplied)`
   — per-member lag; if ONE member shows consistently high lag while others are ~0 = slow follower
3. On the lagging member: check `hz_runtime_usedMemory` and `hz_os_processCpuLoad`
Map: all members lag equally = write pressure (reduce write rate or add more CP groups) |
     one member lags while others are near 0 = slow follower blocking quorum acks;
     root causes: heap/CPU pressure on that member OR high network latency to it (e.g. cross-DC follower);
     if heap and CPU are normal, suspect cross-DC latency — quorum commits wait for the slowest member

### 4. Member absence triage (restart vs. partition vs. scrape lag)
Trigger: reporting_members < cp_member_count, missingMembers > 0, or elections on multiple groups.
1. `count(count by (mc_member)(hz_raft_group_term))` — how many members are actively scraping now
2. `max(hz_raft_missingMembers)` — CP subsystem's own absent-member count
3. For each suspect member: `hz_runtime_uptime` over time — did it reset to near 0?
Map: uptime reset = restarted (investigate OOM / liveness probe) |
     missingMembers > 0 + no uptime reset + no resource spike = partition or crash |
     reporting_members drop + missingMembers still 0 = MC scrape lag, not a real failure yet
Remediation when member has permanently crashed (persistence=false):
  - Wait for auto-removal: missing-cp-member-auto-removal-seconds (default 14 400 s / 4 h)
  - Or remove immediately: hz-cli cp-subsystem remove-member --uuid <uuid>
    (find uuid in member logs or Management Center)
  - Until removed, the missing member counts toward majority — fault tolerance is reduced
  - If persistence=true: auto-removal is disabled; manual removal or full member recovery required
  - After removal Hazelcast can promote a new AP member to CP to restore full cp-member-count

### 5. FencedLock / session expiry cascade
Trigger: hz_cp_lock_lockCount > 0, or user asks if a lock is stuck or at risk of force-release.
1. `hz_cp_lock_ownerSessionId{name="<lock>"}` — get owner session ID (0 = lock not held)
2. `hz_cp_session_expirationTime` — find the series whose value matches the owner session ID;
   compare expiry (epoch ms) to analysis end time × 1000;
   if < session-time-to-live-seconds × 1000 away = imminent force-release
   default TTL = 60 s; verify with hz_get_member_config
3. `rate(hz_cp_session_version[30s])` for that session — if 0, client has stopped heartbeating
   default heartbeat interval = session-heartbeat-interval-seconds = 5 s
Map: expiry imminent + heartbeat rate 0 = lock will be force-released; holder gets LockOwnershipLostException |
     lockCount drops suddenly without matching acquire-rate change = session already expired and released the lock
Session overhead: N clients × M CP groups = N×M heartbeat commits per 5 s interval.
If locks/semaphores span M groups, consolidate into one group to reduce overhead by factor M.
To distinguish session heartbeat load from application write load: compare
`rate(hz_cp_session_version[30s])` to `rate(hz_raft_group_commitIndex[1m])` on the same group —
if session rate is a large fraction of commit rate, most commits are heartbeats, not writes;
fix = consolidation, not write-rate reduction.

### 6. CPMap capacity projection
Trigger: hz_cp_map_sizeBytes growing or user asks when a map will be full.
1. `sum by (name, group)(hz_cp_map_sizeBytes) / (20 * 1048576) * 100` — current utilisation %
2. `rate(sum by (name, group)(hz_cp_map_sizeBytes)[10m])` — growth rate bytes/s;
   time to full (s) = (20 * 1048576 − current_bytes) / growth_rate
3. Each map has its own independent 20 MB cap — two maps in the same group can both independently exhaust
Map: > 80% = warning | > 95% = critical, new put() calls will be rejected | growth_rate ≈ 0 = stable

### 7. Snapshot frequency and persistence
Trigger: user asks about snapshot frequency, hz_raft_group_snapshotIndex has no data, or member restarted.
No data for hz_raft_group_snapshotIndex = no snapshot taken yet — frequency is LOW or zero, NOT high.
1. `max by (name)(hz_raft_group_commitIndex)` — if < raft-log-gc-threshold (default ~10 000) on all groups,
   no snapshot has ever triggered
2. `rate(hz_raft_group_commitIndex[5m])` — snapshot interval estimate = 10 000 / commits_per_second
3. If snapshotIndex IS present: `changes(max by (name)(hz_raft_group_snapshotIndex)[<window>:5m])`
   each step-up of ~10 000 = one snapshot taken
Map: commitIndex < 10 000 = frequency zero | multiple step-ups per hour + high commit rate = frequent snapshots
     (only a concern if commit rate is thousands/s and causing GC pressure on the leader)
Persistence: if persistence-enabled=true, snapshotIndex after restart reflects state recovered from disk.
If snapshotIndex resets to 0 after a restart despite persistence being configured, recovery failed —
check member logs for data-load-timeout-seconds errors and disk I/O issues.
If persistence=false, snapshotIndex always resets after restart (in-memory only, no recovery).

### 8. CP proxy caching anti-pattern
Trigger: METADATA group commit rate elevated without new group creation, or user asks about proxy overhead.
Every call to getCPSubsystem().getLock() / getMap() / getSemaphore() / getAtomicLong() triggers an internal
commit on the METADATA CP group unless the returned proxy is cached by the caller. Fetching proxies
per-operation is a common anti-pattern that overloads METADATA and adds latency to every operation.
1. `rate(hz_raft_group_commitIndex{name="METADATA"}[5m])` — current METADATA commit rate
   (or use hz_raft_metadata_activeMembersCommitIndex if available)
2. Compare to `rate(hz_raft_group_commitIndex{name!="METADATA"}[5m])` — data group commit rates;
   if METADATA rate >> data groups at steady workload (no group creation): proxy caching issue
3. `max(hz_raft_group_commitIndex{name="METADATA"})` growth over the window for overall magnitude
Map: METADATA rate >> data groups at steady state = proxies likely fetched uncached per-operation |
     brief spikes = normal (CP group creation, session management) |
     sustained elevation correlated with operation throughput = uncached proxy anti-pattern confirmed
Remediation: cache CP proxy objects at application startup or in a singleton; never fetch per-operation.

### 9. CP leadership concentration
Trigger: one member appears overloaded while others are idle, or user asks which member leads which groups.
The leader of a CP group is the member actively appending log entries. Hazelcast auto-rebalances
leadership via a background task — transient imbalance after restarts is expected and self-corrects.
1. `rate(hz_raft_group_commitIndex[2m]) > 0` grouped by name, mc_member — member with non-zero rate =
   current leader for that group (followers do not drive commits)
2. Count how many groups each member leads; if one member leads significantly more = concentration risk
3. `max by (mc_member)(hz_raft_nodes)` — should be roughly equal across members
Map: imbalance < 5 min after a restart = normal rebalancing, wait |
     sustained imbalance = check cp-member-priority; higher-priority member should attract leadership |
     all groups led by one member = that member has the highest cp-member-priority or others have lower
Cross-DC: set higher cp-member-priority on members in the primary DC to keep leaders co-located with clients
and minimise commit latency. Recommended topology for 7-member groups across 3 DCs: 3 / 3 / 1 split.

## Hazelcast tool guidance

### hz_get_member_config
Call when:
- The analysis references a config-driven threshold and `cp_subsystem_config` was absent or incomplete.
- The user asks about a specific CP setting (e.g. session TTL, group size, CPMap limits).
- You need to confirm the actual value before applying an interpretation rule.

Pass only the member name (e.g. `hz1`) or address (e.g. `hz1:5701`). Any live member returns
the same cluster-wide CP configuration.

### Log tools (hz_log_summary / hz_get_logs / hz_get_diagnostic_logs)
These tools are available when the log backend is configured (docker or files mode).
If they are not listed, log access is disabled — do not attempt to call them.

When logs may help (e.g. elections, exceptions, timeout errors):

  Step 1 — ALWAYS call `hz_log_summary` first.
           It returns only counts (~50 tokens). Use it to identify which
           member(s) have WARN/ERROR lines before fetching any log content.

  Step 2 — Call `hz_get_logs` with:
           • `members` set to only the affected member(s) from step 1.
           • `level` = "WARN" (default) or "ERROR" to narrow scope.
           • `keywords` derived from Prometheus findings or the user's question
             (e.g. ["election", "WrongGroupException", "timeout", "cp-group"]).
             Keywords filter lines server-side — always set them when possible.
           • `max_lines` = 100 (default) unless the user needs more.

  Step 3 — Only call `hz_get_diagnostic_logs` if the user explicitly asks about
           diagnostics or if step 2 reveals an issue that needs deeper trace.
           This tool is only available in docker backend mode.

Never fetch logs for all members at once unless `hz_log_summary` shows errors
on multiple members — this wastes tokens on clean members.
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mcp_tool_to_anthropic(tool) -> dict:
    return {
        "name": tool.name,
        "description": tool.description or "",
        "input_schema": tool.inputSchema if hasattr(tool, "inputSchema") else {"type": "object", "properties": {}},
    }


def _mcp_tool_to_openai(tool) -> dict:
    return {
        "type": "function",
        "function": {
            "name": tool.name,
            "description": tool.description or "",
            "parameters": tool.inputSchema if hasattr(tool, "inputSchema") else {"type": "object", "properties": {}},
        },
    }


def _serialize_content(content: list) -> list[dict]:
    """Convert SDK response content blocks to plain dicts for the next API call."""
    out = []
    for block in content:
        if block.type == "text":
            out.append({"type": "text", "text": block.text})
        elif block.type == "tool_use":
            out.append({
                "type": "tool_use",
                "id": block.id,
                "name": block.name,
                "input": block.input,
            })
    return out


def _sse(event: str, data) -> str:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


# ---------------------------------------------------------------------------
# MCP session management
# ---------------------------------------------------------------------------

async def _open_mcp_session(stack: AsyncExitStack, url: str):
    """Open an SSE MCP session within an AsyncExitStack."""
    from mcp import ClientSession
    from mcp.client.sse import sse_client
    read, write = await stack.enter_async_context(sse_client(url))
    session = await stack.enter_async_context(ClientSession(read, write))
    await session.initialize()
    return session


async def _gather_tools(sessions: list) -> tuple[list, dict]:
    """
    Collect tools from all sessions and build a routing map.
    Returns (all_tools, {tool_name: session}).
    """
    from mcp import ClientSession
    tool_sessions: dict[str, ClientSession] = {}
    all_tools: list = []
    for session in sessions:
        for t in (await session.list_tools()).tools:
            tool_sessions[t.name] = session
            all_tools.append(t)
    return all_tools, tool_sessions


# ---------------------------------------------------------------------------
# Agentic streaming loop
# ---------------------------------------------------------------------------

async def chat_stream(
    messages: list[dict],
    analysis: str,
    prometheus_url: str,
    start: float,
    end: float,
    model: str,
) -> AsyncIterator[str]:
    """
    Run the MCP-backed agentic loop and yield SSE-formatted chunks.

    Events emitted:
      data: <json text chunk>   — LLM text token (default SSE message)
      event: tool_call          — an MCP tool is being invoked
      event: done               — stream finished
      event: error              — unrecoverable error
    """
    from datetime import datetime, timezone

    fmt = lambda ts: datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    system = (
        CHAT_SYSTEM
        + f"\n\n## Analysis window\n- Start: {fmt(start)}\n- End:   {fmt(end)}\n"
        + f"\n## Completed Analysis\n{analysis}"
    )

    try:
        async with AsyncExitStack() as stack:
            # Always connect to the Prometheus MCP server
            prom_session = await _open_mcp_session(stack, f"{MCP_SERVER_URL}/sse")
            sessions = [prom_session]

            # Optionally connect to the Hazelcast log MCP server
            if MCP_HZ_URL:
                try:
                    hz_session = await _open_mcp_session(stack, f"{MCP_HZ_URL}/sse")
                    sessions.append(hz_session)
                except Exception as exc:
                    yield _sse("warning", f"Hazelcast MCP server unavailable: {exc}")

            all_tools, tool_sessions = await _gather_tools(sessions)

            if model.startswith("claude"):
                async for chunk in _loop_claude(model, system, messages, all_tools, tool_sessions):
                    yield chunk
            else:
                async for chunk in _loop_openai(model, system, messages, all_tools, tool_sessions):
                    yield chunk

    except Exception as exc:
        yield _sse("error", str(exc))


# ---------------------------------------------------------------------------
# Anthropic loop
# ---------------------------------------------------------------------------

async def _loop_claude(model, system, messages, mcp_tools, tool_sessions) -> AsyncIterator[str]:
    import anthropic

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        yield _sse("error", "ANTHROPIC_API_KEY not set")
        return

    llm     = anthropic.AsyncAnthropic(api_key=api_key)
    tools   = [_mcp_tool_to_anthropic(t) for t in mcp_tools]
    history = list(messages)

    for _ in range(MAX_ITERATIONS):
        response_content = []

        async with llm.messages.stream(
            model=model,
            max_tokens=4096,
            system=system,
            tools=tools,
            messages=history,
        ) as stream:
            async for event in stream:
                if event.type == "content_block_delta" and hasattr(event.delta, "text"):
                    yield f"data: {json.dumps(event.delta.text)}\n\n"
            final = await stream.get_final_message()
            response_content = final.content

        tool_uses = [b for b in response_content if b.type == "tool_use"]
        if not tool_uses:
            yield _sse("done", "[DONE]")
            return

        history.append({"role": "assistant", "content": _serialize_content(response_content)})

        tool_results = []
        for tu in tool_uses:
            yield _sse("tool_call", {"name": tu.name, "input": tu.input})
            result_text = await _call_mcp_tool(tool_sessions, tu.name, tu.input)
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": tu.id,
                "content": result_text,
            })
        history.append({"role": "user", "content": tool_results})

    yield _sse("error", "Maximum tool iterations reached without a final answer.")


# ---------------------------------------------------------------------------
# OpenAI loop
# ---------------------------------------------------------------------------

async def _loop_openai(model, system, messages, mcp_tools, tool_sessions) -> AsyncIterator[str]:
    from openai import AsyncOpenAI

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        yield _sse("error", "OPENAI_API_KEY not set")
        return

    client  = AsyncOpenAI(api_key=api_key)
    tools   = [_mcp_tool_to_openai(t) for t in mcp_tools]
    history = [{"role": "system", "content": system}] + list(messages)

    for _ in range(MAX_ITERATIONS):
        text_buf: str = ""
        tool_calls_buf: dict[int, dict] = {}

        stream = await client.chat.completions.create(
            model=model,
            max_tokens=4096,
            stream=True,
            tools=tools,
            messages=history,
        )
        finish_reason = None
        async for chunk in stream:
            delta = chunk.choices[0].delta
            finish_reason = chunk.choices[0].finish_reason or finish_reason

            if delta.content:
                text_buf += delta.content
                yield f"data: {json.dumps(delta.content)}\n\n"

            if delta.tool_calls:
                for tc in delta.tool_calls:
                    idx = tc.index
                    if idx not in tool_calls_buf:
                        tool_calls_buf[idx] = {"id": tc.id, "name": "", "arguments": ""}
                    if tc.id:
                        tool_calls_buf[idx]["id"] = tc.id
                    if tc.function.name:
                        tool_calls_buf[idx]["name"] += tc.function.name
                    if tc.function.arguments:
                        tool_calls_buf[idx]["arguments"] += tc.function.arguments

        if finish_reason != "tool_calls":
            yield _sse("done", "[DONE]")
            return

        tool_calls_list = [
            {
                "id": tool_calls_buf[idx]["id"],
                "type": "function",
                "function": {
                    "name": tool_calls_buf[idx]["name"],
                    "arguments": tool_calls_buf[idx]["arguments"],
                },
            }
            for idx in sorted(tool_calls_buf)
        ]
        assistant_msg: dict = {"role": "assistant", "tool_calls": tool_calls_list}
        if text_buf:
            assistant_msg["content"] = text_buf
        history.append(assistant_msg)

        for tc in tool_calls_list:
            name = tc["function"]["name"]
            try:
                args = json.loads(tc["function"]["arguments"])
            except Exception:
                args = {}
            yield _sse("tool_call", {"name": name, "input": args})
            result_text = await _call_mcp_tool(tool_sessions, name, args)
            history.append({
                "role": "tool",
                "tool_call_id": tc["id"],
                "content": result_text,
            })

    yield _sse("error", "Maximum tool iterations reached without a final answer.")


# ---------------------------------------------------------------------------
# Shared MCP tool executor — routes by tool name to the correct session
# ---------------------------------------------------------------------------

async def _call_mcp_tool(tool_sessions: dict, name: str, args: dict) -> str:
    print(f"[_call_mcp_tool] tool={name} args={args}", flush=True)
    session = tool_sessions.get(name)
    if session is None:
        print(f"[_call_mcp_tool] ERROR: no session for tool={name}", flush=True)
        return json.dumps({"error": f"No MCP session found for tool: {name}"})
    try:
        result = await session.call_tool(name, args)
        text = "\n".join(c.text for c in result.content if hasattr(c, "text"))
        print(f"[_call_mcp_tool] tool={name} result_len={len(text)} result[:200]={text[:200]!r}", flush=True)
        return text
    except Exception as exc:
        print(f"[_call_mcp_tool] tool={name} exception={exc}", flush=True)
        return json.dumps({"error": str(exc)})
