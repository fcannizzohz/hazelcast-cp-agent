"""
Cluster context — describes the topology and workload roles of this CP cluster.

STATIC_CONTEXT is the authoritative fallback.  derive_context() enriches it
at analysis time by querying Prometheus for live values (member list, group
list, group size).  Group roles cannot be inferred from metrics alone, so
they always come from STATIC_CONTEXT.

CP subsystem config is not fetched here — use the hz_get_member_config MCP
tool during agentic chat to read it directly from the container.

Environment:
  MC_URL      Management Center base URL  default: http://host.docker.internal:8080
  MC_CLUSTER  Hazelcast cluster name      default: dev
"""

from __future__ import annotations

import os

import httpx
from dotenv import load_dotenv

from prom import PromMcpClient

load_dotenv()

MC_URL          = os.environ.get("MC_URL",          "http://host.docker.internal:8080").rstrip("/")
MC_CLUSTER      = os.environ.get("MC_CLUSTER",      "dev")
HZ_MCP_URL      = os.environ.get("MCP_HZ_URL",      "http://localhost:8002").rstrip("/")
# Comma-separated host:port addresses used for the Hazelcast REST health check.
# These must be HTTP-reachable from inside the agent container (e.g. host.docker.internal:5701).
# When empty, the agent derives addresses from cp_members with port 5701 — only works when
# the agent container shares a Docker network with the cluster.
HZ_MEMBER_ADDRS = os.environ.get("HZ_MEMBER_ADDRS", "")

# ---------------------------------------------------------------------------
# Static context — update when cluster topology changes
# ---------------------------------------------------------------------------

STATIC_CONTEXT: dict = {
    "cp_members": ["hz1", "hz2", "hz3", "hz4", "hz5"],
    "cp_member_count": 5,
    "group_size": 3,
    "quorum_size": 2,
    "cp_groups": [
        "METADATA",
        "group1", "group2", "group3",
        "group4", "group5", "group6", "group7",
    ],
    "group_roles": {
        "cp_map":     ["group1", "group5"],
        "lock":       ["group2"],
        "semaphore":  ["group3", "group7"],
        "counter":    ["group4", "group6"],
    },
    "cp_map_max_size_mb": 20,
}


# ---------------------------------------------------------------------------
# Cluster health helper
# ---------------------------------------------------------------------------

async def _fetch_cluster_health(members: list[str], hz_member_addrs: str = "") -> dict:
    """
    Call the Hazelcast REST health endpoint on each CP member and return a
    dict of {addr: health_result}.  Unreachable members are recorded as
    {"error": "<reason>"}.

    Endpoint: GET http://<host>:<port>/hazelcast/health
    Response: {"nodeState":"ACTIVE","clusterState":"ACTIVE",
               "clusterSafe":true,"migrationQueueSize":0,"clusterSize":N}

    Addresses are taken from hz_member_addrs (comma-separated host:port) when set,
    otherwise constructed from the members list using port 5701.
    """
    if hz_member_addrs:
        addrs = [a.strip() for a in hz_member_addrs.split(",") if a.strip()]
    else:
        addrs = [f"{m.split(':')[0]}:5701" for m in members]

    results: dict = {}
    async with httpx.AsyncClient(timeout=3.0) as client:
        for addr in addrs:
            host, port = addr.rsplit(":", 1) if ":" in addr else (addr, "5701")
            url = f"http://{host}:{port}/hazelcast/health"
            try:
                r = await client.get(url)
                r.raise_for_status()
                results[addr] = r.json()
            except Exception as exc:
                results[addr] = {"error": str(exc)}
    return results


# ---------------------------------------------------------------------------
# Dynamic derivation
# ---------------------------------------------------------------------------

async def derive_context(
    prom: PromMcpClient,
    *,
    hz_member_addrs: str = "",
) -> dict:
    """
    Query Prometheus to derive what can be observed at runtime, then merge
    with STATIC_CONTEXT (static values win for group_roles which are not
    observable from metrics).

    Fields derived dynamically:
      cp_members       — from mc_member labels on hz_raft_group_memberCount
      cp_member_count  — len(cp_members)
      group_size       — max(hz_raft_group_memberCount) across all series
      quorum_size      — group_size // 2 + 1
      cp_groups        — from name labels on hz_raft_group_term
      group_roles.cp_map — from group labels on hz_cp_map_size
    """
    ctx: dict = {
        **STATIC_CONTEXT,
        "group_roles": dict(STATIC_CONTEXT["group_roles"]),  # shallow copy
    }

    # ── CP members ────────────────────────────────────────────────────────
    try:
        results = await prom.query("hz_raft_group_memberCount")
        members = sorted({
            r["metric"].get("mc_member", "").split(":")[0]
            for r in results
            if r["metric"].get("mc_member")
        } - {""})
        if members:
            ctx["cp_members"] = members
            ctx["cp_member_count"] = len(members)
    except Exception:
        pass

    # ── Group size + quorum ───────────────────────────────────────────────
    try:
        results = await prom.query("max(hz_raft_group_memberCount)")
        if results:
            gs = int(float(results[0]["value"][1]))
            if gs > 0:
                ctx["group_size"] = gs
                ctx["quorum_size"] = gs // 2 + 1
    except Exception:
        pass

    # ── CP groups ─────────────────────────────────────────────────────────
    try:
        results = await prom.query("hz_raft_group_term")
        groups = sorted({
            r["metric"].get("name", "")
            for r in results
            if r["metric"].get("name")
        } - {""})
        if groups:
            ctx["cp_groups"] = groups
    except Exception:
        pass

    # ── CP-Map groups (observable from metric labels) ─────────────────────
    try:
        results = await prom.query("hz_cp_map_size")
        map_groups = sorted({
            r["metric"].get("group", "")
            for r in results
            if r["metric"].get("group")
        } - {""})
        if map_groups:
            ctx["group_roles"]["cp_map"] = map_groups
    except Exception:
        pass

    # ── CP subsystem config via hz-mcp-server ─────────────────────────────
    # Calls the /member-config REST endpoint which reads hazelcast.xml directly
    # from the member container (or mounted file), avoiding any MC API dependency.
    first_member = ctx["cp_members"][0] if ctx.get("cp_members") else STATIC_CONTEXT["cp_members"][0]
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(f"{HZ_MCP_URL}/member-config/{first_member}")
            r.raise_for_status()
        data = r.json()
        # data["config"] is the full hazelcast dict; extract cp-subsystem section
        cp_cfg = (data.get("config") or {}).get("cp-subsystem")
        if cp_cfg:
            ctx["cp_subsystem_config"] = cp_cfg
    except Exception:
        pass  # optional enrichment — analysis proceeds without it

    # ── Hazelcast REST health check (all CP members) ──────────────────────
    # Only included when HZ_MEMBER_ADDRS is explicitly configured, or when at
    # least one member responded successfully — avoids injecting a wall of
    # connection errors into the LLM context when the agent runs in a decoupled
    # network and the derived hz1:5701 addresses are simply not reachable.
    effective_hz_member_addrs = hz_member_addrs or HZ_MEMBER_ADDRS
    if effective_hz_member_addrs:
        health = await _fetch_cluster_health(ctx.get("cp_members", STATIC_CONTEXT["cp_members"]), effective_hz_member_addrs)
        ctx["cluster_health"] = health
    else:
        health = await _fetch_cluster_health(ctx.get("cp_members", STATIC_CONTEXT["cp_members"]), effective_hz_member_addrs)
        if any("error" not in v for v in health.values()):
            ctx["cluster_health"] = health

    return ctx
