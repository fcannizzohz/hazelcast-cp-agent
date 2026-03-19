"""
Cluster context — describes the topology and workload roles of this CP cluster.

STATIC_CONTEXT is the authoritative fallback.  derive_context() enriches it
at analysis time by querying Prometheus for live values (member list, group
list, group size) and fetches the CP subsystem config from Management Center.
Group roles cannot be inferred from metrics alone, so they always come from
STATIC_CONTEXT.

Environment:
  MC_URL      Management Center base URL  default: http://management-center:8080
  MC_CLUSTER  Hazelcast cluster name      default: dev
"""

from __future__ import annotations

import io
import json
import os
import xml.etree.ElementTree as ET
from urllib.parse import quote

import httpx

from prom import PrometheusClient

MC_URL          = os.environ.get("MC_URL",          "http://management-center:8080").rstrip("/")
MC_CLUSTER      = os.environ.get("MC_CLUSTER",      "dev")
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
# Management Center config helpers
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


async def _fetch_cluster_health(members: list[str]) -> dict:
    """
    Call the Hazelcast REST health endpoint on each CP member and return a
    dict of {addr: health_result}.  Unreachable members are recorded as
    {"error": "<reason>"}.

    Endpoint: GET http://<host>:<port>/hazelcast/health
    Response: {"nodeState":"ACTIVE","clusterState":"ACTIVE",
               "clusterSafe":true,"migrationQueueSize":0,"clusterSize":N}

    Addresses are taken from HZ_MEMBER_ADDRS (comma-separated host:port) when set,
    otherwise constructed from the members list using port 5701.
    """
    if HZ_MEMBER_ADDRS:
        addrs = [a.strip() for a in HZ_MEMBER_ADDRS.split(",") if a.strip()]
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


async def _fetch_cp_subsystem_config(member: str) -> dict:
    """
    Fetch member config from MC and return only the cp-subsystem section as a dict.
    Returns {} on any failure (config is optional context enrichment).
    """
    try:
        member_addr = member if ":" in member else f"{member}:5701"
        url = (
            f"{MC_URL}/api/clusters/{MC_CLUSTER}"
            f"/members/{quote(member_addr, safe='')}/memberConfig"
        )
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(url)
            r.raise_for_status()
        xml_str = json.loads(r.text)
        root = ET.parse(io.BytesIO(xml_str.encode("utf-8"))).getroot()
        cp_el = root.find("cp-subsystem")
        if cp_el is None:
            return {}
        return _xml_to_dict(cp_el) or {}
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Dynamic derivation
# ---------------------------------------------------------------------------

async def derive_context(prom: PrometheusClient) -> dict:
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

    # ── CP subsystem config from Management Center ────────────────────────
    first_member = ctx["cp_members"][0] if ctx.get("cp_members") else STATIC_CONTEXT["cp_members"][0]
    cp_config = await _fetch_cp_subsystem_config(first_member)
    if cp_config:
        ctx["cp_subsystem_config"] = cp_config

    # ── Hazelcast REST health check (all CP members) ──────────────────────
    health = await _fetch_cluster_health(ctx.get("cp_members", STATIC_CONTEXT["cp_members"]))
    if health:
        ctx["cluster_health"] = health

    return ctx
