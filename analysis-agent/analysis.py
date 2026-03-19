"""
Convert raw Prometheus results into compact summaries suitable for LLM consumption.

We never send raw time-series arrays to the LLM — only statistical summaries plus
notable events (spikes, sustained anomalies).
"""

from __future__ import annotations

import math
import statistics
from typing import Any


# ---------------------------------------------------------------------------
# Instant query results
# ---------------------------------------------------------------------------

def summarise_instant(results: list[dict]) -> list[dict]:
    """
    Convert Prometheus instant-query results into a compact list.
    Each entry: {labels, value}  — labels stripped of __name__.
    """
    out = []
    for r in results:
        labels = {k: v for k, v in r["metric"].items() if k != "__name__"}
        raw = r["value"][1]
        value = None if raw in ("NaN", "+Inf", "-Inf") else round(float(raw), 4)
        out.append({"labels": labels, "value": value})
    return out


# ---------------------------------------------------------------------------
# Range query results
# ---------------------------------------------------------------------------

def summarise_range(results: list[dict]) -> list[dict]:
    """
    Summarise a Prometheus range query result into per-series statistics.
    Output per series: {labels, min, max, avg, latest, std, spikes, trend}
    """
    out = []
    for r in results:
        labels = {k: v for k, v in r["metric"].items() if k != "__name__"}
        raw_values: list[float] = []
        for _, v in r.get("values", []):
            try:
                fv = float(v)
                if math.isfinite(fv):
                    raw_values.append(fv)
            except (ValueError, TypeError):
                pass

        if not raw_values:
            continue

        avg = statistics.mean(raw_values)
        std = statistics.stdev(raw_values) if len(raw_values) > 1 else 0.0
        spike_threshold = avg + 2 * std if std > 0 else None
        spikes = (
            sum(1 for v in raw_values if v > spike_threshold)
            if spike_threshold is not None
            else 0
        )

        # Simple linear trend: positive = rising, negative = falling
        n = len(raw_values)
        if n >= 2:
            # Mean of second half minus mean of first half
            mid = n // 2
            first_half = statistics.mean(raw_values[:mid])
            second_half = statistics.mean(raw_values[mid:])
            delta = second_half - first_half
            if abs(delta) < 0.01 * (abs(avg) + 1e-9):
                trend = "stable"
            elif delta > 0:
                trend = "rising"
            else:
                trend = "falling"
        else:
            trend = "stable"

        out.append(
            {
                "labels": labels,
                "min": round(min(raw_values), 4),
                "max": round(max(raw_values), 4),
                "avg": round(avg, 4),
                "latest": round(raw_values[-1], 4),
                "std": round(std, 4),
                "spike_count": spikes,
                "trend": trend,
            }
        )
    return out


# ---------------------------------------------------------------------------
# Context builder — assemble the full text block sent to the LLM
# ---------------------------------------------------------------------------

def build_context(
    start_iso: str,
    end_iso: str,
    duration_label: str,
    instant_results: dict[str, Any],
    range_results: dict[str, Any],
    query_meta: dict[str, Any],
) -> str:
    """
    Build a structured text context that will be injected into the LLM prompt.
    """
    lines: list[str] = []

    lines.append("## Analysis Window")
    lines.append(f"- Start : {start_iso} UTC")
    lines.append(f"- End   : {end_iso} UTC")
    lines.append(f"- Span  : {duration_label}")
    lines.append("")

    # ── Instant (current state) ────────────────────────────────────────────
    lines.append("## Current State (snapshot at end of window)")
    lines.append("")

    for q_name, series in instant_results.items():
        meta = query_meta.get(q_name, {})
        lines.append(f"### {q_name}")
        if meta.get("description"):
            lines.append(f"_{meta['description']}_")
        if meta.get("healthy_hint"):
            lines.append(f"Healthy: {meta['healthy_hint']}")
        if not series:
            lines.append("No data")
        else:
            for s in series:
                label_str = _fmt_labels(s["labels"])
                val = s["value"]
                lines.append(f"  - {label_str}: {val}")
        lines.append("")

    # ── Range (time-series summaries) ────────────────────────────────────
    lines.append("## Time-Series Summary (over analysis window)")
    lines.append("")

    for q_name, series in range_results.items():
        meta = query_meta.get(q_name, {})
        lines.append(f"### {q_name}")
        if meta.get("description"):
            lines.append(f"_{meta['description']}_")
        if meta.get("healthy_hint"):
            lines.append(f"Healthy: {meta['healthy_hint']}")
        if meta.get("unit"):
            lines.append(f"Unit: {meta['unit']}")
        if not series:
            lines.append("No data")
        else:
            for s in series:
                label_str = _fmt_labels(s["labels"])
                spike_note = f", {s['spike_count']} spikes" if s["spike_count"] else ""
                lines.append(
                    f"  - {label_str}: "
                    f"min={s['min']}, max={s['max']}, avg={s['avg']}, "
                    f"latest={s['latest']}, trend={s['trend']}{spike_note}"
                )
        lines.append("")

    return "\n".join(lines)


def _fmt_labels(labels: dict) -> str:
    if not labels:
        return "(aggregate)"
    priority = ["name", "group", "mc_member", "role"]
    parts = []
    for k in priority:
        if k in labels:
            parts.append(f"{k}={labels[k]}")
    for k, v in labels.items():
        if k not in priority:
            parts.append(f"{k}={v}")
    return ", ".join(parts) if parts else "(no labels)"
