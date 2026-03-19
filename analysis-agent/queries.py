"""
Curated PromQL queries for Hazelcast CP Subsystem analysis.

Instant queries  → snapshot at the end of the analysis window.
Range queries    → time-series over the window, summarised server-side before
                   being sent to the LLM.
"""

from dataclasses import dataclass


@dataclass
class InstantQuery:
    name: str
    query: str
    description: str
    healthy_hint: str = ""


@dataclass
class RangeQuery:
    name: str
    query: str
    description: str
    unit: str = ""
    healthy_hint: str = ""


# ---------------------------------------------------------------------------
# Instant queries — current state at end of the analysis period
# ---------------------------------------------------------------------------

INSTANT_QUERIES: list[InstantQuery] = [

    # ── Cluster membership ─────────────────────────────────────────────────
    InstantQuery(
        name="reporting_members",
        query="count(count by (mc_member)(hz_raft_group_term))",
        description="Members actively sending metrics to Management Center right now",
        healthy_hint="Must equal cp_member_count; any shortfall = a member is silent (likely down)",
    ),
    InstantQuery(
        name="reachable_cp_members",
        query="max(hz_raft_metadata_activeMembers) - max(hz_raft_missingMembers)",
        description="Reachable CP members per CP subsystem self-report (may lag after a crash)",
        healthy_hint="5 for a 5-node cluster; < 3 risks quorum loss",
    ),
    InstantQuery(
        name="missing_cp_members",
        query="max(hz_raft_missingMembers)",
        description="CP members flagged unreachable by the CP subsystem (may lag after a crash)",
        healthy_hint="0; any value > 0 is a warning",
    ),
    InstantQuery(
        name="total_cp_groups",
        query="max(hz_raft_metadata_groups)",
        description="Total CP groups tracked by METADATA",
        healthy_hint="Stable; increases only when a new group is created",
    ),
    InstantQuery(
        name="terminated_raft_groups",
        query="max(hz_raft_terminatedRaftNodeGroupIds)",
        description="Terminated Raft group IDs",
        healthy_hint="0; any value > 0 indicates a destroyed group",
    ),

    # ── Per-group Raft state ───────────────────────────────────────────────
    InstantQuery(
        name="group_member_counts",
        query="hz_raft_group_memberCount",
        description="Members per CP group (per member view)",
        healthy_hint="3 per group; 2 = degraded, 1 = group unavailable",
    ),
    InstantQuery(
        name="available_log_capacity",
        query="200 - max by (name)(hz_raft_group_lastLogIndex - hz_raft_group_commitIndex)",
        description="Estimated remaining Raft log capacity per group "
                    "(200 - uncommitted entries; assumes default uncommitted-entry-count-to-reject-new-appends=200)",
        healthy_hint="Approaches 0 as log fills; writes rejected at 0; alert at < 50",
    ),
    InstantQuery(
        name="uncommitted_entries",
        query="max by (name)(hz_raft_group_lastLogIndex - hz_raft_group_commitIndex)",
        description="Uncommitted log entries per group (leader has accepted but quorum not yet confirmed)",
        healthy_hint="Near 0 = healthy; approaches 200 = leader will start rejecting new writes",
    ),
    InstantQuery(
        name="snapshot_lag",
        query="max by (name)(hz_raft_group_lastLogIndex)"
              " - (max by (name)(hz_raft_group_snapshotIndex)"
              "    or max by (name)(hz_raft_group_commitIndex) * 0)",
        description="Log entries since the last snapshot per group "
                    "(falls back to lastLogIndex when snapshotIndex is not yet exported — "
                    "i.e. no snapshot has been taken yet)",
        healthy_hint="Resets toward 0 each time a snapshot is taken (every ~10 000 entries); "
                     "sustained near 10 000 without reset = snapshots not happening",
    ),
    InstantQuery(
        name="commit_lag_current",
        query="hz_raft_group_commitIndex - hz_raft_group_lastApplied",
        description="Current commit lag (commitIndex - lastApplied) per group/member",
        healthy_hint="Near 0 in steady state; persistent high value = state machine falling behind",
    ),
    InstantQuery(
        name="raft_terms",
        query="max by (name)(hz_raft_group_term)",
        description="Current Raft term per CP group (increments on each election)",
        healthy_hint="Flat over time = stable; step-ups indicate elections",
    ),
    InstantQuery(
        name="raft_nodes_per_member",
        query="max by (mc_member)(hz_raft_nodes)",
        description="Number of Raft nodes hosted by each member",
        healthy_hint="Should be equal across members (~3 for group-size=3 with 8 groups)",
    ),

    # ── Member presence cross-check ───────────────────────────────────────
    InstantQuery(
        name="member_presence_cross_check",
        query="count(count by (mc_member)(hz_raft_group_term))",
        description="Number of CP members actively sending metrics to MC right now "
                    "(cross-check: compare to cp_member_count and hz_raft_missingMembers "
                    "to distinguish restart / partition / MC scrape lag)",
        healthy_hint="Must equal cp_member_count; "
                     "drop + missingMembers=0 = MC scrape lag (not a real failure); "
                     "drop + missingMembers>0 = member absent from CP subsystem view; "
                     "drop + uptime reset on that member = restart",
    ),

    # ── Per-member follower lag (instant snapshot) ─────────────────────────
    InstantQuery(
        name="follower_lag_per_member_current",
        query="max by (name)(hz_raft_group_commitIndex)"
              " - on(name) group_right()"
              " max by (name, mc_member)(hz_raft_group_lastApplied)",
        description="Current gap between the cluster-max commitIndex and each member's "
                    "lastApplied — identifies which specific member is lagging right now",
        healthy_hint="0 = fully caught up; one member persistently > 0 while others are ~0 "
                     "= slow follower blocking quorum acks (check its heap and CPU); "
                     "all members lag equally = write pressure, not a follower issue",
    ),

    # ── METADATA group health (proxy caching signal) ──────────────────────
    InstantQuery(
        name="metadata_group_commit_rate",
        query='rate(hz_raft_group_commitIndex{name="METADATA"}[5m])',
        description="Current commit rate on the METADATA CP group — elevated rate at steady "
                    "workload (no group creation) may indicate CP proxy objects are being "
                    "fetched without caching; each uncached getCPSubsystem().get*() call "
                    "triggers an internal METADATA commit",
        healthy_hint="Near zero at steady state = proxies are cached correctly; "
                     "sustained high rate proportional to operation throughput = "
                     "application fetching CP proxies per-operation instead of caching them",
    ),

    # ── CP group leadership distribution ──────────────────────────────────
    InstantQuery(
        name="leader_commit_rate_per_member",
        query="rate(hz_raft_group_commitIndex[2m])",
        description="Current commit rate per CP group per member — the member with non-zero "
                    "rate for a given group is its current leader; followers produce zero "
                    "commit-rate. Use to detect leadership concentration on one member.",
        healthy_hint="One member per group should have non-zero rate; if one member has "
                     "non-zero rate for significantly more groups than others = leadership "
                     "is concentrated (bottleneck risk); Hazelcast auto-rebalances — "
                     "transient post-restart imbalance is normal and self-corrects",
    ),

    # ── Member resource health ─────────────────────────────────────────────
    InstantQuery(
        name="member_heap_used_pct",
        query="hz_runtime_usedMemory / (hz_runtime_usedMemory + hz_runtime_freeMemory) * 100",
        description="JVM heap utilisation % per member",
        healthy_hint="< 70 % = healthy; 70–85 % = warning (GC pressure); > 85 % = critical (GC pauses → election risk)",
    ),
    InstantQuery(
        name="member_cpu",
        query="hz_os_processCpuLoad * 100",
        description="Process CPU utilisation % per member",
        healthy_hint="< 70 % = healthy; sustained > 80 % = risk of heartbeat timeouts and elections",
    ),
    InstantQuery(
        name="member_uptime_ms",
        query="hz_runtime_uptime",
        description="JVM uptime in milliseconds per member",
        healthy_hint="Low value (< 300 000 ms = 5 min) relative to other members = recent restart",
    ),

    # ── CP Maps ────────────────────────────────────────────────────────────
    InstantQuery(
        name="cp_map_sizes",
        query="sum by (name, group)(hz_cp_map_size)",
        description="Entry count per CPMap",
        healthy_hint="Depends on workload; watch for unexpected growth or drops",
    ),
    InstantQuery(
        name="cp_map_storage_bytes",
        query="sum by (name, group)(hz_cp_map_sizeBytes)",
        description="Storage bytes per CPMap",
        healthy_hint="Grows with entry count; alert if approaching max-size-mb",
    ),
    InstantQuery(
        name="cp_map_utilization_pct",
        query="sum by (name, group)(hz_cp_map_sizeBytes) / (20 * 1048576) * 100",
        description="CPMap storage utilisation % of the configured 20 MB limit per map",
        healthy_hint="< 70 % = healthy; > 80 % = warning; > 95 % = critical (writes will be rejected)",
    ),

    # ── Data structures ───────────────────────────────────────────────────
    InstantQuery(
        name="semaphore_available",
        query="hz_cp_semaphore_available",
        description="Available permits per ISemaphore (current snapshot)",
        healthy_hint="Equal to initial permit count = idle; 0 = exhausted, clients will block",
    ),
    InstantQuery(
        name="lock_hold_count",
        query="hz_cp_lock_lockCount",
        description="Current concurrent lock holders per FencedLock",
        healthy_hint="0 = idle; 1 = one holder; >1 unexpected for non-reentrant FencedLock",
    ),
    InstantQuery(
        name="lock_acquire_limit",
        query="max by (name)(hz_cp_lock_acquireLimit)",
        description="Configured reentrancy limit per FencedLock",
        healthy_hint="Cross-reference with lock_hold_count; if lockCount approaches acquireLimit the lock is at max reentrant depth",
    ),
    InstantQuery(
        name="lock_owner_session",
        query="hz_cp_lock_ownerSessionId",
        description="Session ID of the current FencedLock owner (0 = no owner)",
        healthy_hint="Non-zero = lock is held; cross-reference with session_expiry_snapshot to detect expiry risk for the holding session",
    ),
    InstantQuery(
        name="atomiclong_values",
        query="hz_cp_atomiclong_value",
        description="Current value of each IAtomicLong counter",
        healthy_hint="Monotonically increasing under load; use rate for throughput signal",
    ),

    # ── CP sessions ───────────────────────────────────────────────────────
    InstantQuery(
        name="session_expiry_snapshot",
        query="hz_cp_session_expirationTime",
        description="Session expiration time in epoch milliseconds per active CP session",
        healthy_hint="Compare each value to the analysis end time (in ms). "
                     "Sessions expiring within 60 000 ms (1 TTL) of the analysis end = imminent expiry risk. "
                     "If such sessions own locks or semaphore permits, their release is unexpected.",
    ),

    # ── CP object lifecycle ────────────────────────────────────────────────
    InstantQuery(
        name="locks_live",
        query="max(hz_cp_lock_summary_live_count)",
        description="Currently active (non-destroyed) FencedLock instance count",
        healthy_hint="Stable = expected; sudden drop = locks destroyed; growth = new locks being created",
    ),
    InstantQuery(
        name="semaphores_live",
        query="max(hz_cp_semaphore_summary_live_count)",
        description="Currently active (non-destroyed) ISemaphore instance count",
        healthy_hint="Stable = expected; frequent creation/destruction = anti-pattern",
    ),
    InstantQuery(
        name="atomiclong_live",
        query="max(hz_cp_atomiclong_summary_live_count)",
        description="Currently active (non-destroyed) IAtomicLong instance count",
        healthy_hint="Stable = expected; frequent creation/destruction = anti-pattern",
    ),
    InstantQuery(
        name="locks_destroyed",
        query="max(hz_cp_lock_summary_destroyed_count)",
        description="Cumulative destroyed FencedLock instance count",
        healthy_hint="0 = healthy; any non-zero = locks have been destroyed (CP objects should be long-lived)",
    ),
    InstantQuery(
        name="semaphores_destroyed",
        query="max(hz_cp_semaphore_summary_destroyed_count)",
        description="Cumulative destroyed ISemaphore instance count",
        healthy_hint="0 = healthy; frequent destruction = anti-pattern",
    ),
    InstantQuery(
        name="atomiclong_destroyed",
        query="max(hz_cp_atomiclong_summary_destroyed_count)",
        description="Cumulative destroyed IAtomicLong instance count",
        healthy_hint="0 = healthy; frequent destruction = anti-pattern",
    ),
]


# ---------------------------------------------------------------------------
# Range queries — time-series over the analysis period, summarised server-side
# ---------------------------------------------------------------------------

RANGE_QUERIES: list[RangeQuery] = [

    # ── Raft consensus ─────────────────────────────────────────────────────
    RangeQuery(
        name="leader_elections",
        query="changes(hz_raft_group_term[5m])",
        description="Leader elections per CP group in 5-minute windows",
        healthy_hint="0 throughout = perfectly stable; occasional 1s = normal; frequent spikes = instability",
    ),
    RangeQuery(
        name="commit_lag_over_time",
        query="hz_raft_group_commitIndex - hz_raft_group_lastApplied",
        description="Replication lag (commitIndex - lastApplied) per group/member over time",
        unit="log entries",
        healthy_hint="Should hover near 0; sustained lag > 100 = follower issue",
    ),
    RangeQuery(
        name="follower_lag_per_member",
        query="max by (name)(hz_raft_group_commitIndex) - on(name) group_right() max by (name, mc_member)(hz_raft_group_lastApplied)",
        description="Gap between cluster-max commitIndex and each member's lastApplied — captures both replication and apply lag per member",
        unit="log entries",
        healthy_hint="0 = fully caught up; sustained > 0 on a specific member = that member is falling behind",
    ),
    RangeQuery(
        name="commit_rate",
        query="rate(hz_raft_group_commitIndex[1m])",
        description="Raft commit rate per CP group",
        unit="entries/s",
        healthy_hint="Proportional to write load; drops to 0 under no traffic; large variance = bursty writes",
    ),
    RangeQuery(
        name="apply_rate",
        query="rate(hz_raft_group_lastApplied[1m])",
        description="State-machine apply rate per CP group",
        unit="entries/s",
        healthy_hint="Should track commit rate closely; divergence = application backlog building",
    ),
    RangeQuery(
        name="uncommitted_entries_over_time",
        query="max by (name)(hz_raft_group_lastLogIndex - hz_raft_group_commitIndex)",
        description="Uncommitted log entries per group over time (leading write-pressure indicator)",
        unit="log entries",
        healthy_hint="Near 0 = healthy; rising trend = write saturation building; sustained near 200 = writes being rejected",
    ),

    # ── Log & snapshot health ──────────────────────────────────────────────
    RangeQuery(
        name="missing_members_over_time",
        query="max(hz_raft_missingMembers)",
        description="Missing CP members over time",
        healthy_hint="0 throughout = all members healthy; any non-zero = member was absent",
    ),
    RangeQuery(
        name="log_capacity_over_time",
        query="200 - max by (name)(hz_raft_group_lastLogIndex - hz_raft_group_commitIndex)",
        description="Estimated available Raft log capacity per group over time "
                    "(200 - uncommitted entries; assumes default uncommitted-entry-count-to-reject-new-appends=200)",
        healthy_hint="Decreasing trend without recovery = snapshots not keeping up; "
                     "approaching 0 = writes will be rejected",
    ),
    RangeQuery(
        name="snapshot_index_over_time",
        query="(max by (name)(hz_raft_group_snapshotIndex))"
              " or (max by (name)(hz_raft_group_commitIndex) * 0)",
        description="Snapshot index per group over time — steps up each time a snapshot is taken "
                    "(shows 0 before the first snapshot is taken)",
        healthy_hint="Regular step-ups = snapshots are happening (every ~10 000 commits); "
                     "flat line over a long window = no snapshots taken = log exhaustion risk",
    ),

    # ── Member resource health ─────────────────────────────────────────────
    RangeQuery(
        name="member_heap_over_time",
        query="hz_runtime_usedMemory",
        description="JVM heap used bytes per member over time",
        unit="bytes",
        healthy_hint="Stable or gently growing = healthy; rapid growth = memory leak; "
                     "near-max followed by drop = GC event; sustained high = GC pressure → election risk",
    ),
    RangeQuery(
        name="member_cpu_over_time",
        query="hz_os_processCpuLoad * 100",
        description="Process CPU utilisation % per member over time",
        unit="%",
        healthy_hint="Stable < 70 % = healthy; sustained > 80 % = risk of heartbeat miss and elections; "
                     "spikes correlated with elections = GC or CPU contention is the root cause",
    ),
    RangeQuery(
        name="member_uptime_over_time",
        query="hz_runtime_uptime",
        description="JVM uptime in milliseconds per member over time",
        unit="ms",
        healthy_hint="Monotonically increasing = stable; sudden drop (reset to near 0) = member restarted during the window",
    ),

    # ── CP Maps ────────────────────────────────────────────────────────────
    RangeQuery(
        name="cp_map_entry_trend",
        query="sum by (name, group)(hz_cp_map_size)",
        description="CPMap entry count trend over time",
        healthy_hint="Stable or expected growth; sudden drops = eviction or destroy",
    ),

    # ── Data structures ───────────────────────────────────────────────────
    RangeQuery(
        name="semaphore_permits_over_time",
        query="hz_cp_semaphore_available",
        description="Available ISemaphore permits over time",
        unit="permits",
        healthy_hint="Stable near initial count = low contention; drops + recovery = healthy burst; sustained at 0 = exhaustion",
    ),
    RangeQuery(
        name="lock_acquire_rate",
        query="changes(hz_cp_lock_lockCount[1m])",
        description="FencedLock lock-state changes per minute (each acquire+release cycle = 2 changes; "
                    "hz_cp_lock_acquireCount is not exported by MC)",
        unit="state changes/min",
        healthy_hint="Zero = no lock activity; non-zero = lock is being used; "
                     "divide by 2 to approximate acquisitions/min for a non-reentrant lock",
    ),
    RangeQuery(
        name="atomiclong_increment_rate",
        query="rate(hz_cp_atomiclong_value[1m])",
        description="IAtomicLong increment rate per counter",
        unit="increments/s",
        healthy_hint="Proportional to counter workload; zero = no counter activity",
    ),
    RangeQuery(
        name="session_heartbeat_rate",
        query="rate(hz_cp_session_version[30s])",
        description="CP session heartbeat rate (version increments per second)",
        unit="heartbeats/s",
        healthy_hint="Non-zero = sessions are active and heartbeating; zero = no active sessions or sessions stalled",
    ),

    # ── METADATA group health (proxy caching signal) ──────────────────────
    RangeQuery(
        name="metadata_commit_rate_over_time",
        query='rate(hz_raft_group_commitIndex{name="METADATA"}[5m])',
        description="METADATA CP group commit rate over time — spikes indicate CP group "
                    "creation, session management, or uncached CP proxy fetches. "
                    "Compare magnitude to data-group commit rates to assess whether "
                    "METADATA is disproportionately loaded.",
        unit="commits/s",
        healthy_hint="Low baseline with brief spikes on demand = healthy; "
                     "sustained elevated rate at steady state (no group creation) = "
                     "application likely not caching CP proxy objects",
    ),

    # ── CP object lifecycle ────────────────────────────────────────────────
    RangeQuery(
        name="cp_object_churn",
        query="max(hz_cp_lock_summary_destroyed_count or hz_cp_semaphore_summary_destroyed_count or hz_cp_atomiclong_summary_destroyed_count)",
        description="Cumulative destroyed CP object count (locks, semaphores, atomic longs) over time",
        healthy_hint="Flat line = no destruction (expected; CP objects are long-lived); "
                     "rising = objects being repeatedly created and destroyed (anti-pattern, adds Raft overhead)",
    ),
]
