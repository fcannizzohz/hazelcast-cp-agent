"""
Prometheus clients.

PrometheusClient — direct HTTP client used only for health checks.
PromMcpClient    — calls prom-mcp-server REST endpoints; used for all
                   analysis queries so that a single tool implementation
                   serves both the analysis pipeline and the LLM chat agent.
"""

import httpx


class PrometheusClient:
    """Thin direct client — kept for health checks only."""

    def __init__(self, base_url: str, timeout: float = 5.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    async def health(self) -> bool:
        """Return True if Prometheus is reachable."""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                r = await client.get(f"{self.base_url}/-/healthy")
            return r.status_code == 200
        except Exception:
            return False


class PromMcpClient:
    """
    Calls prom-mcp-server REST endpoints for instant and range queries.

    Parameters
    ----------
    mcp_url      : base URL of prom-mcp-server (e.g. http://mcp-prometheus:8001)
    prometheus_url: Prometheus URL to forward to the MCP server per-request;
                   when empty the MCP server's own configured URL is used.
    """

    def __init__(self, mcp_url: str, prometheus_url: str = "", timeout: float = 30.0):
        self.mcp_url = mcp_url.rstrip("/")
        self.prometheus_url = prometheus_url
        self.timeout = timeout

    def _extra(self) -> dict:
        return {"url": self.prometheus_url} if self.prometheus_url else {}

    async def query(self, promql: str, timestamp: float | None = None) -> list[dict]:
        """Instant query — returns a list of {metric, value} dicts."""
        params: dict = {"query": promql, **self._extra()}
        if timestamp is not None:
            params["time"] = timestamp
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            r = await client.get(f"{self.mcp_url}/query", params=params)
        data = r.json()
        if r.status_code >= 400 or isinstance(data, dict) and "error" in data:
            raise RuntimeError(data.get("error", f"HTTP {r.status_code}"))
        return data

    async def query_range(
        self,
        promql: str,
        start: float,
        end: float,
        step: str,
    ) -> list[dict]:
        """Range query — returns a list of {metric, values} dicts."""
        params = {"query": promql, "start": start, "end": end, "step": step, **self._extra()}
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            r = await client.get(f"{self.mcp_url}/query_range", params=params)
        data = r.json()
        if r.status_code >= 400 or isinstance(data, dict) and "error" in data:
            raise RuntimeError(data.get("error", f"HTTP {r.status_code}"))
        return data


def choose_step(duration_seconds: float) -> str:
    """Pick a range-query step that yields ~100 data points."""
    step_secs = max(15, int(duration_seconds / 100))
    if step_secs < 60:
        return f"{step_secs}s"
    return f"{step_secs // 60}m"
