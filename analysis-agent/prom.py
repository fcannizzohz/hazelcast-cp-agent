"""Async Prometheus HTTP API client."""

import httpx


class PrometheusClient:
    def __init__(self, base_url: str, timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    async def query(self, promql: str, timestamp: float | None = None) -> list[dict]:
        """Instant query — returns a list of {metric, value} dicts."""
        params: dict = {"query": promql}
        if timestamp is not None:
            params["time"] = timestamp
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            r = await client.get(f"{self.base_url}/api/v1/query", params=params)
            r.raise_for_status()
        data = r.json()
        if data["status"] != "success":
            raise RuntimeError(f"Prometheus query failed: {data}")
        return data["data"]["result"]

    async def query_range(
        self,
        promql: str,
        start: float,
        end: float,
        step: str,
    ) -> list[dict]:
        """Range query — returns a list of {metric, values} dicts."""
        params = {"query": promql, "start": start, "end": end, "step": step}
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            r = await client.get(
                f"{self.base_url}/api/v1/query_range", params=params
            )
            r.raise_for_status()
        data = r.json()
        if data["status"] != "success":
            raise RuntimeError(f"Prometheus range query failed: {data}")
        return data["data"]["result"]

    async def label_values(self, label: str, match: str | None = None) -> list[str]:
        """Return all values for a label (e.g. __name__ for metric discovery)."""
        params: dict = {}
        if match:
            params["match[]"] = match
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            r = await client.get(
                f"{self.base_url}/api/v1/label/{label}/values", params=params
            )
            r.raise_for_status()
        data = r.json()
        if data["status"] != "success":
            raise RuntimeError(f"Prometheus label_values failed: {data}")
        return sorted(data.get("data", []))

    async def health(self) -> bool:
        """Return True if Prometheus is reachable."""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                r = await client.get(f"{self.base_url}/-/healthy")
            return r.status_code == 200
        except Exception:
            return False


def choose_step(duration_seconds: float) -> str:
    """Pick a range-query step that yields ~100 data points."""
    step_secs = max(15, int(duration_seconds / 100))
    if step_secs < 60:
        return f"{step_secs}s"
    return f"{step_secs // 60}m"
