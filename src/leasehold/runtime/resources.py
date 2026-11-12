"""What the container uses, from its cgroup (v2), for rightsizing.

Nomad's allocation metrics have no peak memory on cgroup v2, count page cache as used
memory, and are sampled every scrape. The kernel keeps better numbers per cgroup:
``memory.peak`` (highest usage since the container started), ``memory.stat`` (to
subtract reclaimable file pages), ``memory.events`` (OOM kills inside the container)
and ``cpu.stat`` (CPU time and throttling).

The process's own cgroup is found through ``/proc/self/cgroup``: inside a container
with a private cgroup namespace that is the container's cgroup; elsewhere (tests,
local runs) it's whatever cgroup the process is in. Files the kernel doesn't provide
are skipped.

Metrics (all without labels; the Prometheus scrape adds env, service and component):

    leasehold_container_memory_bytes                 memory.current
    leasehold_container_memory_working_set_bytes     current minus inactive file pages
    leasehold_container_memory_peak_bytes            memory.peak
    leasehold_container_memory_limit_bytes           memory.max (absent when unlimited)
    leasehold_container_oom_kills_total              memory.events oom_kill
    leasehold_container_cpu_seconds_total            cpu.stat usage_usec
    leasehold_container_cpu_throttled_seconds_total  cpu.stat throttled_usec
    leasehold_build_info{version, leasehold}         1
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from pathlib import Path

from prometheus_client import REGISTRY, Info
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily
from prometheus_client.registry import Collector

import leasehold

CGROUP_ROOT = Path("/sys/fs/cgroup")


@dataclass(frozen=True)
class Usage:
    memory_bytes: int | None = None
    working_set_bytes: int | None = None
    peak_bytes: int | None = None
    limit_bytes: int | None = None
    oom_kills: int | None = None
    cpu_seconds: float | None = None
    throttled_seconds: float | None = None


def cgroup_dir(proc_cgroup: Path = Path("/proc/self/cgroup"), root: Path = CGROUP_ROOT) -> Path:
    """The cgroup v2 directory of this process (``root`` when it can't be determined)."""
    try:
        for line in proc_cgroup.read_text().splitlines():
            if line.startswith("0::"):
                return root / line[3:].lstrip("/")
    except OSError:
        pass
    return root


def _int(path: Path) -> int | None:
    try:
        text = path.read_text().strip()
    except OSError:
        return None
    if text == "max":
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _keyed(path: Path) -> dict[str, int]:
    """A flat keyed file such as memory.stat or cpu.stat."""
    try:
        lines = path.read_text().splitlines()
    except OSError:
        return {}
    out = {}
    for line in lines:
        key, _, value = line.partition(" ")
        try:
            out[key] = int(value)
        except ValueError:
            continue
    return out


def read_usage(directory: Path | None = None) -> Usage:
    d = directory if directory is not None else cgroup_dir()
    current = _int(d / "memory.current")
    stat = _keyed(d / "memory.stat")
    working = None
    if current is not None and "inactive_file" in stat:
        working = max(current - stat["inactive_file"], 0)
    events = _keyed(d / "memory.events")
    cpu = _keyed(d / "cpu.stat")
    return Usage(
        memory_bytes=current,
        working_set_bytes=working,
        peak_bytes=_int(d / "memory.peak"),
        limit_bytes=_int(d / "memory.max"),
        oom_kills=events.get("oom_kill"),
        cpu_seconds=cpu["usage_usec"] / 1e6 if "usage_usec" in cpu else None,
        throttled_seconds=cpu["throttled_usec"] / 1e6 if "throttled_usec" in cpu else None,
    )


class CgroupCollector(Collector):
    def __init__(self, directory: Path | None = None):
        self.directory = directory

    def collect(self):
        u = read_usage(self.directory)
        gauges = (
            ("memory_bytes", "Memory in use (memory.current), incl. page cache", u.memory_bytes),
            (
                "memory_working_set_bytes",
                "Memory in use minus inactive file pages",
                u.working_set_bytes,
            ),
            ("memory_peak_bytes", "Highest memory use since start (memory.peak)", u.peak_bytes),
            ("memory_limit_bytes", "Memory limit (memory.max)", u.limit_bytes),
        )
        for name, doc, value in gauges:
            if value is not None:
                yield GaugeMetricFamily(f"leasehold_container_{name}", doc, value=value)
        counters = (
            ("oom_kills", "Processes killed by the OOM killer in this container", u.oom_kills),
            ("cpu_seconds", "CPU time used by the container", u.cpu_seconds),
            ("cpu_throttled_seconds", "Time throttled by a CPU quota", u.throttled_seconds),
        )
        for name, doc, value in counters:
            if value is not None:
                yield CounterMetricFamily(f"leasehold_container_{name}", doc, value=value)


BUILD = Info("leasehold_build", "Component version and leasehold version")
_registered = False
_lock = threading.Lock()


def register(version: str) -> None:
    """Add the cgroup collector to the default registry (once) and set the build info."""
    global _registered
    with _lock:
        if not _registered:
            REGISTRY.register(CgroupCollector())
            _registered = True
    BUILD.info({"version": version or "unknown", "leasehold": leasehold.__version__})


def summary(u: Usage | None = None) -> dict[str, float | int]:
    """Usage figures for a log line (scheduled runs are too short to scrape)."""
    u = u or read_usage()
    out: dict[str, float | int] = {}
    if u.peak_bytes is not None:
        out["memory_peak_bytes"] = u.peak_bytes
    if u.cpu_seconds is not None:
        out["cpu_seconds"] = round(u.cpu_seconds, 3)
    if u.oom_kills:
        out["oom_kills"] = u.oom_kills
    return out
