"""Container usage from cgroup v2 files."""

from __future__ import annotations

from prometheus_client import CollectorRegistry, generate_latest

from leasehold.runtime import resources
from leasehold.runtime.resources import CgroupCollector, Usage, cgroup_dir, read_usage, summary


def _cgroup(tmp_path, **files):
    d = tmp_path / "cg"
    d.mkdir()
    for name, text in files.items():
        (d / name.replace("_", ".", 1)).write_text(text)
    return d


def test_read_usage(tmp_path):
    d = _cgroup(
        tmp_path,
        memory_current="300\n",
        memory_peak="900\n",
        memory_max="max\n",
        memory_stat="anon 200\nfile 100\ninactive_file 80\n",
        memory_events="low 0\nhigh 0\nmax 3\noom 1\noom_kill 1\n",
        cpu_stat="usage_usec 2500000\nuser_usec 2000000\nthrottled_usec 500000\n",
    )
    assert read_usage(d) == Usage(
        memory_bytes=300,
        working_set_bytes=220,
        peak_bytes=900,
        limit_bytes=None,
        oom_kills=1,
        cpu_seconds=2.5,
        throttled_seconds=0.5,
    )


def test_missing_files_are_skipped(tmp_path):
    d = _cgroup(tmp_path, memory_current="300\n")
    r = CollectorRegistry()
    r.register(CgroupCollector(d))
    out = generate_latest(r).decode()
    assert "leasehold_container_memory_bytes 300.0" in out
    assert "peak" not in out and "cpu_seconds" not in out
    assert read_usage(tmp_path / "nothing") == Usage()


def test_collector_names(tmp_path):
    d = _cgroup(
        tmp_path,
        memory_current="10",
        memory_peak="20",
        memory_max="30",
        cpu_stat="usage_usec 1000000\nthrottled_usec 0\n",
        memory_events="oom_kill 0\n",
    )
    r = CollectorRegistry()
    r.register(CgroupCollector(d))
    out = generate_latest(r).decode()
    for line in (
        "leasehold_container_memory_peak_bytes 20.0",
        "leasehold_container_memory_limit_bytes 30.0",
        "leasehold_container_cpu_seconds_total 1.0",
        "leasehold_container_cpu_throttled_seconds_total 0.0",
        "leasehold_container_oom_kills_total 0.0",
    ):
        assert line in out


def test_cgroup_dir(tmp_path):
    proc = tmp_path / "cgroup"
    proc.write_text("0::/system.slice/docker-abc.scope\n")
    assert cgroup_dir(proc, tmp_path) == tmp_path / "system.slice/docker-abc.scope"
    proc.write_text("0::/\n")  # private cgroup namespace (containers)
    assert cgroup_dir(proc, tmp_path) == tmp_path
    assert cgroup_dir(tmp_path / "missing", tmp_path) == tmp_path


def test_summary_and_register():
    assert summary(Usage(peak_bytes=5, cpu_seconds=1.23456, oom_kills=0)) == {
        "memory_peak_bytes": 5,
        "cpu_seconds": 1.235,
    }
    resources.register("abc1234")
    resources.register("abc1234")  # idempotent
    out = generate_latest().decode()
    assert 'leasehold_build_info{leasehold="0.1.0",version="abc1234"} 1.0' in out
