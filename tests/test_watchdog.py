"""The batch in flight, kills and dead letters; run outcomes; cron gaps; lag arithmetic."""

import asyncio
import json
import os
import subprocess
import sys
import time
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
from conftest import FakeControl, FakeKafkaProducer, runtime_cfg, runtime_spec
from test_claims_batches import ChunkConsumer, Msg

from leasehold import cron, naming
from leasehold.api import runs
from leasehold.kafka.lag import LagMonitor, Reading, Target, targets
from leasehold.runtime import processor as proc
from leasehold.runtime.context import Context
from leasehold.runtime.events import EventProducer, ProduceError
from leasehold.runtime.inflight import Journal, SlowWatch, journal_path

TOPIC = "dev.jobs.results.v1"


class Died(BaseException):
    """A kill: nothing after it runs, so the journal isn't cleared."""


def _msgs(n, start=0):
    cfg = runtime_cfg("worker")
    k = FakeKafkaProducer()
    p = EventProducer(cfg, producer=k)
    for i in range(start, n):
        p.emit("results", {"id": str(i)})
    return [
        Msg(TOPIC, v, offset=start + i) for i, (_, _, v) in enumerate(k.messages)
    ]  # fmt: skip


class DeadLetters:
    def __init__(self, error=None):
        self.sent, self.error, self._cb = [], error, None

    def produce(self, topic, value=None, key=None, headers=None, on_delivery=None):
        self.sent.append((topic, value, {k: v.decode() for k, v in headers}))
        self._cb = on_delivery

    def flush(self, timeout):
        self._cb(self.error, None)


def _proc(path, msgs, handler, dlq=None):
    cfg = runtime_cfg("router")
    ctx = Context(
        cfg, client=FakeControl(), producer=EventProducer(cfg, producer=FakeKafkaProducer())
    )
    c = ChunkConsumer(msgs)
    p = proc.Processor(
        ctx, handler, consumer=c, journal=Journal(path), dead_letter_producer=dlq or DeadLetters()
    )
    return p, c


def _die(path, msgs, poison):
    """One process that dies at event ``poison``: the journal stays as it was then."""
    saved = {}

    def handler(events, ctx):
        for e in events:
            if e.offset == poison:
                saved["raw"] = path.read_bytes()
                raise Died

    p, c = _proc(path, msgs, handler)
    with pytest.raises(Died):
        p.step()
    path.write_bytes(saved["raw"])
    return c


def test_kills_are_counted_and_the_event_is_dead_lettered(tmp_path, caplog):
    path = tmp_path / "j"
    for n in range(1, 6):
        _die(path, _msgs(4), poison=2)
        j = Journal(path)
        assert (j.died.kills, j.died.current, j.died.isolated) == (n, (TOPIC, 0, 2), False)
        # A restart that doesn't handle anything doesn't count.
        assert Journal(path).died.kills == n

    # The sixth handles one event at a time: 0 and 1 are committed, 2 kills it.
    c = _die(path, _msgs(4), poison=2)
    assert c.commits == [[(0, 1)], [(0, 2)]]
    assert Journal(path).died.isolated

    dlq, handled = DeadLetters(), []
    msgs = _msgs(4)[2:]
    p, c = _proc(path, msgs, lambda ev, ctx: handled.extend(e.offset for e in ev), dlq)
    assert p.step()
    assert handled == [3]
    assert c.commits == [[(0, 3)], [(0, 4)]]
    (topic, value, headers) = dlq.sent[0]
    assert topic == "dev.jobs.dead-letters.v1"
    assert value == msgs[0].value()
    assert headers["group"] == "dev.jobs.router"
    assert (headers["topic"], headers["offset"], headers["kills"]) == (TOPIC, "2", "6")
    assert len(dlq.sent) == 1
    assert p.journal.died is None and Journal(path).died is None


def test_a_dead_letter_that_is_not_confirmed_is_not_committed(tmp_path):
    path = tmp_path / "j"
    for _ in range(6):
        _die(path, _msgs(3), poison=0)
    p, c = _proc(path, _msgs(3), lambda ev, ctx: None, DeadLetters(error="no such topic"))
    with pytest.raises(ProduceError):
        p.step()
    assert c.commits == []
    assert Journal(path).died.isolated  # still known


def test_a_handler_that_copes_again_clears_the_kills(tmp_path):
    path = tmp_path / "j"
    for _ in range(3):
        _die(path, _msgs(3), poison=1)
    p, c = _proc(path, _msgs(3), lambda ev, ctx: None)
    assert proc.KILLS._value.get() == 3
    assert p.step()
    assert c.commits == [[(0, 3)]]
    assert Journal(path).died is None and proc.KILLS._value.get() == 0


def test_an_unrelated_batch_keeps_the_kills(tmp_path):
    path = tmp_path / "j"
    _die(path, _msgs(3), poison=1)
    p, _ = _proc(path, _msgs(9, start=6), lambda ev, ctx: None)
    assert p.step()
    assert Journal(path).died.kills == 1


def test_an_exception_is_not_a_kill(tmp_path, monkeypatch):
    monkeypatch.setattr(proc.time, "sleep", lambda s: None)
    path = tmp_path / "j"

    def handler(events, ctx):
        raise RuntimeError("down")

    for _ in range(7):
        p, c = _proc(path, _msgs(2), handler)
        assert not p.step()
        assert c.commits == []
    assert Journal(path).died is None


def test_handler_without_a_loop_over_the_batch(tmp_path):
    """The event isn't known: the batch is found back by its first offset."""
    path = tmp_path / "j"
    for _ in range(5):
        raw = {}

        def handler(events, ctx, raw=raw):
            raw["x"] = path.read_bytes()
            raise Died

        p, _ = _proc(path, _msgs(3), handler)
        with pytest.raises(Died):
            p.step()
        path.write_bytes(raw["x"])
    assert Journal(path).died.current is None
    seen = []
    p, _ = _proc(path, _msgs(3), lambda ev, ctx: seen.append([e.offset for e in ev]))
    assert p.step()
    assert seen == [[0], [1], [2]] and Journal(path).died is None


def test_a_torn_or_foreign_file_is_ignored(tmp_path):
    path = tmp_path / "j"
    path.write_bytes(b'{"active": true, "first"')
    assert Journal(path).died is None
    assert Journal(None).died is None
    assert Journal(tmp_path / "missing" / "j").died is None


def test_journal_path(tmp_path):
    assert journal_path({}) is None
    (tmp_path / "data").mkdir()
    got = journal_path({"NOMAD_ALLOC_DIR": str(tmp_path), "NOMAD_TASK_NAME": "t"})
    assert got == tmp_path / "data" / "t-leasehold-inflight.json"


def test_a_real_kill_leaves_the_record(tmp_path):
    """SIGKILL while a regex holds the GIL: the event is in the file, and the stack on stderr."""
    path = tmp_path / "j"
    code = f"""
import re, sys
sys.path.insert(0, {str(Path(__file__).parent)!r})
from test_watchdog import _msgs, _proc
from pathlib import Path
def handler(events, ctx):
    for e in events:
        if e.offset == 1:
            print("at 1", flush=True)
            re.match(r"(a+)+$", "a" * 40 + "b")
p, c = _proc(Path({str(path)!r}), _msgs(3), handler)
p.slow.start()
p.step()
"""
    env = os.environ | {"LEASEHOLD_SLOW_HANDLE_S": "1", "PYTHONUNBUFFERED": "1"}
    child = subprocess.Popen(
        [sys.executable, "-c", code], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env
    )
    assert child.stdout.readline().strip() == b"at 1"
    time.sleep(2.5)
    child.kill()
    _, err = child.communicate()
    assert b"Timeout" in err and b"in match" in err
    died = Journal(path).died
    assert (died.kills, died.current) == (1, (TOPIC, 0, 1))


def test_slow_watch_logs_the_event_once(caplog):
    w = SlowWatch(0.05)
    msgs = _msgs(2)
    from leasehold.runtime.events import parse

    events = [parse(m, naming.TopicRef.parse("jobs.results.v1")) for m in msgs]
    w.begin(events)
    w.current(events[1])
    time.sleep(0.1)
    w.check()
    w.check()
    slow = [r for r in caplog.records if r.message == "a batch is slow"]
    assert len(slow) == 1
    assert (slow[0].fields["offset"], slow[0].fields["at"]) == (1, "event")
    w.end()
    assert w.age() == 0


# --- cron -----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("schedule", "gap"),
    [
        ("*/2 * * * *", timedelta(minutes=2)),
        ("5,35 * * * *", timedelta(minutes=30)),
        ("10 5-20 * * *", timedelta(hours=9)),
        ("0 9,15 * * *", timedelta(hours=18)),
        ("0 6 * * 1", timedelta(days=7)),
        ("0 6 * * 7", timedelta(days=7)),
        ("0 6 * * 1-5", timedelta(days=3)),
        ("30 5 * * *", timedelta(days=1)),
        ("@monthly", timedelta(days=31)),
        ("0 0 1 * 1", timedelta(days=7)),  # either day field
        ("*/20 5-8 * * *", timedelta(hours=20, minutes=20)),
        ("0 0 30 2 *", None),
    ],
)
def test_max_gap(schedule, gap):
    assert cron.max_gap(schedule) == gap


# --- scheduled runs -------------------------------------------------------------------------


def _runlog(rows, newest=1000):
    sent = []

    def respond(request):
        sql = request.content.decode()
        sent.append((sql, dict(request.url.params)))
        if "max(timestamp)) AS t" in sql:
            return httpx.Response(200, text=json.dumps({"t": newest, "n": "1"}) + "\n")
        return httpx.Response(200, text="".join(json.dumps(r) + "\n" for r in rows))

    wanted = runs.jobs([runtime_spec()])
    log = runs.RunLog("http://ch", "u", "p", "logs.app_logs", "dev", wanted,
                      transport=httpx.MockTransport(respond))  # fmt: skip
    return log, sent


def _row(last, success=0, finished=0, failure=0, failed=0, component="planner"):
    return {
        "service": "jobs", "component": component, "last_message": last, "last": 900,
        "success": success, "finished": str(finished), "failure": failure, "failed": str(failed),
    }  # fmt: skip


def test_jobs_allow_twice_the_gap_plus_the_timeout():
    (job,) = runs.jobs([runtime_spec()])
    assert (job.service, job.component) == ("jobs", "planner")
    assert job.allowed_s == 1800  # */5 with a 10m timeout: 20m, so the minimum


def test_run_states():
    log, sent = _runlog([_row("run failed", 500, 3, 900, 1), _row("run failed", component="x")])
    asyncio.run(log.refresh())
    assert list(log.states) == [("jobs", "planner")]  # only components of the services
    st = log.states[("jobs", "planner")]
    assert (st.last_failed, st.last_success_s, st.due_s(1800)) == (True, 500, 2300)
    assert log.newest_s == 1000
    assert sent[1][1]["param_env"] == "dev"


def test_run_without_a_success_is_due_from_its_first_failure():
    log, _ = _runlog([_row("run failed", 0, 0, 700, 2)])
    asyncio.run(log.refresh())
    assert log.states[("jobs", "planner")].due_s(100) == 800


def test_run_log_errors():
    log = runs.RunLog("http://ch", "u", "p", "logs.app_logs", "dev", [],
                      transport=httpx.MockTransport(lambda r: httpx.Response(500)))  # fmt: skip
    with pytest.raises(runs.RunsError):
        asyncio.run(log.refresh())
    with pytest.raises(ValueError):
        runs.RunLog("http://ch", "u", "p", "logs; DROP", "dev", [])


# --- lag ------------------------------------------------------------------------------------


def test_reading():
    r = Reading(0, low=100, high=200, committed=150)
    assert (r.lag, r.skipped, r.unread_share) == (50, 0, 0.5)
    lost = Reading(0, low=100, high=200, committed=40)
    assert (lost.lag, lost.skipped) == (100, 60)
    assert Reading(0, 100, 200, None).lag == 100
    assert Reading(0, 100, 200, None).skipped == 0
    assert Reading(0, 0, 10, 50).lag == 0  # the topic was recreated
    assert Reading(0, 5, 5, 5).unread_share == 0


def test_targets_cover_every_kind_of_consumer():
    got = targets([runtime_spec()], "dev")
    assert [(t.group, t.topic, t.retention_s) for t in got] == [("dev.jobs.router", TOPIC, 3600.0)]


class _F:
    def __init__(self, v):
        self.v = v

    def result(self):
        return self.v


class FakeAdmin:
    """One partition with a message every second from t=0 to ``end``."""

    def __init__(self, end, committed):
        self.end, self.committed = end, committed
        self.topics = {}

    def list_topics(self, timeout):
        t = type("T", (), {"error": None, "partitions": {0: None}})()
        return type("M", (), {"topics": {TOPIC: t}})()

    def list_offsets(self, request, request_timeout):
        out = {}
        for tp, spec in request.items():
            ts = getattr(spec, "_value", None)
            if ts == -2:
                off = 0
            elif ts == -1:
                off = self.end
            else:
                s = max(0, -(-ts // 1000))
                off = s if s < self.end else -1
            out[tp] = _F(type("R", (), {"offset": off})())
        return out

    def list_consumer_group_offsets(self, request, request_timeout):
        tp = request[0].topic_partitions[0]
        tp.offset = self.committed
        return {request[0].group_id: _F(request[0])}


@pytest.mark.parametrize(("committed", "age"), [(1000, 0), (500, 500), (100, 900), (-1001, 1000)])
def test_age_of_the_oldest_unread_message(committed, age):
    t = Target("g", TOPIC, "jobs", 3600.0)
    r = LagMonitor(FakeAdmin(1000, committed), [t], clock=lambda: 1000.0).measure()
    (x,) = r.readings[t]
    assert x.oldest_unread_s == pytest.approx(age, abs=3600 * 0.005)


def test_missing_topic_is_an_error_not_a_zero():
    t = Target("g", "dev.other.v1", "jobs", 3600.0)
    r = LagMonitor(FakeAdmin(10, 0), [t]).measure()
    assert t in r.errors and not r.readings


def test_monitor_settings():
    from leasehold.api.settings import Settings

    base = {"LEASEHOLD_ENV": "dev", "LEASEHOLD_DSN": "d", "LEASEHOLD_SERVICES": "/s"}
    s = Settings.from_env(base)
    assert s.monitor_services is None and s.paused_runs == frozenset()
    s = Settings.from_env(
        base | {"LEASEHOLD_MONITOR_SERVICES": "a, b", "LEASEHOLD_PAUSED_RUNS": "a.planner"}
    )
    assert s.monitor_services == {"a", "b"} and s.paused_runs == {"a.planner"}
