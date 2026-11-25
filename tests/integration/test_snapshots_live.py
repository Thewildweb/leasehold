"""Snapshot datasets end to end against the dev stack: apply (storage + ClickHouse), a
producer writing snapshots with ``SnapshotWriter``, the loader as its own ClickHouse user.

    LEASEHOLD_TEST_CLICKHOUSE=http://localhost:18123 LEASEHOLD_TEST_S3=http://localhost:19000 \\
    LEASEHOLD_TEST_NOMAD=http://localhost:14646 \\
      uv run pytest tests/integration/test_snapshots_live.py

The dev stack's ClickHouse reaches its MinIO as ``http://minio:9000``
(``LEASEHOLD_TEST_S3_FROM_CLICKHOUSE``), and its admin user needs named collection
control (``dev/clickhouse/named-collections.xml``).
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime
from pathlib import Path

import pytest

from leasehold import naming, steps
from leasehold.clickhouse.apply import ClickHouse, ClickHouseError
from leasehold.nomad.apply import Nomad
from leasehold.snapshots import loader as lh_loader
from leasehold.snapshots.loader import Loader, Settings
from leasehold.snapshots.manifest import SnapshotWriter
from leasehold.snapshots.plan import loader_settings
from leasehold.spec import ServiceSpec
from leasehold.storage import apply as sa
from leasehold.storage.client import ObjectStore

CH = os.environ.get("LEASEHOLD_TEST_CLICKHOUSE")
S3 = os.environ.get("LEASEHOLD_TEST_S3")
S3_SECRET = os.environ.get("LEASEHOLD_TEST_S3_SECRET", "dev-admin-secret")
S3_FROM_CH = os.environ.get("LEASEHOLD_TEST_S3_FROM_CLICKHOUSE", "http://minio:9000")
NOMAD = os.environ.get("LEASEHOLD_TEST_NOMAD")
CH_USER, CH_PASSWORD = "admin", "dev-admin"
if CH and "@" in CH:  # credentials in the URL, as the other live tests take them
    scheme, rest = CH.split("://", 1)
    creds, host = rest.split("@", 1)
    CH_USER, CH_PASSWORD = creds.split(":", 1)
    CH = f"{scheme}://{host}"

pytestmark = pytest.mark.skipif(
    not (CH and S3 and NOMAD),
    reason="LEASEHOLD_TEST_CLICKHOUSE / LEASEHOLD_TEST_S3 / LEASEHOLD_TEST_NOMAD not set",
)
ENV = "dev"  # a namespace the dev stack's Nomad has

TABLE = """
CREATE TABLE {table}
(
    snapshot LowCardinality(String),
    id String,
    postcode String
)
ENGINE = MergeTree
PARTITION BY snapshot
ORDER BY (postcode, id);

SELECT {snapshot} AS snapshot, id, postcode FROM {source};
"""


def _spec(name: str) -> ServiceSpec:
    return ServiceSpec.model_validate(
        {
            "spec_version": 1,
            "name": name,
            "description": "x",
            "owner": "t",
            "components": {
                "extract": {
                    "kind": "scheduled",
                    "schedule": "@daily",
                    "node_class": "processing",
                    "resources": {"cpu": 100, "memory": 64},
                    "writes_buckets": ["snapshots"],
                }
            },
            "buckets": {"snapshots": {}},
            "clickhouse": {"database": name, "shared": {"adressen": {"description": "x"}}},
            "snapshots": {
                "bag": {
                    "bucket": "snapshots",
                    "node_class": "processing",
                    "keep": 2,
                    "tables": {
                        "adressen": {"schema_file": "t.sql"},
                        "mutaties": {"schema_file": "t.sql", "mode": "append"},
                    },
                    "checks": [{"table": "adressen", "min_rows": 1, "max_change": 0.5}],
                }
            },
        }
    )


@pytest.fixture
def world(tmp_path):
    name = f"t{uuid.uuid4().hex[:8]}"
    (tmp_path / "t.sql").write_text(TABLE)
    ch = ClickHouse(CH, CH_USER, CH_PASSWORD)
    nomad = Nomad(NOMAD)
    st = sa.Storage.connect(S3, "admin", S3_SECRET)
    yield name, tmp_path, ch, nomad
    # Clean up everything the test created (the dev stack is shared).
    db = f"{ENV}_{name}"
    ch.execute(f"DROP DATABASE IF EXISTS {db} SYNC")
    for (u,) in ch.rows(f"SELECT name FROM system.users WHERE startsWith(name, '{ENV}.{name}.')"):
        ch.execute(f"DROP USER IF EXISTS `{u}`")
    for (c,) in ch.rows(f"SELECT name FROM system.named_collections WHERE name LIKE '%_{name}_%'"):
        ch.execute(f"DROP NAMED COLLECTION IF EXISTS `{c}`")
    for b in st.s3.list_buckets():
        if b.name.startswith(f"{ENV}-{name}-"):
            for o in st.s3.list_objects(b.name, recursive=True):
                st.s3.remove_object(b.name, o.object_name)
            st.s3.remove_bucket(b.name)
    for u in json.loads(st.admin.user_list() or "{}"):
        if u.startswith(f"{ENV}.{name}."):
            st.admin.user_remove(u)
    for p in json.loads(st.admin.policy_list() or "{}"):
        if p.startswith(f"leasehold.{ENV}.{name}."):
            st.admin.policy_remove(p)
    for prefix in (f"nomad/jobs/{name}-", f"leasehold/{name}/"):
        for path in nomad.var_list(prefix, ENV):
            nomad.var_delete(path, ENV)
    ch.close()
    nomad.close()


def _apply(spec, base, yes=True):
    out: list[str] = []
    storage = steps.storage(
        out.append, [spec], ENV, endpoint=S3, access_key="admin", secret_key=S3_SECRET,
        nomad_url=NOMAD, nomad_token=None, yes=yes, prune=False,
    )  # fmt: skip
    clickhouse = steps.clickhouse(
        out.append, [(spec, base)], ENV, url=CH, user=CH_USER, password=CH_PASSWORD,
        kafka_bootstrap="unused:9092", nomad_url=NOMAD, nomad_token=None,
        clickhouse_endpoint=S3_FROM_CH, yes=yes, prune=False,
    )  # fmt: skip
    return storage, clickhouse, out


def _writer(nomad, name, snapshot):
    items = nomad.var_get(f"nomad/jobs/{name}-extract", ENV)["Items"]
    store = ObjectStore.connect(S3, items["LEASEHOLD_S3_ACCESS_KEY"],
                                items["LEASEHOLD_S3_SECRET_KEY"])  # fmt: skip
    return SnapshotWriter(store, f"{ENV}-{name}-snapshots", "bag", snapshot, producer="test")


def _parquet(path: Path, ids: list[str]) -> str:
    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(pa.table({"id": ids, "postcode": ["1234AB"] * len(ids)}), path)
    return str(path)


def _publish(nomad, name, tmp, snapshot, adressen: int, mutaties: int, *, lie: int = 0):
    w = _writer(nomad, name, snapshot)
    w.add_file("adressen", _parquet(tmp / f"{snapshot}-a.parquet",
                                    [f"{snapshot}-{i}" for i in range(adressen)]))  # fmt: skip
    w.add_file("mutaties", _parquet(tmp / f"{snapshot}-m.parquet",
                                    [f"{snapshot}-m{i}" for i in range(mutaties)]))  # fmt: skip
    if lie:  # a manifest that claims more rows than the files have
        t = w.tables["adressen"]
        f = t.files[0].model_copy(update={"rows": t.files[0].rows + lie})
        w.tables["adressen"] = t.model_copy(update={"rows": t.rows + lie, "files": [f]})
    w.commit(source_date="2026-11-08")


def _loader(nomad, name, base, spec) -> Loader:
    settings = loader_settings([(spec, base)], ENV)[f"{name}-load-bag"]
    s = Settings.from_json(json.dumps(settings))
    items = nomad.var_get(f"nomad/jobs/{name}-load-bag", ENV)["Items"]
    ch = ClickHouse(CH, items["LEASEHOLD_CLICKHOUSE_USER"], items["LEASEHOLD_CLICKHOUSE_PASSWORD"])
    return Loader(ch, s, loader="test")


def _count(ch, sql) -> int:
    return int(ch.execute(sql + " FORMAT TSV").strip() or 0)


def test_load_check_switch_rollback(world):
    name, base, ch, nomad = world
    db = f"{ENV}_{name}"
    spec = _spec(name)

    # Apply: everything created, then nothing left to do.
    s, c, out = _apply(spec, base)
    assert s.applied and c.applied, out
    s, c, out = _apply(spec, base, yes=False)
    assert (s.changes, c.changes) == (0, 0), out

    ld = _loader(nomad, name, base, spec)
    view = f"SELECT count() FROM {db}.adressen"
    assert ld.run() == 0  # nothing published yet
    assert _count(ch, view) == 0

    # s1: loaded, switched, every table at once.
    _publish(nomad, name, base, "s1", adressen=4, mutaties=1)
    assert ld.run() == 0
    assert ld.current() == "s1"
    assert _count(ch, view) == 4
    assert _count(ch, f"SELECT count() FROM {db}.mutaties") == 1

    # A reader with SELECT on the view only reads it (the view runs as the definer).
    pw = uuid.uuid4().hex
    reader = f"{ENV}.{name}.reader"
    ch.execute(f"CREATE USER `{reader}` IDENTIFIED WITH sha256_password BY '{pw}'")
    ch.execute(f"GRANT SELECT ON {db}.adressen TO `{reader}`")
    rch = ClickHouse(CH, reader, pw)
    assert _count(rch, view) == 4
    with pytest.raises(ClickHouseError, match="Not enough privileges"):
        rch.execute(f"SELECT count() FROM {db}.adressen__snapshots")
    rch.close()

    # The loader can't touch other tables of the database.
    with pytest.raises(ClickHouseError, match="Not enough privileges"):
        ld.ch.execute(f"DROP TABLE {db}.adressen__snapshots")

    # s2 fails its check (4 → 10 rows is more than 50%): s1 stays, nothing left behind.
    _publish(nomad, name, base, "s2", adressen=10, mutaties=1)
    assert ld.run() == 1
    assert ld.current() == "s1"
    assert _count(ch, view) == 4
    assert ld.partitions("adressen") == {"s1"}
    staging = (
        f"SELECT count() FROM system.tables WHERE database = '{db}' AND name LIKE '_staging_%'"
    )
    assert _count(ch, staging) == 0
    last = ch.rows(f"SELECT status, error FROM {db}.snapshot_loads ORDER BY at DESC LIMIT 1")
    assert last[0][0] == "failed" and "checks failed" in last[0][1]

    # s3 passes, but the dataset has an append table, so s2 is tried first until it has
    # failed max_attempts (3) times. Then s3 loads; the append table shows s1 and s3,
    # not the never-current s2.
    _publish(nomad, name, base, "s3", adressen=5, mutaties=2)
    assert [ld.run() for _ in range(2)] == [1, 1]
    assert ld.current() == "s1"
    assert ld.run() == 0
    assert ld.current() == "s3"
    assert _count(ch, view) == 5
    assert _count(ch, f"SELECT count() FROM {db}.mutaties") == 3

    # s4's manifest lies about its rows: tried max_attempts (3) times, then skipped.
    _publish(nomad, name, base, "s4", adressen=5, mutaties=1, lie=1)
    assert [ld.run() for _ in range(3)] == [1, 1, 1]
    assert ld.current() == "s3"
    assert ld.run() == 0  # s4 is skipped now

    # What a crashed run leaves behind is cleaned up first.
    ch.execute(f"CREATE TABLE {db}._staging_adressen_s9 AS {db}.adressen__snapshots")
    ch.execute(f"INSERT INTO {db}.adressen__snapshots VALUES ('zz', 'x', '1234AB')")
    assert _count(ch, view) == 5  # an unattached snapshot is invisible
    _publish(nomad, name, base, "s5", adressen=6, mutaties=1)
    assert ld.run() == 0
    assert ld.current() == "s5"
    assert _count(ch, staging) == 0
    # keep: 2 → s1 dropped; s3 and s5 kept; zz (never current) gone.
    assert ld.partitions("adressen") == {"s3", "s5"}

    # Rollback: one row, every view follows. s1 is no longer kept.
    lh_loader.rollback(ch, ld.s, "s3")
    assert ld.current() == "s3"
    assert _count(ch, view) == 5
    assert _count(ch, f"SELECT count() FROM {db}.mutaties") == 3  # s1 + s3
    with pytest.raises(lh_loader.LoadError, match="no longer kept"):
        lh_loader.rollback(ch, ld.s, "s1")

    # Outside the window and with a busy check, the loader does nothing.
    ld.s.window = lh_loader.Window(start="00:00", end="00:01")
    ld._now = lambda: datetime.fromisoformat("2026-11-23T12:00:00+00:00")
    assert ld.run() == 0
    ld.s.window = lh_loader.Window()
    ld.s.busy_query = "SELECT 1"
    assert ld.run() == 0
    assert ld.current() == "s3"
    ld.s.busy_query = ""

    # The rollback holds: a normal run in the window doesn't reload s5.
    assert ld.held() == "s3"
    assert ld.run() == 0
    assert ld.current() == "s3"
    # Released: still no s5 (it was current before, so only an explicit load brings it
    # back); a newer snapshot loads.
    assert lh_loader.release(ch, ld.s) == "s3"
    with pytest.raises(lh_loader.LoadError, match="isn't held"):
        lh_loader.release(ch, ld.s)
    assert ld.run() == 0
    assert ld.current() == "s3"
    _publish(nomad, name, base, "s6", adressen=6, mutaties=1)
    assert ld.run() == 0
    assert ld.current() == "s6"
    assert _count(ch, view) == 6

    # An explicit load ends a hold.
    lh_loader.rollback(ch, ld.s, "s5")
    assert ld.held() == "s5"
    assert ld.run(force=True, snapshot="s6") == 0
    assert (ld.current(), ld.held()) == ("s6", "")
    ld.ch.close()


def test_rollback_to_a_snapshot_with_an_empty_table(world):
    name, base, ch, nomad = world
    db = f"{ENV}_{name}"
    spec = _spec(name)
    _apply(spec, base)
    ld = _loader(nomad, name, base, spec)
    _publish(nomad, name, base, "s1", adressen=4, mutaties=0)
    assert ld.run() == 0
    _publish(nomad, name, base, "s2", adressen=5, mutaties=2)
    assert ld.run() == 0
    assert ld.partitions("mutaties") == {"s2"}  # an empty table has no partition
    assert ld.empty_tables("s1") == {"mutaties"} and ld.empty_tables("s2") == set()

    lh_loader.rollback(ch, ld.s, "s1")
    assert ld.current() == "s1"
    assert _count(ch, f"SELECT count() FROM {db}.adressen") == 4
    assert _count(ch, f"SELECT count() FROM {db}.mutaties") == 0

    # A table with recorded rows must still have its partition.
    lh_loader.rollback(ch, ld.s, "s2")
    ch.execute(f"ALTER TABLE {db}.adressen__snapshots DROP PARTITION 's1'")
    with pytest.raises(lh_loader.LoadError, match="no longer kept in adressen"):
        lh_loader.rollback(ch, ld.s, "s1")
    ld.ch.close()


def test_reload_of_the_same_snapshot_replaces_its_rows(world):
    name, base, ch, nomad = world
    db = f"{ENV}_{name}"
    spec = _spec(name)
    _apply(spec, base)
    ld = _loader(nomad, name, base, spec)

    def rows(table):
        n = f"SELECT count(), uniqExact(snapshot, id) FROM {db}.{table}"
        return [tuple(map(int, ch.rows(n)[0])) for n in (n, n + "__snapshots")]

    _publish(nomad, name, base, "s1", adressen=4, mutaties=3)
    assert ld.run() == 0
    # The same files again: the same rows, once.
    assert ld.run(force=True, snapshot="s1") == 0
    assert rows("adressen") == [(4, 4)] * 2
    assert rows("mutaties") == [(3, 3)] * 2
    # A rollback to it and a reload.
    _publish(nomad, name, base, "s2", adressen=4, mutaties=1)
    assert ld.run() == 0
    lh_loader.rollback(ch, ld.s, "s1")
    assert ld.run(force=True, snapshot="s1") == 0
    assert rows("adressen") == [(4, 4), (8, 8)]
    assert rows("mutaties") == [(3, 3), (4, 4)]
    # Published again with fewer rows, and none in mutaties: the old rows go.
    _publish(nomad, name, base, "s1", adressen=3, mutaties=0)
    assert ld.run(force=True, snapshot="s1") == 0
    assert rows("adressen") == [(3, 3), (7, 7)]
    assert rows("mutaties") == [(0, 0), (1, 1)]
    assert ld.partitions("mutaties") == {"s2"}
    ld.ch.close()


def test_two_runs_leave_each_other_alone(world):
    name, base, ch, nomad = world
    db = f"{ENV}_{name}"
    spec = _spec(name)
    _apply(spec, base)
    one, two = _loader(nomad, name, base, spec), _loader(nomad, name, base, spec)
    _publish(nomad, name, base, "s1", adressen=4, mutaties=1)

    # Run one has finished another snapshot and is loading s1: staging and a partition.
    for status in ("started", "current"):
        one._record("s0", status)
    one._record("s1", "started")
    assert two.other_run_active()
    assert two.run(force=True) == 0
    assert two.current() == "s0"  # nothing loaded
    mine = naming.snapshot_staging_table("adressen", "s1", one.run_id)
    ch.execute(f"CREATE TABLE {db}.{mine} AS {db}.adressen__snapshots")
    stale = naming.snapshot_staging_table("adressen", "s1", "0" * 32)
    ch.execute(f"CREATE TABLE {db}.{stale} AS {db}.adressen__snapshots")
    ch.execute(f"INSERT INTO {db}.adressen__snapshots VALUES ('s1', 'x', '1234AB')")
    ch.execute(f"INSERT INTO {db}.adressen__snapshots VALUES ('zz', 'x', '1234AB')")

    def staging():
        q = f"SELECT name FROM system.tables WHERE database = '{db}' AND name LIKE '\\_staging%'"
        return {r[0] for r in ch.rows(q)}

    two.clean_up()
    assert staging() == {mine}
    assert two.partitions("adressen") == {"s1"}
    # Both passed the check at the same moment: run two's failure leaves run one's
    # partition, its load doesn't touch run one's staging table.
    two._record("s1", "started")
    two._undo("s1", [], ["adressen"])
    two._record("s1", "failed")
    assert two.partitions("adressen") == {"s1"}
    two.load(two.manifests()[0])
    assert staging() == {mine}
    assert _count(ch, f"SELECT count() FROM {db}.adressen") == 4
    # Run one ends: its staging table is a leftover.
    one._record("s1", "failed")
    two.clean_up()
    assert staging() == set()
    assert two.partitions("adressen") == {"s1"}
    one.ch.close()
    two.ch.close()


def test_clickhouse_reads_only_its_bucket(world):
    """The named collection can't be pointed at another bucket."""
    name, base, ch, nomad = world
    spec = _spec(name)
    _apply(spec, base)
    ld = _loader(nomad, name, base, spec)
    with pytest.raises(ClickHouseError, match="Override not allowed|BAD_ARGUMENTS"):
        ld.ch.execute(
            f"SELECT count() FROM s3({ld.s.collection}, url = 'http://minio:9000/other/x', "
            "format = 'One')"
        )
    ld.ch.close()


DERIVED = """
CREATE TABLE {table} (postcode String, n UInt64) ENGINE = MergeTree ORDER BY postcode;
SELECT postcode, count() AS n FROM {database}.adressen GROUP BY postcode;
"""


def test_derived_table_reads_a_snapshot_view(world):
    """In one apply on an empty environment: the snapshot view exists before the derived
    table's refresh view."""
    name, base, ch, nomad = world
    db = f"{ENV}_{name}"
    (base / "counts.sql").write_text(DERIVED)
    spec = _spec(name)
    data = spec.model_dump(mode="json", by_alias=True, exclude_unset=True)
    data["clickhouse"]["derived"] = {"counts": {"schema_file": "counts.sql", "refresh": "1h"}}
    spec = ServiceSpec.model_validate(data)

    s, c, out = _apply(spec, base)
    assert c.applied, out
    s, c, out = _apply(spec, base, yes=False)
    assert (s.changes, c.changes) == (0, 0), out

    _publish(nomad, name, base, "s1", adressen=4, mutaties=1)
    assert _loader(nomad, name, base, spec).run() == 0
    ch.execute(f"SYSTEM REFRESH VIEW {db}.counts_refresh")
    ch.execute(f"SYSTEM WAIT VIEW {db}.counts_refresh")
    assert ch.rows(f"SELECT postcode, n FROM {db}.counts") == [["1234AB", "4"]]
