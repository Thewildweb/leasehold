"""Snapshot datasets: spec, schema files, naming, plan, storage user, loader job, manifest."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from leasehold import naming
from leasehold.clickhouse.schema import SchemaFileError
from leasehold.nomad.render import Platform
from leasehold.nomad.render import plan as nomad_plan
from leasehold.snapshots import plan as sp
from leasehold.snapshots.loader import LoadError, Settings, Window, _brace
from leasehold.snapshots.manifest import Manifest, SnapshotWriter
from leasehold.snapshots.schema import parse_snapshot
from leasehold.spec import ServiceSpec
from leasehold.storage import plan as storage_plan

TABLE_SQL = """
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


def spec_dict(**snap) -> dict:
    dataset = {
        "bucket": "snapshots",
        "node_class": "processing",
        "tables": {
            "adressen": {"schema_file": "adressen.sql"},
            "mutaties": {"schema_file": "adressen.sql", "mode": "append"},
        },
        "checks": [{"table": "adressen", "min_rows": 1, "max_change": 0.05}],
    } | snap
    return {
        "spec_version": 1,
        "name": "bag",
        "description": "x",
        "owner": "t",
        "components": {
            "extract": {
                "kind": "scheduled",
                "schedule": "0 6 * * *",
                "node_class": "processing",
                "resources": {"cpu": 100, "memory": 64},
                "writes_buckets": ["snapshots"],
            }
        },
        "buckets": {"snapshots": {"retention": "400d"}},
        "clickhouse": {"database": "bag", "shared": {"adressen": {"description": "x"}}},
        "snapshots": {"bag": dataset},
    }


@pytest.fixture
def spec() -> ServiceSpec:
    return ServiceSpec.model_validate(spec_dict())


@pytest.fixture
def base(tmp_path: Path) -> Path:
    (tmp_path / "adressen.sql").write_text(TABLE_SQL)
    return tmp_path


# --- spec --------------------------------------------------------------------------------------


def test_spec_valid_and_loader_component(spec):
    loaders = spec.snapshot_loader_components()
    assert list(loaders) == ["load-bag"]
    assert "load-bag" in spec.all_components()
    assert spec.clickhouse_tables() == {"adressen", "mutaties"}


@pytest.mark.parametrize(
    "change, message",
    [
        ({"bucket": "nope"}, "unknown bucket 'nope'"),
        ({"checks": [{"table": "other", "min_rows": 1}]}, "is not a table of this dataset"),
        ({"checks": [{"table": "mutaties", "max_change": 0.1}]}, "only for replace tables"),
        ({"tables": {"snapshot_loads": {"schema_file": "x.sql"}}}, "reserved table"),
    ],
)
def test_spec_errors(change, message):
    with pytest.raises(ValueError, match=message):
        ServiceSpec.model_validate(spec_dict(**change))


def test_spec_needs_clickhouse():
    d = spec_dict()
    del d["clickhouse"]
    with pytest.raises(ValueError, match="add a clickhouse section"):
        ServiceSpec.model_validate(d)


def test_load_prefix_reserved():
    d = spec_dict()
    d["components"]["load-x"] = d["components"].pop("extract")
    with pytest.raises(ValueError, match="reserved for snapshot loaders"):
        ServiceSpec.model_validate(d)


def test_check_needs_a_limit():
    with pytest.raises(ValueError, match="min_rows or max_change"):
        ServiceSpec.model_validate(spec_dict(checks=[{"table": "adressen"}]))


# --- schema files and naming -------------------------------------------------------------------


def test_schema_ok():
    s = parse_snapshot(TABLE_SQL)
    assert "{snapshot}" in s.select


@pytest.mark.parametrize(
    "broken, message",
    [
        (TABLE_SQL.replace("snapshot LowCardinality(String),\n", ""), "column 'snapshot"),
        (TABLE_SQL.replace("PARTITION BY snapshot\n", ""), "PARTITION BY snapshot"),
        (TABLE_SQL.replace("ENGINE = MergeTree", "ENGINE = Memory"), "MergeTree"),
        (TABLE_SQL.replace("{snapshot} AS snapshot", "'x' AS snapshot"), "{snapshot}"),
    ],
)
def test_schema_errors(broken, message):
    with pytest.raises(SchemaFileError, match=message):
        parse_snapshot(broken)


def test_names():
    assert naming.snapshot_loader_component("bag") == "load-bag"
    assert naming.snapshot_table("adressen") == "adressen__snapshots"
    run = "0123456789abcdef" * 2
    assert (
        naming.snapshot_staging_table("adressen", "2026-11-08", run)
        == f"_staging_adressen_2026_11_08_{run}"
    )
    with pytest.raises(naming.NamingError):
        naming.snapshot_staging_table("adressen", "2026-11-08", "x")
    assert naming.snapshot_manifest_key("bag", "2026-11-08") == "bag/2026-11-08/manifest.json"
    assert naming.snapshot_loader_user("dev", "bag", "bag") == "dev.bag.load-bag"
    assert naming.clickhouse_definer_user("prod", "bag") == "bag.clickhouse.definer"
    assert naming.snapshot_collection("dev", "bag", "snapshots") == "leasehold_dev_bag_snapshots"
    assert naming.snapshot_collection("prod", "my-svc", "a-b") == "leasehold_my_svc_a_b"
    with pytest.raises(naming.NamingError):
        naming.check_snapshot_id("../x")
    with pytest.raises(naming.NamingError):
        naming.check_snapshot_id("a b")


# --- plan --------------------------------------------------------------------------------------


def test_plan_views_and_grants(spec, base):
    (p,) = sp.plan([(spec, base)], "dev")
    assert p.database == "dev_bag"
    (d,) = p.datasets
    assert d.bucket == "dev-bag-snapshots"
    tables = {t.name: t for t in d.tables}
    replace = tables["adressen"].view_select()
    assert (
        "snapshot = (SELECT snapshot FROM dev_bag.snapshot_current WHERE dataset = 'bag')"
        in replace
    )
    append = tables["mutaties"].view_select()
    assert "status = 'current'" in append and "snapshot <=" in append
    view = tables["adressen"].create_view_sql(p.definer)
    assert "DEFINER = `dev.bag.clickhouse.definer` SQL SECURITY DEFINER" in view
    rows = {g.row() for g in d.grants(("ops.heavy_jobs",))}
    assert ("CREATE TABLE", "dev_bag", "_staging_", None) in rows
    assert ("ALTER DELETE", "dev_bag", "adressen__snapshots", None) in rows
    assert ("NAMED COLLECTION", None, None, "leasehold_dev_bag_snapshots") in rows
    assert ("READ", None, None, "S3") in rows
    assert ("SELECT", "ops", "heavy_jobs", None) in rows
    # Nothing on views, sinks or the whole database.
    assert not any(r[1] == "dev_bag" and r[2] is None for r in rows)
    assert ("INSERT", "dev_bag", "adressen", None) not in rows


def test_grant_on_prefix_is_unquoted():
    g = sp.Grant("SELECT", "dev_bag", "_staging_*")
    assert g.on() == "`dev_bag`._staging_*"
    assert sp.Grant("NAMED COLLECTION", object="c").on() == "`c`"
    assert sp.Grant("READ", object="S3").on() == "S3"


def test_insert_sql(spec, base):
    (p,) = sp.plan([(spec, base)], "dev")
    t = p.datasets[0].tables[0]
    sql = t.insert_sql("dev_bag._staging_adressen_s1", "s1", "s3(c, filename = 'k')")
    assert "'s1' AS snapshot" in sql and "FROM s3(c, filename = 'k')" in sql


def test_storage_plan_has_clickhouse_reader(spec):
    p = storage_plan([spec], "dev")
    users = {u.user: u for u in p.users}
    reader = users["dev.bag.clickhouse"]
    assert reader.read == {"dev-bag-snapshots"} and not reader.write
    assert reader.variable == "leasehold/bag/clickhouse-storage"
    assert users["dev.bag.extract"].write == {"dev-bag-snapshots"}


def test_loader_job(spec, base):
    window = {"start": "05:00", "end": "09:00", "timezone": "Europe/Amsterdam"}
    settings = sp.loader_settings([(spec, base)], "dev", window, "SELECT 0")
    platform = Platform(
        registry="ghcr.io/x", api_url="http://api", kafka_bootstrap="k:9092",
        clickhouse_url="http://ch:8123", snapshot_image="ghcr.io/x/leasehold-base:abc",
        snapshot_settings=settings, s3_endpoint="http://minio:9000",
    )  # fmt: skip
    jobs = {j.job_id: j for j in nomad_plan([spec], "dev", "0123abc", platform).jobs}
    job = jobs["bag-load-bag"].job
    assert job["Periodic"]["Specs"] == ["*/20 5-8 * * *"]
    assert job["Periodic"]["TimeZone"] == "Europe/Amsterdam"
    task = job["TaskGroups"][0]["Tasks"][0]
    assert task["Config"] == {
        "image": "ghcr.io/x/leasehold-base:abc",
        "command": "leasehold",
        "args": ["snapshot", "load"],
    }
    env = task["Env"]
    s = Settings.from_json(env["LEASEHOLD_SNAPSHOT"])
    assert s.collection == "leasehold_dev_bag_snapshots" and s.busy_query == "SELECT 0"
    assert set(s.tables) == {"adressen", "mutaties"}
    assert env["LEASEHOLD_CLICKHOUSE_URL"] == "http://ch:8123"
    assert "nomad/jobs/bag-load-bag" in task["Templates"][0]["EmbeddedTmpl"]
    assert "Networks" not in job["TaskGroups"][0]


def test_loader_job_needs_image(spec, base):
    settings = sp.loader_settings([(spec, base)], "dev")
    platform = Platform(registry="r", api_url="a", clickhouse_url="c", snapshot_settings=settings,
                        s3_endpoint="s")  # fmt: skip
    with pytest.raises(ValueError, match="snapshot image"):
        nomad_plan([spec], "dev", "0123abc", platform)


# --- window ------------------------------------------------------------------------------------


def _at(s: str) -> datetime:
    return datetime.fromisoformat(s).replace(tzinfo=ZoneInfo("Europe/Amsterdam"))


def test_window():
    w = Window(start="05:00", end="09:00", timezone="Europe/Amsterdam", skip_first_sunday=True)
    assert w.allows(_at("2026-11-23T05:00"))
    assert w.allows(_at("2026-11-23T08:59"))
    assert not w.allows(_at("2026-11-23T09:00"))
    assert not w.allows(_at("2026-11-23T04:59"))
    assert not w.allows(_at("2026-12-06T06:00"))  # first Sunday
    assert w.allows(_at("2026-12-13T06:00"))  # second Sunday
    assert w.cron() == "*/20 5-8 * * *"
    assert Window(start="05:00", end="09:30").cron() == "*/20 5-9 * * *"
    assert Window().cron() == "*/20 0-23 * * *"


def test_environment_window_rejects_midnight():
    from leasehold.environment import Window as EnvWindow

    with pytest.raises(ValueError, match="end after it starts"):
        EnvWindow(start="22:00", end="02:00")
    with pytest.raises(ValueError, match="unknown timezone"):
        EnvWindow(timezone="Mars/Olympus")


# --- manifest ----------------------------------------------------------------------------------


class FakeStore:
    def __init__(self):
        self.objects: dict[tuple[str, str], bytes] = {}

    def put_file(self, bucket, key, path, content_type):
        self.objects[(bucket, key)] = Path(path).read_bytes()

    def put_bytes(self, bucket, key, data, content_type):
        self.objects[(bucket, key)] = data


def _parquet(path: Path, n: int) -> Path:
    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(pa.table({"id": [str(i) for i in range(n)], "postcode": ["1234AB"] * n}), path)
    return path


def test_writer_uploads_then_manifest(tmp_path):
    store = FakeStore()
    w = SnapshotWriter(store, "dev-bag-snapshots", "bag", "2026-11-08", producer="t")
    w.add_file("adressen", str(_parquet(tmp_path / "a.parquet", 3)))
    w.add_file("adressen", str(_parquet(tmp_path / "b.parquet", 2)))
    assert ("dev-bag-snapshots", "bag/2026-11-08/manifest.json") not in store.objects
    m = w.commit(source_date="2026-11-08")
    raw = store.objects[("dev-bag-snapshots", "bag/2026-11-08/manifest.json")]
    back = Manifest.model_validate_json(raw)
    assert back == m
    t = back.tables["adressen"]
    assert t.rows == 5 and [f.rows for f in t.files] == [3, 2]
    assert [f.key for f in t.files] == [
        "bag/2026-11-08/adressen/part-00000.parquet",
        "bag/2026-11-08/adressen/part-00001.parquet",
    ]
    assert t.columns == [("id", "string"), ("postcode", "string")]


def test_writer_refuses_other_columns(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    w = SnapshotWriter(FakeStore(), "b", "bag", "s1")
    w.add_file("adressen", str(_parquet(tmp_path / "a.parquet", 1)))
    pq.write_table(pa.table({"other": [1]}), tmp_path / "c.parquet")
    with pytest.raises(ValueError, match="other columns"):
        w.add_file("adressen", str(tmp_path / "c.parquet"))


def test_manifest_key_checks():
    m = Manifest.model_validate(
        {
            "dataset": "bag",
            "snapshot": "s1",
            "created_at": "2026-11-22T00:00:00Z",
            "tables": {
                "adressen": {
                    "rows": 2,
                    "columns": [["id", "string"]],
                    "files": [
                        {
                            "key": "bag/s2/adressen/x.parquet",
                            "rows": 1,
                            "bytes": 1,
                            "sha256": "0" * 64,
                        },
                        {"key": "bag/s1/adressen/{a,b}", "rows": 0, "bytes": 1, "sha256": "0" * 64},
                    ],
                }
            },
        }
    )
    problems = m.check_keys()
    assert any("bag/s2/adressen/x.parquet" in p for p in problems)
    assert any("{a,b}" in p for p in problems)
    assert any("rows 2 != the sum" in p for p in problems)


def test_brace():
    assert _brace(["d/x/a.parquet", "d/x/b.parquet"]) == "d/x/{a.parquet,b.parquet}"
    with pytest.raises(LoadError):
        _brace(["d/x/a.parquet", "d/y/b.parquet"])


def test_settings_roundtrip(spec, base):
    settings = sp.loader_settings([(spec, base)], "prod", {"start": "05:00", "end": "09:00"})
    s = Settings.from_json(json.dumps(settings["bag-load-bag"]))
    assert s.database == "bag" and s.bucket == "bag-snapshots" and not s.replace_only
    assert s.window.start == "05:00" and s.checks == [["adressen", 1, 0.05]]


def test_disk_becomes_ephemeral_disk(base):
    d = spec_dict()
    d["components"]["extract"]["resources"]["disk"] = 30000
    spec = ServiceSpec.model_validate(d)
    settings = sp.loader_settings([(spec, base)], "dev")
    platform = Platform(registry="r", api_url="a", clickhouse_url="c", snapshot_image="i",
                        snapshot_settings=settings, s3_endpoint="s")  # fmt: skip
    jobs = {j.job_id: j for j in nomad_plan([spec], "dev", "0123abc", platform).jobs}
    assert jobs["bag-extract"].job["TaskGroups"][0]["EphemeralDisk"] == {"SizeMB": 30000}
    assert "EphemeralDisk" not in jobs["bag-load-bag"].job["TaskGroups"][0]
