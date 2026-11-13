"""Dashboards rendered from service.yml, and the dashboards section of the spec."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from leasehold.grafana import Datasources, plan
from leasehold.spec import SpecError, find_specs, load_spec, read_dashboard_files

FIXTURES = Path(__file__).parent / "fixtures"


def _services():
    out = []
    for path in find_specs([FIXTURES]):
        spec = load_spec(path)
        out.append((spec, read_dashboard_files(path, spec)))
    return out


def _by_uid(p):
    return {d.uid: d for d in p.dashboards}


def _all(node, key):
    """Every value of ``key`` anywhere in a JSON tree."""
    if isinstance(node, dict):
        for k, v in node.items():
            if k == key:
                yield v
            yield from _all(v, key)
    elif isinstance(node, list):
        for v in node:
            yield from _all(v, key)


def test_uids_titles_and_folders():
    prod = _by_uid(plan(_services(), "prod"))
    assert set(prod) == {
        "lh-prod--cockpit",
        "lh-prod--alerts",
        "lh--fleet",
        "lh-prod-certwatch",
        "lh-prod-certwatch--logs",
        "lh-prod-resolver",
    }
    assert prod["lh--fleet"].folder_uid == "lhf-prod--platform"
    assert prod["lh-prod-certwatch"].title == "certwatch"
    assert prod["lh-prod-certwatch"].folder_uid == "lhf-prod-certwatch"
    dev = _by_uid(plan(_services(), "dev"))
    assert "lh--fleet" not in dev  # one cluster, one fleet dashboard (prod plan)
    assert dev["lh-dev-certwatch"].title == "certwatch (dev)"
    assert dev["lh-dev-certwatch--logs"].title == "certwatch: Logs per CT log (dev)"


def test_prod_and_dev_never_share_uids():
    prod = plan(_services(), "prod")
    dev = plan(_services(), "dev")
    assert not {d.uid for d in prod.dashboards} & {d.uid for d in dev.dashboards}
    assert not {f.uid for f in prod.folders} & {f.uid for f in dev.folders}


def test_uids_fit_grafana_limit():
    for env in ("prod", "abcdefghijkl"):
        p = plan(_services(), env)
        assert all(len(d.uid) <= 40 for d in p.dashboards)
        assert all(len(f.uid) <= 40 for f in p.folders)


def test_environment_specific_queries():
    d = _by_uid(plan(_services(), "dev"))["lh-dev-certwatch"].dashboard
    exprs = list(_all(d, "expr"))
    sqls = list(_all(d, "rawSql"))
    # Nomad namespace and env label of dev, not prod.
    assert any('namespace="dev", exported_job="certwatch-worker"' in e for e in exprs)
    assert any('namespace="dev", parent_id="certwatch-planner"' in e for e in exprs)
    assert any('env="dev", task_type="certwatch.chunk"' in e for e in exprs)
    assert any('topic="dev.certwatch.entries.v1"' in e for e in exprs)
    assert any('group="dev.certwatch.clickhouse.domains"' in e for e in exprs)
    # ClickHouse: the dev database, in the cockpit tile and the expects check.
    assert sqls and all("dev_certwatch." in s for s in sqls)
    assert not any("{database}" in s or "{namespace}" in s for s in sqls + exprs)


def test_tiles_on_cockpit_and_service_dashboard():
    p = _by_uid(plan(_services(), "prod"))
    cockpit = p["lh-prod--cockpit"].dashboard
    titles = [x["title"] for x in cockpit["panels"]]
    assert titles.count("Domains found (24h)") == 1
    assert [x["title"] for x in cockpit["panels"] if x["type"] == "row"] == [
        "certwatch",
        "resolver",
    ]
    service = p["lh-prod-certwatch"].dashboard
    assert service["panels"][0] == service["panels"][0] | {"type": "row", "title": "Overview"}
    tile = next(x for x in service["panels"] if x["title"] == "Domains found (24h)")
    steps = tile["fieldConfig"]["defaults"]["thresholds"]["steps"]
    assert steps == [{"color": "red", "value": None}, {"color": "green", "value": 1}]


def test_variables_and_tags():
    d = _by_uid(plan(_services(), "dev"))["lh-dev-certwatch"].dashboard
    variables = {v["name"]: v["query"] for v in d["templating"]["list"]}
    assert variables == {"env": "dev", "namespace": "dev", "database": "dev_certwatch"}
    assert d["tags"] == ["leasehold", "env:dev", "service:certwatch"]


def test_own_dashboard_is_rewritten():
    d = _by_uid(plan(_services(), "prod", Datasources(clickhouse="ch-prod")))[
        "lh-prod-certwatch--logs"
    ].dashboard
    assert "id" not in d and "version" not in d
    assert d["uid"] == "lh-prod-certwatch--logs"
    # Exported environment tags replaced; the service's own tags kept.
    assert d["tags"] == ["certwatch", "leasehold", "env:prod", "service:certwatch"]
    # Our variables replace the exported ones; others stay.
    variables = {v["name"]: v.get("query") for v in d["templating"]["list"]}
    assert variables["database"] == "certwatch" and variables["log"] == "argon,xenon"
    # Data sources point at this Grafana's UIDs.
    assert {r["uid"] for r in _all(d, "datasource")} == {"ch-prod"}


def test_panels_do_not_overlap():
    for dash in plan(_services(), "prod").dashboards:
        if dash.source.startswith("file:"):
            continue
        cells = set()
        for panel in dash.dashboard["panels"]:
            g = panel["gridPos"]
            assert g["x"] + g["w"] <= 24
            for x in range(g["x"], g["x"] + g["w"]):
                for y in range(g["y"], g["y"] + g["h"]):
                    assert (x, y) not in cells, (dash.uid, panel["title"])
                    cells.add((x, y))


def test_plan_is_deterministic():
    a = plan(_services(), "prod")
    b = plan(_services(), "prod")
    assert [json.dumps(d.dashboard, sort_keys=True) for d in a.dashboards] == [
        json.dumps(d.dashboard, sort_keys=True) for d in b.dashboards
    ]


# --- spec -------------------------------------------------------------------------------------


def _write(tmp_path, mutate):
    data = yaml.safe_load((FIXTURES / "certwatch" / "service.yml").read_text())
    mutate(data)
    path = tmp_path / "service.yml"
    path.write_text(yaml.safe_dump(data))
    (tmp_path / "dashboards").mkdir(exist_ok=True)
    (tmp_path / "dashboards" / "logs.json").write_text(
        (FIXTURES / "certwatch" / "dashboards" / "logs.json").read_text()
    )
    return path


def _tile(**kw):
    return {"title": "t", "datasource": "prometheus", "query": "1"} | kw


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda d: d["dashboards"].update(cockpit=[_tile()] * 5), "at most 4"),
        (
            lambda d: d["dashboards"].update(
                cockpit=[_tile(datasource="clickhouse", query="SELECT 1 FROM certwatch.x")]
            ),
            "hardcodes database",
        ),
        (
            lambda d: (
                d.pop("clickhouse"),
                d.pop("expects"),
                d["dashboards"].update(cockpit=[_tile(datasource="clickhouse", query="SELECT 1")]),
            ),
            "needs a clickhouse section",
        ),
        (
            lambda d: d["dashboards"].update(
                cockpit=[_tile(thresholds=[{"color": "red", "from": 1}])]
            ),
            "base colour",
        ),
        (
            lambda d: d["dashboards"].update(
                cockpit=[
                    _tile(
                        thresholds=[
                            {"color": "red"},
                            {"color": "green", "from": 5},
                            {"color": "blue", "from": 2},
                        ]
                    )
                ]
            ),
            "must increase",
        ),
        (lambda d: d["dashboards"].update(files=["../x.json"]), "inside the service"),
        (lambda d: d["dashboards"].update(files=["dashboards/missing.json"]), "missing.json"),
    ],
)
def test_dashboards_section_rejected(tmp_path, mutate, message):
    with pytest.raises(SpecError) as e:
        load_spec(_write(tmp_path, mutate))
    assert any(message in m for m in e.value.messages), e.value.messages


def test_dashboard_file_must_be_a_dashboard(tmp_path):
    path = _write(tmp_path, lambda d: None)
    (tmp_path / "dashboards" / "logs.json").write_text('{"title": "no panels"}')
    with pytest.raises(SpecError, match="not a Grafana dashboard"):
        load_spec(path)


def _balanced(expr: str) -> bool:
    depth = 0
    for ch in expr:
        depth += {"(": 1, ")": -1}.get(ch, 0)
        if depth < 0:
            return False
    return depth == 0 and expr.count('"') % 2 == 0


def test_fleet_dashboard():
    d = _by_uid(plan(_services(), "prod"))["lh--fleet"].dashboard
    exprs = list(_all(d, "expr"))
    assert len(exprs) > 30 and all(_balanced(e) for e in exprs)
    assert d["tags"] == ["leasehold", "env:prod", "fleet"]
    assert [v["name"] for v in d["templating"]["list"]] == [
        "node_class",
        "namespace",
        "fit_cpu",
        "fit_mem",
    ]
    rows = [p["title"] for p in d["panels"] if p["type"] == "row"]
    assert rows == ["Fleet now", "Over time", "Nodes", "Jobs over the selected period"]
    tables = [p for p in d["panels"] if p["type"] == "table"]
    for t in tables:
        assert all(x["instant"] and x["format"] == "table" for x in t["targets"])
        names = {o["matcher"]["options"] for o in t["fieldConfig"]["overrides"]}
        calcs = [x["options"] for x in t["transformations"] if x["id"] == "calculateField"]
        assert names == {f"Value #{x['refId']}" for x in t["targets"]} | {c["alias"] for c in calcs}
        # Calculated columns only refer to query fields or earlier calculated columns.
        known = {f"Value #{x['refId']}" for x in t["targets"]}
        for c in calcs:
            for side in ("left", "right"):
                assert c["binary"][side]["matcher"]["options"] in known
            known.add(c["alias"])
    jobs = next(t for t in tables if t["title"].startswith("Jobs"))
    assert len(jobs["targets"]) == 7
    # Node memory metrics are MB, allocation memory metrics bytes.
    assert not any("nomad_client_allocs_memory_allocated" in e and "1048576" in e for e in exprs)


def test_service_dashboard_resources():
    d = _by_uid(plan(_services(), "dev"))["lh-dev-certwatch"].dashboard
    exprs = list(_all(d, "expr"))
    assert not any("memory_rss" in e or "allocs_restart" in e for e in exprs)
    assert any(
        'leasehold_container_memory_peak_bytes{env="dev", service="certwatch", '
        'component="worker"}' in e
        for e in exprs
    )
    assert any("leasehold_worker_tasks_total" in e for e in exprs)
    titles = [p["title"] for p in d["panels"]]
    assert "worker: CPU per task" in titles and "planner: CPU per task" not in titles


def test_apply_only_reduces_the_plan():
    from leasehold.grafana.apply import only

    p = only(plan(_services(), "prod"), {"lh--fleet"})
    assert [d.uid for d in p.dashboards] == ["lh--fleet"]
    assert [f.uid for f in p.folders] == ["lhf-prod--platform"]


def test_alert_inbox_dashboard():
    p = _by_uid(plan(_services(), "dev"))
    inbox = p["lh-dev--alerts"]
    assert inbox.folder_uid == "lhf-dev--platform"
    d = inbox.dashboard
    assert d["title"] == "Alert inbox (dev)"
    assert [v["name"] for v in d["templating"]["list"]] == ["status", "service", "priority"]
    sql = [t["rawSql"] for panel in d["panels"] for t in panel.get("targets", [])]
    assert sql and all("dev_leasehold.alerts" in q for q in sql)
    assert all("${status:singlequote}" in q for q in sql)
    assert (
        _by_uid(plan(_services(), "prod"))["lh-prod--alerts"]
        .dashboard["panels"][0]["targets"][0]["rawSql"]
        .count("leasehold.alerts")
        == 1
    )
