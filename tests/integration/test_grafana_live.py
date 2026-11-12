"""Applying dashboards to a running Grafana (the dev stack's, port 13000).

Runs only when LEASEHOLD_TEST_GRAFANA is set, e.g.:

    LEASEHOLD_TEST_GRAFANA=http://admin:dev-admin@localhost:13000 uv run pytest tests/integration

Uses a random environment and deletes its dashboards and folders afterwards.
"""

from __future__ import annotations

import os
import secrets
import urllib.parse
from pathlib import Path

import pytest

from leasehold.grafana import plan
from leasehold.grafana.apply import Grafana, apply, diff
from leasehold.spec import find_specs, load_spec, read_dashboard_files

URL = os.environ.get("LEASEHOLD_TEST_GRAFANA")
FIXTURES = Path(__file__).parent.parent / "fixtures"

pytestmark = pytest.mark.skipif(not URL, reason="LEASEHOLD_TEST_GRAFANA not set")


def _client() -> Grafana:
    u = urllib.parse.urlsplit(URL)
    base = f"{u.scheme}://{u.hostname}:{u.port}"
    return Grafana(base, user=u.username, password=u.password)


def _services():
    out = []
    for path in find_specs([FIXTURES]):
        spec = load_spec(path)
        out.append((spec, read_dashboard_files(path, spec)))
    return out


@pytest.fixture
def env():
    e = "it" + secrets.token_hex(4)
    yield e
    g = _client()
    p = plan(_services(), e)
    for d in p.dashboards:
        g.request("DELETE", f"/api/dashboards/uid/{d.uid}")
    for f in p.folders:
        g.request("DELETE", f"/api/folders/{f.uid}")


def test_apply_idempotent_and_detects_edits(env):
    g = _client()
    p = plan(_services(), env)
    d = diff(g, p)
    assert sorted(c.action for c in d.changes) == ["create"] * 4 + ["create-folder"] * 3
    apply(g, d)
    assert diff(g, p).empty

    # Dashboards land in their folders.
    meta = g.get(f"/api/dashboards/uid/lh-{env}-certwatch")["meta"]
    assert meta["folderUid"] == f"lhf-{env}-certwatch"

    # An edit made in the Grafana UI is detected and overwritten.
    current = g.get(f"/api/dashboards/uid/lh-{env}-resolver")["dashboard"]
    current["title"] = "edited by hand"
    g.send(
        "POST",
        "/api/dashboards/db",
        {"dashboard": current, "overwrite": True, "folderUid": f"lhf-{env}-resolver"},
    )
    d = diff(g, p)
    assert [(c.action, c.uid) for c in d.changes] == [("update", f"lh-{env}-resolver")]
    apply(g, d)
    assert g.get(f"/api/dashboards/uid/lh-{env}-resolver")["dashboard"]["title"] == (
        f"resolver ({env})"
    )
    assert diff(g, p).empty


def test_stale_dashboards_reported_and_pruned(env):
    g = _client()
    apply(g, diff(g, plan(_services(), env)))
    # resolver's service.yml is gone.
    smaller = plan([s for s in _services() if s[0].name != "resolver"], env)
    d = diff(g, smaller)
    assert [c.uid for c in d.stale] == [f"lh-{env}-resolver"]
    apply(g, d)  # without prune: kept
    assert g.get(f"/api/dashboards/uid/lh-{env}-resolver") is not None
    apply(g, d, prune=True)
    assert g.get(f"/api/dashboards/uid/lh-{env}-resolver") is None
