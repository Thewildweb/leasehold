"""Compare a ``GrafanaPlan`` with a Grafana instance and apply it, over Grafana's HTTP API.

- Missing folders and dashboards are created; changed ones are overwritten. That
  includes edits made in the Grafana UI: a managed dashboard is changed by editing
  ``service.yml`` or the dashboard file, not in Grafana.
- Dashboards tagged ``leasehold`` and ``env:<env>`` that the plan doesn't contain are
  reported as stale, and deleted only with ``prune``. Folders are never deleted.
"""

from __future__ import annotations

import base64
import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any

from leasehold.grafana.render import MANAGED_TAG, DashboardPlan, FolderPlan, GrafanaPlan, env_tag

_VOLATILE = ("id", "version")


class GrafanaError(RuntimeError):
    pass


class Grafana:
    """A small Grafana API client: a service-account token, or basic auth."""

    def __init__(
        self,
        url: str,
        token: str | None = None,
        user: str | None = None,
        password: str | None = None,
        timeout: float = 15,
    ):
        self.url = url.rstrip("/")
        self.timeout = timeout
        if token:
            self.auth = f"Bearer {token}"
        elif user and password:
            self.auth = "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()
        else:
            raise ValueError("a token, or a user and password, is required")

    def request(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.url + path, data=data, method=method)
        req.add_header("Authorization", self.auth)
        req.add_header("Accept", "application/json")
        if data is not None:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read()
                return resp.status, json.loads(raw) if raw else None
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                payload = json.loads(raw) if raw else None
            except json.JSONDecodeError:
                payload = raw.decode(errors="replace")
            return e.code, payload

    def get(self, path: str) -> Any | None:
        """GET; None on 404."""
        status, body = self.request("GET", path)
        if status == 404:
            return None
        if status >= 300:
            raise GrafanaError(f"GET {path}: {status} {body}")
        return body

    def send(self, method: str, path: str, body: Any = None) -> Any:
        status, resp = self.request(method, path, body)
        if status >= 300:
            raise GrafanaError(f"{method} {path}: {status} {resp}")
        return resp


@dataclass(frozen=True)
class Change:
    action: str  # create-folder | rename-folder | create | update | move | delete
    uid: str
    title: str
    detail: str = ""
    folder: FolderPlan | None = field(default=None, compare=False)
    dashboard: DashboardPlan | None = field(default=None, compare=False)

    def __str__(self) -> str:
        s = f"{self.action:<13} {self.uid}  {self.title}"
        return s + (f"  ({self.detail})" if self.detail else "")


@dataclass
class GrafanaDiff:
    changes: list[Change] = field(default_factory=list)
    stale: list[Change] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def empty(self) -> bool:
        return not self.changes and not self.stale


def _comparable(d: dict) -> dict:
    return {k: v for k, v in d.items() if k not in _VOLATILE}


def only(p: GrafanaPlan, uids: set[str]) -> GrafanaPlan:
    """The plan reduced to the given dashboards and their folders."""
    dashboards = [d for d in p.dashboards if d.uid in uids]
    folders = {d.folder_uid for d in dashboards}
    return GrafanaPlan(p.env, [f for f in p.folders if f.uid in folders], dashboards)


def diff(g: Grafana, p: GrafanaPlan) -> GrafanaDiff:
    d = GrafanaDiff()
    for f in p.folders:
        have = g.get(f"/api/folders/{urllib.parse.quote(f.uid)}")
        if have is None:
            d.changes.append(Change("create-folder", f.uid, f.title, folder=f))
        elif have.get("title") != f.title:
            d.changes.append(
                Change("rename-folder", f.uid, f.title, f"was {have.get('title')!r}", folder=f)
            )
    for want in p.dashboards:
        have = g.get(f"/api/dashboards/uid/{urllib.parse.quote(want.uid)}")
        if have is None:
            d.changes.append(Change("create", want.uid, want.title, want.source, dashboard=want))
            continue
        current = have.get("dashboard", {})
        folder = have.get("meta", {}).get("folderUid")
        if _comparable(current) != _comparable(want.dashboard):
            d.changes.append(
                Change("update", want.uid, want.title, "differs from service.yml", dashboard=want)
            )
        elif folder != want.folder_uid:
            d.changes.append(
                Change("move", want.uid, want.title, f"from folder {folder}", dashboard=want)
            )
    wanted = {x.uid for x in p.dashboards}
    query = urllib.parse.urlencode(
        [("tag", MANAGED_TAG), ("tag", env_tag(p.env)), ("type", "dash-db"), ("limit", "5000")]
    )
    for hit in g.get(f"/api/search?{query}") or []:
        if hit["uid"] not in wanted:
            d.stale.append(Change("delete", hit["uid"], hit["title"], "no service.yml declares it"))
    return d


def apply(g: Grafana, d: GrafanaDiff, prune: bool = False) -> None:
    """Apply in order: folders first, then dashboards; stale dashboards only with prune."""
    for c in d.changes:
        if c.action == "create-folder":
            g.send("POST", "/api/folders", {"uid": c.uid, "title": c.title})
        elif c.action == "rename-folder":
            g.send("PUT", f"/api/folders/{c.uid}", {"title": c.title, "overwrite": True})
    for c in d.changes:
        if c.dashboard is None:
            continue
        g.send(
            "POST",
            "/api/dashboards/db",
            {
                "dashboard": c.dashboard.dashboard | {"id": None},
                "folderUid": c.dashboard.folder_uid,
                "overwrite": True,
                "message": f"leasehold apply ({c.dashboard.source})",
            },
        )
    if prune:
        for c in d.stale:
            g.send("DELETE", f"/api/dashboards/uid/{c.uid}")
