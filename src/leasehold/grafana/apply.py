"""Compare a ``GrafanaPlan`` with a Grafana instance and apply it, over Grafana's HTTP API.

- Missing folders and dashboards are created; changed ones are overwritten. That
  includes edits made in the Grafana UI: a managed dashboard is changed by editing
  ``service.yml`` or the dashboard file, not in Grafana.
- Dashboards tagged ``leasehold`` and ``env:<env>`` that the plan doesn't contain are
  reported as stale, and deleted only with ``prune``. Folders are never deleted.
- Alert rules (provisioning API): created when missing, overwritten when the
  ``leasehold_hash`` annotation differs from the plan's. Rules labelled
  ``managed_by=leasehold`` and ``env=<env>`` that the plan doesn't contain are stale
  (deleted only with ``prune``). A rule with a planned UID that leasehold didn't
  create is left alone, with a warning. Each rule group's evaluation interval is set
  after its rules.
"""

from __future__ import annotations

import base64
import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any

from leasehold.grafana.alerts import HASH_ANNOTATION, MANAGED_LABEL, MANAGED_VALUE, RulePlan
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
    # create-folder | rename-folder | create | update | move | delete
    # | create-rule | update-rule | delete-rule | group-interval
    action: str
    uid: str
    title: str
    detail: str = ""
    folder: FolderPlan | None = field(default=None, compare=False)
    dashboard: DashboardPlan | None = field(default=None, compare=False)
    rule: RulePlan | None = field(default=None, compare=False)
    group: tuple[str, str, int] | None = field(default=None, compare=False)  # folder, group, s

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
    return GrafanaPlan(p.env, [f for f in p.folders if f.uid in folders], dashboards, [])


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
    _diff_rules(g, p, d)
    return d


def _managed(rule: dict, env: str) -> bool:
    labels = rule.get("labels") or {}
    return labels.get(MANAGED_LABEL) == MANAGED_VALUE and labels.get("env") == env


def _diff_rules(g: Grafana, p: GrafanaPlan, d: GrafanaDiff) -> None:
    existing = g.get("/api/v1/provisioning/alert-rules") or []
    have = {r["uid"]: r for r in existing}
    for want in p.rules:
        cur = have.get(want.uid)
        if cur is None:
            d.changes.append(Change("create-rule", want.uid, want.title, rule=want))
        elif not _managed(cur, p.env):
            d.warnings.append(f"alert rule {want.uid} exists but wasn't created by leasehold")
        elif (cur.get("annotations") or {}).get(HASH_ANNOTATION) != want.rule["annotations"][
            HASH_ANNOTATION
        ] or (cur.get("folderUID"), cur.get("ruleGroup")) != (want.folder_uid, want.group):
            d.changes.append(Change("update-rule", want.uid, want.title, "differs", rule=want))
    wanted = {r.uid for r in p.rules}
    for r in existing:
        if _managed(r, p.env) and r["uid"] not in wanted:
            d.stale.append(
                Change("delete-rule", r["uid"], r.get("title", ""), "no service.yml declares it")
            )
    groups = {(r.folder_uid, r.group): r.interval_s for r in p.rules}
    for (folder, group), interval in sorted(groups.items()):
        cur = g.get(_group_path(folder, group))
        if cur is None or cur.get("interval") != interval:
            was = "new" if cur is None else f"was {cur.get('interval')}s"
            d.changes.append(
                Change(
                    "group-interval",
                    f"{folder}/{group}",
                    f"every {interval}s",
                    was,
                    group=(folder, group, interval),
                )
            )


def _group_path(folder: str, group: str) -> str:
    q = urllib.parse.quote
    return f"/api/v1/provisioning/folder/{q(folder)}/rule-groups/{q(group)}"


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
    for c in d.changes:
        if c.action == "create-rule":
            g.send("POST", "/api/v1/provisioning/alert-rules", c.rule.rule)
        elif c.action == "update-rule":
            g.send("PUT", f"/api/v1/provisioning/alert-rules/{c.uid}", c.rule.rule)
    for c in d.changes:
        if c.action == "group-interval":
            folder, group, interval = c.group
            cur = g.get(_group_path(folder, group))
            if cur is None:
                raise GrafanaError(f"rule group {folder}/{group} missing after adding its rules")
            g.send("PUT", _group_path(folder, group), cur | {"interval": interval})
    if prune:
        for c in d.stale:
            if c.action == "delete-rule":
                g.send("DELETE", f"/api/v1/provisioning/alert-rules/{c.uid}")
            else:
                g.send("DELETE", f"/api/dashboards/uid/{c.uid}")
