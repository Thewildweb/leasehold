"""Compare a ``NomadPlan`` with a Nomad cluster and apply it, over Nomad's HTTP API.

- Each job is planned with ``/v1/job/<id>/plan`` (Nomad's own diff, and whether it
  can be placed), then registered with the index from that plan, so a job changed by
  someone else in between is refused instead of overwritten.
- A job with the same ID that ``apply`` doesn't manage (no ``leasehold_service`` in its
  ``Meta``) is never touched: prod shares namespace ``default`` with v1/v2 jobs.
- The running count of a long-running job is kept, clamped to the spec's range.
- Jobs of the given services that the plan no longer contains are reported as stale
  and stopped (``Purge`` false, so history stays) only with ``prune``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import httpx

from leasehold.nomad.render import MANAGED_META, JobPlan, NomadPlan, with_count


class NomadError(RuntimeError):
    pass


class Nomad:
    """A small Nomad API client with an ACL token."""

    def __init__(self, url: str, token: str | None = None, timeout: float = 30):
        headers = {"X-Nomad-Token": token} if token else {}
        self._http = httpx.Client(base_url=url.rstrip("/"), headers=headers, timeout=timeout)

    def close(self) -> None:
        self._http.close()

    def _call(self, method: str, path: str, **kw: Any) -> Any:
        try:
            r = self._http.request(method, path, **kw)
        except httpx.HTTPError as e:
            raise NomadError(f"{method} {path}: {e}") from None
        if r.status_code == 404:
            return None
        if r.status_code >= 400:
            raise NomadError(f"{method} {path}: {r.status_code} {r.text.strip()[:500]}")
        return r.json() if r.content else None

    def namespace_exists(self, namespace: str) -> bool:
        return self._call("GET", f"/v1/namespace/{namespace}") is not None

    def job(self, job_id: str, namespace: str) -> dict[str, Any] | None:
        return self._call("GET", f"/v1/job/{job_id}", params={"namespace": namespace})

    def jobs(self, namespace: str) -> list[dict[str, Any]]:
        return self._call("GET", "/v1/jobs", params={"namespace": namespace, "meta": "true"}) or []

    def plan(self, job: dict[str, Any]) -> dict[str, Any]:
        return self._call(
            "POST",
            f"/v1/job/{job['ID']}/plan",
            params={"namespace": job["Namespace"]},
            json={"Job": job, "Diff": True},
        )

    def register(self, job: dict[str, Any], modify_index: int) -> dict[str, Any]:
        return self._call(
            "POST",
            "/v1/jobs",
            params={"namespace": job["Namespace"]},
            json={"Job": job, "EnforceIndex": True, "JobModifyIndex": modify_index},
        )

    # --- Variables -------------------------------------------------------------------------

    def var_get(self, path: str, namespace: str) -> dict[str, Any] | None:
        """``{"Items": {...}, "ModifyIndex": n, ...}``, or None when it doesn't exist."""
        return self._call("GET", f"/v1/var/{path}", params={"namespace": namespace})

    def var_list(self, prefix: str, namespace: str) -> list[str]:
        out = self._call("GET", "/v1/vars", params={"prefix": prefix, "namespace": namespace})
        return [v["Path"] for v in out or []]

    def var_put(self, path: str, namespace: str, items: dict[str, str], modify_index: int) -> None:
        """Write a variable; ``modify_index`` 0 means it must not exist yet (check-and-set)."""
        try:
            r = self._http.put(
                f"/v1/var/{path}",
                params={"namespace": namespace, "cas": modify_index},
                json={"Namespace": namespace, "Path": path, "Items": items},
            )
        except httpx.HTTPError as e:
            raise NomadError(f"PUT /v1/var/{path}: {e}") from None
        if r.status_code == 409:
            raise NomadError(f"variable {namespace}/{path} changed meanwhile; run again")
        if r.status_code >= 400:
            raise NomadError(f"PUT /v1/var/{path}: {r.status_code} {r.text.strip()[:300]}")

    def var_delete(self, path: str, namespace: str) -> None:
        self._call("DELETE", f"/v1/var/{path}", params={"namespace": namespace})

    def stop(self, job_id: str, namespace: str) -> dict[str, Any]:
        return self._call(
            "DELETE", f"/v1/job/{job_id}", params={"namespace": namespace, "purge": "false"}
        )


@dataclass
class JobChange:
    job: JobPlan
    kind: str  # "create" or "update"
    modify_index: int
    diff_lines: list[str]
    placement_failures: list[str]

    def __str__(self) -> str:
        head = f"{self.kind:<8} {self.job.namespace}/{self.job.job_id}"
        return "\n".join([head, *(f"           {line}" for line in self.diff_lines)])


@dataclass
class NomadDiff:
    changes: list[JobChange] = field(default_factory=list)
    stale: list[tuple[str, str]] = field(default_factory=list)  # (namespace, job id)
    warnings: list[str] = field(default_factory=list)

    @property
    def empty(self) -> bool:
        return not self.changes


def _fmt(value: Any) -> str:
    return "∅" if value in (None, "") else str(value)


def diff_lines(d: dict[str, Any] | None, prefix: str = "") -> list[str]:
    """Nomad's plan diff as one line per changed field."""
    if not d:
        return []
    out: list[str] = []
    for f in d.get("Fields") or []:
        if f.get("Type") == "None":
            continue
        out.append(f"{prefix}{f['Name']}: {_fmt(f.get('Old'))} → {_fmt(f.get('New'))}")
    for o in d.get("Objects") or []:
        if o.get("Type") == "None":
            continue
        out.extend(diff_lines(o, f"{prefix}{o['Name']}."))
    for tg in d.get("TaskGroups") or []:
        if tg.get("Type") == "None":
            continue
        out.extend(diff_lines(tg, f"{prefix}group[{tg['Name']}]."))
        for t in tg.get("Tasks") or []:
            if t.get("Type") != "None":
                out.extend(diff_lines(t, f"{prefix}task[{t['Name']}]."))
    return out


def _placement_failures(plan_result: dict[str, Any]) -> list[str]:
    out = []
    for tg, m in (plan_result.get("FailedTGAllocs") or {}).items():
        reasons = []
        full = 0  # Nomad names every machine a distinct_property constraint filtered
        for k in ("ConstraintFiltered", "DimensionExhausted", "ClassExhausted"):
            for reason, n in (m.get(k) or {}).items():
                if reason.startswith("distinct_property: ${node.unique.id}="):
                    full += n
                else:
                    reasons.append(f"{reason} ({n})")
        if full:
            reasons.insert(0, f"max_per_node reached on {full} machines")
        nodes = m.get("NodesEvaluated", 0)
        unplaced = m.get("CoalescedFailures", 0) + 1
        why = "; ".join(reasons) or "no eligible nodes"
        out.append(f"group {tg}: {unplaced} can't be placed ({nodes} nodes evaluated): {why}")
    return out


def diff(nomad: Nomad, plan: NomadPlan, services: list[str]) -> NomadDiff:
    if not nomad.namespace_exists(plan.namespace):
        raise NomadError(f"Nomad namespace {plan.namespace!r} doesn't exist; create it first")
    d = NomadDiff()
    planned = {j.job_id for j in plan.jobs}
    for jp in plan.jobs:
        current = nomad.job(jp.job_id, jp.namespace)
        if current is not None:
            meta = current.get("Meta") or {}
            if meta.get(MANAGED_META) != jp.service:
                d.warnings.append(
                    f"{jp.namespace}/{jp.job_id} exists and isn't managed by leasehold "
                    f"(Meta.{MANAGED_META} = {meta.get(MANAGED_META)!r}); skipped"
                )
                continue
            if not current.get("Stop"):
                groups = current.get("TaskGroups") or []
                jp = with_count(jp, groups[0].get("Count") if groups else None)
        result = nomad.plan(jp.job)
        lines = diff_lines(result.get("Diff"))
        failures = _placement_failures(result)
        if current is not None and not current.get("Stop") and not lines:
            continue
        d.changes.append(
            JobChange(
                job=jp,
                kind="create" if current is None else "update",
                modify_index=int(result.get("JobModifyIndex") or 0),
                diff_lines=lines if current is not None else [],
                placement_failures=failures,
            )
        )
        d.warnings.extend(f"{jp.job_id}: {f}" for f in failures)

    for job in nomad.jobs(plan.namespace):
        meta = job.get("Meta") or {}
        if (
            meta.get(MANAGED_META) in services
            and meta.get("leasehold_env") == plan.env
            and job["ID"] not in planned
            and job.get("Status") != "dead"
            # Runs of a periodic job copy its Meta; they belong to their parent.
            and not job.get("ParentID")
        ):
            d.stale.append((plan.namespace, job["ID"]))
    return d


def apply(nomad: Nomad, d: NomadDiff, prune: bool = False) -> list[str]:
    """Register the changed jobs (and stop stale ones with ``prune``); returns eval IDs."""
    evals = []
    for c in d.changes:
        r = nomad.register(c.job.job, c.modify_index)
        if r and r.get("EvalID"):
            evals.append(r["EvalID"])
    if prune:
        for namespace, job_id in d.stale:
            r = nomad.stop(job_id, namespace)
            if r and r.get("EvalID"):
                evals.append(r["EvalID"])
    return evals
