"""Checks across all services: the whole set of ``service.yml`` files must fit together."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable

from leasehold.spec.models import ServiceSpec


def check_registry(specs: Iterable[ServiceSpec], *, partial: bool = False) -> list[str]:
    """Return cross-service errors; an empty list means the set is consistent.

    With ``partial=True``, references to services that aren't in the set are
    accepted (useful when validating a single service on its own). References to
    services that *are* in the set must still resolve.
    """
    specs = list(specs)
    errors: list[str] = []

    for name, n in Counter(s.name for s in specs).items():
        if n > 1:
            errors.append(f"service name {name!r} is used by {n} services")

    dbs = Counter(s.clickhouse.database for s in specs if s.clickhouse)
    for db, n in dbs.items():
        if n > 1:
            errors.append(f"ClickHouse database {db!r} is claimed by {n} services")

    known_services = {s.name for s in specs}
    topics = {ref for s in specs for ref in s.own_topic_refs()}

    for s in specs:
        for ref in sorted(s.consumed_topic_refs()):
            if ref in topics:
                continue
            if partial and ref.service not in known_services:
                continue
            errors.append(f"{s.name}: consumes {ref}, which no service produces")

    return errors
