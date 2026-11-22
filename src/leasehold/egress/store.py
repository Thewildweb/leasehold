"""Reading the ``egress`` schema from the control API (async)."""

from __future__ import annotations

from psycopg import AsyncConnection, sql

from leasehold.egress.schema import LATEST, SCHEMA


class EgressNotReady(RuntimeError):
    """The egress schema is missing or not migrated."""


async def _version(conn: AsyncConnection, schema: str) -> int:
    cur = await conn.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = 'schema_migrations'",
        (schema,),
    )
    if await cur.fetchone() is None:
        return 0
    cur = await conn.execute(
        sql.SQL("SELECT max(version) FROM {}.schema_migrations").format(sql.Identifier(schema))
    )
    return (await cur.fetchone())[0] or 0


async def provider_rules(conn: AsyncConnection, schema: str = SCHEMA) -> dict:
    """Providers with their budget per egress per minute, the CNAME and range rules, and
    the shared-IP setting: what planners need to tag targets."""
    if await _version(conn, schema) < LATEST:
        raise EgressNotReady(f"schema {schema} is not migrated; run `leasehold egress migrate`")
    s = sql.Identifier(schema)

    async def rows(stmt: str) -> list[tuple]:
        cur = await conn.execute(sql.SQL(stmt).format(s=s))
        return await cur.fetchall()

    providers = {
        r[0]: {"kind": r[1], "per_egress_per_min": r[2]}
        for r in await rows("SELECT name, kind, per_egress_per_min FROM {s}.providers")
    }
    cnames = {r[0]: r[1] for r in await rows("SELECT suffix, provider FROM {s}.provider_cnames")}
    ranges = {
        str(r[0]): r[1] for r in await rows("SELECT network, provider FROM {s}.provider_ranges")
    }
    settings = {r[0]: r[1] for r in await rows("SELECT name, value FROM {s}.settings")}
    return {
        "providers": providers,
        "cnames": cnames,
        "ranges": ranges,
        "shared_ip": settings.get("shared_ip"),
    }
