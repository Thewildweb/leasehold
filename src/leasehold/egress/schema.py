"""The ``egress`` Postgres schema: registry, providers, rules and budget settings.

One schema for all environments, next to their frontier schemas: environments share
the egress addresses, so they share their budgets. ``leasehold egress migrate``
creates it; ``leasehold egress apply <dir>`` writes the configuration files into it
(``leasehold.egress.config``).

Applying never deletes an egress: one that is no longer in the registry is set to
``disabled``, so allocations and usage that refer to it stay valid. It reads and writes
only registry rows (``source = 'registry'``): fixed proxy exits (``source = 'live'``),
pools and accounts are managed through the control API (``leasehold.egress.proxies``),
and an operator's ``override_status`` on a registry row is left alone.

These functions use a synchronous psycopg connection; they run from the CLI.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

import psycopg
from psycopg import sql

from leasehold.egress.config import EgressConfig

SCHEMA = "egress"

MIGRATIONS: list[tuple[int, list[str]]] = [
    (
        1,
        [
            """
            CREATE TABLE {s}.egresses (
              address    inet        PRIMARY KEY,
              host       text,
              subnet     cidr        NOT NULL,
              ipv6       cidr,
              asn        bigint,
              country    text,
              kind       text        NOT NULL CHECK (kind IN ('datacenter', 'residential')),
              status     text        NOT NULL CHECK (status IN ('active', 'drain', 'disabled')),
              cost       numeric,
              updated_at timestamptz NOT NULL DEFAULT now(),
              CHECK (address << subnet OR address = subnet)
            )
            """,
            # NULL budget: no provider budget, the target limit applies.
            """
            CREATE TABLE {s}.providers (
              name               text        PRIMARY KEY,
              kind               text        NOT NULL
                                 CHECK (kind IN ('cdn', 'platform', 'parking')),
              per_egress_per_min int         CHECK (per_egress_per_min >= 0),
              updated_at         timestamptz NOT NULL DEFAULT now()
            )
            """,
            """
            CREATE TABLE {s}.provider_cnames (
              suffix   text PRIMARY KEY,
              provider text NOT NULL REFERENCES {s}.providers (name) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE {s}.provider_ranges (
              network  cidr PRIMARY KEY,
              provider text NOT NULL REFERENCES {s}.providers (name) ON DELETE CASCADE
            )
            """,
            # planning, classes, providers, shared_ip, network: the sections of budgets.yml.
            """
            CREATE TABLE {s}.settings (
              name       text        PRIMARY KEY,
              value      jsonb       NOT NULL,
              updated_at timestamptz NOT NULL DEFAULT now()
            )
            """,
        ],
    ),
]

MIGRATIONS.append(
    (
        2,
        [
            # NULL: any task type may use the egress.
            "ALTER TABLE {s}.egresses ADD COLUMN task_types text[]",
            # One holder per target across all environments: a worker (later the
            # machine's agent) on one egress. Expired rows are taken over and deleted by
            # the owner's sweeper; revoked ones get no new tasks and are not renewed.
            """
            CREATE TABLE {s}.target_leases (
              target       text        PRIMARY KEY,
              egress       inet        NOT NULL,
              holder       text        NOT NULL,
              env          text        NOT NULL,
              lease_id     uuid        NOT NULL UNIQUE,
              provider     text        NOT NULL DEFAULT '',
              interval_ms  int         NOT NULL CHECK (interval_ms > 0),
              duration_ms  int         NOT NULL CHECK (duration_ms > 0),
              cap          int         NOT NULL CHECK (cap > 0),
              granted      int         NOT NULL CHECK (granted >= 0),
              granted_at   timestamptz NOT NULL DEFAULT now(),
              expires_at   timestamptz NOT NULL,
              revoked_at   timestamptz,
              revoke_reason text
            )
            """,
            "CREATE INDEX target_leases_expires ON {s}.target_leases (expires_at)",
            # Pushback from a target cools it for every egress until `until`.
            """
            CREATE TABLE {s}.target_cooldowns (
              target     text        PRIMARY KEY,
              until      timestamptz NOT NULL,
              reason     text        NOT NULL,
              egress     inet,
              env        text        NOT NULL,
              updated_at timestamptz NOT NULL DEFAULT now()
            )
            """,
            "CREATE INDEX target_cooldowns_until ON {s}.target_cooldowns (until)",
            # Requests granted per (egress, target) per hour, for the pair budget.
            """
            CREATE TABLE {s}.pair_usage (
              egress   inet        NOT NULL,
              target   text        NOT NULL,
              hour     timestamptz NOT NULL,
              requests int         NOT NULL,
              PRIMARY KEY (egress, target, hour)
            )
            """,
            "CREATE INDEX pair_usage_hour ON {s}.pair_usage (hour)",
        ],
    )
)

MIGRATIONS.append(
    (
        3,
        [
            # Tokens per (window, egress, provider, class) from the budget planner;
            # dispatchers add to `used` at grant time. Class '*' is for any class.
            """
            CREATE TABLE {s}.allocations (
              window_start timestamptz NOT NULL,
              window_end   timestamptz NOT NULL,
              egress       inet        NOT NULL,
              provider     text        NOT NULL,
              class        text        NOT NULL,
              tokens       int         NOT NULL CHECK (tokens >= 0),
              used         int         NOT NULL DEFAULT 0 CHECK (used >= 0),
              PRIMARY KEY (window_start, egress, provider, class)
            )
            """,
            "CREATE INDEX allocations_end ON {s}.allocations (window_end)",
            # Due tasks within the planning horizon per (environment, provider, class),
            # published by each environment's API; the planner's forecast.
            """
            CREATE TABLE {s}.demand (
              env        text        NOT NULL,
              provider   text        NOT NULL,
              class      text        NOT NULL,
              due        int         NOT NULL CHECK (due >= 0),
              updated_at timestamptz NOT NULL DEFAULT now(),
              PRIMARY KEY (env, provider, class)
            )
            """,
        ],
    )
)

MIGRATIONS.append(
    (
        4,
        [
            # What the pushback was (the worker's cause): what a clear may lift, and
            # whether it escalates the target to the next tier. NULL: from before.
            """
            ALTER TABLE {s}.target_cooldowns ADD COLUMN cause text
              CHECK (cause IN ('challenge', 'block', 'rate', 'retry_after'))
            """,
            # One row per clear request (POST /v1/targets/{target}/clear), refused ones too.
            """
            CREATE TABLE {s}.target_clears (
              id              bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
              target          text        NOT NULL,
              cleared         boolean     NOT NULL,
              refused         text,
              cooldown_until  timestamptz,
              cooldown_reason text,
              cooldown_cause  text,
              cooldown_egress text,
              cooldown_env    text,
              source          text        NOT NULL,
              reason          text        NOT NULL,
              evidence        jsonb,
              caller          text        NOT NULL,
              env             text        NOT NULL,
              at              timestamptz NOT NULL DEFAULT now()
            )
            """,
            "CREATE INDEX target_clears_target ON {s}.target_clears (target, at)",
        ],
    )
)

MIGRATIONS.append(
    (
        5,
        [
            # Proxies (leasehold.egress.proxies). An account is one contract with one
            # vendor: its credentials (encrypted, leasehold.egress.secrets), plan and
            # state. `state` is what the account does, `status` what the operator wants.
            """
            CREATE TABLE {s}.proxy_accounts (
              name                   text        PRIMARY KEY,
              vendor                 text        NOT NULL,
              adapter                text        NOT NULL DEFAULT 'generic',
              adapter_config         jsonb       NOT NULL DEFAULT '{{}}',
              credentials            bytea,
              key_id                 text,
              credentials_fp         text,
              credentials_updated_at timestamptz,
              usage_credentials      bytea,
              usage_key_id           text,
              usage_fp               text,
              usage_updated_at       timestamptz,
              cap_gb                 numeric     CHECK (cap_gb > 0),
              cap_period             text        NOT NULL DEFAULT 'month'
                                     CHECK (cap_period IN ('month', 'day')),
              period_anchor          int         NOT NULL DEFAULT 1
                                     CHECK (period_anchor BETWEEN 1 AND 28),
              paid_until             date,
              concurrency            int         CHECK (concurrency > 0),
              warn_remaining_gb      numeric     CHECK (warn_remaining_gb > 0),
              state                  text        NOT NULL DEFAULT 'ok'
                                     CHECK (state IN ('ok', 'quota_exhausted', 'payment_failed',
                                                      'suspended', 'auth_failed', 'unreachable')),
              state_reason           text,
              state_since            timestamptz NOT NULL DEFAULT now(),
              status                 text        NOT NULL DEFAULT 'disabled'
                                     CHECK (status IN ('active', 'drain', 'disabled')),
              envs                   text[]      NOT NULL DEFAULT '{{prod,dev}}',
              vendor_used_bytes      bigint,
              vendor_limit_bytes     bigint,
              vendor_remaining_bytes bigint,
              vendor_period_end      timestamptz,
              vendor_seen_at         timestamptz,
              vendor_error           text,
              vendor_error_since     timestamptz,
              notified               jsonb       NOT NULL DEFAULT '{{}}',
              created_at             timestamptz NOT NULL DEFAULT now(),
              updated_at             timestamptz NOT NULL DEFAULT now(),
              removed_at             timestamptz
            )
            """,
            # Fixed exits: a vendor's endpoints (host:port), each one exit IP that the
            # probe learns; the egress row (below) is made once the address is known.
            """
            CREATE TABLE {s}.proxy_exits (
              id              bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
              account         text        NOT NULL REFERENCES {s}.proxy_accounts (name),
              endpoint        text        NOT NULL UNIQUE,
              kind            text        NOT NULL
                              CHECK (kind IN ('datacenter', 'residential', 'isp', 'mobile')),
              country         text,
              task_types      text[],
              address         inet,
              address_seen_at timestamptz,
              last_error      text,
              created_at      timestamptz NOT NULL DEFAULT now(),
              updated_at      timestamptz NOT NULL DEFAULT now(),
              removed_at      timestamptz
            )
            """,
            # Rotating gateways: no address before the request; keyed `pool:<name>`.
            """
            CREATE TABLE {s}.proxy_pools (
              name                 text        PRIMARY KEY,
              account              text        NOT NULL REFERENCES {s}.proxy_accounts (name),
              kind                 text        NOT NULL
                                   CHECK (kind IN ('datacenter', 'residential', 'isp', 'mobile')),
              endpoint             text        NOT NULL,
              username_template    text        NOT NULL DEFAULT '{{user}}',
              sticky               boolean     NOT NULL DEFAULT false,
              max_sticky_ttl_s     int         CHECK (max_sticky_ttl_s > 0),
              countries            text[]      NOT NULL DEFAULT '{{}}',
              cost_per_gb          numeric,
              concurrency          int         CHECK (concurrency > 0),
              egress_weight        int         NOT NULL DEFAULT 4 CHECK (egress_weight > 0),
              exit_ip_header       text,
              status               text        NOT NULL DEFAULT 'active'
                                   CHECK (status IN ('active', 'drain', 'disabled')),
              override_status      text        CHECK (override_status IN ('drain', 'disabled')),
              override_reason      text,
              consecutive_failures int         NOT NULL DEFAULT 0,
              task_types           text[],
              source               text        NOT NULL DEFAULT 'live' CHECK (source = 'live'),
              created_at           timestamptz NOT NULL DEFAULT now(),
              updated_at           timestamptz NOT NULL DEFAULT now(),
              removed_at           timestamptz
            )
            """,
            # Bytes and requests through each account, per pool or exit (`ref`), env,
            # task type and day; workers report bytes with their outcomes.
            """
            CREATE TABLE {s}.proxy_usage (
              account   text   NOT NULL,
              ref       text   NOT NULL,
              env       text   NOT NULL,
              task_type text   NOT NULL,
              day       date   NOT NULL,
              bytes     bigint NOT NULL DEFAULT 0,
              requests  bigint NOT NULL DEFAULT 0,
              PRIMARY KEY (account, day, ref, env, task_type)
            )
            """,
            # Connect failures through an account's exits and pools, for the correlation
            # rule (several exits failing together is the account, not the exits).
            # `gateway`: the proxy answered CONNECT with 502/503/504, recorded with the
            # target; it counts only once several distinct targets fail on one exit.
            """
            CREATE TABLE {s}.proxy_failures (
              account text        NOT NULL,
              ref     text        NOT NULL,
              env     text        NOT NULL,
              kind    text        NOT NULL DEFAULT 'connect'
                                  CHECK (kind IN ('connect', 'gateway')),
              target  text,
              at      timestamptz NOT NULL DEFAULT now()
            )
            """,
            "CREATE INDEX proxy_failures_account ON {s}.proxy_failures (account, at)",
            "CREATE INDEX proxy_failures_ref ON {s}.proxy_failures (ref, at)",
            """
            CREATE TABLE {s}.proxy_audit (
              id          bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
              at          timestamptz NOT NULL DEFAULT now(),
              actor       text        NOT NULL,
              env         text        NOT NULL,
              action      text        NOT NULL,
              object_type text        NOT NULL,
              object      text        NOT NULL,
              changes     jsonb       NOT NULL DEFAULT '{{}}'
            )
            """,
            "CREATE INDEX proxy_audit_object ON {s}.proxy_audit (object_type, object, at)",
            # The tier a target is on after a block escalated it (leasehold.egress.tiers);
            # no row: the task type's first tier.
            """
            CREATE TABLE {s}.target_tiers (
              target    text        PRIMARY KEY,
              tier      text        NOT NULL,
              since     timestamptz NOT NULL DEFAULT now(),
              reason    text        NOT NULL,
              evidence  jsonb,
              task_type text        NOT NULL,
              env       text        NOT NULL
            )
            """,
            "CREATE INDEX target_tiers_since ON {s}.target_tiers (since)",
            # Egresses: registry rows (VPSes) and live rows (fixed proxy exits).
            "ALTER TABLE {s}.egresses DROP CONSTRAINT egresses_kind_check",
            """
            ALTER TABLE {s}.egresses ADD CONSTRAINT egresses_kind_check
              CHECK (kind IN ('datacenter', 'residential', 'isp', 'mobile'))
            """,
            """
            ALTER TABLE {s}.egresses
              ADD COLUMN source text NOT NULL DEFAULT 'registry'
                CHECK (source IN ('registry', 'live')),
              ADD COLUMN account text REFERENCES {s}.proxy_accounts (name),
              ADD COLUMN endpoint text,
              ADD COLUMN exit_id bigint REFERENCES {s}.proxy_exits (id),
              ADD COLUMN override_status text CHECK (override_status IN ('drain', 'disabled')),
              ADD COLUMN override_reason text,
              ADD COLUMN consecutive_failures int NOT NULL DEFAULT 0,
              ADD COLUMN removed_at timestamptz,
              ADD CONSTRAINT egresses_proxy_fields
                CHECK ((account IS NULL) = (endpoint IS NULL))
            """,
            # The egress key becomes text: an address, or `pool:<name>` for a pool.
            "ALTER TABLE {s}.target_leases ALTER COLUMN egress TYPE text USING host(egress)",
            "ALTER TABLE {s}.target_cooldowns ALTER COLUMN egress TYPE text USING host(egress)",
            "ALTER TABLE {s}.pair_usage ALTER COLUMN egress TYPE text USING host(egress)",
            "ALTER TABLE {s}.allocations ALTER COLUMN egress TYPE text USING host(egress)",
            # A routed target lease: the proxy account, its tier, the sticky session with
            # its TTL, the country the route was planned for and the credentials
            # fingerprint the route was rendered with (a refresh renders the same route).
            """
            ALTER TABLE {s}.target_leases
              ADD COLUMN account text,
              ADD COLUMN tier text NOT NULL DEFAULT 'datacenter',
              ADD COLUMN session text,
              ADD COLUMN session_ttl_s int,
              ADD COLUMN country text,
              ADD COLUMN route_fp text
            """,
            "CREATE INDEX target_leases_account ON {s}.target_leases (account) "
            "WHERE account IS NOT NULL",
            # Cooldowns per tier: a block cools the target on the tier it happened on;
            # rate pushback ('*') on every tier.
            "ALTER TABLE {s}.target_cooldowns ADD COLUMN tier text NOT NULL DEFAULT '*'",
            "ALTER TABLE {s}.target_cooldowns DROP CONSTRAINT target_cooldowns_pkey",
            "ALTER TABLE {s}.target_cooldowns ADD PRIMARY KEY (target, tier)",
        ],
    )
)

MIGRATIONS.append(
    (
        6,
        [
            # Registry exits (leasehold.exit): a VPS's address also reached through a
            # CONNECT proxy on its private address, by workers that have no address of
            # their own. `exit_override` takes the exit out of service (the probe, an
            # operator) without touching the VPS's own workers.
            """
            ALTER TABLE {s}.egresses
              ADD COLUMN exit_endpoint text,
              ADD COLUMN exit_override text CHECK (exit_override IN ('drain', 'disabled')),
              ADD COLUMN exit_override_reason text,
              ADD CONSTRAINT egresses_exit_registry
                CHECK (exit_endpoint IS NULL OR account IS NULL)
            """,
            # A target lease routed through a registry exit: via = 'exit'.
            "ALTER TABLE {s}.target_leases ADD COLUMN via text",
            "CREATE INDEX target_leases_via ON {s}.target_leases (egress) WHERE via IS NOT NULL",
        ],
    )
)

# Tables the dispatchers of every environment write; the schema owner grants them to
# the other environments' roles (`leasehold egress migrate --writer <role>`).
# Allocations only take spending (`used`): other environments never create or resize them.
# Proxy rows are changed through the owner's API; other environments only record what
# their workers report (usage, failures, account and exit health).
SHARED_WRITE = {
    "target_leases": "INSERT, UPDATE",
    "target_cooldowns": "INSERT, UPDATE",
    "pair_usage": "INSERT, UPDATE",
    "demand": "INSERT, UPDATE",
    "allocations": "UPDATE (used)",
    "target_clears": "INSERT",
    "target_tiers": "INSERT, UPDATE",
    "proxy_usage": "INSERT, UPDATE",
    "proxy_failures": "INSERT, DELETE",  # a success clears the route's failures
    "proxy_audit": "INSERT",
    "proxy_accounts": "UPDATE (state, state_reason, state_since, updated_at)",
    "proxy_pools": "UPDATE (consecutive_failures, override_status, override_reason, updated_at)",
    "egresses": "UPDATE (consecutive_failures, override_status, override_reason, "
    "exit_override, exit_override_reason, updated_at)",
}
# Columns a read-only role (Grafana) may not select: the ciphertexts.
SECRET_COLUMNS = {"proxy_accounts": ("credentials", "usage_credentials")}
SHARED_WRITE_TABLES = tuple(SHARED_WRITE)

LATEST = MIGRATIONS[-1][0]


def grant_writers(conn: psycopg.Connection, roles: list[str], schema: str = SCHEMA) -> None:
    """Let other environments' roles read the schema and write the dispatch tables.
    Idempotent; run after every migration."""
    for role in roles:
        r = sql.Identifier(role)
        s = sql.Identifier(schema)
        conn.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(s, r))
        conn.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(s, r))
        for t, privileges in SHARED_WRITE.items():
            conn.execute(
                sql.SQL("GRANT {} ON {}.{} TO {}").format(
                    sql.SQL(privileges), s, sql.Identifier(t), r
                )
            )
        conn.execute(sql.SQL("GRANT USAGE ON ALL SEQUENCES IN SCHEMA {} TO {}").format(s, r))


def grant_readers(conn: psycopg.Connection, roles: list[str], schema: str = SCHEMA) -> None:
    """Let read-only roles (dashboards) read the schema, except the credential columns.
    Idempotent; run after every migration."""
    for role in roles:
        r = sql.Identifier(role)
        s = sql.Identifier(schema)
        conn.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(s, r))
        conn.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(s, r))
        for table, secret in SECRET_COLUMNS.items():
            t = sql.Identifier(table)
            cols = [
                c[0]
                for c in conn.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
                    (schema, table),
                )
                if c[0] not in secret
            ]
            conn.execute(sql.SQL("REVOKE SELECT ON {}.{} FROM {}").format(s, t, r))
            conn.execute(
                sql.SQL("GRANT SELECT ({}) ON {}.{} TO {}").format(
                    sql.SQL(", ").join(map(sql.Identifier, cols)), s, t, r
                )
            )


def _q(stmt: str, schema: str) -> sql.Composed:
    return sql.SQL(stmt).format(s=sql.Identifier(schema))


def current_version(conn: psycopg.Connection, schema: str = SCHEMA) -> int:
    exists = conn.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = 'schema_migrations'",
        (schema,),
    ).fetchone()
    if not exists:
        return 0
    row = conn.execute(_q("SELECT max(version) FROM {s}.schema_migrations", schema)).fetchone()
    return row[0] or 0


def migrate(conn: psycopg.Connection, schema: str = SCHEMA) -> list[int]:
    """Apply pending migrations in one transaction. Returns the versions applied."""
    applied: list[int] = []
    with conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"leasehold-migrate:{schema}",))
        exists = conn.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s", (schema,)).fetchone()
        if not exists:
            conn.execute(_q("CREATE SCHEMA {s}", schema))
        conn.execute(
            _q(
                "CREATE TABLE IF NOT EXISTS {s}.schema_migrations ("
                "version int PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())",
                schema,
            )
        )
        done = {r[0] for r in conn.execute(_q("SELECT version FROM {s}.schema_migrations", schema))}
        for version, statements in MIGRATIONS:
            if version in done:
                continue
            for stmt in statements:
                conn.execute(_q(stmt, schema))
            conn.execute(
                _q("INSERT INTO {s}.schema_migrations (version) VALUES (%s)", schema), (version,)
            )
            applied.append(version)
    return applied


# --- apply ------------------------------------------------------------------------------------

_EGRESS_COLS = (
    "host",
    "subnet",
    "ipv6",
    "asn",
    "country",
    "kind",
    "status",
    "cost",
    "task_types",
    "exit_endpoint",
)


@dataclass
class EgressDiff:
    egresses: list[tuple[str, str, dict]] = field(default_factory=list)  # (action, address, row)
    # (action, name, (kind, budget per egress per minute))
    providers: list[tuple[str, str, tuple[str, int | None] | None]] = field(default_factory=list)
    cnames: tuple[int, int] = (0, 0)  # (added, removed)
    ranges: tuple[int, int] = (0, 0)
    settings: list[str] = field(default_factory=list)
    desired: EgressConfig | None = None

    @property
    def empty(self) -> bool:
        return not (
            self.egresses or self.providers or any(self.cnames) or any(self.ranges) or self.settings
        )

    def lines(self) -> list[str]:
        out = []
        for action, addr, row in self.egresses:
            detail = row.get("_detail", "")
            out.append(f"{action:<8} egress {addr}" + (f"  {detail}" if detail else ""))
        for action, name, want in self.providers:
            if want is None:
                out.append(f"{action:<8} provider {name}")
                continue
            kind, budget = want
            b = "no budget" if budget is None else f"{budget}/min per egress"
            out.append(f"{action:<8} provider {name}  {kind}, {b}")
        for what, (add, rem) in (("cname rules", self.cnames), ("range rules", self.ranges)):
            if add or rem:
                out.append(f"{'rules':<8} {what}: +{add} -{rem}")
        for name in self.settings:
            out.append(f"{'setting':<8} {name}")
        return out


def _settings_rows(cfg: EgressConfig) -> dict[str, dict]:
    b = cfg.budgets.model_dump(mode="json")
    return {
        k: b[k]
        for k in (
            "planning",
            "classes",
            "providers",
            "shared_ip",
            "network",
            "targets",
            "pair",
            "cooldown",
            "proxies",
        )
    }


def _norm(key: str, v):
    if v is None:
        return None
    if key == "cost":
        return float(v)
    if key == "task_types":
        return sorted(v)
    return str(v)


def diff(conn: psycopg.Connection, cfg: EgressConfig, schema: str = SCHEMA) -> EgressDiff:
    if current_version(conn, schema) < LATEST:
        raise RuntimeError(f"schema {schema} is not migrated; run `leasehold egress migrate`")
    d = EgressDiff(desired=cfg)

    cols = ", ".join(_EGRESS_COLS)
    have = {
        str(r[0]): dict(zip(_EGRESS_COLS, r[1:], strict=True))
        for r in conn.execute(
            _q(f"SELECT address, {cols} FROM {{s}}.egresses WHERE source = 'registry'", schema)
        )
    }
    want = {e.address: e.model_dump() for e in cfg.egresses}
    for addr, w in sorted(want.items()):
        h = have.get(addr)
        if h is None:
            d.egresses.append(("add", addr, w))
            continue
        changed = [k for k in _EGRESS_COLS if _norm(k, h[k]) != _norm(k, w[k])]
        if changed:
            detail = ", ".join(f"{k}: {h[k]} -> {w[k]}" for k in changed)
            d.egresses.append(("update", addr, {**w, "_detail": detail}))
    for addr, h in sorted(have.items()):
        if addr not in want and h["status"] != "disabled":
            d.egresses.append(("disable", addr, {"_detail": "not in the registry"}))

    have_p = {
        r[0]: (r[1], r[2])
        for r in conn.execute(
            _q("SELECT name, kind, per_egress_per_min FROM {s}.providers", schema)
        )
    }
    want_p = _providers(cfg)
    for name, want in sorted(want_p.items()):
        if name not in have_p:
            d.providers.append(("add", name, want))
        elif have_p[name] != want:
            d.providers.append(("update", name, want))
    for name in sorted(set(have_p) - set(want_p)):
        d.providers.append(("remove", name, None))

    have_c = {
        r[0]: r[1]
        for r in conn.execute(_q("SELECT suffix, provider FROM {s}.provider_cnames", schema))
    }
    have_r = {
        str(r[0]): r[1]
        for r in conn.execute(_q("SELECT network, provider FROM {s}.provider_ranges", schema))
    }
    d.cnames = _count_changes(have_c, cfg.rules.cnames)
    d.ranges = _count_changes(have_r, cfg.rules.ranges)

    have_s = {r[0]: r[1] for r in conn.execute(_q("SELECT name, value FROM {s}.settings", schema))}
    for name, value in _settings_rows(cfg).items():
        if have_s.get(name) != value:
            d.settings.append(name)
    return d


def _providers(cfg: EgressConfig) -> dict[str, tuple[str, int | None]]:
    return {name: (kind, cfg.provider_budget(name)) for name, kind in cfg.rules.providers.items()}


def _count_changes(have: dict[str, str], want: dict[str, str]) -> tuple[int, int]:
    added = sum(1 for k, v in want.items() if have.get(k) != v)
    removed = sum(1 for k, v in have.items() if want.get(k) != v)
    return added, removed


def apply(conn: psycopg.Connection, d: EgressDiff, schema: str = SCHEMA) -> None:
    """Apply a diff in one transaction. Rules and settings are replaced as a whole."""
    cfg = d.desired
    assert cfg is not None
    with conn.transaction():
        for action, addr, row in d.egresses:
            if action == "disable":
                conn.execute(
                    _q(
                        "UPDATE {s}.egresses SET status = 'disabled', updated_at = now() "
                        "WHERE address = %s AND source = 'registry'",
                        schema,
                    ),
                    (addr,),
                )
                continue
            conn.execute(
                _q(
                    f"""
                    INSERT INTO {{s}}.egresses (address, {", ".join(_EGRESS_COLS)})
                    VALUES (%s, {", ".join(["%s"] * len(_EGRESS_COLS))})
                    ON CONFLICT (address) DO UPDATE SET
                      {", ".join(f"{c} = EXCLUDED.{c}" for c in _EGRESS_COLS)},
                      updated_at = now()
                    WHERE egresses.source = 'registry'
                    """,
                    schema,
                ),
                [addr] + [row[c] for c in _EGRESS_COLS],
            )
        if d.providers or any(d.cnames) or any(d.ranges):
            want_p = _providers(cfg)
            conn.execute(_q("DELETE FROM {s}.provider_cnames", schema))
            conn.execute(_q("DELETE FROM {s}.provider_ranges", schema))
            conn.execute(
                _q("DELETE FROM {s}.providers WHERE NOT (name = ANY(%s))", schema),
                (list(want_p),),
            )
            for name, (kind, budget) in want_p.items():
                conn.execute(
                    _q(
                        "INSERT INTO {s}.providers (name, kind, per_egress_per_min) "
                        "VALUES (%s, %s, %s) ON CONFLICT (name) DO UPDATE SET "
                        "kind = EXCLUDED.kind, per_egress_per_min = EXCLUDED.per_egress_per_min, "
                        "updated_at = now()",
                        schema,
                    ),
                    (name, kind, budget),
                )
            _insert_pairs(conn, schema, "provider_cnames", "suffix", "text", cfg.rules.cnames)
            _insert_pairs(conn, schema, "provider_ranges", "network", "cidr", cfg.rules.ranges)
        for name, value in _settings_rows(cfg).items():
            if name in d.settings:
                conn.execute(
                    _q(
                        "INSERT INTO {s}.settings (name, value) VALUES (%s, %s) "
                        "ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value, "
                        "updated_at = now()",
                        schema,
                    ),
                    (name, json.dumps(value)),
                )


def _insert_pairs(
    conn, schema: str, table: str, col: str, col_type: str, pairs: dict[str, str]
) -> None:
    if not pairs:
        return
    conn.execute(
        sql.SQL(
            "INSERT INTO {s}.{t} ({c}, provider) "
            "SELECT k::{ty}, v FROM jsonb_each_text(%s::jsonb) AS x(k, v)"
        ).format(
            s=sql.Identifier(schema),
            t=sql.Identifier(table),
            c=sql.Identifier(col),
            ty=sql.SQL(col_type),
        ),
        (json.dumps(pairs),),
    )
