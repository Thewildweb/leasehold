"""Admin endpoints for proxy accounts, pools and fixed exits (``leasehold proxy ...``).

All need an admin token. Proxy rows are shared by every environment and changed only
through the schema owner's API (``LEASEHOLD_EGRESS_OWNER``); other APIs answer 409.
Credentials go in, never out: no response carries a password, a username, a usage key
or a ciphertext, only fingerprints. Request bodies are not logged.

    GET   /v1/admin/proxies                         accounts, pools, exits (?account=, ?unused=1)
    POST  /v1/admin/proxies/accounts                add an account
    PATCH /v1/admin/proxies/accounts/{name}         change caps, adapter, envs, ...
    POST  /v1/admin/proxies/accounts/{name}/credentials   {username, password} or {usage_key}
    POST  /v1/admin/proxies/accounts/{name}/{drain|disable|enable|probe|poll}
    POST  /v1/admin/proxies/pools                   add a rotating pool
    PATCH /v1/admin/proxies/pools/{name}
    POST  /v1/admin/proxies/pools/{name}/{drain|disable|enable|remove}
    POST  /v1/admin/proxies/exits                   add a fixed exit (an endpoint)
    PATCH /v1/admin/proxies/exits/{address}
    POST  /v1/admin/proxies/exits/{address}/{drain|disable|enable|remove}
    POST  /v1/admin/proxies/import                  a vendor's host:port:user:password list
    POST  /v1/admin/proxies/rekey                   re-encrypt with the newest key
"""

import asyncio
import logging
import re
from datetime import date
from typing import Annotated, Any, Literal

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, SecretStr

from leasehold.egress.probe import Prober
from leasehold.egress.proxy_admin import ProxyAdmin, ProxyAdminError, parse_lines
from leasehold.egress.quota import QuotaKeeper
from leasehold.egress.secrets import SecretsError

log = logging.getLogger("leasehold.api.proxies")

Name = Annotated[str, Field(pattern=r"^[a-z0-9][a-z0-9-]{0,62}$")]
Country = Annotated[str, Field(pattern=r"^[A-Za-z]{2}$")]
Kind = Literal["datacenter", "residential", "isp", "mobile"]

_URL_CREDS = re.compile(r"(\w+://)[^/@\s]+@")


def redact_text(text: str) -> str:
    """Drop ``user:pass@`` from any URL in a text (proxy error excerpts)."""
    return _URL_CREDS.sub(r"\1***@", text or "")[:200]


class _M(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AccountIn(_M):
    name: Name
    vendor: str | None = Field(default=None, max_length=100)
    adapter: str | None = Field(default=None, max_length=100)
    adapter_config: dict | None = None
    cap_gb: float | None = Field(default=None, gt=0)
    cap_period: Literal["month", "day"] | None = None
    period_anchor: int | None = Field(default=None, ge=1, le=28)
    paid_until: date | None = None
    concurrency: int | None = Field(default=None, ge=1)
    envs: list[str] | None = None
    warn_remaining_gb: float | None = Field(default=None, gt=0)


class AccountPatch(_M):
    vendor: str | None = Field(default=None, max_length=100)
    adapter: str | None = Field(default=None, max_length=100)
    adapter_config: dict | None = None
    cap_gb: float | None = Field(default=None, gt=0)
    cap_period: Literal["month", "day"] | None = None
    period_anchor: int | None = Field(default=None, ge=1, le=28)
    paid_until: date | None = None
    concurrency: int | None = Field(default=None, ge=1)
    envs: list[str] | None = None
    warn_remaining_gb: float | None = Field(default=None, gt=0)
    clear: list[Literal["cap_gb", "paid_until", "concurrency", "warn_remaining_gb"]] = Field(
        default_factory=list, description="Fields to set back to none"
    )


class CredentialsIn(_M):
    username: SecretStr | None = None
    password: SecretStr | None = None
    usage_key: SecretStr | None = None


class PoolIn(_M):
    name: Name
    account: Name
    kind: Kind
    endpoint: str = Field(max_length=300)
    username_template: str | None = Field(default=None, max_length=300)
    sticky: bool | None = None
    max_sticky_ttl_s: int | None = Field(default=None, gt=0)
    countries: list[Country] | None = None
    cost_per_gb: float | None = Field(default=None, ge=0)
    concurrency: int | None = Field(default=None, ge=1)
    egress_weight: int | None = Field(default=None, ge=1)
    exit_ip_header: str | None = Field(default=None, max_length=100)
    task_types: list[str] | None = None


class PoolPatch(_M):
    kind: Kind | None = None
    endpoint: str | None = Field(default=None, max_length=300)
    username_template: str | None = Field(default=None, max_length=300)
    sticky: bool | None = None
    max_sticky_ttl_s: int | None = Field(default=None, gt=0)
    countries: list[Country] | None = None
    cost_per_gb: float | None = Field(default=None, ge=0)
    concurrency: int | None = Field(default=None, ge=1)
    egress_weight: int | None = Field(default=None, ge=1)
    exit_ip_header: str | None = Field(default=None, max_length=100)
    task_types: list[str] | None = None


class ExitIn(_M):
    account: Name
    endpoint: str = Field(max_length=300)
    kind: Kind = "isp"
    country: Country | None = None
    task_types: list[str] | None = None
    address: str | None = None
    subnet: str | None = None
    asn: int | None = Field(default=None, ge=0)


class ExitPatch(_M):
    country: Country | None = None
    task_types: list[str] | None = None
    kind: Kind | None = None
    asn: int | None = Field(default=None, ge=0)
    subnet: str | None = None


class ImportIn(_M):
    account: Name
    kind: Kind
    lines: SecretStr = Field(description="host:port:username:password per line")
    scheme: Literal["http", "https", "socks5"] = "http"
    vendor: str | None = Field(default=None, max_length=100)
    adapter: str | None = Field(default=None, max_length=100)
    country: Country | None = None
    pool: Name | None = None
    countries: list[Country] | None = None
    task_types: list[str] | None = None
    probe: bool = Field(default=True, description="Learn the exits' addresses after import")


def _fields(model: BaseModel, skip: tuple[str, ...] = ()) -> dict[str, Any]:
    return {k: v for k, v in model.model_dump(exclude_none=True).items() if k not in skip}


def register_proxy_routes(
    app: FastAPI,
    pool,
    admin: ProxyAdmin,
    dispatcher,
    registry,
    settings,
    caller_dep,
    keyring,
    probe_transport=None,
    adapters: dict | None = None,
) -> None:
    background: set[asyncio.Task] = set()

    @app.exception_handler(ProxyAdminError)
    async def _admin_error(request, exc: ProxyAdminError):
        return JSONResponse({"detail": str(exc)}, status_code=exc.status)

    @app.exception_handler(SecretsError)
    async def _secrets_error(request, exc: SecretsError):
        return JSONResponse({"detail": f"secrets: {exc}"}, status_code=503)

    async def admin_caller(c=Depends(caller_dep)):  # noqa: B008
        if not c.admin:
            raise HTTPException(403, f"{c.name} is not an admin")
        return c

    async def writer(c=Depends(admin_caller)):  # noqa: B008
        if not settings.egress_owner:
            raise HTTPException(
                409, "proxies are changed through the egress schema owner's API (prod)"
            )
        return c

    Admin = Annotated[Any, Depends(admin_caller)]
    Writer = Annotated[Any, Depends(writer)]

    async def prober() -> Prober:
        async with pool.connection() as conn:
            cfg = (await dispatcher.budgets(conn)).proxies
        return Prober(admin, cfg.echo_url, transport=probe_transport)

    def later(coro) -> None:
        t = asyncio.create_task(coro)
        background.add(t)
        t.add_done_callback(background.discard)

    async def probe_account(name: str) -> dict:
        p = await prober()
        async with pool.connection() as conn:
            exits = await p.recheck_exits(conn, account=name)
            result = await p.probe_account(conn, name)
        return {
            "exits": exits,
            "probe": {
                "ok": result.address is not None,
                "status": result.status,
                "error": result.error,
            },
        }

    async def probe_quietly(name: str) -> None:
        try:
            await probe_account(name)
        except Exception:  # noqa: BLE001 - a background probe only logs
            log.exception("proxy probe failed", extra={"fields": {"account": name}})

    async def poll_account(name: str, actor: str) -> dict:
        """Run the account's usage adapter once, whatever its status: a new account
        (disabled) gets the vendor's plan size before drain or enable, which need a limit."""
        keeper = QuotaKeeper(admin, adapters or {})
        async with pool.connection() as conn:
            await admin.account(conn, name)
            result = (await keeper.poll(conn, account=name))[name]
            row = await admin.account(conn, name)
            await admin.audit(conn, actor, "poll", "account", name, {"result": result})
        return {
            "account": name,
            "result": result,
            **{
                k: row[k]
                for k in (
                    "vendor_used_bytes",
                    "vendor_limit_bytes",
                    "vendor_remaining_bytes",
                    "vendor_period_end",
                    "vendor_seen_at",
                    "vendor_error",
                )
            },
        }

    @app.get("/v1/admin/proxies")
    async def list_proxies(c: Admin, account: str | None = None, unused: bool = False):
        async with pool.connection() as conn:
            out = await admin.listing(conn, account)
        named = {
            n for spec in registry.egress_specs.values() if spec is not None for n in spec.names()
        }
        out["named_by_task_types"] = sorted(named)
        if unused:
            used = named | {p["account"] for p in out["pools"] if p["name"] in named}
            listed = {a["name"] for a in out["accounts"]} | {p["name"] for p in out["pools"]}
            out["unused"] = sorted(listed - used)
        return out

    @app.post("/v1/admin/proxies/accounts")
    async def add_account(req: AccountIn, c: Writer):
        async with pool.connection() as conn:
            await admin.add_account(conn, c.name, req.name, **_fields(req, ("name",)))
        return {"added": req.name, "status": "disabled"}

    @app.patch("/v1/admin/proxies/accounts/{name}")
    async def update_account(name: str, req: AccountPatch, c: Writer):
        fields = _fields(req, ("clear",))
        for k in req.clear:
            fields[k] = None
        async with pool.connection() as conn:
            changed = await admin.update_account(conn, c.name, name, **fields)
        return {"changed": sorted(changed)}

    @app.post("/v1/admin/proxies/accounts/{name}/credentials")
    async def set_credentials(name: str, req: CredentialsIn, c: Writer):
        out: dict[str, Any] = {}
        async with pool.connection() as conn:
            if req.username is not None or req.password is not None:
                if req.username is None or req.password is None:
                    raise HTTPException(422, "username and password go together")
                out["credentials_fp"] = await admin.set_credentials(
                    conn,
                    c.name,
                    name,
                    req.username.get_secret_value(),
                    req.password.get_secret_value(),
                )
            if req.usage_key is not None:
                out["usage_fp"] = await admin.set_usage_key(
                    conn, c.name, name, req.usage_key.get_secret_value().encode()
                )
        if not out:
            raise HTTPException(422, "give username and password, or usage_key")
        if "credentials_fp" in out:
            later(probe_quietly(name))
        return out

    @app.post("/v1/admin/proxies/accounts/{name}/{action}")
    async def account_action(
        name: str, action: Literal["drain", "disable", "enable", "probe", "poll"], c: Writer
    ):
        if action == "poll":
            return await poll_account(name, c.name)
        if action == "probe":
            async with pool.connection() as conn:
                await admin.account(conn, name)
            return await probe_account(name)
        async with pool.connection() as conn:
            changes = await admin.account_status(conn, c.name, name, action)
        return {"account": name, "action": action, "changed": changes}

    @app.post("/v1/admin/proxies/pools")
    async def add_pool(req: PoolIn, c: Writer):
        async with pool.connection() as conn:
            await admin.add_pool(
                conn, c.name, req.name, req.account, **_fields(req, ("name", "account"))
            )
        return {"added": req.name, "key": f"pool:{req.name}"}

    @app.patch("/v1/admin/proxies/pools/{name}")
    async def update_pool(name: str, req: PoolPatch, c: Writer):
        async with pool.connection() as conn:
            changed = await admin.update_pool(conn, c.name, name, **_fields(req))
        return {"changed": sorted(changed)}

    @app.post("/v1/admin/proxies/pools/{name}/{action}")
    async def pool_action(
        name: str, action: Literal["drain", "disable", "enable", "remove"], c: Writer
    ):
        async with pool.connection() as conn:
            await admin.pool_status(conn, c.name, name, action)
        return {"pool": name, "action": action}

    @app.post("/v1/admin/proxies/exits")
    async def add_exit(req: ExitIn, c: Writer):
        async with pool.connection() as conn:
            exit_id = await admin.add_exit(
                conn,
                c.name,
                req.account,
                req.endpoint,
                kind=req.kind,
                country=req.country.upper() if req.country else None,
                task_types=req.task_types,
                address=req.address,
                subnet=req.subnet,
                asn=req.asn,
            )
        if req.address is None:
            later(probe_quietly(req.account))
        return {"added": exit_id}

    @app.patch("/v1/admin/proxies/exits/{address}")
    async def update_exit(address: str, req: ExitPatch, c: Writer):
        async with pool.connection() as conn:
            changed = await admin.update_exit(conn, c.name, address, **_fields(req))
        return {"changed": sorted(changed)}

    @app.post("/v1/admin/proxies/exits/{address}/{action}")
    async def exit_action(
        address: str, action: Literal["drain", "disable", "enable", "remove"], c: Writer
    ):
        async with pool.connection() as conn:
            await admin.exit_status(conn, c.name, address, action)
        return {"egress": address, "action": action}

    @app.post("/v1/admin/proxies/import")
    async def import_list(req: ImportIn, c: Writer):
        lines = parse_lines(req.lines.get_secret_value(), req.scheme)
        async with pool.connection() as conn:
            r = await admin.import_list(
                conn,
                c.name,
                req.account,
                req.kind,
                lines,
                vendor=req.vendor,
                adapter=req.adapter,
                country=req.country.upper() if req.country else None,
                pool=req.pool,
                countries=req.countries,
                task_types=req.task_types,
            )
        if req.probe and (r.exits_created or r.credentials_changed):
            later(probe_quietly(req.account))
        return {
            "account": r.account,
            "account_created": r.account_created,
            "credentials_changed": r.credentials_changed,
            "exits_created": r.exits_created,
            "exits_existing": r.exits_existing,
            "pools_created": r.pools_created,
            "pools_updated": r.pools_updated,
            "note": "new accounts start disabled; check the exits, then enable",
        }

    @app.post("/v1/admin/proxies/rekey")
    async def rekey(c: Writer):
        async with pool.connection() as conn:
            n = await admin.rekey(conn, c.name)
        return {"reencrypted": n, "key_id": keyring.current}
