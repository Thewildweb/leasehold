"""``leasehold proxy ...``: proxy accounts, pools and fixed exits, through the control API.

The CLI talks to the schema owner's control API (``LEASEHOLD_API_URL``, an admin token
in ``LEASEHOLD_TOKEN``); it never connects to Postgres and never needs the secrets key.
Credentials are read from stdin or a prompt, never from the command line (shell history,
``ps``), and are never printed:

    pass proxies/resi-a | leasehold proxy account set-credentials resi-a
    leasehold proxy import --account isp-a --kind isp --country NL < list.txt

stdin for ``set-credentials`` is ``username:password`` on one line, or the username and
the password on two lines; ``--usage-key`` reads one line, the usage API key.
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
import sys
from typing import Any

from leasehold.client import ClientError, ControlClient, Unavailable

KINDS = ("datacenter", "residential", "isp", "mobile")


def _client() -> ControlClient:
    url = os.environ.get("LEASEHOLD_API_URL")
    token = os.environ.get("LEASEHOLD_TOKEN")
    if not url or not token:
        raise SystemExit("set LEASEHOLD_API_URL and LEASEHOLD_TOKEN (an admin token)")
    return ControlClient(url, token=token, retries=1)


def _call(method: str, path: str, body: Any = None) -> Any:
    c = _client()
    try:
        return c._request(method, path, body, repeatable=method in ("GET", "PATCH"))
    except ClientError as e:
        detail = e.detail if isinstance(e.detail, str) else json.dumps(e.detail)
        raise SystemExit(f"error {e.status}: {detail}") from None
    except Unavailable as e:
        raise SystemExit(f"control API unavailable: {e}") from None
    finally:
        c.close()


def _print(data: Any) -> None:
    print(json.dumps(data, indent=2, sort_keys=True, default=str))


def _csv(v: str | None) -> list[str] | None:
    return [x.strip() for x in v.split(",") if x.strip()] if v else None


def _read_secret_lines(n_prompt: list[tuple[str, bool]]) -> list[str]:
    """From stdin when it isn't a terminal, else by prompt (hidden for secrets)."""
    if not sys.stdin.isatty():
        lines = [ln.rstrip("\r\n") for ln in sys.stdin.read().splitlines() if ln.strip()]
        return lines
    out = []
    for label, hidden in n_prompt:
        out.append(getpass.getpass(f"{label}: ") if hidden else input(f"{label}: "))
    return out


def _credentials() -> tuple[str, str]:
    lines = _read_secret_lines([("username", False), ("password", True)])
    if len(lines) == 1 and ":" in lines[0]:
        user, _, password = lines[0].partition(":")
    elif len(lines) >= 2:
        user, password = lines[0], lines[1]
    else:
        raise SystemExit("expected username:password, or the username and password on two lines")
    if not user or not password:
        raise SystemExit("the username and the password must not be empty")
    return user, password


# --- commands -------------------------------------------------------------------------------


def _account_fields(args: argparse.Namespace) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k in ("vendor", "adapter", "cap_gb", "cap_period", "concurrency", "warn_remaining_gb"):
        v = getattr(args, k, None)
        if v is not None:
            out[k] = v
    if getattr(args, "anchor", None) is not None:
        out["period_anchor"] = args.anchor
    if getattr(args, "paid_until", None):
        out["paid_until"] = args.paid_until
    if getattr(args, "envs", None):
        out["envs"] = _csv(args.envs)
    if getattr(args, "adapter_config", None):
        try:
            out["adapter_config"] = json.loads(args.adapter_config)
        except json.JSONDecodeError as e:
            raise SystemExit(f"--adapter-config is not JSON: {e}") from None
    return out


def _cmd_account_add(args) -> int:
    _print(
        _call("POST", "/v1/admin/proxies/accounts", {"name": args.name, **_account_fields(args)})
    )
    return 0


def _cmd_account_update(args) -> int:
    body = _account_fields(args)
    if args.clear:
        body["clear"] = _csv(args.clear)
    _print(_call("PATCH", f"/v1/admin/proxies/accounts/{args.name}", body))
    return 0


def _cmd_account_credentials(args) -> int:
    if args.usage_key:
        lines = _read_secret_lines([("usage API key", True)])
        if len(lines) != 1:
            raise SystemExit("expected the usage key on one line")
        body = {"usage_key": lines[0]}
    else:
        user, password = _credentials()
        body = {"username": user, "password": password}
    _print(_call("POST", f"/v1/admin/proxies/accounts/{args.name}/credentials", body))
    return 0


def _cmd_account_action(args) -> int:
    _print(_call("POST", f"/v1/admin/proxies/accounts/{args.name}/{args.action}"))
    return 0


def _pool_fields(args) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k in ("kind", "endpoint", "cost_per_gb", "exit_ip_header", "concurrency"):
        v = getattr(args, k, None)
        if v is not None:
            out[k] = v
    if args.username_template is not None:
        out["username_template"] = args.username_template
    if args.sticky is not None:
        out["sticky"] = args.sticky
    if args.max_ttl is not None:
        out["max_sticky_ttl_s"] = args.max_ttl
    if args.weight is not None:
        out["egress_weight"] = args.weight
    if args.countries:
        out["countries"] = _csv(args.countries)
    if args.task_types:
        out["task_types"] = _csv(args.task_types)
    return out


def _cmd_pool_add(args) -> int:
    body = {"name": args.name, "account": args.account, **_pool_fields(args)}
    _print(_call("POST", "/v1/admin/proxies/pools", body))
    return 0


def _cmd_pool_update(args) -> int:
    _print(_call("PATCH", f"/v1/admin/proxies/pools/{args.name}", _pool_fields(args)))
    return 0


def _cmd_pool_action(args) -> int:
    _print(_call("POST", f"/v1/admin/proxies/pools/{args.name}/{args.action}"))
    return 0


def _cmd_exit_add(args) -> int:
    body: dict[str, Any] = {"account": args.account, "endpoint": args.endpoint, "kind": args.kind}
    for k in ("address", "subnet", "asn", "country"):
        if getattr(args, k) is not None:
            body[k] = getattr(args, k)
    if args.task_types:
        body["task_types"] = _csv(args.task_types)
    _print(_call("POST", "/v1/admin/proxies/exits", body))
    return 0


def _cmd_exit_update(args) -> int:
    body: dict[str, Any] = {}
    for k in ("country", "asn", "subnet", "kind"):
        if getattr(args, k) is not None:
            body[k] = getattr(args, k)
    if args.task_types:
        body["task_types"] = _csv(args.task_types)
    _print(_call("PATCH", f"/v1/admin/proxies/exits/{args.address}", body))
    return 0


def _cmd_exit_action(args) -> int:
    _print(_call("POST", f"/v1/admin/proxies/exits/{args.address}/{args.action}"))
    return 0


def _cmd_import(args) -> int:
    if sys.stdin.isatty():
        raise SystemExit("pipe the list in: leasehold proxy import ... < list.txt")
    text = sys.stdin.read()
    body: dict[str, Any] = {
        "account": args.account,
        "kind": args.kind,
        "lines": text,
        "scheme": args.scheme,
        "probe": not args.no_probe,
    }
    for k in ("vendor", "adapter", "country", "pool"):
        if getattr(args, k) is not None:
            body[k] = getattr(args, k)
    if args.countries:
        body["countries"] = _csv(args.countries)
    if args.task_types:
        body["task_types"] = _csv(args.task_types)
    if args.pool and not args.countries:
        raise SystemExit("a pool needs --countries")
    _print(_call("POST", "/v1/admin/proxies/import", body))
    return 0


def _cmd_list(args) -> int:
    q = []
    if args.account:
        q.append(f"account={args.account}")
    if args.unused:
        q.append("unused=1")
    data = _call("GET", "/v1/admin/proxies" + ("?" + "&".join(q) if q else ""))
    if args.json:
        _print(data)
        return 0
    for a in data["accounts"]:
        rem = "-" if a["remaining_ratio"] is None else f"{a['remaining_ratio']:.0%}"
        gb = "" if a["remaining_gb"] is None else f" ({a['remaining_gb']} GB left)"
        print(
            f"account {a['name']:<20} {a['vendor']:<12} {a['status']:<8} {a['state']:<16} "
            f"remaining {rem}{gb}  credentials {a.get('credentials_fp') or 'none'}"
        )
    for p in data["pools"]:
        over = f" [{p['override_status']}: {p['override_reason']}]" if p["override_status"] else ""
        print(
            f"pool    {p['name']:<20} {p['account']:<12} {p['kind']:<11} {p['status']}{over} "
            f"{p['endpoint']} countries {','.join(p['countries'] or []) or 'any'}"
        )
    for x in data["exits"]:
        over = f" [{x['override_status']}: {x['override_reason']}]" if x["override_status"] else ""
        print(
            f"exit    {x['endpoint']:<36} {x['account']:<12} {x['kind']:<6} "
            f"{x['address'] or '(address not learned)'} asn {x['asn'] or '-'} "
            f"{x['country'] or ''}{over}"
            + (f"  last error: {x['last_error']}" if x["last_error"] else "")
        )
    if args.unused:
        print("not named by any task type: " + (", ".join(data.get("unused", [])) or "none"))
    return 0


def _cmd_rekey(args) -> int:
    _print(_call("POST", "/v1/admin/proxies/rekey"))
    return 0


# --- parser -----------------------------------------------------------------------------------


def _account_options(p: argparse.ArgumentParser) -> None:
    p.add_argument("--vendor")
    p.add_argument("--adapter", help="usage adapter: generic, or an installed one")
    p.add_argument("--adapter-config", help="JSON: the adapter's settings and error rules")
    p.add_argument("--cap-gb", type=float, help="our own cap per period, in GB (10^9 bytes)")
    p.add_argument("--period", dest="cap_period", choices=("month", "day"))
    p.add_argument("--anchor", type=int, help="day of the month the period starts (1-28)")
    p.add_argument("--paid-until", help="YYYY-MM-DD")
    p.add_argument("--concurrency", type=int, help="the vendor's limit on open connections")
    p.add_argument("--envs", help="environments that may use it, e.g. prod,dev")
    p.add_argument("--warn-remaining-gb", type=float, help="also warn below this many GB left")


def _pool_options(p: argparse.ArgumentParser, add: bool) -> None:
    p.add_argument("--kind", choices=KINDS, required=add)
    p.add_argument("--endpoint", required=add, help="scheme://host:port of the gateway")
    p.add_argument(
        "--username-template", help="the vendor's syntax: {user} {country} {session} {ttl_min}"
    )
    p.add_argument("--sticky", action=argparse.BooleanOptionalAction, default=None)
    p.add_argument("--max-ttl", type=int, help="longest sticky session, seconds")
    p.add_argument("--countries", help="e.g. NL,BE")
    p.add_argument("--cost-per-gb", type=float)
    p.add_argument("--weight", type=int, help="how many egresses the pool counts as (default 4)")
    p.add_argument("--exit-ip-header")
    p.add_argument("--concurrency", type=int)
    p.add_argument("--task-types", help="task types that may use it (default: any)")


def add_parser(sub) -> None:
    pr = sub.add_parser(
        "proxy", help="proxy accounts, pools and exits (through the control API, admin token)"
    )
    ps = pr.add_subparsers(dest="proxy_command", required=True)

    acc = ps.add_parser("account", help="proxy accounts: one contract with one vendor")
    accs = acc.add_subparsers(dest="account_command", required=True)
    a = accs.add_parser("add", help="add an account (it starts disabled)")
    a.add_argument("name")
    _account_options(a)
    a.set_defaults(func=_cmd_account_add)
    u = accs.add_parser("update", help="change an account's settings")
    u.add_argument("name")
    _account_options(u)
    u.add_argument("--clear", help="set back to none: cap_gb,paid_until,concurrency,...")
    u.set_defaults(func=_cmd_account_update)
    c = accs.add_parser(
        "set-credentials", help="store the account's credentials (stdin or prompt, never argv)"
    )
    c.add_argument("name")
    c.add_argument("--usage-key", action="store_true", help="the usage API key instead")
    c.set_defaults(func=_cmd_account_credentials)
    for action, help_ in (
        ("drain", "no new routes; running leases go on"),
        ("disable", "no routes; running leases end"),
        ("enable", "active and ok again (without a probe)"),
        ("probe", "learn the exits' addresses and probe the account now"),
    ):
        x = accs.add_parser(action, help=help_)
        x.add_argument("name")
        x.set_defaults(func=_cmd_account_action, action=action)

    pool = ps.add_parser("pool", help="rotating pools (gateways)")
    pools = pool.add_subparsers(dest="pool_command", required=True)
    a = pools.add_parser("add", help="add a pool")
    a.add_argument("name")
    a.add_argument("--account", required=True)
    _pool_options(a, add=True)
    a.set_defaults(func=_cmd_pool_add)
    u = pools.add_parser("update", help="change a pool")
    u.add_argument("name")
    _pool_options(u, add=False)
    u.set_defaults(func=_cmd_pool_update)
    for action in ("drain", "disable", "enable", "remove"):
        x = pools.add_parser(action, help=f"{action} a pool")
        x.add_argument("name")
        x.set_defaults(func=_cmd_pool_action, action=action)

    ex = ps.add_parser("exit", help="fixed exits (one address each)")
    exs = ex.add_subparsers(dest="exit_command", required=True)
    a = exs.add_parser("add", help="add one endpoint; the probe learns its address")
    a.add_argument("--account", required=True)
    a.add_argument("--endpoint", required=True)
    a.add_argument("--kind", choices=KINDS, default="isp")
    a.add_argument("--address")
    a.add_argument("--subnet")
    a.add_argument("--asn", type=int)
    a.add_argument("--country")
    a.add_argument("--task-types")
    a.set_defaults(func=_cmd_exit_add)
    u = exs.add_parser("update", help="change a live exit")
    u.add_argument("address")
    u.add_argument("--country")
    u.add_argument("--asn", type=int)
    u.add_argument("--subnet")
    u.add_argument("--kind", choices=KINDS)
    u.add_argument("--task-types")
    u.set_defaults(func=_cmd_exit_update)
    for action in ("drain", "disable", "enable", "remove"):
        x = exs.add_parser(action, help=f"{action} an egress (drain/disable/enable: VPSes too)")
        x.add_argument("address")
        x.set_defaults(func=_cmd_exit_action, action=action)

    im = ps.add_parser("import", help="a vendor's host:port:username:password list from stdin")
    im.add_argument("--account", required=True)
    im.add_argument("--kind", choices=KINDS, required=True)
    im.add_argument("--country", help="exit country of fixed exits")
    im.add_argument("--pool", help="the line is a rotating gateway: make this pool")
    im.add_argument("--countries", help="the pool's exit countries, e.g. NL")
    im.add_argument("--vendor")
    im.add_argument("--adapter")
    im.add_argument("--scheme", choices=("http", "https", "socks5"), default="http")
    im.add_argument("--task-types")
    im.add_argument("--no-probe", action="store_true", help="don't learn addresses now")
    im.set_defaults(func=_cmd_import)

    ls = ps.add_parser("list", help="accounts, pools and exits (no credentials)")
    ls.add_argument("--account")
    ls.add_argument("--unused", action="store_true", help="also: what no task type names")
    ls.add_argument("--json", action="store_true")
    ls.set_defaults(func=_cmd_list)

    rk = ps.add_parser("rekey", help="re-encrypt every credential with the newest key")
    rk.set_defaults(func=_cmd_rekey)
