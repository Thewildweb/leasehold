"""Egress configuration files: the registry, provider rules and budgets.

An operations repository keeps three files in one directory, applied to Postgres
with ``leasehold egress apply <dir>``:

``registry.yml``: the source addresses outgoing requests may use::

    defaults: { asn: 64500, country: NL, kind: datacenter, status: active }
    egresses:
      - { address: 192.0.2.10, host: worker-1, ipv6: "2001:db8:1::/64" }
      - { address: 192.0.2.11, host: worker-2 }

The subnet of an IPv4 egress is its /24 unless ``subnet`` is given. An egress that runs
an exit (``leasehold.exit``) gives its endpoint on the private network, for task types
that reach the datacenter tier through exits (``datacenter_via: exit``)::

      - { address: 192.0.2.12, host: worker-3, exit: { endpoint: "http://100.64.0.12:3130" } }

``providers.yml``: network groups that score an egress across many domains (CDNs,
WAFs, hosted platforms), and how a target is recognised::

    providers:
      example-cdn:
        cname: [cdn.example.net]          # a name in the CNAME chain ends with it
        cidr: [198.51.100.0/24]           # the target IP is in it
        cidr_files: [ranges/example.txt]  # one prefix per line, '#' comments

``budgets.yml``: planning settings and limits; see ``Budgets``.
"""

from __future__ import annotations

import ipaddress
import re
from datetime import timedelta
from pathlib import Path
from typing import Annotated, Literal

import yaml
from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    model_validator,
)

from leasehold.types import Duration

REGISTRY_FILE = "registry.yml"
PROVIDERS_FILE = "providers.yml"
BUDGETS_FILE = "budgets.yml"

ProviderName = Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9-]*$", max_length=60)]
_HOSTNAME = re.compile(r"^([a-z0-9_]([a-z0-9_-]*[a-z0-9])?\.)*[a-z0-9]+\Z")
Hostname = Annotated[
    str,
    StringConstraints(
        pattern=r"^([a-z0-9_]([a-z0-9_-]*[a-z0-9])?\.)*[a-z0-9]+$", min_length=1, max_length=253
    ),
]
EgressKind = Literal["datacenter", "residential"]
EgressStatus = Literal["active", "drain", "disabled"]
# cdn and platform: score clients across their customers' domains; parking: registrar
# parking and for-sale servers, whose domains need few fetches rather than a budget.
ProviderKind = Literal["cdn", "platform", "parking"]
# Kinds whose targets are leased per domain; they must have a budget, which holds the
# rate across their domains.
PER_DOMAIN_KINDS = ("cdn", "platform")

# Provider names of targets recognised by their shared IP rather than a rule.
SHARED_IP_PREFIX = "ip:"


class ConfigError(ValueError):
    pass


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


# --- registry ---------------------------------------------------------------------------------


class EgressDefaults(_Strict):
    asn: int | None = Field(default=None, ge=1, le=4294967295)
    country: str | None = Field(default=None, pattern=r"^[A-Z]{2}$")
    kind: EgressKind = "datacenter"
    status: EgressStatus = "active"


class ExitEntry(_Strict):
    endpoint: str = Field(
        description="http://host:port of the exit on the private network (leasehold.exit)"
    )

    @model_validator(mode="after")
    def _check(self) -> ExitEntry:
        m = re.fullmatch(r"http://([^/:@\s]+|\[[0-9a-fA-F:]+\]):(\d{1,5})", self.endpoint)
        if m is None or not 0 < int(m.group(2)) < 65536:
            raise ValueError(f"exit endpoint must be http://host:port, not {self.endpoint!r}")
        return self


class EgressEntry(_Strict):
    address: str
    host: str | None = Field(default=None, max_length=100)
    subnet: str | None = None
    ipv6: str | None = None
    asn: int | None = Field(default=None, ge=1, le=4294967295)
    country: str | None = Field(default=None, pattern=r"^[A-Z]{2}$")
    kind: EgressKind | None = None
    status: EgressStatus | None = None
    cost: float | None = Field(default=None, ge=0)
    task_types: list[str] | None = Field(
        default=None, description="Only these task types may use this egress; unset: any"
    )
    exit: ExitEntry | None = None


class Egress(_Strict):
    """One egress with defaults applied and its subnet derived."""

    address: str
    host: str | None
    subnet: str
    ipv6: str | None
    asn: int | None
    country: str | None
    kind: EgressKind
    status: EgressStatus
    cost: float | None
    task_types: list[str] | None = None
    exit_endpoint: str | None = None


class RegistryFile(_Strict):
    defaults: EgressDefaults = EgressDefaults()
    egresses: list[EgressEntry] = Field(default_factory=list)

    def resolved(self) -> list[Egress]:
        out: list[Egress] = []
        seen: set[str] = set()
        d = self.defaults
        for e in self.egresses:
            try:
                addr = ipaddress.ip_address(e.address)
            except ValueError:
                raise ConfigError(f"egress {e.address!r}: not an IP address") from None
            if str(addr) in seen:
                raise ConfigError(f"egress {addr} is listed twice")
            seen.add(str(addr))
            if e.subnet is not None:
                subnet = _network(e.subnet, f"egress {addr}: subnet")
                if addr not in subnet:
                    raise ConfigError(f"egress {addr} is not in its subnet {subnet}")
            else:
                prefix = 24 if addr.version == 4 else 64
                subnet = ipaddress.ip_network(f"{addr}/{prefix}", strict=False)
            ipv6 = None
            if e.ipv6 is not None:
                net = _network(e.ipv6, f"egress {addr}: ipv6", strict=False)
                if net.version != 6:
                    raise ConfigError(f"egress {addr}: ipv6 {e.ipv6!r} is not an IPv6 network")
                ipv6 = str(net)
            out.append(
                Egress(
                    address=str(addr),
                    host=e.host,
                    subnet=str(subnet),
                    ipv6=ipv6,
                    asn=e.asn if e.asn is not None else d.asn,
                    country=e.country or d.country,
                    kind=e.kind or d.kind,
                    status=e.status or d.status,
                    cost=e.cost,
                    task_types=sorted(e.task_types) if e.task_types is not None else None,
                    exit_endpoint=e.exit.endpoint if e.exit is not None else None,
                )
            )
        return out


# --- providers --------------------------------------------------------------------------------


class ProviderEntry(_Strict):
    kind: ProviderKind
    description: str | None = None
    cname: list[Hostname] = Field(default_factory=list)
    cidr: list[str] = Field(default_factory=list)
    cidr_files: list[str] = Field(default_factory=list)


class ProvidersFile(_Strict):
    providers: dict[ProviderName, ProviderEntry] = Field(default_factory=dict)


class ProviderRules(_Strict):
    """Provider rules with every range file read: what is stored and served."""

    cnames: dict[str, str] = Field(default_factory=dict)  # suffix -> provider
    ranges: dict[str, str] = Field(default_factory=dict)  # network -> provider
    providers: dict[str, ProviderKind] = Field(default_factory=dict)  # name -> kind


# --- budgets ----------------------------------------------------------------------------------


class Planning(_Strict):
    window: Duration = timedelta(seconds=60)
    horizon: Duration = timedelta(minutes=15)
    lend_after: Duration = timedelta(seconds=30)

    @model_validator(mode="after")
    def _check(self) -> Planning:
        if self.window <= timedelta(0):
            raise ValueError("planning.window must be positive")
        if self.horizon < self.window:
            raise ValueError("planning.horizon must be at least one window")
        if not timedelta(0) <= self.lend_after <= self.window:
            raise ValueError("planning.lend_after must be within the window")
        return self


class Classes(_Strict):
    order: list[Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9-]*$")]] = Field(
        default_factory=lambda: ["critical", "normal", "bulk"], min_length=1
    )
    dev_offset: int = Field(default=1, ge=0)
    min_share: dict[str, float] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _check(self) -> Classes:
        if len(set(self.order)) != len(self.order):
            raise ValueError("classes.order lists a class twice")
        for name, share in self.min_share.items():
            if name not in self.order:
                raise ValueError(f"classes.min_share: unknown class {name!r}")
            if not 0 <= share <= 1:
                raise ValueError(f"classes.min_share.{name} must be between 0 and 1")
        if sum(self.min_share.values()) > 1:
            raise ValueError("classes.min_share adds up to more than 1")
        return self


class ProviderBudgets(_Strict):
    default: int | None = Field(
        default=None, ge=0, description="Per egress per minute, for every provider in the rules"
    )
    per_kind: dict[ProviderKind, int | None] = Field(
        default_factory=dict, description="Instead of the default, for providers of a kind"
    )
    overrides: dict[ProviderName, int | None] = Field(default_factory=dict)

    def for_provider(self, name: str, kind: str) -> int | None:
        if name in self.overrides:
            return self.overrides[name]
        if kind in self.per_kind:
            return self.per_kind[kind]
        return self.default


class SharedIp(_Strict):
    min_domains: int = Field(ge=2)
    per_egress: int = Field(ge=0, description="Per minute")
    fleet_per_second: float = Field(gt=0)


class Network(_Strict):
    per_subnet: int | None = Field(default=None, ge=0, description="Per minute")
    per_asn: int | None = Field(default=None, ge=0, description="Per minute")


def _target_pattern(v: str) -> str:
    """A target's name (a host or an IP address, as tasks carry it) or ``*.`` and a host."""
    try:
        return str(ipaddress.ip_address(v))
    except ValueError:
        pass
    if not _HOSTNAME.match(v.removeprefix("*.")) or len(v) > 253:
        raise ValueError(f"{v!r} is not a lower-case host, an IP address or '*.' and a host")
    return v


class TargetOverride(_Strict):
    """Limits of the targets a pattern matches; an unset one is the general limit."""

    interval: Duration | None = Field(default=None, description="Instead of targets.interval")
    per_hour: int | None = Field(
        default=None, ge=1, description="Instead of pair.per_hour, per egress and target"
    )

    @model_validator(mode="after")
    def _check(self) -> TargetOverride:
        if self.interval is None and self.per_hour is None:
            raise ValueError("a target override sets interval, per_hour or both")
        if self.interval is not None and self.interval <= timedelta(0):
            raise ValueError("a target override's interval must be positive")
        return self


class Targets(_Strict):
    """The pace towards one target (a server, or a domain behind a CDN or platform),
    for the whole fleet: one egress holds the target at a time and sends at most one
    request per ``interval``.

    ``overrides`` sets limits by target, before its kind::

        overrides:
          api.example.com: { interval: 3s }
          "*.example.net": { interval: 10s, per_hour: 100 }

    The key is the target's exact name, or ``*.`` and a host for every name below that
    host (not the host itself). One entry applies: the exact name, else the longest
    matching pattern. A pattern sets each target's own limit, not one they share.
    """

    interval: Duration = timedelta(seconds=5)
    per_kind: dict[Literal["cdn", "platform", "parking", "shared_ip", "none"], Duration] = Field(
        default_factory=dict, description="Instead of interval, by the target's kind"
    )
    overrides: dict[Annotated[str, AfterValidator(_target_pattern)], TargetOverride] = Field(
        default_factory=dict, description="Instead of the interval and the pair budget, by target"
    )

    @model_validator(mode="after")
    def _check(self) -> Targets:
        for v in (self.interval, *self.per_kind.values()):
            if v <= timedelta(0):
                raise ValueError("targets: intervals must be positive")
        return self

    def override_for(self, target: str) -> TargetOverride | None:
        if target in self.overrides:
            return self.overrides[target]
        rest = target
        while "." in rest:
            rest = rest.split(".", 1)[1]
            if f"*.{rest}" in self.overrides:
                return self.overrides[f"*.{rest}"]
        return None

    def interval_for(self, kind: str, target: str = "") -> timedelta:
        override = self.override_for(target)
        if override is not None and override.interval is not None:
            return override.interval
        return self.per_kind.get(kind or "none", self.interval)  # type: ignore[call-overload]


class Pair(_Strict):
    per_hour: int | None = Field(
        default=None,
        ge=1,
        description="Requests per (egress, target) per hour; then the target goes to "
        "another egress. Unset: no pair budget",
    )


class Cooldown(_Strict):
    max: Duration = Field(
        default=timedelta(hours=6), description="Longest cooldown a worker can set on a target"
    )


class Proxies(_Strict):
    """Proxy accounts, exits and pools (leasehold.egress.proxies)."""

    tier_ttl: Duration = Field(
        default=timedelta(days=30),
        description="A target escalated to a higher tier starts at the first again after this",
    )
    max_connect_failures: int = Field(
        default=5, ge=1, description="Connect failures in a row that disable an exit or pool"
    )
    unreachable_exits: int = Field(
        default=3,
        ge=2,
        description="Exits or pools of one account failing within unreachable_window, with no "
        "success between: the account is unreachable",
    )
    unreachable_window: Duration = timedelta(seconds=60)
    gateway_targets: int = Field(
        default=3,
        ge=2,
        description="A proxy answering CONNECT with 502/503/504 counts as a connect failure "
        "of the exit or pool only once this many distinct targets got it within "
        "gateway_window (one dead site is the site, not the exit)",
    )
    gateway_window: Duration = timedelta(minutes=10)
    exit_recheck: Duration = Field(
        default=timedelta(hours=1), description="How often each fixed exit's address is checked"
    )
    probe_interval: Duration = Field(
        default=timedelta(minutes=10), description="How often an account that isn't ok is probed"
    )
    usage_poll: Duration = Field(
        default=timedelta(minutes=15), description="How often vendors' usage APIs are read"
    )
    usage_poll_warn_after: Duration = Field(
        default=timedelta(hours=1), description="A usage poll failing this long is a warning"
    )
    echo_url: str = Field(
        default="https://api.ipify.org",
        description="IP echo: answers with the caller's address as text",
    )
    remove_after: Duration = Field(
        default=timedelta(days=30), description="Removed exits and pools are deleted after this"
    )
    warn_used: float = Field(default=0.9, gt=0, lt=1, description="Warning: share of quota used")
    paid_until_warn: Duration = Field(
        default=timedelta(days=7), description="Warning: an account's paid_until is this close"
    )
    drift_warn: float = Field(
        default=0.10,
        gt=0,
        lt=1,
        description="Info: our byte count and the vendor's differ by this share of the plan",
    )


class Budgets(_Strict):
    planning: Planning = Planning()
    classes: Classes = Classes()
    providers: ProviderBudgets = ProviderBudgets()
    shared_ip: SharedIp | None = None
    network: Network = Network()
    targets: Targets = Targets()
    pair: Pair = Pair()
    cooldown: Cooldown = Cooldown()
    proxies: Proxies = Proxies()

    def pair_per_hour(self, target: str) -> int | None:
        """Requests per (egress, target) per hour; None: no pair budget."""
        override = self.targets.override_for(target)
        if override is not None and override.per_hour is not None:
            return override.per_hour
        return self.pair.per_hour


# --- loading ----------------------------------------------------------------------------------


class EgressConfig(_Strict):
    egresses: list[Egress]
    rules: ProviderRules
    budgets: Budgets

    def provider_budget(self, provider: str) -> int | None:
        """Per egress per minute; None: no provider budget (the target limit applies)."""
        if provider.startswith(SHARED_IP_PREFIX):
            return self.budgets.shared_ip.per_egress if self.budgets.shared_ip else None
        if provider not in self.rules.providers:
            return None
        return self.budgets.providers.for_provider(provider, self.rules.providers[provider])


def _network(
    value: str, what: str, strict: bool = True
) -> ipaddress.IPv4Network | ipaddress.IPv6Network:
    """Strict: host bits set (``10.0.0.1/8``) is an error, usually a typo in the prefix."""
    try:
        return ipaddress.ip_network(value, strict=strict)
    except ValueError as e:
        raise ConfigError(f"{what}: {value!r} is not a network ({e})") from None


def _yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        data = yaml.safe_load(path.read_text())
    except yaml.YAMLError as e:
        raise ConfigError(f"{path}: {e}") from None
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ConfigError(f"{path}: expected a mapping")
    return data


def _model(cls, path: Path):
    try:
        return cls.model_validate(_yaml(path))
    except ValueError as e:
        if isinstance(e, ConfigError):
            raise
        raise ConfigError(f"{path}: {e}") from None


def resolve_rules(providers: ProvidersFile, base: Path) -> ProviderRules:
    cnames: dict[str, str] = {}
    ranges: dict[str, str] = {}
    for name, p in sorted(providers.providers.items()):
        for suffix in p.cname:
            if suffix in cnames and cnames[suffix] != name:
                raise ConfigError(f"cname {suffix} is listed for {cnames[suffix]} and {name}")
            cnames[suffix] = name
        prefixes = list(p.cidr)
        for f in p.cidr_files:
            path = base / f
            if not path.exists():
                raise ConfigError(f"provider {name}: {path} does not exist")
            for line in path.read_text().splitlines():
                line = line.split("#", 1)[0].strip()
                if line:
                    prefixes.append(line)
        for prefix in prefixes:
            net = str(_network(prefix, f"provider {name}: cidr"))
            if net in ranges and ranges[net] != name:
                raise ConfigError(f"range {net} is listed for {ranges[net]} and {name}")
            ranges[net] = name
    kinds = {name: p.kind for name, p in sorted(providers.providers.items())}
    return ProviderRules(cnames=cnames, ranges=ranges, providers=kinds)


def load(directory: str | Path) -> EgressConfig:
    base = Path(directory)
    if not base.is_dir():
        raise ConfigError(f"{base} is not a directory")
    registry: RegistryFile = _model(RegistryFile, base / REGISTRY_FILE)
    providers: ProvidersFile = _model(ProvidersFile, base / PROVIDERS_FILE)
    budgets: Budgets = _model(Budgets, base / BUDGETS_FILE)
    rules = resolve_rules(providers, base)
    for name in budgets.providers.overrides:
        if name not in rules.providers:
            raise ConfigError(f"{BUDGETS_FILE}: providers.overrides: unknown provider {name!r}")
    cfg = EgressConfig(egresses=registry.resolved(), rules=rules, budgets=budgets)
    unlimited = [
        name
        for name, kind in rules.providers.items()
        if kind in PER_DOMAIN_KINDS and cfg.provider_budget(name) is None
    ]
    if unlimited:
        raise ConfigError(
            f"{BUDGETS_FILE}: no budget for {', '.join(unlimited)}: their targets are leased "
            "per domain, so only a provider budget limits them (set providers.default)"
        )
    return cfg
