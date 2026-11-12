"""Runtime configuration, from environment variables the Nomad job sets.

LEASEHOLD_ENV             prod | dev | ...                      (required)
LEASEHOLD_SERVICE         service name                          (required)
LEASEHOLD_COMPONENT       component name                        (required)
LEASEHOLD_SERVICE_FILE    the service.yml in the image (default /app/service.yml)
LEASEHOLD_VERSION         git SHA of the image (default: unknown)
LEASEHOLD_API_URL         control API, e.g. http://control-api.service:8400
LEASEHOLD_KAFKA_BOOTSTRAP e.g. kafka.service:9092 (needed when the component
                          produces or consumes)
LEASEHOLD_KAFKA_USERNAME  this component's SCRAM user (default: from naming)
LEASEHOLD_KAFKA_PASSWORD  from the component's Nomad Variable
LEASEHOLD_HTTP_PORT       /health and /metrics; Nomad's NOMAD_PORT_http wins.
                          0 = don't serve (default for scheduled components)
LEASEHOLD_DRAIN_TIMEOUT_S how long to finish in-flight work after SIGTERM
                          (default 25; keep it below the job's kill_timeout)
LEASEHOLD_TIMEOUT_S       scheduled components: kill the run after this long
LEASEHOLD_PAYLOAD_MODELS  processors: JSON, consumed topic reference → payload model
                          of the service that produces it
"""

from __future__ import annotations

import json
import os
import socket
from dataclasses import dataclass, field
from pathlib import Path

from leasehold import naming
from leasehold.spec import ServiceSpec, load_spec
from leasehold.spec.models import Component


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class RuntimeConfig:
    env: str
    service: str
    component: str
    spec: ServiceSpec
    version: str = "unknown"
    instance: str = "local"
    kafka_bootstrap: str | None = None
    kafka_username: str | None = None
    kafka_password: str | None = None
    http_port: int = 0
    drain_timeout: float = 25.0
    run_timeout: float | None = None
    # Consumed topic reference (e.g. "certlog.entries.v1") → "module:Class".
    payload_models: dict[str, str] = field(default_factory=dict, hash=False)

    @property
    def component_spec(self) -> Component:
        return self.spec.components[self.component]

    @property
    def name(self) -> str:
        return f"{self.service}.{self.component}"

    def kafka_conf(self) -> dict[str, str]:
        if not self.kafka_bootstrap:
            raise ConfigError("set LEASEHOLD_KAFKA_BOOTSTRAP")
        conf = {"bootstrap.servers": self.kafka_bootstrap}
        if self.kafka_password:
            conf |= {
                "security.protocol": "SASL_PLAINTEXT",
                "sasl.mechanism": "SCRAM-SHA-512",
                "sasl.username": self.kafka_username
                or naming.kafka_principal(self.env, self.service, self.component),
                "sasl.password": self.kafka_password,
            }
        return conf

    @classmethod
    def from_env(
        cls, environ: dict[str, str] | None = None, spec: ServiceSpec | None = None
    ) -> RuntimeConfig:
        e = os.environ if environ is None else environ
        missing = [
            k for k in ("LEASEHOLD_ENV", "LEASEHOLD_SERVICE", "LEASEHOLD_COMPONENT") if not e.get(k)
        ]
        if missing:
            raise ConfigError(f"missing environment variables: {', '.join(missing)}")
        env = naming.check_env(e["LEASEHOLD_ENV"])
        if spec is None:
            spec = load_spec(Path(e.get("LEASEHOLD_SERVICE_FILE", "/app/service.yml")))
        service, component = e["LEASEHOLD_SERVICE"], e["LEASEHOLD_COMPONENT"]
        if spec.name != service:
            raise ConfigError(f"service.yml is for {spec.name!r}, not {service!r}")
        if component not in spec.components:
            raise ConfigError(f"{service} has no component {component!r}")
        port = e.get("NOMAD_PORT_http") or e.get("LEASEHOLD_HTTP_PORT") or "0"
        timeout = e.get("LEASEHOLD_TIMEOUT_S")
        try:
            models = json.loads(e.get("LEASEHOLD_PAYLOAD_MODELS") or "{}")
        except ValueError as err:
            raise ConfigError(f"LEASEHOLD_PAYLOAD_MODELS is not JSON: {err}") from None
        return cls(
            env=env,
            service=service,
            component=component,
            spec=spec,
            version=e.get("LEASEHOLD_VERSION", "unknown"),
            instance=e.get("NOMAD_ALLOC_ID") or socket.gethostname(),
            kafka_bootstrap=e.get("LEASEHOLD_KAFKA_BOOTSTRAP") or None,
            kafka_username=e.get("LEASEHOLD_KAFKA_USERNAME") or None,
            kafka_password=e.get("LEASEHOLD_KAFKA_PASSWORD") or None,
            http_port=int(port),
            drain_timeout=float(e.get("LEASEHOLD_DRAIN_TIMEOUT_S", "25")),
            run_timeout=float(timeout) if timeout else None,
            payload_models=models,
        )
