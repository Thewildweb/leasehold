"""Loading ``service.yml`` files, with readable errors."""

from __future__ import annotations

import json
from pathlib import Path

import yaml
from pydantic import ValidationError

from leasehold.spec.models import ServiceSpec

SPEC_FILENAME = "service.yml"


class SpecError(Exception):
    """A ``service.yml`` that can't be loaded; ``messages`` holds one line per problem."""

    def __init__(self, path: Path, messages: list[str]):
        self.path = path
        self.messages = messages
        super().__init__(f"{path}: " + "; ".join(messages))


def _format(exc: ValidationError) -> list[str]:
    out: list[str] = []
    for err in exc.errors(include_url=False):
        loc = ".".join(str(p) for p in err["loc"])
        msg = err["msg"].removeprefix("Value error, ")
        for line in msg.splitlines():  # cross-checks report several problems at once
            out.append(f"{loc}: {line}" if loc else line)
    return out


def load_spec(path: str | Path) -> ServiceSpec:
    path = Path(path)
    try:
        data = yaml.safe_load(path.read_text())
    except OSError as e:
        raise SpecError(path, [str(e)]) from e
    except yaml.YAMLError as e:
        raise SpecError(path, [f"invalid YAML: {e}"]) from e
    if not isinstance(data, dict):
        raise SpecError(path, ["expected a mapping at the top level"])
    try:
        spec = ServiceSpec.model_validate(data)
    except ValidationError as e:
        raise SpecError(path, _format(e)) from e
    errors = [m for f in spec.dashboards.files if (m := _check_dashboard_file(path.parent / f))]
    if spec.clickhouse:
        for sname, sink in spec.clickhouse.sinks.items():
            if m := _check_schema_file(path.parent / sink.schema_file):
                errors.append(f"clickhouse.sinks.{sname}.schema_file: {m}")
    if errors:
        raise SpecError(path, errors)
    return spec


def read_dashboard_files(spec_path: str | Path, spec: ServiceSpec) -> dict[str, dict]:
    """The service's own dashboard files, by the name listed in ``service.yml``."""
    base = Path(spec_path).parent
    return {f: json.loads((base / f).read_text()) for f in spec.dashboards.files}


def _check_schema_file(path: Path) -> str | None:
    from leasehold.clickhouse.schema import SchemaFileError, parse

    try:
        parse(path.read_text())
    except OSError as e:
        return str(e)
    except SchemaFileError as e:
        return f"{path.name}: {e}"
    return None


def _check_dashboard_file(path: Path) -> str | None:
    where = f"dashboards.files: {path.name}"
    try:
        data = json.loads(path.read_text())
    except OSError as e:
        return f"{where}: {e.strerror}"
    except json.JSONDecodeError as e:
        return f"{where}: invalid JSON: {e}"
    if not isinstance(data, dict) or not isinstance(data.get("panels"), list):
        return f"{where}: not a Grafana dashboard (no 'panels' list)"
    return None


def find_specs(paths: list[str | Path]) -> list[Path]:
    """Expand directories into the ``service.yml`` files below them."""
    found: list[Path] = []
    for p in map(Path, paths):
        if p.is_dir():
            found.extend(sorted(p.rglob(SPEC_FILENAME)))
        else:
            found.append(p)
    return found
