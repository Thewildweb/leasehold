"""Loading ``service.yml`` files, with readable errors."""

from __future__ import annotations

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
        return ServiceSpec.model_validate(data)
    except ValidationError as e:
        raise SpecError(path, _format(e)) from e


def find_specs(paths: list[str | Path]) -> list[Path]:
    """Expand directories into the ``service.yml`` files below them."""
    found: list[Path] = []
    for p in map(Path, paths):
        if p.is_dir():
            found.extend(sorted(p.rglob(SPEC_FILENAME)))
        else:
            found.append(p)
    return found
