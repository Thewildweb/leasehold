"""Payload models: each topic's ``payload_model`` (``module:Class``, a pydantic model).

Producing: the payload is validated against the topic's model and sent as the
model's JSON, so defaults are filled in and types are normalised. An invalid
payload raises ``InvalidPayload`` in the component that made it.

Consuming (processors): each event's payload is validated against the model of
the topic it came from; invalid events are skipped and counted. The models of
consumed topics belong to other services; the Nomad job passes their paths in
``LEASEHOLD_PAYLOAD_MODELS``, and the component's image must contain them.

Models are imported when the component starts, so a missing or wrong model stops
the component at once instead of at the first event.
"""

from __future__ import annotations

import importlib
from typing import Any

from pydantic import BaseModel, ValidationError


class PayloadModelError(ValueError):
    """A payload model can't be imported."""


def load_model(path: str) -> type[BaseModel]:
    module, sep, name = path.partition(":")
    if not sep or not module or not name:
        raise PayloadModelError(f"payload model {path!r}: expected 'module:Class'")
    try:
        obj: Any = importlib.import_module(module)
        for part in name.split("."):
            obj = getattr(obj, part)
    except (ImportError, AttributeError) as e:
        raise PayloadModelError(
            f"payload model {path!r} can't be imported ({e}); is its package in the image?"
        ) from None
    if not (isinstance(obj, type) and issubclass(obj, BaseModel)):
        raise PayloadModelError(f"payload model {path!r} is not a pydantic model")
    return obj


def load_models(paths: dict[str, str]) -> dict[str, type[BaseModel]]:
    """key (a topic name or reference) → model."""
    return {key: load_model(path) for key, path in paths.items()}


def validated(model: type[BaseModel], payload: Any) -> dict[str, Any]:
    """The payload as the model's JSON; ``ValidationError`` if it doesn't fit."""
    if isinstance(payload, model):
        return payload.model_dump(mode="json")
    if isinstance(payload, BaseModel):
        payload = payload.model_dump(mode="json")
    return model.model_validate(payload).model_dump(mode="json")


def error_summary(e: ValidationError, limit: int = 3) -> str:
    """Short and without the payload's values (they may be large or sensitive)."""
    parts = [
        f"{'.'.join(str(x) for x in err['loc']) or '(payload)'}: {err['msg']}"
        for err in e.errors()[:limit]
    ]
    more = e.error_count() - limit
    return "; ".join(parts) + (f"; and {more} more" if more > 0 else "")
