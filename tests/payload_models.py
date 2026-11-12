"""Payload models of the runtime test service (conftest.RUNTIME_SPEC)."""

from pydantic import BaseModel, ConfigDict


class R(BaseModel):
    model_config = ConfigDict(extra="allow")
    id: str


class S(BaseModel):
    model_config = ConfigDict(extra="allow")
    n: int = 0
