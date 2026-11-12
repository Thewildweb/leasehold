"""Authentication and permissions of the control API, without a database."""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from types import SimpleNamespace

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from leasehold.api.auth import (
    Authenticator,
    AuthError,
    Caller,
    Forbidden,
    Registry,
    hash_token,
    load_tokens,
    new_token,
)
from leasehold.spec import find_specs, load_spec

FIXTURES = Path(__file__).parent / "fixtures"

KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
OTHER_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)


class FakeJwks:
    """Stands in for PyJWKClient: always returns our test public key."""

    def get_signing_key_from_jwt(self, token):
        return SimpleNamespace(key=KEY.public_key())


@pytest.fixture(scope="module")
def registry():
    return Registry([load_spec(p) for p in find_specs([FIXTURES])])


def _jwt(key=KEY, **claims):
    base = {
        "aud": "leasehold",
        "exp": int(time.time()) + 300,
        "nomad_namespace": "default",
        "nomad_job_id": "resolver-worker",
        "nomad_allocation_id": "alloc-1",
    }
    base.update(claims)
    base = {k: v for k, v in base.items() if v is not None}
    return jwt.encode(base, key, algorithm="RS256")


def _auth(registry, env="prod", tokens=()):
    return Authenticator(env, registry, list(tokens), jwks_client=FakeJwks())


def authenticate(a, token):
    return asyncio.run(a.authenticate(f"Bearer {token}"))


def test_workload_identity(registry):
    c = authenticate(_auth(registry), _jwt())
    assert (c.name, c.service, c.component, c.instance) == (
        "resolver.worker",
        "resolver",
        "worker",
        "alloc-1",
    )


def test_periodic_child_job_maps_to_parent(registry):
    c = authenticate(_auth(registry), _jwt(nomad_job_id="certwatch-planner/periodic-1731300000"))
    assert c.name == "certwatch.planner"


@pytest.mark.parametrize(
    ("claims", "message"),
    [
        ({"nomad_namespace": "dev"}, "namespace"),
        ({"aud": "something-else"}, "(?i)audience"),
        ({"exp": int(time.time()) - 10}, "expired"),
        ({"nomad_job_id": "unknown-job"}, "not a component"),
        ({"nomad_job_id": None}, "nomad_job_id"),
    ],
)
def test_workload_identity_rejected(registry, claims, message):
    with pytest.raises(AuthError, match=message):
        authenticate(_auth(registry), _jwt(**claims))


def test_wrong_signature_rejected(registry):
    with pytest.raises(AuthError, match="Signature"):
        authenticate(_auth(registry), _jwt(key=OTHER_KEY))


def test_dev_api_only_accepts_dev_namespace(registry):
    a = _auth(registry, env="dev")
    assert authenticate(a, _jwt(nomad_namespace="dev")).name == "resolver.worker"
    with pytest.raises(AuthError, match="namespace"):
        authenticate(a, _jwt(nomad_namespace="default"))


def test_no_jwks_configured(registry):
    a = Authenticator("prod", registry, [])
    with pytest.raises(AuthError, match="not accepted"):
        authenticate(a, _jwt())


def test_static_tokens(registry, tmp_path):
    admin, worker = new_token(), new_token()
    path = tmp_path / "tokens.json"
    path.write_text(
        json.dumps(
            {
                "tokens": [
                    {"sha256": hash_token(admin), "admin": "erik"},
                    {
                        "sha256": hash_token(worker),
                        "service": "resolver",
                        "component": "worker",
                    },
                ]
            }
        )
    )
    a = _auth(registry, tokens=load_tokens(path))
    assert authenticate(a, admin) == Caller(name="admin:erik", admin=True)
    assert authenticate(a, worker).name == "resolver.worker"
    with pytest.raises(AuthError, match="unknown token"):
        authenticate(a, new_token())
    with pytest.raises(AuthError, match="missing"):
        asyncio.run(a.authenticate(None))


def test_permissions(registry):
    worker = Caller("resolver.worker", "resolver", "worker")
    bridge = Caller("resolver.new-domains", "resolver", "new-domains")
    other = Caller("certwatch.worker", "certwatch", "worker")
    admin = Caller("admin:erik", admin=True)

    registry.require_lease(worker, {"resolver.a_record"})
    with pytest.raises(Forbidden):
        registry.require_lease(other, {"resolver.a_record"})
    with pytest.raises(Forbidden):  # a processor creates tasks but doesn't lease them
        registry.require_lease(bridge, {"resolver.a_record"})

    registry.require_create(bridge, "resolver.a_record")
    registry.require_create(admin, "resolver.a_record")
    with pytest.raises(Forbidden):  # another service's worker can't create them
        registry.require_create(other, "resolver.a_record")
    with pytest.raises(Forbidden):  # the worker itself doesn't declare creates_tasks
        registry.require_create(worker, "resolver.a_record")
