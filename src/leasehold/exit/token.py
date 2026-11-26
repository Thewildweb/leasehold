"""Exit tokens: the proxy credentials of a target lease routed through a registry exit.

The dispatcher signs one per lease (and again on every renewal); the exit verifies it
offline with the same key, so a connection needs no call to the control API.

- username: the target lease id;
- password: ``v1.<claims>.<mac>``, both base64url without padding. ``claims`` is the
  JSON object ``{"l": lease id, "e": egress address, "t": target, "v": environment,
  "x": expiry (unix seconds)}``; ``mac`` is HMAC-SHA256 over ``v1.<claims>``.

The exit accepts a token when the MAC matches one of its keys (the current one and,
during a rotation, the previous one), the lease id equals the username, the egress is
the exit's own address and the expiry has not passed. The target is not checked: a page
loads third-party hosts through the same exit. It is in the token for the exit's logs.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from datetime import datetime

VERSION = "v1"
MIN_KEY_BYTES = 32


class TokenError(ValueError):
    """A token that does not verify. The message is safe to log (no key material)."""


@dataclass(frozen=True)
class Claims:
    lease_id: str
    egress: str
    target: str
    env: str
    expires_at: int  # unix seconds


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _unb64(text: str) -> bytes:
    try:
        return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))
    except (binascii.Error, ValueError):
        raise TokenError("malformed token") from None


def parse_key(text: str) -> bytes:
    """A key as configured: hex or base64url, at least 32 bytes."""
    text = text.strip()
    try:
        key = bytes.fromhex(text)
    except ValueError:
        key = _unb64(text)
    if len(key) < MIN_KEY_BYTES:
        raise ValueError(f"an exit key needs at least {MIN_KEY_BYTES} bytes")
    return key


def parse_keys(text: str) -> list[bytes]:
    """Comma-separated keys, the current one first."""
    keys = [parse_key(k) for k in text.split(",") if k.strip()]
    if not keys:
        raise ValueError("no exit key")
    return keys


def _mac(key: bytes, signed: str) -> bytes:
    return hmac.new(key, signed.encode(), hashlib.sha256).digest()


def sign(key: bytes, claims: Claims) -> tuple[str, str]:
    """(username, password) for a lease."""
    body = json.dumps(
        {
            "l": claims.lease_id,
            "e": claims.egress,
            "t": claims.target,
            "v": claims.env,
            "x": int(claims.expires_at),
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    signed = f"{VERSION}.{_b64(body)}"
    return claims.lease_id, f"{signed}.{_b64(_mac(key, signed))}"


def sign_lease(
    key: bytes, lease_id: str, egress: str, target: str, env: str, expires_at: datetime
) -> tuple[str, str]:
    return sign(key, Claims(str(lease_id), egress, target, env, int(expires_at.timestamp())))


def verify(
    keys: list[bytes], username: str, password: str, egress: str, now: float | None = None
) -> Claims:
    """The claims of a valid token for this exit; ``TokenError`` otherwise."""
    parts = password.split(".")
    if len(parts) != 3 or parts[0] != VERSION:
        raise TokenError("malformed token")
    signed = f"{parts[0]}.{parts[1]}"
    mac = _unb64(parts[2])
    if not any(hmac.compare_digest(mac, _mac(k, signed)) for k in keys):
        raise TokenError("bad signature")
    try:
        d = json.loads(_unb64(parts[1]))
        claims = Claims(str(d["l"]), str(d["e"]), str(d["t"]), str(d["v"]), int(d["x"]))
    except (ValueError, KeyError, TypeError):
        raise TokenError("malformed claims") from None
    if claims.lease_id != username:
        raise TokenError("token is for another lease")
    if claims.egress != egress:
        raise TokenError("token is for another egress")
    if claims.expires_at <= (time.time() if now is None else now):
        raise TokenError("token expired")
    return claims
