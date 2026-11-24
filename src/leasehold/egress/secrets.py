"""Encryption of proxy credentials at rest: AES-GCM in the application.

Proxy credentials and usage API keys are stored in Postgres encrypted with a key that
only the control API has (``LEASEHOLD_SECRETS_KEY`` or ``LEASEHOLD_SECRETS_KEYS``). The
database alone, a dump or a read-only role holds ciphertext only. Encryption happens
here and not in Postgres (pgcrypto), so the key never travels in a SQL statement, where
statement logs and ``pg_stat_statements`` could keep it.

Keys are 32 random bytes, base64 encoded. ``LEASEHOLD_SECRETS_KEYS`` holds several as
``id:base64,id:base64`` (the last one encrypts, all decrypt), so a key can be rotated:
add the new key at the end, run ``leasehold proxy rekey``, then remove the old one.
``LEASEHOLD_SECRETS_KEY`` is one key; its id is derived from the key.

A ciphertext is ``b"v1" + nonce (12 bytes) + AES-GCM output``. The associated data binds
it to its row and column (``proxy_accounts/<name>/credentials``), so a ciphertext copied
into another row doesn't decrypt.

``fingerprint`` is the first 12 hex digits of the plaintext's SHA-256: shown in listings
and audit rows to tell versions apart, never the secret itself.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import os
from dataclasses import dataclass

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

VERSION = b"v1"
NONCE_BYTES = 12


class SecretsError(Exception):
    """No key configured, an unknown key id, or a ciphertext that doesn't decrypt."""


def fingerprint(plaintext: bytes) -> str:
    return hashlib.sha256(plaintext).hexdigest()[:12]


def new_key() -> str:
    """A new key, base64 encoded, for the vault."""
    return base64.b64encode(os.urandom(32)).decode()


def _decode_key(text: str) -> bytes:
    try:
        key = base64.b64decode(text.strip(), validate=True)
    except (binascii.Error, ValueError) as e:
        raise SecretsError("a secrets key is not valid base64") from e
    if len(key) != 32:
        raise SecretsError("a secrets key must be 32 bytes")
    return key


def key_id_of(key: bytes) -> str:
    return "k" + hashlib.sha256(b"leasehold-key-id:" + key).hexdigest()[:8]


@dataclass(frozen=True)
class Sealed:
    ciphertext: bytes
    key_id: str
    fingerprint: str


class Keyring:
    """The configured keys: the newest encrypts, all decrypt."""

    def __init__(self, keys: list[tuple[str, bytes]]):
        ids = [i for i, _ in keys]
        if len(set(ids)) != len(ids):
            raise SecretsError("secrets key ids must be unique")
        self._keys = dict(keys)
        self.current = ids[-1] if ids else None

    def __repr__(self) -> str:
        return f"Keyring(ids={list(self._keys)}, current={self.current!r})"

    @property
    def configured(self) -> bool:
        return self.current is not None

    @property
    def ids(self) -> list[str]:
        return list(self._keys)

    @classmethod
    def from_env(cls, environ: dict[str, str] | None = None) -> Keyring:
        e = os.environ if environ is None else environ
        many = (e.get("LEASEHOLD_SECRETS_KEYS") or "").strip()
        one = (e.get("LEASEHOLD_SECRETS_KEY") or "").strip()
        if many and one:
            raise SecretsError("set LEASEHOLD_SECRETS_KEYS or LEASEHOLD_SECRETS_KEY, not both")
        keys: list[tuple[str, bytes]] = []
        if many:
            for part in many.split(","):
                kid, sep, text = part.strip().partition(":")
                if not sep or not kid:
                    raise SecretsError("LEASEHOLD_SECRETS_KEYS: entries are id:base64")
                keys.append((kid, _decode_key(text)))
        elif one:
            key = _decode_key(one)
            keys.append((key_id_of(key), key))
        return cls(keys)

    def seal(self, plaintext: bytes, context: str) -> Sealed:
        if self.current is None:
            raise SecretsError("no secrets key configured (LEASEHOLD_SECRETS_KEY)")
        nonce = os.urandom(NONCE_BYTES)
        ct = AESGCM(self._keys[self.current]).encrypt(nonce, plaintext, context.encode())
        return Sealed(VERSION + nonce + ct, self.current, fingerprint(plaintext))

    def open(self, ciphertext: bytes, key_id: str, context: str) -> bytes:
        key = self._keys.get(key_id)
        if key is None:
            raise SecretsError(f"secrets key {key_id!r} is not configured")
        data = bytes(ciphertext)
        if not data.startswith(VERSION) or len(data) < len(VERSION) + NONCE_BYTES + 16:
            raise SecretsError("not a leasehold ciphertext")
        nonce = data[len(VERSION) : len(VERSION) + NONCE_BYTES]
        try:
            return AESGCM(key).decrypt(nonce, data[len(VERSION) + NONCE_BYTES :], context.encode())
        except InvalidTag:
            raise SecretsError(f"ciphertext doesn't decrypt with key {key_id!r}") from None


def context(table: str, name: str, column: str) -> str:
    return f"{table}/{name}/{column}"
