"""Claim check: payloads too large for a Kafka message, stored in a bucket.

A topic with ``large_payloads: <bucket>`` in ``service.yml`` lets its producers emit
events above the inline limit. The producer stores the payload, as gzip-compressed JSON,
at ``claims/<topic ref>/dt=YYYY-MM-DD/<event_id>.json.gz`` in that bucket, and sends the
event with ``payload: null`` and::

    "payload_ref": {
      "bucket": "headhunter.archive",   # bucket reference, resolved per environment
      "key": "claims/headhunter.responses.v1/dt=2026-11-17/<event_id>.json.gz",
      "sha256": "…",                   # of the stored object (the gzip bytes)
      "size": 123456,                   # bytes of the stored object
      "encoding": "gzip"
    }

Consumers (``run_processor``, archive sinks) resolve the reference before they look at
the payload. Claims expire one day after the topic's retention (a lifecycle rule
``apply`` sets), so they live as long as the events that point at them; an archive sink
writes the resolved payload into its files, not the reference.
"""

from __future__ import annotations

import gzip
import hashlib
import json
from datetime import UTC, datetime
from typing import Any

from leasehold import naming
from leasehold.storage.client import AccessDenied, NotFound, ObjectStore, StorageError

# Largest claimed payload, compressed. Far above what a fetcher should store per event.
MAX_CLAIM_BYTES = 64 * 1024 * 1024
ENCODING = "gzip"
CONTENT_TYPE = "application/json"


class ClaimError(RuntimeError):
    """A payload_ref that can't be resolved."""


class ClaimNotReadable(ClaimError):
    """This component has no read grant on the claim's bucket."""


class ClaimMissing(ClaimError):
    """The object isn't there (expired, or never written)."""


def claim_key(ref: naming.TopicRef, event_id: str, when: datetime | None = None) -> str:
    day = (when or datetime.now(UTC)).strftime("%Y-%m-%d")
    return f"{naming.claims_prefix(ref)}dt={day}/{event_id}.json.gz"


def store(
    objects: ObjectStore,
    env: str,
    bucket: naming.BucketRef,
    topic: naming.TopicRef,
    event_id: str,
    payload_json: bytes,
) -> dict[str, Any]:
    """Store one payload (its JSON) and return the ``payload_ref`` for the envelope."""
    data = gzip.compress(payload_json, compresslevel=6, mtime=0)
    if len(data) > MAX_CLAIM_BYTES:
        raise ClaimError(
            f"{topic}: payload is {len(data)} bytes compressed (max {MAX_CLAIM_BYTES})"
        )
    key = claim_key(topic, event_id)
    objects.put_bytes(naming.bucket_for(env, bucket), key, data, CONTENT_TYPE)
    return {
        "bucket": str(bucket),
        "key": key,
        "sha256": hashlib.sha256(data).hexdigest(),
        "size": len(data),
        "encoding": ENCODING,
    }


def resolve(objects: ObjectStore | None, env: str, ref: Any, component: str = "") -> Any:
    """The payload a ``payload_ref`` points at. ``component``: for error messages."""
    if not isinstance(ref, dict) or not {"bucket", "key", "sha256", "size"} <= ref.keys():
        raise ClaimError(f"invalid payload_ref: {ref!r}")
    try:
        bucket_ref = naming.BucketRef.parse(str(ref["bucket"]))
    except naming.NamingError as e:
        raise ClaimError(f"invalid payload_ref bucket: {e}") from None
    hint = (
        f"{component or 'this component'} can't read claims in bucket {bucket_ref}; "
        f"add '{bucket_ref}' to its reads_buckets in service.yml"
    )
    if objects is None:
        raise ClaimNotReadable(hint)
    bucket = naming.bucket_for(env, bucket_ref)
    try:
        data = objects.get_bytes(bucket, str(ref["key"]), max_size=MAX_CLAIM_BYTES)
    except AccessDenied:
        raise ClaimNotReadable(hint) from None
    except NotFound:
        raise ClaimMissing(f"claim {bucket}/{ref['key']} is gone (expired?)") from None
    except StorageError as e:
        raise ClaimError(str(e)) from None
    if len(data) != int(ref["size"]) or hashlib.sha256(data).hexdigest() != ref["sha256"]:
        raise ClaimError(f"claim {bucket}/{ref['key']}: size or sha256 doesn't match")
    encoding = ref.get("encoding", ENCODING)
    if encoding != ENCODING:
        raise ClaimError(f"claim {bucket}/{ref['key']}: unknown encoding {encoding!r}")
    try:
        return json.loads(gzip.decompress(data))
    except (OSError, ValueError) as e:
        raise ClaimError(f"claim {bucket}/{ref['key']}: {e}") from None
