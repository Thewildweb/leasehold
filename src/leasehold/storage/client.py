"""Object storage for components at runtime: plain S3, nothing provider-specific.

    LEASEHOLD_S3_ENDPOINT    e.g. http://minio.service:9000 (set by the Nomad job)
    LEASEHOLD_S3_REGION      optional
    LEASEHOLD_S3_ACCESS_KEY  this component's user (from its Nomad Variable)
    LEASEHOLD_S3_SECRET_KEY

Bucket names come from ``naming.bucket_for(env, ref)``; this client takes actual names.
"""

from __future__ import annotations

import io
import os
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from urllib.parse import urlsplit

from minio import Minio
from minio.error import S3Error

# Multipart uploads above this size, in parts of this size.
PART_SIZE = 16 * 1024 * 1024


class StorageError(RuntimeError):
    pass


class NotFound(StorageError):
    pass


class AccessDenied(StorageError):
    pass


def _wrap(e: S3Error, what: str) -> StorageError:
    if e.code in ("NoSuchKey", "NoSuchBucket"):
        return NotFound(f"{what}: {e.code}")
    if e.code == "AccessDenied":
        return AccessDenied(f"{what}: access denied")
    return StorageError(f"{what}: {e.code}: {e.message}")


@dataclass
class ObjectInfo:
    key: str
    size: int


class ObjectStore:
    def __init__(self, s3: Minio):
        self.s3 = s3

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> ObjectStore:
        e = os.environ if environ is None else environ
        endpoint = e.get("LEASEHOLD_S3_ENDPOINT")
        access, secret = e.get("LEASEHOLD_S3_ACCESS_KEY"), e.get("LEASEHOLD_S3_SECRET_KEY")
        if not (endpoint and access and secret):
            raise StorageError(
                "object storage isn't configured: set LEASEHOLD_S3_ENDPOINT, "
                "LEASEHOLD_S3_ACCESS_KEY and LEASEHOLD_S3_SECRET_KEY (a component gets them "
                "when it declares reads_buckets or writes_buckets)"
            )
        return cls.connect(endpoint, access, secret, e.get("LEASEHOLD_S3_REGION", ""))

    @classmethod
    def connect(cls, endpoint: str, access_key: str, secret_key: str, region: str = ""):
        u = urlsplit(endpoint if "://" in endpoint else f"https://{endpoint}")
        return cls(
            Minio(
                u.netloc,
                access_key=access_key,
                secret_key=secret_key,
                secure=u.scheme == "https",
                region=region or None,
            )
        )

    def put_bytes(self, bucket: str, key: str, data: bytes, content_type: str) -> None:
        try:
            self.s3.put_object(bucket, key, io.BytesIO(data), len(data), content_type=content_type)
        except S3Error as e:
            raise _wrap(e, f"put {bucket}/{key}") from None

    def put_file(self, bucket: str, key: str, path: str, content_type: str) -> None:
        """Upload a local file; multipart above ``PART_SIZE``."""
        try:
            self.s3.fput_object(bucket, key, path, content_type=content_type, part_size=PART_SIZE)
        except S3Error as e:
            raise _wrap(e, f"put {bucket}/{key}") from None

    def get_bytes(self, bucket: str, key: str, max_size: int | None = None) -> bytes:
        try:
            r = self.s3.get_object(bucket, key)
        except S3Error as e:
            raise _wrap(e, f"get {bucket}/{key}") from None
        try:
            data = r.read(max_size + 1 if max_size is not None else None)
            if max_size is not None and len(data) > max_size:
                raise StorageError(f"get {bucket}/{key}: larger than {max_size} bytes")
            return data
        finally:
            r.close()
            r.release_conn()

    def list(self, bucket: str, prefix: str) -> Iterator[ObjectInfo]:
        try:
            for o in self.s3.list_objects(bucket, prefix=prefix, recursive=True):
                yield ObjectInfo(o.object_name, o.size or 0)
        except S3Error as e:
            raise _wrap(e, f"list {bucket}/{prefix}") from None

    def delete(self, bucket: str, key: str) -> None:
        try:
            self.s3.remove_object(bucket, key)
        except S3Error as e:
            raise _wrap(e, f"delete {bucket}/{key}") from None
