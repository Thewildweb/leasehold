"""Object storage: buckets declared in ``service.yml``, and a user per component.

``plan`` derives what should exist; ``apply`` compares it with MinIO and applies it;
``client`` is what components use at runtime (S3 only, provider-neutral).
"""

from leasehold.storage.plan import StoragePlan, plan

__all__ = ["StoragePlan", "plan"]
