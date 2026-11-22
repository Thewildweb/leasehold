"""Snapshots: datasets published as files in a bucket and loaded by ClickHouse.

- ``manifest``: the manifest format and ``SnapshotWriter``, which producers use to
  upload a snapshot's files and write its manifest last.
- ``schema``: snapshot table schema files.
- ``plan`` / ``apply``: what ``leasehold apply`` creates in ClickHouse.
- ``loader``: one run of a dataset's loader (``leasehold snapshot load``).
"""
