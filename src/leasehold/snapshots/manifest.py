"""The snapshot manifest: everything a producer and the loader share.

A snapshot is a set of files under ``<dataset>/<snapshot>/<table>/`` in a bucket and a
manifest at ``<dataset>/<snapshot>/manifest.json``, written last. A snapshot without its
manifest is incomplete and ignored; a rerun of the producer overwrites the same keys.

``SnapshotWriter`` is how a producer writes one::

    w = SnapshotWriter(store, bucket, dataset="bag", snapshot="2026-11-08")
    w.add_file("panden", "/tmp/panden-0000.parquet")        # uploads, counts rows
    w.commit(source_date="2026-11-08", source={"url": "..."})  # manifest last

Files are Parquet; ``add_file`` reads the row count and columns from the file's footer
(``pyarrow``, the ``archive`` extra) and its sha256 while uploading.
"""

from __future__ import annotations

import hashlib
import os
import re
from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from leasehold import naming

FORMAT = "leasehold.snapshot/1"


class FileEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    key: str
    rows: int = Field(ge=0)
    bytes: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class TableEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rows: int = Field(ge=0)
    columns: list[tuple[str, str]] = Field(
        description="(name, type) from the files' schema, in file order"
    )
    files: list[FileEntry]


class Manifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    format: Literal["leasehold.snapshot/1"] = FORMAT
    dataset: str
    snapshot: str
    source_date: str | None = Field(default=None, description="Date of the data (YYYY-MM-DD)")
    previous: str | None = Field(default=None, description="The snapshot this one follows")
    created_at: datetime
    producer: str = ""
    source: dict[str, Any] = Field(default_factory=dict)
    tables: dict[str, TableEntry]

    @field_validator("snapshot")
    @classmethod
    def _snapshot_id(cls, v: str) -> str:
        return naming.check_snapshot_id(v)

    def check_keys(self) -> list[str]:
        """Problems with file keys: every file must be under its table's prefix."""
        out = []
        for t, entry in self.tables.items():
            prefix = naming.snapshot_file_prefix(self.dataset, self.snapshot, t)
            for f in entry.files:
                rest = f.key[len(prefix) :]
                if not f.key.startswith(prefix) or not rest or "/" in rest or not _SAFE.match(rest):
                    out.append(f"{t}: file {f.key!r} is not a plain name under {prefix}")
            if sum(f.rows for f in entry.files) != entry.rows:
                out.append(f"{t}: rows {entry.rows} != the sum over its files")
        return out


_SAFE = re.compile(r"^[0-9A-Za-z][0-9A-Za-z._-]*$")


class SnapshotWriter:
    """Uploads one snapshot's files and writes its manifest (see the module docstring)."""

    def __init__(
        self, store, bucket: str, dataset: str, snapshot: str, producer: str | None = None
    ):
        self.store = store
        self.bucket = bucket
        self.dataset = dataset
        self.snapshot = naming.check_snapshot_id(snapshot)
        naming.snapshot_manifest_key(dataset, snapshot)  # checks the dataset name
        self.producer = producer or _producer()
        self.tables: dict[str, TableEntry] = {}

    def add_file(self, table: str, path: str, name: str | None = None) -> FileEntry:
        """Upload one Parquet file of ``table``; ``name`` defaults to a numbered part."""
        import pyarrow.parquet as pq

        meta = pq.read_metadata(path)
        schema = pq.read_schema(path)
        columns = [(f.name, str(f.type)) for f in schema]
        entry = self.tables.get(table)
        if entry is not None and entry.columns != columns:
            raise ValueError(f"{table}: {path} has other columns than the table's earlier files")
        n = len(entry.files) if entry else 0
        key = naming.snapshot_file_prefix(self.dataset, self.snapshot, table) + (
            name or f"part-{n:05d}.parquet"
        )
        digest = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                digest.update(chunk)
        self.store.put_file(self.bucket, key, path, "application/vnd.apache.parquet")
        fe = FileEntry(
            key=key, rows=meta.num_rows, bytes=os.path.getsize(path), sha256=digest.hexdigest()
        )
        if entry is None:
            entry = TableEntry(rows=0, columns=columns, files=[])
        self.tables[table] = TableEntry(
            rows=entry.rows + fe.rows, columns=columns, files=[*entry.files, fe]
        )
        return fe

    def commit(
        self,
        *,
        source_date: str | None = None,
        previous: str | None = None,
        source: dict[str, Any] | None = None,
    ) -> Manifest:
        """Write the manifest. After this the snapshot is complete and can be loaded."""
        if not self.tables:
            raise ValueError("a snapshot needs at least one table")
        m = Manifest(
            dataset=self.dataset,
            snapshot=self.snapshot,
            source_date=source_date,
            previous=previous,
            created_at=datetime.now(UTC),
            producer=self.producer,
            source=source or {},
            tables=self.tables,
        )
        problems = m.check_keys()
        if problems:
            raise ValueError("; ".join(problems))
        self.store.put_bytes(
            self.bucket,
            naming.snapshot_manifest_key(self.dataset, self.snapshot),
            m.model_dump_json(indent=1).encode(),
            "application/json",
        )
        return m


def _producer() -> str:
    service = os.environ.get("LEASEHOLD_SERVICE", "")
    component = os.environ.get("LEASEHOLD_COMPONENT", "")
    version = os.environ.get("LEASEHOLD_VERSION", "")
    name = "-".join(p for p in (service, component) if p)
    return f"{name}@{version}" if version else name
