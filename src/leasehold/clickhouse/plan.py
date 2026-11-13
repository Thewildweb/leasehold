"""What ClickHouse should contain for a set of services in one environment.

Per service with a ``clickhouse`` section, in database ``<env prefix><database>``:

- per sink ``<sink>``:
  - ``<sink>``: the target table, from the schema file (data; never dropped by apply);
  - ``<sink>_kafka``: a Kafka-engine table, one ``message String`` column
    (``JSONAsString``), consumer group ``<service>.clickhouse.<sink>``, as the service's
    sink user; broken JSON is passed on as an error row instead of stopping the sink;
  - ``<sink>_mv``: materialized view, the schema file's SELECT over valid messages,
    into ``<sink>``;
  - ``<sink>_errors_mv``: materialized view, invalid messages into ``sink_errors``;
- ``sink_errors``: invalid messages of all sinks in the database, kept 30 days.

Every object carries a comment starting with ``leasehold:``; ``apply`` uses it to
find what it manages and to see what changed.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

from leasehold import naming
from leasehold.clickhouse.schema import SOURCE, TABLE, SinkSchema, parse
from leasehold.spec.models import ServiceSpec

MARK = "leasehold:"
ERRORS_TABLE = "sink_errors"
ERRORS_TTL_DAYS = 30


@dataclass(frozen=True)
class SinkPlan:
    service: str
    sink: str
    database: str
    topic: str
    group: str
    principal: str
    credential_path: str  # Nomad Variable with the sink user's password
    namespace: str
    schema: SinkSchema
    consumers: int = 1

    @property
    def table(self) -> str:
        return f"{self.database}.{self.sink}"

    @property
    def kafka_table(self) -> str:
        return f"{self.database}.{self.sink}_kafka"

    @property
    def mv(self) -> str:
        return f"{self.database}.{self.sink}_mv"

    @property
    def errors_mv(self) -> str:
        return f"{self.database}.{self.sink}_errors_mv"

    def table_comment(self) -> str:
        return f"{MARK} sink={self.sink} table={self.schema.table_hash()}"

    def pipeline_comment(self, kafka_bootstrap: str, credential_version: str) -> str:
        """Changes whenever the Kafka table or a view must be recreated."""
        parts = [
            self.schema.select_hash(),
            self.topic,
            self.group,
            self.principal,
            kafka_bootstrap,
            credential_version,
            "2",  # bump when the generated SQL below changes
        ]
        if self.consumers > 1:  # left out at 1, so existing pipelines aren't recreated
            parts.append(f"consumers={self.consumers}")
        fp = hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]
        return f"{MARK} sink={self.sink} pipeline={fp}"

    def create_table_sql(self) -> str:
        sql = self.schema.create_table.replace(TABLE, self.table, 1)
        return f"{sql}\nCOMMENT {_quote(self.table_comment())}"

    def create_kafka_sql(self, kafka_bootstrap: str, password: str, comment: str) -> str:
        settings = {
            "kafka_broker_list": kafka_bootstrap,
            "kafka_topic_list": self.topic,
            "kafka_group_name": self.group,
            "kafka_format": "JSONAsString",
            "kafka_handle_error_mode": "stream",
            "kafka_security_protocol": "sasl_plaintext",
            "kafka_sasl_mechanism": "SCRAM-SHA-512",
            "kafka_sasl_username": self.principal,
            "kafka_sasl_password": password,
        }
        if self.consumers > 1:
            settings["kafka_num_consumers"] = str(self.consumers)
            settings["kafka_thread_per_consumer"] = "1"
        body = ",\n    ".join(f"{k} = {_quote(v)}" for k, v in settings.items())
        return (
            f"CREATE TABLE {self.kafka_table} (message String)\n"
            f"ENGINE = Kafka\nSETTINGS\n    {body}\nCOMMENT {_quote(comment)}"
        )

    def create_mv_sql(self, comment: str) -> str:
        source = (
            "(SELECT message, _topic, _partition, _offset, _timestamp "
            f"FROM {self.kafka_table} WHERE length(_error) = 0)"
        )
        select = self.schema.select.replace(SOURCE, source, 1)
        return (
            f"CREATE MATERIALIZED VIEW {self.mv} TO {self.table}\n"
            f"COMMENT {_quote(comment)}\nAS {select}"
        )

    def create_errors_mv_sql(self, comment: str) -> str:
        return (
            f"CREATE MATERIALIZED VIEW {self.errors_mv} TO {self.database}.{ERRORS_TABLE}\n"
            f"COMMENT {_quote(comment)}\n"
            f"AS SELECT {_quote(self.sink)} AS sink, _topic AS topic, _partition AS partition,\n"
            "    _offset AS offset, _timestamp AS kafka_time, _error AS error, "
            "_raw_message AS raw\n"
            f"FROM {self.kafka_table} WHERE length(_error) > 0"
        )


@dataclass(frozen=True)
class DatabasePlan:
    service: str
    name: str
    sinks: tuple[SinkPlan, ...]

    def comment(self) -> str:
        return f"{MARK} service={self.service}"

    def create_sql(self) -> str:
        return f"CREATE DATABASE IF NOT EXISTS {self.name} COMMENT {_quote(self.comment())}"

    def errors_table_sql(self) -> str:
        return (
            f"CREATE TABLE IF NOT EXISTS {self.name}.{ERRORS_TABLE}\n(\n"
            "    sink LowCardinality(String),\n"
            "    topic LowCardinality(String),\n"
            "    partition UInt64,\n"
            "    offset UInt64,\n"
            "    kafka_time Nullable(DateTime),\n"
            "    error String,\n"
            "    raw String,\n"
            "    inserted_at DateTime DEFAULT now()\n)\n"
            "ENGINE = MergeTree\nORDER BY (sink, inserted_at)\n"
            f"TTL inserted_at + INTERVAL {ERRORS_TTL_DAYS} DAY\n"
            f"COMMENT {_quote(f'{MARK} sink errors')}"
        )


@dataclass(frozen=True)
class ClickHousePlan:
    env: str
    databases: tuple[DatabasePlan, ...]

    def sinks(self) -> list[SinkPlan]:
        return [s for d in self.databases for s in d.sinks]


def _quote(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def plan(services: list[tuple[ServiceSpec, Path]], env: str) -> ClickHousePlan:
    """``services``: each spec with the directory of its ``service.yml``."""
    naming.check_env(env)
    databases = []
    for spec, base in sorted(services, key=lambda x: x[0].name):
        if spec.clickhouse is None or not spec.clickhouse.sinks:
            continue
        db = naming.clickhouse_database(env, spec.clickhouse.database)
        sinks = []
        for sname, sink in sorted(spec.clickhouse.sinks.items()):
            schema = parse((base / sink.schema_file).read_text())
            sinks.append(
                SinkPlan(
                    service=spec.name,
                    sink=sname,
                    database=db,
                    topic=naming.topic_for(env, spec.sink_topic_ref(sink.topic)),
                    group=naming.clickhouse_sink_group(env, spec.name, sname),
                    principal=naming.clickhouse_sink_principal(env, spec.name),
                    credential_path=naming.clickhouse_kafka_variable(spec.name),
                    namespace=naming.nomad_namespace(env),
                    schema=schema,
                    consumers=sink.consumers,
                )
            )
        databases.append(DatabasePlan(spec.name, db, tuple(sinks)))
    return ClickHousePlan(env, tuple(databases))
