"""Helpers for tests against a running Postgres (LEASEHOLD_TEST_POSTGRES).

    docker compose -f dev/docker-compose.yml up -d --wait postgres
    LEASEHOLD_TEST_POSTGRES=postgresql://leasehold:dev-leasehold@localhost:15432/leasehold \\
      uv run pytest tests/integration

Each test module uses a fresh random environment, so its schema is
``it<hex>_frontier``, and drops it afterwards.
"""

from __future__ import annotations

import os
import secrets
from contextlib import contextmanager

import psycopg
import pytest
from psycopg import sql

from leasehold.frontier import schema
from leasehold.spec import ServiceSpec

DSN = os.environ.get("LEASEHOLD_TEST_POSTGRES")

requires_postgres = pytest.mark.skipif(not DSN, reason="LEASEHOLD_TEST_POSTGRES not set")

# Two services: `jobs` has a one-shot and a recurring task type and a worker for
# each; `feeder` is a second service, to check ownership rules.
SPECS = [
    ServiceSpec.model_validate(
        {
            "spec_version": 1,
            "name": "jobs",
            "description": "test service",
            "owner": "test",
            "components": {
                "planner": {
                    "kind": "scheduled",
                    "schedule": "*/5 * * * *",
                    "creates_tasks": ["jobs.once", "jobs.repeat"],
                    "node_class": "services",
                    "resources": {"cpu": 100, "memory": 64},
                },
                "worker": {
                    "kind": "task-worker",
                    "consumes_tasks": ["jobs.once", "jobs.repeat"],
                    "node_class": "processing",
                    "resources": {"cpu": 100, "memory": 64},
                    "count": {"min": 1, "max": 2},
                },
            },
            "frontier": {
                "task_types": {
                    "jobs.once": {
                        "key": "id",
                        "max_attempts": 3,
                        "retry_backoff": "1m",
                        "retry_backoff_max": "3m",
                        "lease_duration": "30s",
                    },
                    "jobs.repeat": {
                        "key": "id",
                        "recrawl_interval": "1d",
                        "hash_partitions": 4,
                    },
                }
            },
        }
    ),
    ServiceSpec.model_validate(
        {
            "spec_version": 1,
            "name": "feeder",
            "description": "second test service",
            "owner": "test",
            "components": {
                "worker": {
                    "kind": "task-worker",
                    "consumes_tasks": ["feeder.item"],
                    "node_class": "processing",
                    "resources": {"cpu": 100, "memory": 64},
                    "count": {"min": 1, "max": 1},
                },
            },
            "frontier": {"task_types": {"feeder.item": {"key": "id"}}},
        }
    ),
]


@contextmanager
def fresh_env():
    """A migrated frontier schema for a random environment, with SPECS applied."""
    env = "it" + secrets.token_hex(4)
    with psycopg.connect(DSN, autocommit=True) as conn:
        schema.migrate(conn, env)
        schema.apply_task_types(conn, env, schema.diff_task_types(conn, env, SPECS))
        try:
            yield env
        finally:
            conn.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema.schema_name(env)))
            )
