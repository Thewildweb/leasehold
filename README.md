# leasehold

A crawl platform built around one rule: rate limits are checked before a request goes
out, not after a site complains. A Postgres control plane schedules the work; Kafka
carries the results into ClickHouse.

## How it works

- **Frontier.** Tasks live in a Postgres table. Workers lease them from the control API,
  which enforces per-target, per-egress-IP and per-provider limits up front.
- **Leases.** A target is held by one egress IP at a time, so its rate limit is enforced
  locally by the leaseholder.
- **Data plane.** Workers emit one event per attempt to Kafka. ClickHouse ingests through
  Kafka tables and materialized views; there is no inserter code.
- **Desired state in git.** Each service is declared in a `service.yml`. `leasehold apply`
  derives everything for an environment from those files: Kafka topics and ACLs, the
  frontier's task types, credentials, ClickHouse sinks, Grafana dashboards and alert
  rules, and Nomad jobs.

Stack: Python 3.12, Postgres, Kafka, ClickHouse, Grafana, Nomad.

## Layout

| Path | What |
|---|---|
| `src/leasehold/spec` | the `service.yml` schema (`leasehold schema` prints it as JSON Schema) |
| `src/leasehold/runtime` | `run_worker`, `run_processor`, `run_scheduled`: the process a component runs in |
| `src/leasehold/api` | the control API: leases, task upserts, the alert inbox |
| `src/leasehold/{kafka,frontier,clickhouse,grafana,nomad}` | one apply step each |
| `src/leasehold/egress` | egress addresses, providers and budgets, shared by all environments; provider tagging for planners |
| `src/leasehold/agent` | the egress agent: one per machine and environment, the only process there that talks to the dispatcher |
| `src/leasehold/mocks` | a mock CT log for tests and the dev stack |
| `dev/` | the dev stack (Docker Compose) |
| `images/` | the base image that component images build on |

## Development

```bash
uv sync
uv run pytest                                          # unit tests
docker compose -f dev/docker-compose.yml up -d --wait  # dev stack on loopback ports
uv run pytest tests/integration                        # needs the LEASEHOLD_TEST_* variables
uv run leasehold apply path/to/services -e dev/environment.yml --sha <git-sha>  # dry run
```

`leasehold --help` lists the commands; every apply step is a dry run unless `--yes`.

Egress addresses, provider rules and budgets live in one directory of YAML files
(`registry.yml`, `providers.yml`, `budgets.yml`; formats in `leasehold.egress.config`),
written to a Postgres schema all environments share:

```bash
uv run leasehold egress migrate --yes
uv run leasehold egress apply path/to/egress          # dry run
```

A component is a function the runtime calls per task:

```python
from leasehold.runtime import run_worker


def handle(task, ctx):
    ctx.emit("entries", fetch(task.payload))


if __name__ == "__main__":
    raise SystemExit(run_worker(handle, concurrency=8))
```

## License

TBD
