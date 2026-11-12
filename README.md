# leasehold

A polite crawl platform: a Postgres control plane that leases targets to egress IPs before any request goes out, and a Kafka data plane that classifies and processes every response.

> Early development. Nothing here is usable yet.

## Design

- Rate limits per target, per egress IP and per provider are checked before a request is sent.
- A target (an IP, or a domain behind a CDN) is leased to one egress IP at a time, so the per-target limit is enforced locally by the leaseholder.
- Tasks are scheduled from a Postgres table (the frontier). Every response is written to Kafka and from there classified, processed and stored in ClickHouse.
- Stack: Python, Postgres, Kafka, ClickHouse, Nomad.

## Development

```bash
uv sync
uv run pytest                                   # unit tests
docker compose -f dev/docker-compose.yml up -d --wait
LEASEHOLD_TEST_KAFKA=localhost:19092 \
LEASEHOLD_TEST_POSTGRES=postgresql://leasehold:dev-leasehold@localhost:15432/leasehold \
LEASEHOLD_TEST_GRAFANA=http://admin:dev-admin@localhost:13000 \
  uv run pytest tests/integration
```

The dev stack runs Kafka (KRaft, SASL/SCRAM, ACLs enforced), the Karapace schema
registry, kafka-ui, Postgres 16 and Grafana 11.3 on loopback ports 19092, 18081, 18080,
15432 and 13000.
`dev/kafka/server.properties` is also the reference configuration for production.

Running the control API against the dev stack:

```bash
export LEASEHOLD_DSN=postgresql://leasehold:dev-leasehold@localhost:15432/leasehold
uv run leasehold frontier migrate --env dev --yes
uv run leasehold frontier apply path/to/services --env dev --yes
uv run leasehold api token --admin you > entry.json   # prints the token on stderr
echo "{\"tokens\": [$(cat entry.json)]}" > tokens.json
LEASEHOLD_ENV=dev LEASEHOLD_SERVICES=path/to/services LEASEHOLD_TOKENS_FILE=tokens.json \
  uv run leasehold api serve
```

Dashboards for a set of services (dry run unless `--yes`):

```bash
uv run leasehold grafana plan path/to/services --env dev
LEASEHOLD_GRAFANA_PASSWORD=dev-admin uv run leasehold grafana apply path/to/services \
  --env dev --url http://localhost:13000 --user admin
```

Nomad jobs, one per component, with images tagged by git SHA (dry run unless `--yes`):

```bash
uv run leasehold nomad plan path/to/services --env dev --sha <git-sha> \
  --api-url http://control-api:8400 --kafka-bootstrap kafka:9092 --out jobs/
NOMAD_TOKEN=... uv run leasehold nomad apply path/to/services --env dev --sha <git-sha> \
  --api-url http://control-api:8400 --kafka-bootstrap kafka:9092 --url http://nomad:4646
```

Kafka credentials per component, stored in Nomad Variables that the jobs read
(dry run unless `--yes`):

```bash
LEASEHOLD_KAFKA_PASSWORD=dev-admin NOMAD_TOKEN=... uv run leasehold credentials apply \
  path/to/services --env dev --bootstrap localhost:19092 --url http://nomad:4646
```

ClickHouse sinks (database, target tables from the schema files, Kafka tables and
views; dry run unless `--yes`):

```bash
LEASEHOLD_CLICKHOUSE_PASSWORD=dev-admin NOMAD_TOKEN=... uv run leasehold clickhouse apply \
  path/to/services --env dev --url http://localhost:18123 --user admin \
  --kafka-bootstrap kafka:9092 --nomad-url http://nomad:4646
```

`nomad apply` keeps the running count of a job (within the spec's `count` range), never
touches a job it doesn't manage, and reports jobs of a service that the spec no
longer has (`--prune` stops them).

## Components

A component is a Python program that runs inside the leasehold runtime:

```python
from leasehold.runtime import Retry, run_worker


def handle(task, ctx):
    ctx.emit("entries", {"log_name": task.payload["log"], "entries": fetch(task.key)})


if __name__ == "__main__":
    raise SystemExit(run_worker(handle, concurrency=8))
```

`run_worker`, `run_processor` and `run_scheduled` read their configuration from
the environment (`leasehold.runtime.config`), log JSON to stdout, serve
`/health` and `/metrics`, and stop gracefully on SIGTERM. Component images build
on the base image:

```bash
docker build -f images/base/Dockerfile --target runtime -t leasehold-base .
docker build -f images/base/Dockerfile --target control-api -t leasehold-control-api .
```

`images/component.Dockerfile.example` shows a component image.

## License

TBD
