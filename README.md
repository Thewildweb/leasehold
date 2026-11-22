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
  frontier's task types, credentials, buckets, ClickHouse sinks, archive sinks, Grafana
  dashboards and alert rules, and Nomad jobs.
- **Object storage.** Bodies and other large data go to S3-compatible buckets (MinIO in
  the dev stack), not ClickHouse: archive sinks write topics to Parquet, and payloads too
  large for Kafka travel by claim check.

Stack: Python 3.12, Postgres, Kafka, ClickHouse, Grafana, Nomad, MinIO.

## Layout

| Path | What |
|---|---|
| `src/leasehold/spec` | the `service.yml` schema (`leasehold schema` prints it as JSON Schema) |
| `src/leasehold/runtime` | `run_worker`, `run_processor`, `run_scheduled`: the process a component runs in |
| `src/leasehold/api` | the control API: leases, task upserts, the alert inbox |
| `src/leasehold/{kafka,frontier,storage,clickhouse,grafana,nomad}` | one apply step each |
| `src/leasehold/egress` | egress addresses, providers and budgets, shared by all environments; provider tagging for planners |
| `src/leasehold/agent` | the egress agent: one per machine and environment, the only process there that talks to the dispatcher |
| `src/leasehold/archive` | archive sinks: a topic to Parquet files in a bucket |
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

`run_processor` hands its handler batches of at most `batch_size` events or
`batch_max_bytes` bytes (default 64 MB; `LEASEHOLD_BATCH_MAX_BYTES` overrides it),
whichever comes first; a single larger message is a batch of its own. Kafka's prefetch
queue is capped at half the byte limit, so a processor holds about 1.5 times the limit
in messages.

A component is a function the runtime calls per task:

```python
from leasehold.runtime import run_worker


def handle(task, ctx):
    ctx.emit("entries", fetch(task.payload))


if __name__ == "__main__":
    raise SystemExit(run_worker(handle, concurrency=8))
```

## Buckets, archive sinks and the claim check

A service declares its buckets like its topics. Only its own components write them;
any component reads a bucket it names (`<service>.<bucket>` for another service's):

```yaml
buckets:
  archive: {}                    # kept
  scratch: { retention: 7d }     # lifecycle expiry, whole days
components:
  worker:  { ..., writes_buckets: [archive] }
  reader:  { ..., reads_buckets: [archive, other-service.raw] }
```

`leasehold storage plan|apply` (a step of `leasehold apply`) creates the buckets
(`<service>-<bucket>`, `<env>-` in front outside prod) with their lifecycle rules, one
user and policy per component limited to its buckets, and puts the user's keys in the
job's Nomad Variable. Buckets are never deleted; stale users and policies are removed
with `--prune`.

**Claim check.** With `large_payloads: <bucket>` on a topic, an event whose payload is
over the inline limit stores the payload in that bucket and carries a `payload_ref`;
`run_processor` resolves it before calling the handler. A consumer needs the bucket in
its `reads_buckets` (`leasehold validate` checks it). Claims expire one day after the
topic's retention.

**Archive sinks** write a topic to Parquet (zstd), one file per Kafka partition and
offset range, by hour:
`<topic ref>/dt=YYYY-MM-DD/hh=HH/p<partition>-<first offset>-<last offset>.parquet`.

```yaml
archives:
  responses:
    topic: responses
    bucket: archive
    node_class: processing
    columns:                       # payload fields as typed columns
      domain: string
      status: int32
      hops: json                   # lists and objects as JSON text
      body_raw: { type: base64, field: body_b64, text_fallback: body }
    drop: [body_text_dropped]      # left out of the archive
```

Every file has the envelope (`event_id`, `ts`, `task_key`, ...), the Kafka coordinates
(`_partition`, `_offset`, `_kafka_ts`), the declared columns, and `payload`: the payload
fields that aren't columns, as JSON text, so a field added to the model later is kept
without a schema change. A value that doesn't fit its column's type is null there and
stays in `payload`. A `base64` column with `text_fallback` holds the decoded bytes, or,
for events without them, the text field encoded as UTF-8; `<column>_from_text` says
which. Claimed payloads are resolved and archived as columns (claims expire, the archive
doesn't).

`dt`/`hh` is the hour of the Kafka timestamp of a file's first message; a late message
stays in the open file, so widen a time filter by an hour. Offsets are committed after
the upload; a replay replaces the file it was writing, so rows are not duplicated.

Reading the archive of the dev stack (service `example`, bucket `archive`), with DuckDB:

```sql
INSTALL httpfs; LOAD httpfs;
CREATE SECRET dev (TYPE s3, KEY_ID 'admin', SECRET 'dev-admin-secret',
                   ENDPOINT 'localhost:19000', URL_STYLE 'path', USE_SSL false);
SELECT dt, hh, status, count() AS n, sum(octet_length(body_raw)) AS body_bytes
FROM read_parquet('s3://dev-example-archive/example.responses.v1/**/*.parquet',
                  hive_partitioning = true)
WHERE dt = '2026-11-17'
GROUP BY ALL ORDER BY ALL;
```

and with ClickHouse's `s3()` table function (MinIO is `minio:9000` inside the compose
network):

```sql
SELECT _path, status, domain, length(body_raw) AS body_bytes,
       JSONExtractInt(payload, 'schema_version') AS payload_version
FROM s3('http://minio:9000/dev-example-archive/example.responses.v1/dt=2026-11-17/**/*.parquet',
        'admin', 'dev-admin-secret', 'Parquet')
WHERE status >= 400
LIMIT 10;
```

## License

TBD
