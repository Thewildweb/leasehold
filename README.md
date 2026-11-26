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
| `src/leasehold/exit` | the registry exit: a CONNECT proxy on a VPS for target leases routed through its address, with offline exit tokens |
| `src/leasehold/archive` | archive sinks: a topic to Parquet files in a bucket |
| `src/leasehold/snapshots` | snapshot datasets: files in a bucket that ClickHouse loads, the manifest, the loader |
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

Proxies are live data in the same schema, not files: proxy accounts (one contract with
one vendor, with its credentials, quota and state), fixed exits (an egress with its own
address, reached through the proxy) and rotating pools (a gateway, keyed `pool:<name>`).
They are managed through the control API with `leasehold proxy ...`; credentials are
encrypted with a key only the control API has (`LEASEHOLD_SECRETS_KEY`). A task type uses
proxies only through its `egress:` block in `service.yml`: tiers in escalation order
(`datacenter`, `isp`, `residential`, `mobile`), each proxy tier naming the accounts or
pools it may use. A block moves a target to the next tier; a task type without
`datacenter` never reaches a target from our own addresses. A task type with
`datacenter_via: exit` reaches the datacenter tier through the exits of registry egresses
(`exit: {endpoint: ...}` in `registry.yml`, `leasehold exit serve` on the VPS) instead of
the worker's own address: for workers that have none, such as a sandboxed browser. See `leasehold.egress.tiers`,
`leasehold.egress.proxies` and `leasehold.runtime.proxy`. An account is drained or enabled only
with a limit (`--cap-gb`, or one its usage API reports); `leasehold proxy account poll`
reads the usage API once while the account is still disabled. Account states, quota warnings and
disabled exits alert through platform rules that `leasehold grafana apply` renders into
the platform folder, one alert per account (`leasehold.grafana.alerts`).

```bash
pass proxies/example-isp | uv run leasehold proxy account set-credentials example-isp
uv run leasehold proxy import --account example-isp --kind isp --country NL < list.txt
uv run leasehold proxy list
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

## Snapshot datasets

A dataset that arrives as one large update on an interval (a register published as a
monthly file) doesn't go through Kafka. The service writes the snapshot as Parquet files
plus a manifest to one of its buckets; ClickHouse loads the files itself, in the
environment's load window. The producer and the loader share only the bucket and the
manifest, so the producer can be any process that writes that layout.

```yaml
buckets:
  snapshots: { retention: 400d }
components:
  extract: { kind: scheduled, ..., writes_buckets: [snapshots] }
clickhouse:
  database: registry
  shared: { addresses: { description: "Source: ... (CC BY 4.0)" } }   # readers get the view
snapshots:
  registry:                        # the dataset; its loader is the component load-registry
    bucket: snapshots
    node_class: services
    keep: 2                        # snapshots kept in ClickHouse (to roll back to)
    tables:
      addresses: { schema_file: schema/addresses.sql }               # current snapshot only
      changes:   { schema_file: schema/changes.sql, mode: append }   # every loaded snapshot
    checks:
      - { table: addresses, min_rows: 1000000, max_change: 0.05 }
```

**Files.** `<dataset>/<snapshot>/<table>/<file>.parquet`, then
`<dataset>/<snapshot>/manifest.json` last; a snapshot without a manifest is ignored. The
manifest (`leasehold.snapshot/1`) lists each table's columns, files, rows, bytes and
sha256. A producer writes it with `SnapshotWriter`:

```python
from leasehold.snapshots.manifest import SnapshotWriter
from leasehold.storage.client import ObjectStore

w = SnapshotWriter(ObjectStore.from_env(), bucket, dataset="registry", snapshot="2026-11-08")
w.add_file("addresses", "/tmp/addresses-0.parquet")  # uploads; rows from the footer
w.commit(source_date="2026-11-08")  # the manifest, last
```

**Schema files** are like a sink's: `CREATE TABLE {table} (...)` with a column
`snapshot LowCardinality(String)`, a MergeTree engine and `PARTITION BY snapshot`, then
`SELECT {snapshot} AS snapshot, ... FROM {source}`, where `{source}` is the table's files.

**What `apply` creates**, in the service's database: `<table>__snapshots` (one partition
per kept snapshot), a view `<table>` that readers use (`SQL SECURITY DEFINER` as the
service's definer user, so readers need a grant on the view only), `snapshot_loads` (one
row per state change of a load) and `snapshot_current`. A shared table's description
(licence, attribution) is the first part of its view's comment, above the `leasehold:`
marker line; shared sinks and derived tables carry it the same way. In object storage,
a read-only user for ClickHouse, whose keys go into a named collection with the
bucket's URL, none of them overridable; datasets on the same bucket share that
collection. A loader user per dataset, limited to `_staging_*` tables, its
`__snapshots` partitions, `snapshot_loads` and its named collection. A periodic job
`<service>-load-<dataset>` that runs `leasehold snapshot load`.

**One load.** The loader checks the files against the manifest, loads each table into a
new `_staging_*` table, compares row counts and runs the checks, attaches each as a new
partition (`REPLACE PARTITION ... FROM`, a partition no view shows yet), and then
inserts one `current` row: every view switches at once. Until that row nothing a reader
sees has changed; on an error the loader drops what it made and records `failed`.
Partitions beyond `keep` are dropped afterwards.

**When.** The environment file's `snapshots` section sets the window and a busy check,
so heavy loads stay away from backups:

```yaml
snapshots:
  window: { start: "05:00", end: "09:00", timezone: Europe/Amsterdam, skip_first_sunday: true }
  busy_query: SELECT count() FROM ops.heavy_jobs WHERE finished_at IS NULL
  busy_tables: [ops.heavy_jobs]
storage:
  clickhouse_endpoint: http://127.0.0.1:9000   # object storage as ClickHouse reaches it
jobs:
  snapshot_image: ghcr.io/example/leasehold-base:<sha>
```

`leasehold snapshot status|rollback --to <snapshot>|release|retry --snapshot <snapshot>`
(with `-e <environment file> --service --dataset`) show the loads, make a kept snapshot
current again, end the hold a rollback puts on the dataset, or let the loader try a
snapshot again that failed `max_attempts` times. After a rollback the loader loads
nothing until `release`; after that it loads only snapshots newer than the current one
that were never current. A snapshot rolled back from comes back only with
`leasehold snapshot load --now --snapshot <id>`. The
ClickHouse user that runs `apply` needs named collection control
(`named_collection_control`; the dev stack sets it in `dev/clickhouse/`).

## Sandboxed components

A component that runs untrusted code (a browser) declares a `sandbox`. Its job's group
then runs in its own network namespace (Nomad bridge mode; the tasks reach each other
on 127.0.0.1), with the component's own task, the extra `tasks` and an optional
`netfilter` prestart task:

```yaml
components:
  render:
    kind: task-worker
    ...
    sandbox:
      user: 10000                  # the component's own task; group defaults to the user
      seccomp: /etc/leasehold/seccomp/chromium.json   # on the Nomad client
      tmpfs: { /tmp: 256M }
      cores: 8                     # whole cores (Nomad `cores`), instead of `cpu`
      netfilter:                   # prestart: installs the rules and exits
        command: [/app/netfilter.sh, "10001", "20000"]
      tasks:
        gateway:
          user: 10001
          command: [/app/gateway, --listen, "127.0.0.1:3128"]
          resources: { cpu: 500, memory: 256 }
        browser:
          user: 20000
          image: ghcr.io/example/chromium:131.0   # default: the component's image
          command: [chromium, --headless]
          resources: { cpu: 2000, memory: 2048 }
          shm_size: 2G
          tmpfs: { /tmp: 1G, /home/pwuser: 512M }
          runtime: runsc           # gVisor
```

Every task runs as its numeric `user:group` (never 0) with a read-only root file
system, all capabilities dropped and `no-new-privileges`, plus its own `seccomp`,
`tmpfs`, `shm_size`, `runtime` and `cores`. Sizes are `512K`, `256M`, `2G`. Only the
component's own task gets the template with its secrets, the Nomad Variables and the
workload identity (and the egress agent's socket, when it uses one); it is the group's
leader, so the other tasks stop with it. Extra tasks get only their own `env`, which may
not hold secrets, and no host volumes. `netfilter` runs as root with `NET_ADMIN` as its
only capability; its rules come from the service (a script in its image and its
arguments). No task declares capabilities, and every task in the group shares the
allocation directory (`$NOMAD_ALLOC_DIR`). Bridge mode needs the CNI plugins on the
Nomad clients, and the Docker plugin must allow `NET_ADMIN` (`allow_caps`) and each
runtime used (`allow_runtimes`).

## License

TBD
