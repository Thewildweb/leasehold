# leasehold

**Lease targets, not requests.**

A polite crawl platform: a Postgres control plane that leases targets to egress IPs before any request goes out, and a Kafka data plane that classifies and processes every response.

> Early development. Nothing here is usable yet.

## Idea

- **Admission control before the request.** Rate limits per target, per egress IP and per provider are enforced up front, never detected after the fact.
- **Lease targets, not requests.** One egress IP owns a target at a time, so per-target limits are local, with no coordination per request.
- **Kafka for results, Postgres for work.** Tasks are scheduled from a Postgres frontier; every response flows through Kafka into classification, processing and ClickHouse.
- **Boring technology.** Python, Postgres, Kafka, ClickHouse, Nomad.

## License

TBD
