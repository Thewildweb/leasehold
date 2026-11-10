# leasehold

A polite crawl platform: a Postgres control plane that leases targets to egress IPs before any request goes out, and a Kafka data plane that classifies and processes every response.

> Early development. Nothing here is usable yet.

## Design

- Rate limits per target, per egress IP and per provider are checked before a request is sent.
- A target (an IP, or a domain behind a CDN) is leased to one egress IP at a time, so the per-target limit is enforced locally by the leaseholder.
- Tasks are scheduled from a Postgres table (the frontier). Every response is written to Kafka and from there classified, processed and stored in ClickHouse.
- Stack: Python, Postgres, Kafka, ClickHouse, Nomad.

## License

TBD
