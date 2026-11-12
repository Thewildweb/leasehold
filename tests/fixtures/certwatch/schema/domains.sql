-- One row per (domain, certificate) from successful chunk attempts in
-- certwatch.entries.v1. Same columns as the v2 table (chunk_id is the
-- chunk's start index); duplicates collapse on (domain, cert_fingerprint).
CREATE TABLE {table}
(
    domain String,
    cert_fingerprint String,
    log_name LowCardinality(String),
    chunk_id UInt64,
    cert_issuer String,
    cert_not_before DateTime64(3, 'UTC'),
    cert_not_after DateTime64(3, 'UTC'),
    org_name Nullable(String),
    org_country Nullable(String),
    org_locality Nullable(String),
    org_state Nullable(String),
    tld LowCardinality(String),
    is_wildcard Bool,
    ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (domain, cert_fingerprint);

SELECT
    lower(name) AS domain,
    JSONExtractString(cert, 'fingerprint') AS cert_fingerprint,
    JSONExtractString(message, 'payload', 'log_name') AS log_name,
    JSONExtractUInt(message, 'payload', 'chunk_start') AS chunk_id,
    JSONExtractString(cert, 'issuer') AS cert_issuer,
    parseDateTime64BestEffortOrZero(JSONExtractString(cert, 'not_before'), 3, 'UTC') AS cert_not_before,
    parseDateTime64BestEffortOrZero(JSONExtractString(cert, 'not_after'), 3, 'UTC') AS cert_not_after,
    nullIf(JSONExtractString(cert, 'org_name'), '') AS org_name,
    nullIf(JSONExtractString(cert, 'org_country'), '') AS org_country,
    nullIf(JSONExtractString(cert, 'org_locality'), '') AS org_locality,
    nullIf(JSONExtractString(cert, 'org_state'), '') AS org_state,
    arrayElement(splitByChar('.', lower(name)), -1) AS tld,
    startsWith(name, '*.') AS is_wildcard
FROM {source}
ARRAY JOIN JSONExtractArrayRaw(message, 'payload', 'certificates') AS cert
ARRAY JOIN JSONExtract(cert, 'domains', 'Array(String)') AS name
WHERE JSONExtractString(message, 'payload', 'status') = 'success';
