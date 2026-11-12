-- One row per chunk attempt (success or failure), from certwatch.entries.v1.
-- Same columns as the v2 table, plus event_id: a message read twice
-- (at-least-once delivery) collapses into one row.
CREATE TABLE {table}
(
    event_id UUID,
    log_name LowCardinality(String),
    chunk_start UInt64,
    chunk_end UInt64,
    status Enum8('success' = 1, 'failed' = 2, 'error' = 3),
    domains_extracted UInt32,
    duration_ms UInt32,
    error_short String,
    worker_id LowCardinality(String),
    attempted_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(attempted_at)
ORDER BY (log_name, chunk_start, attempted_at, event_id);

SELECT
    toUUIDOrZero(JSONExtractString(message, 'event_id')) AS event_id,
    JSONExtractString(message, 'payload', 'log_name') AS log_name,
    JSONExtractUInt(message, 'payload', 'chunk_start') AS chunk_start,
    JSONExtractUInt(message, 'payload', 'chunk_end') AS chunk_end,
    CAST(
        if(JSONExtractString(message, 'payload', 'status') IN ('success', 'failed', 'error'),
           JSONExtractString(message, 'payload', 'status'), 'error'),
        'Enum8(\'success\' = 1, \'failed\' = 2, \'error\' = 3)'
    ) AS status,
    JSONExtractUInt(message, 'payload', 'domains_extracted') AS domains_extracted,
    JSONExtractUInt(message, 'payload', 'duration_ms') AS duration_ms,
    JSONExtractString(message, 'payload', 'error_short') AS error_short,
    JSONExtractString(message, 'worker') AS worker_id,
    parseDateTime64BestEffortOrZero(JSONExtractString(message, 'ts'), 3, 'UTC') AS attempted_at
FROM {source};
