-- Plan log: one row per planner run. The watermark of the last successful run is where
-- the next run starts.
CREATE TABLE {table}
(
    planner LowCardinality(String),
    task_type LowCardinality(String),
    run_id UUID,
    status Enum8('ok' = 1, 'skipped' = 2, 'failed' = 3),
    full UInt8,
    backlog_before UInt64,
    backlog_target UInt64,
    selected UInt64,
    inserted UInt64,
    updated UInt64,
    watermark_from String,
    watermark String,
    duration_ms UInt32,
    error_short String,
    finished_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(finished_at)
ORDER BY (planner, finished_at)
TTL toDateTime(finished_at) + INTERVAL 1 YEAR;

SELECT
    JSONExtractString(message, 'payload', 'planner') AS planner,
    JSONExtractString(message, 'payload', 'task_type') AS task_type,
    toUUIDOrZero(JSONExtractString(message, 'payload', 'run_id')) AS run_id,
    CAST(
        if(JSONExtractString(message, 'payload', 'status') IN ('ok', 'skipped', 'failed'),
           JSONExtractString(message, 'payload', 'status'), 'failed'),
        'Enum8(\'ok\' = 1, \'skipped\' = 2, \'failed\' = 3)'
    ) AS status,
    toUInt8(JSONExtractBool(message, 'payload', 'full')) AS full,
    JSONExtractUInt(message, 'payload', 'backlog_before') AS backlog_before,
    JSONExtractUInt(message, 'payload', 'backlog_target') AS backlog_target,
    JSONExtractUInt(message, 'payload', 'selected') AS selected,
    JSONExtractUInt(message, 'payload', 'inserted') AS inserted,
    JSONExtractUInt(message, 'payload', 'updated') AS updated,
    JSONExtractString(message, 'payload', 'watermark_from') AS watermark_from,
    JSONExtractString(message, 'payload', 'watermark') AS watermark,
    toUInt32(JSONExtractUInt(message, 'payload', 'duration_ms')) AS duration_ms,
    JSONExtractString(message, 'payload', 'error_short') AS error_short,
    parseDateTime64BestEffortOrZero(JSONExtractString(message, 'ts'), 3, 'UTC') AS finished_at
FROM {source}
WHERE JSONExtractString(message, 'payload', 'kind') = 'run';
