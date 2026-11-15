-- Plan log: one row per key a planner queued. Created by apply for every service with
-- a planner; planners read it to skip keys they queued recently (exclude_planned_for).
CREATE TABLE {table}
(
    planner LowCardinality(String),
    task_type LowCardinality(String),
    key String,
    priority UInt8,
    run_id UUID,
    planned_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(planned_at)
ORDER BY (planner, key, planned_at)
TTL toDateTime(planned_at) + INTERVAL 90 DAY;

SELECT
    JSONExtractString(message, 'payload', 'planner') AS planner,
    JSONExtractString(message, 'payload', 'task_type') AS task_type,
    key,
    toUInt8(JSONExtractUInt(message, 'payload', 'priority')) AS priority,
    toUUIDOrZero(JSONExtractString(message, 'payload', 'run_id')) AS run_id,
    parseDateTime64BestEffortOrZero(JSONExtractString(message, 'ts'), 3, 'UTC') AS planned_at
FROM {source}
ARRAY JOIN JSONExtract(message, 'payload', 'keys', 'Array(String)') AS key
WHERE JSONExtractString(message, 'payload', 'kind') = 'keys';
