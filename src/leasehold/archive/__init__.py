"""Archive sinks: a Kafka topic written to Parquet files in a bucket.

``layout`` turns events into rows (pure, no pyarrow at import), ``sink`` runs the
consumer (needs the ``archive`` extra: pyarrow).
"""
