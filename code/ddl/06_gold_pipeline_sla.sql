CREATE TABLE IF NOT EXISTS glue.ecommerce_lakehouse.gold_pipeline_sla (
    summary_date        DATE,
    event_hour          INT,
    event_type          STRING      COMMENT 'view / cart / purchase',
    events              BIGINT,
    lag_p50             DOUBLE,
    lag_p95             DOUBLE,
    lag_p99             DOUBLE,
    lag_max             DOUBLE,
    updated_at          TIMESTAMP
)
USING iceberg
PARTITIONED BY (summary_date)
TBLPROPERTIES (
    'format-version'    = '2',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode'  = 'copy-on-write',
    'write.delete.mode' = 'copy-on-write'
);
