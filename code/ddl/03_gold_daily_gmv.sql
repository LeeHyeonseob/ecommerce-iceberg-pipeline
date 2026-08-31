CREATE TABLE IF NOT EXISTS glue.ecommerce_lakehouse.gold_daily_gmv (
    summary_date        DATE,
    gmv                 DOUBLE,
    purchase_cnt        BIGINT,
    unique_buyers       BIGINT,
    avg_price           DOUBLE,
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
