CREATE TABLE IF NOT EXISTS glue.ecommerce_lakehouse.silver_events (
    event_id            STRING      COMMENT '원본 9컬럼 sha256. 논리적 PK',

    event_time          TIMESTAMP   COMMENT '이벤트 발생 시각',
    event_date          DATE        COMMENT '파티션 키',

    event_type          STRING      COMMENT 'view / cart / purchase',
    user_id             STRING,
    user_session        STRING,
    product_id          STRING,
    category_id         STRING,
    category_code       STRING,
    category_l1         STRING,
    category_l2         STRING,
    category_l3         STRING,
    brand               STRING,
    price               DOUBLE      COMMENT '이벤트 시점 가격',

    kafka_partition     INT,
    kafka_offset        BIGINT,
    kafka_timestamp     TIMESTAMP   COMMENT 'producer 발행 시각',
    ingest_ts           TIMESTAMP   COMMENT 'Flink S3 적재 시각',
    pipeline_lag_sec    DOUBLE      COMMENT 'ingest_ts - kafka_timestamp',

    updated_at          TIMESTAMP
)
USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES (
    'format-version'               = '2',
    'write.update.mode'            = 'copy-on-write',
    'write.merge.mode'             = 'copy-on-write',
    'write.delete.mode'            = 'copy-on-write',
    'write.target-file-size-bytes' = '134217728'
);
