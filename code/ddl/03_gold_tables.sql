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


CREATE TABLE IF NOT EXISTS glue.ecommerce_lakehouse.gold_funnel_daily (
    summary_date            DATE,
    category_l1             STRING      COMMENT "'unknown' 포함. 전체 합계는 'ALL'",

    funnels                 BIGINT,
    views                   BIGINT,
    carts                   BIGINT,
    purchases               BIGINT      COMMENT '세션 내 전환',
    purchases_later         BIGINT      COMMENT 'cross-session 전환',

    view_to_cart            DOUBLE,
    cart_to_purchase        DOUBLE,
    view_to_purchase        DOUBLE,
    abandon_rate            DOUBLE      COMMENT '세션 내 기준',
    abandon_rate_final      DOUBLE      COMMENT 'cross-session 포함',

    cart_value              DOUBLE,
    lost_revenue            DOUBLE,

    updated_at              TIMESTAMP
)
USING iceberg
PARTITIONED BY (summary_date)
TBLPROPERTIES (
    'format-version'    = '2',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode'  = 'copy-on-write',
    'write.delete.mode' = 'copy-on-write'
);


CREATE TABLE IF NOT EXISTS glue.ecommerce_lakehouse.gold_category_gmv (
    summary_date        DATE,
    dim_type            STRING      COMMENT "'category_l1' / 'brand' / 'product'",
    dim_value           STRING,
    gmv                 DOUBLE,
    gmv_share           DOUBLE,
    purchase_cnt        BIGINT,
    rank_in_day         INT         COMMENT '해당 dim_type 내 GMV 내림차순 순위',
    cum_gmv_share       DOUBLE      COMMENT '순위 누적 비중. 파레토는 이 값으로 읽는다',
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


CREATE TABLE IF NOT EXISTS glue.ecommerce_lakehouse.gold_data_quality (
    summary_date            DATE,
    total_events            BIGINT,
    view_cnt                BIGINT,
    cart_cnt                BIGINT,
    purchase_cnt            BIGINT,
    cart_view_ratio         DOUBLE      COMMENT '이상 감지 핵심 지표',
    purchase_view_ratio     DOUBLE,
    null_category_rate      DOUBLE,
    null_brand_rate         DOUBLE,
    price_null_cnt          BIGINT      COMMENT '캐스팅 실패 건수',
    price_nonpositive_cnt   BIGINT,
    updated_at              TIMESTAMP
)
USING iceberg
PARTITIONED BY (summary_date)
TBLPROPERTIES (
    'format-version'    = '2',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode'  = 'copy-on-write',
    'write.delete.mode' = 'copy-on-write'
);
