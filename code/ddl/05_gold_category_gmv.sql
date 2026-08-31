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
