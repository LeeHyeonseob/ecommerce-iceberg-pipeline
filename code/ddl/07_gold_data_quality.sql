CREATE TABLE IF NOT EXISTS glue.ecommerce_lakehouse.gold_data_quality (
    summary_date              DATE,
    total_events              BIGINT,
    view_cnt                  BIGINT,
    cart_cnt                  BIGINT,
    purchase_cnt              BIGINT,
    cart_view_ratio           DOUBLE      COMMENT '이상 감지 핵심 지표',
    purchase_view_ratio       DOUBLE,
    null_category_rate        DOUBLE,
    null_brand_rate           DOUBLE,
    price_null_cnt            BIGINT      COMMENT '캐스팅 실패 건수',
    price_nonpositive_cnt     BIGINT,
    purchase_without_view_cnt BIGINT      COMMENT '같은 세션에 view 없이 발생한 구매. 세션 안만 본 것이라 귀속 불가와는 다르다',
    updated_at                TIMESTAMP
)
USING iceberg
PARTITIONED BY (summary_date)
TBLPROPERTIES (
    'format-version'    = '2',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode'  = 'copy-on-write',
    'write.delete.mode' = 'copy-on-write'
);
