CREATE TABLE IF NOT EXISTS glue.ecommerce_lakehouse.gold_funnel_daily (
    summary_date            DATE,
    category_l1             STRING      COMMENT "'unknown' 포함. 전체 합계는 'ALL'",

    funnels                 BIGINT,
    views                   BIGINT,
    carts                   BIGINT,
    purchases               BIGINT      COMMENT '세션 내 전환',
    purchases_later         BIGINT      COMMENT 'cross-session 전환',

    views_carted            BIGINT      COMMENT '보고 담은 퍼널. view_to_cart의 분자',
    views_purchased         BIGINT      COMMENT '보고 산 퍼널. view_to_purchase의 분자',
    carts_purchased         BIGINT      COMMENT '담고 그 세션에서 산 퍼널. purchases와 다르다 - 구매의 55.8%는 장바구니를 안 거친다',
    carts_converted_later   BIGINT      COMMENT '담고 나중에 다른 세션에서 산 퍼널. 이탈률에는 purchases_later가 아니라 이 값을 쓴다',

    view_to_cart            DOUBLE      COMMENT '이하 비율은 일별 파생값. 기간 집계는 위 카운트를 SUM해서 다시 나눌 것',
    cart_to_purchase        DOUBLE      COMMENT 'carts_purchased / carts. purchases로 나누면 110%가 나온다',
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
