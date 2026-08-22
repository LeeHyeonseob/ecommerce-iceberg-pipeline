CREATE TABLE IF NOT EXISTS glue.ecommerce_lakehouse.silver_funnel (
    funnel_key              STRING      COMMENT 'sha256(user_session || product_id). 논리적 PK',
    user_session            STRING,
    user_id                 STRING      COMMENT 'cross-session 전환 매칭 키',
    product_id              STRING,
    funnel_date             DATE        COMMENT '파티션 키. 퍼널 시작 시점 기준',

    viewed                  INT         COMMENT '0/1. 이하 세 플래그는 동일 세션 내 기준',
    carted                  INT,
    purchased               INT         COMMENT '동일 세션 + 윈도우 내 구매만',

    first_view_ts           TIMESTAMP,
    first_cart_ts           TIMESTAMP,
    first_purchase_ts       TIMESTAMP,

    view_count              INT,
    cart_count              INT,
    purchase_count          INT,

    view_to_cart_sec        BIGINT,
    cart_to_purchase_sec    BIGINT,
    view_to_purchase_sec    BIGINT      COMMENT 'cart를 건너뛴 구매(세션 내 구매의 44.8%)도 측정하기 위해 별도 보관',

    converted_later         INT         COMMENT '다른 세션에서 윈도우 내 구매',
    later_purchase_ts       TIMESTAMP,
    later_purchase_gap_sec  BIGINT      COMMENT 'first_cart_ts로부터 경과 시간',

    cart_price              DOUBLE      COMMENT 'first_cart_ts 시점 가격. 놓친 매출 계산용',
    purchase_price          DOUBLE      COMMENT 'first_purchase_ts 시점 가격',
    category_l1             STRING,
    brand                   STRING,

    updated_at              TIMESTAMP
)
USING iceberg
PARTITIONED BY (funnel_date)
TBLPROPERTIES (
    'format-version'               = '2',
    'write.update.mode'            = 'copy-on-write',
    'write.merge.mode'             = 'copy-on-write',
    'write.delete.mode'            = 'copy-on-write',
    'write.target-file-size-bytes' = '134217728'
);
