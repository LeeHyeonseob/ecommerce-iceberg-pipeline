-- 기존 Silver 테이블 전환용. 신규 환경은 01/02 DDL에서 이미 MOR로 생성된다.
ALTER TABLE glue.ecommerce_lakehouse.silver_events SET TBLPROPERTIES (
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode'  = 'merge-on-read',
    'write.delete.mode' = 'merge-on-read'
);

ALTER TABLE glue.ecommerce_lakehouse.silver_funnel SET TBLPROPERTIES (
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode'  = 'merge-on-read',
    'write.delete.mode' = 'merge-on-read'
);
