-- 6회차 과제: rewrite_data_files / rewrite_position_delete_files / rewrite_manifests
--
-- 실행:
--   ./infra/spark-sql-iceberg.sh -f code/health-queries/compaction_demo.sql 2>&1 | tee compaction_log.txt
--
-- 운영 테이블(silver_events 등)은 파티션당 파일이 1개고 전부 COW라 세 프로시저 모두 0을 반환한다.
-- 그래서 조각난 MOR 테이블을 따로 만들어 실행 결과가 드러나게 한다.
-- 데이터는 실제 프로젝트의 gold_category_gmv에서 가져온다.

SET spark.sql.cli.print.header=true;

-- ================= 준비 =================
DROP TABLE IF EXISTS glue.ecommerce_lakehouse.compaction_demo PURGE;

-- MOR로 둬야 DELETE가 position delete 파일을 만든다. COW면 파일을 통째로 다시 써서 안 생긴다.
-- distribution-mode=none이어야 REPARTITION 힌트가 살아 파일이 8개로 쪼개진다
CREATE TABLE glue.ecommerce_lakehouse.compaction_demo (
    dt DATE, dim_value STRING, gmv DOUBLE
) USING iceberg PARTITIONED BY (dt)
TBLPROPERTIES (
    'format-version'          = '2',
    'write.delete.mode'       = 'merge-on-read',
    'write.update.mode'       = 'merge-on-read',
    'write.distribution-mode' = 'none'
);

INSERT INTO glue.ecommerce_lakehouse.compaction_demo
SELECT /*+ REPARTITION(8) */ DATE '2019-10-01', dim_value, gmv
FROM glue.ecommerce_lakehouse.gold_category_gmv
WHERE summary_date <= DATE '2019-10-08';

DELETE FROM glue.ecommerce_lakehouse.compaction_demo WHERE gmv < 100;
DELETE FROM glue.ecommerce_lakehouse.compaction_demo WHERE gmv >= 100 AND gmv < 1000;


-- ================= 1. 컴팩션 전 상태 =================
SELECT '========== 1. 컴팩션 전 ==========' AS section;

SELECT COUNT(*) AS total_files, AVG(file_size_in_bytes) AS avg_bytes,
       MIN(file_size_in_bytes) AS min_bytes, MAX(file_size_in_bytes) AS max_bytes,
       SUM(file_size_in_bytes) AS total_bytes
FROM glue.ecommerce_lakehouse.compaction_demo.files;

SELECT COUNT(*) AS manifests FROM glue.ecommerce_lakehouse.compaction_demo.manifests;
SELECT COUNT(*) AS snapshots FROM glue.ecommerce_lakehouse.compaction_demo.snapshots;

-- content 0 = 데이터 파일, 1 = position delete, 2 = equality delete
SELECT content, COUNT(*) AS files, SUM(file_size_in_bytes) AS bytes
FROM glue.ecommerce_lakehouse.compaction_demo.files GROUP BY content ORDER BY content;


-- ================= 2. rewrite_position_delete_files =================
-- data files보다 먼저 돌려야 한다. 데이터를 1개로 합친 뒤엔 delete도 1개가 돼 0을 반환한다.
-- 8 -> 8로 유지되는 건 정상이다. position delete는 데이터 파일당 1개가 원칙이라 이미 최소 상태고,
-- 재작성 바이트가 실제로 작업했다는 증거다
SELECT '========== 2. rewrite_position_delete_files ==========' AS section;

CALL glue.system.rewrite_position_delete_files(
    table   => 'ecommerce_lakehouse.compaction_demo',
    options => map('min-input-files', '2')
);


-- ================= 3. rewrite_data_files =================
SELECT '========== 3. rewrite_data_files ==========' AS section;

CALL glue.system.rewrite_data_files(
    table    => 'ecommerce_lakehouse.compaction_demo',
    strategy => 'binpack',
    options  => map(
        'target-file-size-bytes',       '134217728',
        'min-input-files',              '2',
        'partial-progress.enabled',     'true',
        'partial-progress.max-commits', '10'
    )
);


-- ================= 4. rewrite_manifests =================
SELECT '========== 4. rewrite_manifests ==========' AS section;

CALL glue.system.rewrite_manifests('ecommerce_lakehouse.compaction_demo');


-- ================= 5. 컴팩션 후 상태 =================
SELECT '========== 5. 컴팩션 후 ==========' AS section;

SELECT COUNT(*) AS total_files, AVG(file_size_in_bytes) AS avg_bytes,
       MIN(file_size_in_bytes) AS min_bytes, MAX(file_size_in_bytes) AS max_bytes,
       SUM(file_size_in_bytes) AS total_bytes
FROM glue.ecommerce_lakehouse.compaction_demo.files;

SELECT COUNT(*) AS manifests FROM glue.ecommerce_lakehouse.compaction_demo.manifests;

-- 데이터 파일은 8 -> 1로 줄고 delete 파일 8개는 남는다. rewrite_data_files가 삭제를 적용해
-- 새 파일을 만들었으므로, 이 delete들은 사라진 옛 데이터 파일을 가리키는 dangling delete다
SELECT content, COUNT(*) AS files, SUM(file_size_in_bytes) AS bytes
FROM glue.ecommerce_lakehouse.compaction_demo.files GROUP BY content ORDER BY content;
