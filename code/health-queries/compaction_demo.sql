-- 6회차 과제: rewrite_data_files / rewrite_position_delete_files / rewrite_manifests
--
-- 실행: spark-runner의 spark-sql로 이 파일을 실행한다.
-- code/pipelines/spark_session.py와 동일한 Glue/Iceberg/S3A 설정을 함께 전달해야 하며,
-- 그 설정을 묶어주는 편의 스크립트는 커밋돼 있지 않다.
-- run_ddl.py는 code/ddl/만 대상으로 하므로 이 파일에는 쓸 수 없다.
--
-- 운영 테이블은 파티션당 파일 수가 적어 기본 옵션으로는 세 procedure 모두 0을 반환하기 쉽다.
-- (Silver는 MOR, Gold는 COW다. Gold에는 애초에 position delete가 생기지 않는다.)
-- 그래서 조각난 MOR 테이블을 따로 만들어 실행 결과가 드러나게 한다.
-- 데이터는 실제 프로젝트의 gold_category_gmv에서 가져온다.
--
-- 순서는 운영(iceberg_maintenance.py)과 동일하게 data files를 먼저 재작성한다.

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


-- ================= 2. rewrite_data_files =================
-- 먼저 돌린다. 데이터 파일을 합치면서 position delete를 실제로 적용하므로,
-- 이 시점의 기존 delete 파일들은 사라진 옛 데이터 파일을 가리키는 dangling delete가 된다.
SELECT '========== 2. rewrite_data_files ==========' AS section;

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


-- ================= 3. rewrite_position_delete_files =================
-- data files 재작성으로 생긴 dangling delete를 정리하는 단계다.
-- min-input-files 기본값은 5라서 명시하지 않으면 후보로 잡히고도 실제 재작성이 0건이 될 수 있다.
-- 정리 결과는 아래 5번의 content별 집계로 확인한다.
SELECT '========== 3. rewrite_position_delete_files ==========' AS section;

CALL glue.system.rewrite_position_delete_files(
    table   => 'ecommerce_lakehouse.compaction_demo',
    options => map('min-input-files', '2')
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

-- 2번에서 데이터 파일이 몇 개로 합쳐졌는지, 3번이 dangling delete(content=1)를 실제로
-- 줄였는지를 1번의 같은 집계와 비교한다. 각 procedure의 반환값(재작성 파일 수·바이트)도 함께 본다.
SELECT content, COUNT(*) AS files, SUM(file_size_in_bytes) AS bytes
FROM glue.ecommerce_lakehouse.compaction_demo.files GROUP BY content ORDER BY content;
