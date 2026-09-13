-- Silver 테이블별 data/position delete 파일 상태
-- Silver는 write.target-file-size-bytes=134217728이다. 128MB 미만 파일 수는 목표 크기 미달 현황이며,
-- 파티션 내 복수 파일 누적과 함께 판단한다(파티션별 현황은 detail/01_partition_file_health.sql).
SELECT table_name,
       SUM(CASE WHEN content = 0 THEN 1 ELSE 0 END) AS data_file_count,
       ROUND(AVG(CASE WHEN content = 0 THEN file_size_in_bytes END) / 1024 / 1024, 2)
         AS avg_data_file_mb,
       ROUND(MIN(CASE WHEN content = 0 THEN file_size_in_bytes END) / 1024 / 1024, 2)
         AS min_data_file_mb,
       ROUND(MAX(CASE WHEN content = 0 THEN file_size_in_bytes END) / 1024 / 1024, 2)
         AS max_data_file_mb,
       SUM(CASE WHEN content = 0 AND file_size_in_bytes < 134217728 THEN 1 ELSE 0 END)
         AS small_data_file_count,
       SUM(CASE WHEN content = 1 THEN 1 ELSE 0 END) AS position_delete_file_count,
       SUM(CASE WHEN content = 1 THEN record_count ELSE 0 END)
         AS position_delete_record_count
FROM (
  SELECT 'silver_events' AS table_name, content, file_size_in_bytes, record_count
  FROM glue.ecommerce_lakehouse.silver_events.files
  UNION ALL
  SELECT 'silver_funnel', content, file_size_in_bytes, record_count
  FROM glue.ecommerce_lakehouse.silver_funnel.files
) f
GROUP BY table_name ORDER BY table_name;
