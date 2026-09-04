-- Silver 데이터 파일 수·크기·128MB 미만 파일 수
SELECT table_name,
       COUNT(*) AS file_count,
       ROUND(AVG(file_size_in_bytes) / 1024 / 1024, 2) AS avg_file_mb,
       ROUND(MIN(file_size_in_bytes) / 1024 / 1024, 2) AS min_file_mb,
       ROUND(MAX(file_size_in_bytes) / 1024 / 1024, 2) AS max_file_mb,
       SUM(CASE WHEN file_size_in_bytes < 134217728 THEN 1 ELSE 0 END) AS small_file_count
FROM (
  SELECT 'silver_events' AS table_name, file_size_in_bytes
  FROM glue.ecommerce_lakehouse.silver_events.files WHERE content = 0
  UNION ALL
  SELECT 'silver_funnel', file_size_in_bytes
  FROM glue.ecommerce_lakehouse.silver_funnel.files WHERE content = 0
) f
GROUP BY table_name ORDER BY table_name;
