-- Gold 데이터 파일 수·크기·128MB 미만 파일 수
SELECT table_name,
       COUNT(*) AS file_count,
       ROUND(AVG(file_size_in_bytes) / 1024 / 1024, 2) AS avg_file_mb,
       ROUND(MIN(file_size_in_bytes) / 1024 / 1024, 2) AS min_file_mb,
       ROUND(MAX(file_size_in_bytes) / 1024 / 1024, 2) AS max_file_mb,
       SUM(CASE WHEN file_size_in_bytes < 134217728 THEN 1 ELSE 0 END) AS small_file_count
FROM (
  SELECT 'gold_daily_gmv' AS table_name, file_size_in_bytes FROM glue.ecommerce_lakehouse.gold_daily_gmv.files WHERE content = 0
  UNION ALL SELECT 'gold_funnel_daily', file_size_in_bytes FROM glue.ecommerce_lakehouse.gold_funnel_daily.files WHERE content = 0
  UNION ALL SELECT 'gold_category_gmv', file_size_in_bytes FROM glue.ecommerce_lakehouse.gold_category_gmv.files WHERE content = 0
  UNION ALL SELECT 'gold_pipeline_sla', file_size_in_bytes FROM glue.ecommerce_lakehouse.gold_pipeline_sla.files WHERE content = 0
  UNION ALL SELECT 'gold_data_quality', file_size_in_bytes FROM glue.ecommerce_lakehouse.gold_data_quality.files WHERE content = 0
) f
GROUP BY table_name ORDER BY table_name;
