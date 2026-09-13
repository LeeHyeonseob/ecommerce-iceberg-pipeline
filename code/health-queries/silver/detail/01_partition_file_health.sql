-- Silver 파티션별 data/position delete 파일 상태
-- __TABLE_FILTER__와 __PARTITION_LIMIT__는 health_check.py가 --detail-tables/--detail-partitions로 치환한다.
-- 전 파티션을 훑으면 metadata 스캔이 커지므로 테이블별 최근 파티션으로 제한한다.
SELECT table_name,
       partition_date,
       data_file_count,
       avg_data_file_mb,
       small_data_file_count,
       position_delete_file_count,
       position_delete_record_count
FROM (
  SELECT table_name,
         partition_date,
         SUM(CASE WHEN content = 0 THEN 1 ELSE 0 END) AS data_file_count,
         ROUND(AVG(CASE WHEN content = 0 THEN file_size_in_bytes END) / 1024 / 1024, 2)
           AS avg_data_file_mb,
         SUM(CASE WHEN content = 0 AND file_size_in_bytes < 134217728 THEN 1 ELSE 0 END)
           AS small_data_file_count,
         SUM(CASE WHEN content = 1 THEN 1 ELSE 0 END) AS position_delete_file_count,
         SUM(CASE WHEN content = 1 THEN record_count ELSE 0 END) AS position_delete_record_count,
         ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_date DESC) AS recency_rank
  FROM (
    SELECT 'silver_events' AS table_name,
           partition.event_date AS partition_date,
           content, file_size_in_bytes, record_count
    FROM glue.ecommerce_lakehouse.silver_events.files
    UNION ALL
    SELECT 'silver_funnel' AS table_name,
           partition.funnel_date AS partition_date,
           content, file_size_in_bytes, record_count
    FROM glue.ecommerce_lakehouse.silver_funnel.files
  ) f
  WHERE __TABLE_FILTER__
  GROUP BY table_name, partition_date
) ranked
WHERE recency_rank <= __PARTITION_LIMIT__
ORDER BY table_name, partition_date;
