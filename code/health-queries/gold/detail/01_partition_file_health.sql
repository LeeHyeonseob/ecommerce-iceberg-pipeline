-- Gold 파티션별 data 파일 상태(COW이므로 position delete는 대상 아님)
-- __TABLE_FILTER__와 __PARTITION_LIMIT__는 health_check.py가 --detail-tables/--detail-partitions로 치환한다.
-- Gold는 파일 크기가 아니라 data_file_count가 1을 넘는지가 compaction 신호다.
SELECT table_name,
       partition_date,
       data_file_count,
       avg_data_file_mb
FROM (
  SELECT table_name,
         partition_date,
         COUNT(*) AS data_file_count,
         ROUND(AVG(file_size_in_bytes) / 1024 / 1024, 2) AS avg_data_file_mb,
         ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_date DESC) AS recency_rank
  FROM (
    SELECT 'gold_daily_gmv' AS table_name, partition.summary_date AS partition_date, file_size_in_bytes
    FROM glue.ecommerce_lakehouse.gold_daily_gmv.files WHERE content = 0
    UNION ALL
    SELECT 'gold_funnel_daily', partition.summary_date, file_size_in_bytes
    FROM glue.ecommerce_lakehouse.gold_funnel_daily.files WHERE content = 0
    UNION ALL
    SELECT 'gold_category_gmv', partition.summary_date, file_size_in_bytes
    FROM glue.ecommerce_lakehouse.gold_category_gmv.files WHERE content = 0
    UNION ALL
    SELECT 'gold_pipeline_sla', partition.summary_date, file_size_in_bytes
    FROM glue.ecommerce_lakehouse.gold_pipeline_sla.files WHERE content = 0
    UNION ALL
    SELECT 'gold_data_quality', partition.summary_date, file_size_in_bytes
    FROM glue.ecommerce_lakehouse.gold_data_quality.files WHERE content = 0
  ) f
  WHERE __TABLE_FILTER__
  GROUP BY table_name, partition_date
) ranked
WHERE recency_rank <= __PARTITION_LIMIT__
ORDER BY table_name, partition_date;
