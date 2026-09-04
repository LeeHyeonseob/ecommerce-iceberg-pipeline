-- 테이블별 최신 Iceberg 스냅샷 시각과 누적 개수
SELECT table_name,
       MAX(committed_at) AS latest_snapshot_at,
       COUNT(*) AS snapshot_count
FROM (
  SELECT 'silver_events' AS table_name, committed_at FROM glue.ecommerce_lakehouse.silver_events.snapshots
  UNION ALL SELECT 'silver_funnel', committed_at FROM glue.ecommerce_lakehouse.silver_funnel.snapshots
  UNION ALL SELECT 'gold_daily_gmv', committed_at FROM glue.ecommerce_lakehouse.gold_daily_gmv.snapshots
  UNION ALL SELECT 'gold_funnel_daily', committed_at FROM glue.ecommerce_lakehouse.gold_funnel_daily.snapshots
  UNION ALL SELECT 'gold_category_gmv', committed_at FROM glue.ecommerce_lakehouse.gold_category_gmv.snapshots
  UNION ALL SELECT 'gold_pipeline_sla', committed_at FROM glue.ecommerce_lakehouse.gold_pipeline_sla.snapshots
  UNION ALL SELECT 'gold_data_quality', committed_at FROM glue.ecommerce_lakehouse.gold_data_quality.snapshots
) s
GROUP BY table_name ORDER BY table_name;
