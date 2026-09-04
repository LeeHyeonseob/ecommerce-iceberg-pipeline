-- 테이블별 현재 매니페스트 파일 수
SELECT table_name, COUNT(*) AS manifest_count
FROM (
  SELECT 'silver_events' AS table_name FROM glue.ecommerce_lakehouse.silver_events.manifests
  UNION ALL SELECT 'silver_funnel' FROM glue.ecommerce_lakehouse.silver_funnel.manifests
  UNION ALL SELECT 'gold_daily_gmv' FROM glue.ecommerce_lakehouse.gold_daily_gmv.manifests
  UNION ALL SELECT 'gold_funnel_daily' FROM glue.ecommerce_lakehouse.gold_funnel_daily.manifests
  UNION ALL SELECT 'gold_category_gmv' FROM glue.ecommerce_lakehouse.gold_category_gmv.manifests
  UNION ALL SELECT 'gold_pipeline_sla' FROM glue.ecommerce_lakehouse.gold_pipeline_sla.manifests
  UNION ALL SELECT 'gold_data_quality' FROM glue.ecommerce_lakehouse.gold_data_quality.manifests
) m
GROUP BY table_name ORDER BY table_name;
