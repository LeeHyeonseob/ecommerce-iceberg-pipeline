-- Gold 데이터 최신 집계 날짜
SELECT 'gold_daily_gmv' AS table_name, MAX(summary_date) AS latest_date FROM glue.ecommerce_lakehouse.gold_daily_gmv
UNION ALL SELECT 'gold_funnel_daily', MAX(summary_date) FROM glue.ecommerce_lakehouse.gold_funnel_daily
UNION ALL SELECT 'gold_category_gmv', MAX(summary_date) FROM glue.ecommerce_lakehouse.gold_category_gmv
UNION ALL SELECT 'gold_pipeline_sla', MAX(summary_date) FROM glue.ecommerce_lakehouse.gold_pipeline_sla
UNION ALL SELECT 'gold_data_quality', MAX(summary_date) FROM glue.ecommerce_lakehouse.gold_data_quality
ORDER BY table_name;
