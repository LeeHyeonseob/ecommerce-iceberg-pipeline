-- Silver 데이터 최신 파티션
SELECT 'silver_events' AS table_name, MAX(event_date) AS latest_date
FROM glue.ecommerce_lakehouse.silver_events
UNION ALL
SELECT 'silver_funnel', MAX(funnel_date)
FROM glue.ecommerce_lakehouse.silver_funnel
ORDER BY table_name;
