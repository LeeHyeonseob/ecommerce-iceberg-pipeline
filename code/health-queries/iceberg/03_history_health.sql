-- 테이블별 HEAD 전환 시각과 현재 계보에 속하지 않는 스냅샷 수
-- (우리 파이프라인은 branch/tag/cherry-pick을 쓰지 않으므로, 0이 아니면 사실상 롤백 신호)
SELECT table_name,
       MAX(made_current_at) AS latest_head_change_at,
       SUM(CASE WHEN NOT is_current_ancestor THEN 1 ELSE 0 END) AS non_ancestor_snapshot_count
FROM (
  SELECT 'silver_events' AS table_name, made_current_at, is_current_ancestor FROM glue.ecommerce_lakehouse.silver_events.history
  UNION ALL SELECT 'silver_funnel', made_current_at, is_current_ancestor FROM glue.ecommerce_lakehouse.silver_funnel.history
  UNION ALL SELECT 'gold_daily_gmv', made_current_at, is_current_ancestor FROM glue.ecommerce_lakehouse.gold_daily_gmv.history
  UNION ALL SELECT 'gold_funnel_daily', made_current_at, is_current_ancestor FROM glue.ecommerce_lakehouse.gold_funnel_daily.history
  UNION ALL SELECT 'gold_category_gmv', made_current_at, is_current_ancestor FROM glue.ecommerce_lakehouse.gold_category_gmv.history
  UNION ALL SELECT 'gold_pipeline_sla', made_current_at, is_current_ancestor FROM glue.ecommerce_lakehouse.gold_pipeline_sla.history
  UNION ALL SELECT 'gold_data_quality', made_current_at, is_current_ancestor FROM glue.ecommerce_lakehouse.gold_data_quality.history
) h
GROUP BY table_name ORDER BY table_name;
