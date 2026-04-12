-- Metabase native question export
-- card_id: 106
-- card_name: Периодические комиссии
-- query_type: native
-- display: table
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - dashboard `PURR Analytics Marketplace` (id=4), tab `Ozon`

SELECT
  cost_date,
  fee_group,
  fee_name,
  amount
FROM finance_period_costs
WHERE {{cost_date}} 
  AND amount <> 0
ORDER BY cost_date DESC, fee_group, fee_name;
