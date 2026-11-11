-- Parsewell AI · Usage, cost and margin dashboard
-- Paste each query into a Databricks AI/BI dashboard as its own dataset.
-- Pick your workspace's catalog in the editor. The queries use the `parsewell` schema.


-- 1. Company scorecard by month: revenue, model cost, gross margin
SELECT
  month,
  ROUND(SUM(revenue_usd), 0)                                            AS revenue_usd,
  ROUND(SUM(inference_cost_usd), 0)                                     AS inference_cost_usd,
  ROUND(SUM(gross_margin_usd), 0)                                       AS gross_margin_usd,
  ROUND(SUM(gross_margin_usd) / NULLIF(SUM(revenue_usd), 0) * 100, 1)   AS gross_margin_pct
FROM parsewell.gold_customer_monthly_margin
GROUP BY month
ORDER BY month;


-- 2. Margin by plan by month
SELECT
  month,
  plan,
  ROUND(SUM(revenue_usd), 0)                                            AS revenue_usd,
  ROUND(SUM(inference_cost_usd), 0)                                     AS inference_cost_usd,
  ROUND(SUM(gross_margin_usd) / NULLIF(SUM(revenue_usd), 0) * 100, 1)   AS gross_margin_pct
FROM parsewell.gold_customer_monthly_margin
GROUP BY month, plan
ORDER BY month, plan;


-- 3. Model cost per 1,000 successful pages, by week (shows the 15 Aug price increase)
SELECT
  DATE_TRUNC('WEEK', event_date)                                        AS week,
  ROUND(SUM(inference_cost_usd) / NULLIF(SUM(billable_pages), 0) * 1000, 2) AS cost_per_1k_pages_usd,
  ROUND(SUM(large_model_pages) / NULLIF(SUM(billable_pages), 0) * 100, 1)   AS large_model_share_pct
FROM parsewell.gold_customer_daily_usage
GROUP BY 1
ORDER BY 1;


-- 4. Accounts that need action this week
SELECT
  company_name,
  plan,
  health_status,
  reason,
  pages_last_28d,
  usage_change_pct,
  error_rate_pct,
  last_month_margin_pct
FROM parsewell.gold_customer_health
WHERE health_status <> 'healthy'
ORDER BY CASE health_status
           WHEN 'at_risk' THEN 1
           WHEN 'margin_review' THEN 2
           ELSE 3
         END;


-- 5. Enterprise contracts ranked by last month's margin
SELECT
  company_name,
  billable_pages,
  large_model_share_pct,
  revenue_usd,
  inference_cost_usd,
  gross_margin_pct
FROM parsewell.gold_customer_monthly_margin
WHERE plan = 'enterprise'
  AND month = (SELECT MAX(month) FROM parsewell.gold_customer_monthly_margin)
ORDER BY gross_margin_pct;


-- 6. One account over time (change the name to explore)
SELECT
  month,
  billable_pages,
  large_model_share_pct,
  revenue_usd,
  inference_cost_usd,
  gross_margin_pct
FROM parsewell.gold_customer_monthly_margin
WHERE company_name = 'Helix Freight'
ORDER BY month;


-- 7. How much raw data was excluded, and why
SELECT category, rows, share_of_raw_pct
FROM parsewell.gold_data_quality
ORDER BY rows DESC;
