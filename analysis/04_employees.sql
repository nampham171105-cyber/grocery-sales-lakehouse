-- Tình huống 4 — Năng lực thúc đẩy doanh số của nhân viên

-- 4.1 Doanh số và số hoá đơn của từng nhân viên
SELECT revenue_rank, employee_id, full_name, revenue, total_orders, total_units,
       round(avg_order_value, 2) AS avg_order_value, round(revenue_share_pct, 2) AS revenue_share_pct
FROM delta.mart.employee_performance ORDER BY revenue_rank;

-- 4.2 Top performers (percentile >= 80%) và Underperformers (<= 20%)
SELECT performance_tier, employee_id, full_name, revenue, round(revenue_vs_avg_pct, 2) AS vs_team_avg_pct
FROM delta.mart.employee_performance
WHERE performance_tier <> 'Average' ORDER BY performance_tier DESC, revenue DESC;

-- 4.3 Xu hướng theo tháng và độ ổn định (cv_pct càng thấp càng ổn định)
SELECT employee_id, full_name, year_month, revenue, round(mom_growth_pct, 2) AS mom_growth_pct
FROM delta.mart.employee_monthly ORDER BY employee_id, year_month;

SELECT DISTINCT employee_id, full_name, round(avg_monthly_revenue, 2) AS avg_monthly_revenue, round(cv_pct, 2) AS cv_pct
FROM delta.mart.employee_monthly ORDER BY cv_pct;
