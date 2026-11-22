-- Tình huống 3 — Hành vi & chân dung khách hàng

-- 3.1 Phân khúc theo Frequency (số hoá đơn) x Monetary (tổng chi tiêu)
SELECT segment, count(*) AS customers, round(sum(total_spend), 2) AS total_spend,
       round(avg(total_orders), 2) AS avg_orders, round(avg(CAST(total_spend AS double)), 2) AS avg_spend
FROM delta.mart.customer_360 WHERE total_orders > 0
GROUP BY segment ORDER BY total_spend DESC;

-- 3.2 Khách quay lại (Repeat) vs mua một lần (One-time)
SELECT customer_type, count(*) AS customers,
       round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct_customers,
       round(sum(total_spend), 2) AS total_spend
FROM delta.mart.customer_360 WHERE customer_type <> 'No purchase'
GROUP BY customer_type;

-- 3.3 AOV và basket size (toàn kỳ và theo tháng)
SELECT round(sum(revenue) / sum(total_orders), 2) AS aov,
       round(CAST(sum(total_lines) AS double) / sum(total_orders), 3) AS avg_lines_per_basket,
       round(CAST(sum(total_units) AS double) / sum(total_orders), 3) AS avg_units_per_basket
FROM delta.mart.sales_monthly;

SELECT year_month, aov, round(basket_size_lines, 3) AS basket_lines, round(basket_size_units, 3) AS basket_units
FROM delta.mart.sales_monthly ORDER BY year_month;
