-- Tình huống 5 — Doanh thu theo địa giới (theo thành phố của khách hàng)

-- 5.1 Quốc gia -> thành phố
SELECT country_name, sum(revenue) AS country_revenue, sum(total_units) AS units,
       sum(buying_customers) AS buying_customers
FROM delta.mart.geo_performance GROUP BY country_name ORDER BY country_revenue DESC;

SELECT global_revenue_rank, country_name, city_name, revenue, round(revenue_share_pct, 2) AS share_pct,
       rank_in_country, round(city_share_in_country_pct, 2) AS share_in_country_pct
FROM delta.mart.geo_performance ORDER BY global_revenue_rank LIMIT 20;

-- 5.2 Chênh lệch sản lượng giữa các vùng so với mức trung bình thành phố
SELECT city_name, country_name, total_units, round(units_vs_avg_pct, 2) AS units_vs_avg_pct
FROM delta.mart.geo_performance ORDER BY total_units DESC;

-- 5.3 Chỉ số thẩm định chiến lược vùng: doanh thu/khách, mức thâm nhập, AOV
SELECT city_name, country_name, registered_customers, buying_customers,
       round(penetration_pct, 2) AS penetration_pct, round(revenue_per_customer, 2) AS revenue_per_customer,
       round(avg_order_value, 2) AS aov
FROM delta.mart.geo_performance ORDER BY revenue_per_customer DESC;
