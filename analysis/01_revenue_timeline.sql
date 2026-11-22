-- Tình huống 1 — Hiệu suất doanh thu theo dòng thời gian  (chạy trên Trino / Superset SQL Lab)

-- 1.1 Tổng doanh thu thực tế (sau chiết khấu, tính từ quantity * price * (1 - discount))
SELECT sum(revenue) AS total_net_revenue, sum(gross_revenue) AS total_gross_revenue,
       sum(gross_revenue) - sum(revenue) AS total_discount
FROM delta.mart.sales_monthly;

-- 1.1b Doanh thu và tăng trưởng theo tháng
SELECT year_month, revenue, mom_growth_pct FROM delta.mart.sales_monthly ORDER BY year_month;

-- 1.2 Tỷ trọng đóng góp và biến động của từng danh mục theo tháng
SELECT year_month, category_name, revenue, round(share_pct, 2) AS share_pct, round(mom_growth_pct, 2) AS mom_growth_pct
FROM delta.mart.category_monthly
ORDER BY year_month, revenue DESC;
