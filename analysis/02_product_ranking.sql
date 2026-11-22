-- Tình huống 2 — Phân loại sản phẩm

-- 2.1 Top 5 bán chạy nhất / 5 kém hiệu quả nhất theo doanh thu 4 tháng
SELECT 'TOP 5' AS grp, revenue_rank AS rnk, product_name, category_name, units_sold, revenue
FROM delta.mart.product_performance WHERE is_top5
UNION ALL
SELECT 'BOTTOM 5', bottom_rank, product_name, category_name, units_sold, revenue
FROM delta.mart.product_performance WHERE is_bottom5
ORDER BY 1 DESC, 2;

-- 2.2 Tương quan số lượng - doanh thu và nhóm "nhu cầu cao nhưng giá trị thấp"
-- (Dữ liệu không có giá vốn nên dùng revenue_per_unit + mức chiết khấu làm đại diện cho biên lợi nhuận)
SELECT corr(CAST(units_sold AS double), CAST(revenue AS double)) AS corr_units_revenue
FROM delta.mart.product_performance;

SELECT product_name, category_name, units_sold, revenue, round(revenue_per_unit, 2) AS revenue_per_unit,
       round(avg_discount_pct, 2) AS avg_discount_pct, rank_gap
FROM delta.mart.product_performance
WHERE demand_value_segment = 'High demand / Low value'
ORDER BY units_sold DESC;

SELECT demand_value_segment, count(*) AS products, sum(revenue) AS revenue
FROM delta.mart.product_performance GROUP BY 1 ORDER BY revenue DESC;

-- 2.3 Ảnh hưởng của Class tới tiêu thụ
SELECT class, total_products, total_units, revenue, round(revenue_share_pct, 2) AS revenue_share_pct,
       round(units_share_pct, 2) AS units_share_pct, round(revenue_per_unit, 2) AS revenue_per_unit,
       round(revenue_per_product, 2) AS revenue_per_product
FROM delta.mart.class_performance ORDER BY revenue DESC;
