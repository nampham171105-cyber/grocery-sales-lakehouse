-- Chạy:  docker exec -i postgres psql -U admin -d grocery < load_data/load_csv_to_db.sql
-- Yêu cầu: 7 file CSV đặt trong ./dataset (được mount vào /tmp/dataset của container postgres).
-- Thứ tự cột trong CSV phải khớp Data Dictionary (Mục 5 của đề bài).

COPY categories FROM '/tmp/dataset/categories.csv' DELIMITER ',' CSV HEADER NULL 'NULL';
COPY countries  FROM '/tmp/dataset/countries.csv'  DELIMITER ',' CSV HEADER NULL 'NULL';
COPY cities     FROM '/tmp/dataset/cities.csv'     DELIMITER ',' CSV HEADER NULL 'NULL';
COPY customers  FROM '/tmp/dataset/customers.csv'  DELIMITER ',' CSV HEADER NULL 'NULL';
COPY employees  FROM '/tmp/dataset/employees.csv'  DELIMITER ',' CSV HEADER NULL 'NULL';
COPY products   FROM '/tmp/dataset/products.csv'   DELIMITER ',' CSV HEADER NULL 'NULL';
COPY sales      FROM '/tmp/dataset/sales.csv'      DELIMITER ',' CSV HEADER NULL 'NULL';

SELECT 'categories' t, count(*) FROM categories UNION ALL
SELECT 'countries', count(*) FROM countries UNION ALL
SELECT 'cities', count(*) FROM cities UNION ALL
SELECT 'customers', count(*) FROM customers UNION ALL
SELECT 'employees', count(*) FROM employees UNION ALL
SELECT 'products', count(*) FROM products UNION ALL
SELECT 'sales', count(*) FROM sales;