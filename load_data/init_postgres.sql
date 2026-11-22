-- Nguồn OLTP: Grocery Sales Database (7 bảng).
-- Tên cột đổi sang snake_case (Postgres không phân biệt hoa/thường khi không quote).
-- CỐ Ý không khai báo FOREIGN KEY: dữ liệu nguồn có thể chứa khoá ngoại mồ côi, và việc
-- phát hiện chúng là nhiệm vụ của lớp Data Quality (Gold), không phải của lúc import.

CREATE TABLE categories (
    category_id    int primary key,
    category_name  varchar(45)
);

CREATE TABLE countries (
    country_id     int primary key,
    country_name   varchar(45),
    country_code   varchar(2)
);

CREATE TABLE cities (
    city_id        int primary key,
    city_name      varchar(45),
    zipcode        decimal(5,0),
    country_id     int
);

CREATE TABLE customers (
    customer_id     int primary key,
    first_name      varchar(45),
    middle_initial  varchar(1),
    last_name       varchar(45),
    city_id         int,
    address         varchar(120)
);

CREATE TABLE employees (
    employee_id     int primary key,
    first_name      varchar(45),
    middle_initial  varchar(1),
    last_name       varchar(45),
    birth_date      timestamp,
    gender          varchar(10),
    city_id         int,
    hire_date       timestamp
);

CREATE TABLE products (
    product_id     int primary key,
    product_name   varchar(100),
    price          decimal(10,4),   -- từ điển ghi DECIMAL(4,0) sẽ cắt phần thập phân, nên mở rộng
    category_id    int,
    class          varchar(15),
    modify_date    timestamp,
    resistant      varchar(15),
    is_allergic    varchar(10),
    vitality_days  decimal(3,0)
);

CREATE TABLE sales (
    sales_id            int primary key,
    sales_person_id     int,
    customer_id         int,
    product_id          int,
    quantity            int,
    discount            decimal(10,2),
    total_price         decimal(10,2),
    sales_date          timestamp,
    transaction_number  varchar(25)
);

CREATE INDEX idx_sales_date ON sales (sales_date);
