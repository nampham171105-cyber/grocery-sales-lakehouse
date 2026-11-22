# Grocery Sales Data Lakehouse — Tích hợp Data Quality Framework

Xây dựng data lakehouse kiến trúc medallion (**Bronze → Silver → Gold → Mart**) trên **Delta Lake**, điều phối bằng **Apache Airflow**, truy vấn qua **Trino**, trực quan hoá bằng **Superset**, để giải 5 tình huống phân tích (14 nhiệm vụ) của đề bài *Phân tích dữ liệu bán hàng siêu thị* trên bộ dữ liệu **Grocery Sales Database** (7 bảng, snapshot 4 tháng).

Điểm nhấn của dự án không chỉ là "extract – transform – load" mà là câu hỏi: **làm sao biết con số trên dashboard là đúng?** Mỗi layer đều ghi lại bằng chứng: cái gì đã kiểm tra, cái gì pass, cái gì bị loại và vì sao.

## Kiến trúc

```
PostgreSQL (7 bảng CSV)
      │  JDBC  (sales: incremental theo sales_date, song song theo sales_id; 6 bảng còn lại: full snapshot)
      ▼
  ┌─────────┐   schema + min row-count
  │ BRONZE  │──────────────────────────────┐
  └────┬────┘                              │
       │ trim, na.drop, dedupe, business rule
       ▼                                   │
  ┌─────────┐   quarantine dòng bị loại    │    control schema (Delta)
  │ SILVER  │──────────────────────────────┤    ├── job_control     (watermark)
  └────┬────┘                              │    ├── quality_checks  (catalog rule)
       │ star schema + surrogate key       ├───▶├── quality_metrics (kết quả từng check)
       ▼                                   │    ├── failed_records  (dòng bị quarantine)
  ┌─────────┐   grain, orphan FK,          │    └── audit_log       (trạng thái mỗi run)
  │  GOLD   │   total_price vs công thức   │
  └────┬────┘──────────────────────────────┤
       │ tổng hợp theo tình huống          │
       ▼                                   │
  ┌─────────┐   grain + đối soát với Gold  │
  │  MART   │──────────────────────────────┘
  └─────────┘
       │
       ▼
   Trino  ──▶  Superset
```

Toàn bộ dữ liệu lưu bằng Delta Lake trên MinIO, quản lý bởi Hive Metastore, truy vấn được từ cả Spark lẫn Trino.

## Mô hình dữ liệu

**Gold — star schema**

| Bảng | Grain | Ghi chú |
|---|---|---|
| `fact_sales` | 1 dòng hàng của hoá đơn (`sales_id`) | `net_amount = quantity × price × (1 − discount)` **tự tính**, giữ `source_total_price` để đối chiếu |
| `dim_product` | sản phẩm | gộp `category_name`, `class`, `resistant`, `is_allergic`… |
| `dim_customer`, `dim_employee` | khách hàng / nhân viên | trỏ tới `dim_geography` |
| `dim_geography` | thành phố | gộp `cities` + `countries` |
| `dim_category`, `dim_date` | danh mục / ngày | `dim_date` sinh từ chuỗi ngày 2017-2019 |

Surrogate key là hash tất định (`sha2`) của natural key nên ổn định giữa các lần chạy. Dimension dùng **SCD1 (ghi đè snapshot)** — xem mục *Hạn chế*.

**Mart — ánh xạ với đề bài**

| Mart | Nhiệm vụ | Query mẫu |
|---|---|---|
| `sales_monthly` | 1.1, 3.3 (doanh thu, tăng trưởng MoM, AOV, basket size) | `analysis/01`, `03` |
| `category_monthly` | 1.2 (tỷ trọng & biến động danh mục theo tháng) | `analysis/01` |
| `product_performance` | 2.1, 2.2 (rank, top/bottom 5, "nhu cầu cao – giá trị thấp") | `analysis/02` |
| `class_performance` | 2.3 (ảnh hưởng của `Class`) | `analysis/02` |
| `customer_360` | 3.1, 3.2 (phân khúc Frequency × Monetary, Repeat vs One-time) | `analysis/03` |
| `employee_performance` | 4.1, 4.2 (doanh số, hoá đơn, top/under performer) | `analysis/04` |
| `employee_monthly` | 4.3 (xu hướng + độ ổn định `cv_pct`) | `analysis/04` |
| `geo_performance` | 5.1, 5.2, 5.3 (thành phố/quốc gia, chênh lệch sản lượng, thâm nhập) | `analysis/05` |

## Framework kiểm soát chất lượng dữ liệu

Mỗi bước validate đều: **(1)** tính ra một metric cụ thể, **(2)** ghi vào `control.quality_metrics` dù pass hay fail, **(3)** phân loại `BLOCKING` (raise, dừng pipeline) hay `WARNING` (chỉ log), **(4)** quarantine dòng vi phạm vào `control.failed_records`, **(5)** ghi `audit_log` cho từng run (`PASS` / `PARTIAL_FAIL` / `FAIL`).

| Layer | Check | Dimension | Severity |
|---|---|---|---|
| Bronze | schema (thiếu cột, sai kiểu) | validity | BLOCKING |
| Bronze | số dòng tối thiểu | completeness | WARNING |
| Silver | % dòng mất khi làm sạch (tách *bị loại* vs *trùng khoá*) | completeness | BLOCKING (>10%) |
| Silver | `quantity > 0`, `discount ∈ [0,1]`, `price > 0` | validity | BLOCKING — dòng vi phạm bị loại + quarantine |
| Silver | `hire_date > birth_date`, `vitality_days ≥ 0` | validity | WARNING |
| Gold | grain của từng dim/fact | integrity | BLOCKING |
| Gold | khoá ngoại mồ côi (`customer`, `product`, `employee`, `country`) | integrity | WARNING (>5%) |
| Gold | **`total_price` nguồn vs `quantity × price × (1 − discount)`** | accuracy | WARNING |
| Mart | grain + **đối soát tổng doanh thu với `fact_sales`** | consistency / accuracy | BLOCKING |

Check `total_price` tồn tại vì đề bài nêu rõ cột này *"cần kiểm tra tính chính xác bằng công thức tính trực tiếp"*; toàn bộ mart dùng `net_amount` tự tính, không dùng `total_price`.

## Quyết định thiết kế đáng nói

- **Incremental chỉ ở nơi có ý nghĩa.** Chỉ `sales` có cột thời gian tăng dần (`sales_date`) nên dùng high-watermark (`control.job_control`). 6 bảng master/tham chiếu nhỏ hoặc không có cột thời gian (`customers`, `cities`…) nạp lại full mỗi lần. Từ Silver trở lên watermark dựa vào `ingest_at` do pipeline sinh ra.
- **Đọc JDBC song song** cho `sales` bằng `partitionColumn=sales_id` (4 partition) vì bảng này có hàng triệu dòng.
- **Quarantine thật sự:** dòng vi phạm rule BLOCKING bị loại khỏi Silver *và* được ghi lại, thay vì chỉ ghi nhận rồi để lọt xuống.
- **Không có giá vốn trong dữ liệu**, nên "biên lợi nhuận thấp" ở nhiệm vụ 2.2 được xấp xỉ bằng `revenue_per_unit` và `avg_discount_pct`.

## Cách chạy

```bash
# 1. Đặt 7 file CSV vào ./dataset  (categories, cities, countries, customers, employees, products, sales)
docker compose up -d --build

# 2. Tạo bảng nguồn tự động ở lần khởi động đầu; nạp CSV:
docker exec -i postgres psql -U admin -d grocery < load_data/load_csv_to_db.sql

# 3. Airflow UI: localhost:8090 (admin / admin123) -> trigger DAG `lakehouse_pipeline`
# 4. Trino: localhost:8085   |   Superset: localhost:8088 (admin / admin123)
```

Kết nối Superset → Trino: `trino://trino@trino:8080/delta`. Chạy các file trong `analysis/` ở SQL Lab hoặc tạo dataset từ `delta.mart.*` để dựng dashboard.

## Cấu trúc project

```
docker-compose.yml, docker/, conf/, hive-conf/, trino-conf/   # hạ tầng (Spark, MinIO, HMS, Trino, Airflow, Superset)
airflow/dags/lakehouse_pipeline.py   # 7 bronze → 7 silver → 7 gold → 8 mart (song song theo bảng)
spark/jobs/        ingest_bronze.py | transform_silver.py | build_gold.py | build_mart.py
spark/utils/       job_control.py (watermark + metrics/audit/quarantine) | validation.py
load_data/         init_postgres.sql | load_csv_to_db.sql | init_lakehouse.sh | init-superset.sh
analysis/          SQL trả lời 14 nhiệm vụ + truy vấn bằng chứng data quality
dataset/           đặt CSV tại đây (đã .gitignore)
```

## Đã kiểm thử gì, chưa kiểm thử gì

Logic Silver → Gold → Mart đã được chạy thử bằng PySpark cục bộ trên dữ liệu **tổng hợp** có cài lỗi cố ý (ngày null, trùng `sales_id`, khách hàng mồ côi, `total_price = 0`): quarantine, check orphan, check số tiền và đối soát 8 mart đều hoạt động đúng (chênh lệch 0.0000).

**Chưa** được chạy: toàn bộ stack Docker, đọc JDBC từ Postgres, nhánh `MERGE` của Delta (lần chạy thứ hai trở đi), Trino, Superset, và dữ liệu Grocery Sales thật. Vài điểm cần để ý khi chạy thật:

- Nếu `sales_date` null chiếm >10% `sales`, check `row_loss_pct` sẽ chặn pipeline — hãy xem `analysis/00_data_quality.sql` rồi chỉnh `max_loss_pct` trong `transform_silver.py` cho phù hợp.
- Thứ tự cột CSV phải khớp Data Dictionary (Mục 5 đề bài) vì `COPY` map theo vị trí.
- `docker/Dockerfile.superset` giữ nguyên từ khung gốc (đường dẫn `python3.10` của venv) — có thể cần chỉnh theo phiên bản image Superset bạn kéo về.

## Hạn chế & hướng cải thiện

- Dimension là SCD1; chưa có SCD2 (ví dụ lịch sử giá sản phẩm) vì snapshot chỉ có 4 tháng và `fact_sales` chốt giá tại thời điểm build.
- Bản ghi bị xoá ở nguồn (bảng full-load) không bị xoá ở Silver.
- Rule còn hard-code trong `BRONZE_RULES` / `SILVER_CONFIG`; bước tiếp theo là đọc động từ `control.quality_checks`.
- Cảnh báo lỗi qua Airflow (Slack webhook) và tối ưu hoá pipeline.
