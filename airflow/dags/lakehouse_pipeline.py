from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_CONN_ID = "spark_default"
SPARK_JOBS_DIR = "/opt/spark/jobs"
SPARK_UTILS_DIR = "/opt/spark/utils"

BRONZE_PRIORITY, SILVER_PRIORITY, GOLD_PRIORITY, MART_PRIORITY = 400, 300, 200, 100
BRONZE_SILVER_TIMEOUT_MIN = 30   # sales có hàng triệu dòng -> nới timeout so với bản gốc
GOLD_MART_TIMEOUT_MIN = 40

default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


HEAVY_TABLES = {"sales", "fact_sales"}


def make_spark_task(task_id, script, table, priority_weight, timeout_minutes=BRONZE_SILVER_TIMEOUT_MIN):
    is_heavy = table in HEAVY_TABLES
    return SparkSubmitOperator(
        task_id=task_id,
        application=f"{SPARK_JOBS_DIR}/{script}",
        conn_id=SPARK_CONN_ID,
        priority_weight=priority_weight,
        weight_rule="absolute",
        execution_timeout=timedelta(minutes=timeout_minutes),
        conf={
            "spark.cores.max": "2" if is_heavy else "1",
            "spark.executor.memory": "1800m" if is_heavy else "1g",
            "spark.sql.shuffle.partitions": "8" if is_heavy else "4",
            "spark.driver.host": "airflow-scheduler",  # chỉ đường cho worker gọi về driver
            "spark.driver.bindAddress": "0.0.0.0",
        },
        py_files=f"{SPARK_UTILS_DIR}/job_control.py,{SPARK_UTILS_DIR}/validation.py",
        env_vars={"PYTHONPATH": "/opt/spark"},
        application_args=["--table", table],
        verbose=False,
    )


with DAG(
    dag_id="lakehouse_pipeline",
    description="Grocery Sales: Bronze -> Silver -> Gold (star schema) -> Mart, mỗi bảng 1 task",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,   # 2 worker x 2 core = 4 core khả dụng
    tags=["lakehouse", "spark", "grocery"],
) as dag:

    TABLES = ["categories", "countries", "cities", "customers", "employees", "products", "sales"]

    # ---------------- BRONZE ----------------
    with TaskGroup(group_id="bronze"):
        bronze = {t: make_spark_task(f"bronze_{t}", "ingest_bronze.py", t, BRONZE_PRIORITY) for t in TABLES}

    # ---------------- SILVER ----------------
    with TaskGroup(group_id="silver"):
        silver = {t: make_spark_task(f"silver_{t}", "transform_silver.py", t, SILVER_PRIORITY) for t in TABLES}

    # Ghép cặp theo từng bảng (KHÔNG dùng expand() để giữ tính song song theo bảng)
    for t in TABLES:
        bronze[t] >> silver[t]

    # ---------------- GOLD ----------------
    with TaskGroup(group_id="gold"):
        g = {
            name: make_spark_task(f"gold_{name}", "build_gold.py", name, GOLD_PRIORITY, GOLD_MART_TIMEOUT_MIN)
            for name in ["dim_date", "dim_category", "dim_geography", "dim_product",
                         "dim_customer", "dim_employee", "fact_sales"]
        }

    silver["categories"] >> g["dim_category"]
    [silver["cities"], silver["countries"]] >> g["dim_geography"]
    [silver["products"], g["dim_category"]] >> g["dim_product"]
    [silver["customers"], g["dim_geography"]] >> g["dim_customer"]
    [silver["employees"], g["dim_geography"]] >> g["dim_employee"]
    # dim_date không phụ thuộc Silver (sinh từ chuỗi ngày)
    [silver["sales"], g["dim_product"], g["dim_customer"], g["dim_employee"], g["dim_date"]] >> g["fact_sales"]

    # ---------------- MART ----------------
    with TaskGroup(group_id="mart"):
        marts = [
            make_spark_task(f"mart_{name}", "build_mart.py", name, MART_PRIORITY, GOLD_MART_TIMEOUT_MIN)
            for name in ["sales_monthly", "category_monthly", "product_performance", "class_performance",
                         "customer_360", "employee_performance", "employee_monthly", "geo_performance"]
        ]
    g["fact_sales"] >> marts
