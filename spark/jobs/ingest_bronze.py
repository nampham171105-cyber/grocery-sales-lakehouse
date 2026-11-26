"""Bronze: nạp nguyên trạng từ PostgreSQL vào Delta (+ ingest_at, source), kiểm tra schema/row-count."""
import argparse
import os
from datetime import datetime, date

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, max as spark_max

from validation import validate_schema, validate_minimum_row_count
from job_control import generate_run_id, get_watermark, insert_log, insert_audit

JDBC_URL = os.getenv("SOURCE_JDBC_URL", "jdbc:postgresql://postgres:5432/grocery")
JDBC_USER = os.getenv("SOURCE_DB_USER", "admin")
JDBC_PASSWORD = os.getenv("SOURCE_DB_PASSWORD", "admin123")

# mode:
#   incremental -> chỉ bảng có cột thời gian tăng dần đáng tin cậy (sales.sales_date), dùng high-watermark
#   full        -> bảng master/tham chiếu nhỏ hoặc không có cột thời gian: nạp lại toàn bộ (overwrite) mỗi lần
BRONZE_RULES = {
    "categories": {
        "expected_columns": {"category_id": "int", "category_name": "string"},
        "mode": "full", "min_rows": 1,
    },
    "countries": {
        "expected_columns": {"country_id": "int", "country_name": "string", "country_code": "string"},
        "mode": "full", "min_rows": 1,
    },
    "cities": {
        "expected_columns": {"city_id": "int", "city_name": "string", "zipcode": "decimal(5,0)", "country_id": "int"},
        "mode": "full", "min_rows": 1,
    },
    "customers": {
        "expected_columns": {
            "customer_id": "int", "first_name": "string", "middle_initial": "string",
            "last_name": "string", "city_id": "int", "address": "string",
        },
        "mode": "full", "min_rows": 1, "partition_column": "customer_id",
    },
    "employees": {
        "expected_columns": {
            "employee_id": "int", "first_name": "string", "middle_initial": "string", "last_name": "string",
            "birth_date": "timestamp", "gender": "string", "city_id": "int", "hire_date": "timestamp",
        },
        "mode": "full", "min_rows": 1,
    },
    "products": {
        "expected_columns": {
            "product_id": "int", "product_name": "string", "price": "decimal(10,4)", "category_id": "int",
            "class": "string", "modify_date": "timestamp", "resistant": "string",
            "is_allergic": "string", "vitality_days": "decimal(3,0)",
        },
        "mode": "full", "min_rows": 1,
    },
    "sales": {
        "expected_columns": {
            "sales_id": "int", "sales_person_id": "int", "customer_id": "int", "product_id": "int",
            "quantity": "int", "discount": "decimal(10,2)", "total_price": "decimal(10,2)",
            "sales_date": "timestamp", "transaction_number": "string",
        },
        "mode": "incremental", "min_rows": 1,
        "watermark_column": "sales_date", "partition_column": "sales_id",
    },
}
NUM_PARTITIONS = 4


def create_spark_session():
    return SparkSession.builder.appName("IngestBronze").enableHiveSupport().getOrCreate()


def _base_reader(spark):
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL).option("user", JDBC_USER)
        .option("password", JDBC_PASSWORD).option("driver", "org.postgresql.Driver")
        .option("fetchsize", "10000")
    )


def _read_from_postgres(spark, table_name, watermark_column, last_watermark, partition_column):
    if last_watermark is None:
        print("[Load] FULL LOAD")
        dbtable = table_name
    else:
        print(f"[Load] Incremental load: {watermark_column} > {last_watermark}")
        dbtable = f"(SELECT * FROM {table_name} WHERE {watermark_column} > TIMESTAMP '{last_watermark}') AS source"

    reader = _base_reader(spark).option("dbtable", dbtable)

    # Đọc song song theo khoảng giá trị của khoá số nguyên (nhanh hơn nhiều cho sales ~ hàng triệu dòng)
    if partition_column:
        bounds = (
            _base_reader(spark)
            .option("dbtable", f"(SELECT MIN({partition_column}) AS lo, MAX({partition_column}) AS hi FROM {dbtable}) AS b")
            .load().first()
        )
        if bounds["lo"] is not None and bounds["hi"] is not None and bounds["lo"] != bounds["hi"]:
            reader = (
                reader.option("partitionColumn", partition_column)
                .option("lowerBound", int(bounds["lo"])).option("upperBound", int(bounds["hi"]))
                .option("numPartitions", NUM_PARTITIONS)
            )
    return reader.load()


def ingest_table(spark, table_name):
    layer = "bronze"
    run_id = generate_run_id(layer, table_name)
    started_at = datetime.now()
    rundate = str(date.today())

    bronze_table = f"bronze.bronze_{table_name}"
    rules = BRONZE_RULES[table_name]
    incremental = rules["mode"] == "incremental"
    watermark_column = rules.get("watermark_column")

    input_rows = output_rows = 0
    df = None

    try:
        last_watermark = get_watermark(spark, layer, table_name) if incremental else None
        print(f"[Watermark] Previous: {last_watermark} (mode={rules['mode']})")

        df = _read_from_postgres(spark, table_name, watermark_column, last_watermark, rules.get("partition_column"))

        # ---- Data Quality: Validity (schema) + Completeness (min rows) ----
        validate_schema(
            spark, run_id, df, rules["expected_columns"],
            layer=layer, table_name=table_name, rundate=rundate, severity="BLOCKING",
        )

        df = df.persist(StorageLevel.MEMORY_AND_DISK)
        input_rows = df.count()

        validate_minimum_row_count(
            spark, run_id, df, rules["min_rows"],
            layer=layer, table_name=table_name, rundate=rundate,
            severity="WARNING",  # incremental ít dòng vẫn hợp lệ
        )

        ingest_time = spark.sql("SELECT current_timestamp()").first()[0]
        df_out = df.withColumn("ingest_at", lit(ingest_time)).withColumn("source", lit("postgresql"))

        if last_watermark is None:
            (df_out.write.format("delta").mode("overwrite")
             .option("overwriteSchema", "true").saveAsTable(bronze_table))
            print(f"[BRONZE] (Re)created {bronze_table}")
        else:
            df_out.write.format("delta").mode("append").saveAsTable(bronze_table)
            print(f"[BRONZE] APPEND completed: {bronze_table}")

        output_rows = input_rows

        if incremental:
            new_watermark = df.agg(spark_max(col(watermark_column)).alias("m")).first()["m"]
            wm_col = watermark_column
        else:
            new_watermark, wm_col = ingest_time, "ingest_at"   # full-load: chỉ ghi lineage
        print(f"[Watermark] New: {new_watermark}")

        if new_watermark is not None:
            insert_log(spark=spark, layer=layer, table_name=table_name,
                       watermark_column=wm_col, watermark_value=new_watermark, rundate=rundate)

        insert_audit(
            spark=spark, run_id=run_id, layer=layer, table_name=table_name, status="PASS",
            started_at=started_at, finished_at=datetime.now(),
            input_rows=input_rows, output_rows=output_rows, rejected_rows=0, rundate=rundate,
        )
        print(f"[AUDIT] SUCCESS run_id={run_id}")

    except Exception as e:
        insert_audit(
            spark=spark, run_id=run_id, layer=layer, table_name=table_name, status="FAIL",
            started_at=started_at, finished_at=datetime.now(),
            input_rows=input_rows, output_rows=output_rows, rejected_rows=None,
            error_message=str(e)[:2000], rundate=rundate,
        )
        print(f"[AUDIT] FAILED run_id={run_id}: {e}")
        raise
    finally:
        if df is not None:
            df.unpersist()


def parse_args():
    parser = argparse.ArgumentParser(description="Ingest một bảng từ PostgreSQL vào Bronze layer")
    parser.add_argument("--table", required=True, choices=list(BRONZE_RULES.keys()))
    return parser.parse_args()


def main():
    table_name = parse_args().table
    spark = create_spark_session()
    print(f"--- BẮT ĐẦU LOAD TỪ DATABASE: {table_name} ---")
    try:
        ingest_table(spark, table_name)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
