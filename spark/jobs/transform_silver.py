"""Silver: làm sạch (trim, na.drop, dedupe), kiểm tra business rule, quarantine dòng lỗi."""
import argparse
from datetime import datetime, date

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, max as spark_max

from job_control import generate_run_id, get_watermark, insert_log, insert_audit
from validation import validate_row_count, validate_business_rule, dedupe_by_key

# business_rules: tên -> (hàm điều kiện HỢP LỆ, severity, ngưỡng % vi phạm)
#   BLOCKING: loại dòng vi phạm + quarantine, raise nếu vượt ngưỡng | WARNING: chỉ ghi metric
SILVER_CONFIG = {
    "categories": {
        "columns": ["category_id", "category_name"],
        "unique_keys": ["category_id"],
        "not_null": ["category_id", "category_name"],
        "business_rules": {},
    },
    "countries": {
        "columns": ["country_id", "country_name", "country_code"],
        "unique_keys": ["country_id"],
        "not_null": ["country_id", "country_name"],
        "business_rules": {},
    },
    "cities": {
        "columns": ["city_id", "city_name", "zipcode", "country_id"],
        "unique_keys": ["city_id"],
        "not_null": ["city_id", "city_name"],
        "business_rules": {},
    },
    "customers": {
        "columns": ["customer_id", "first_name", "middle_initial", "last_name", "city_id", "address"],
        "unique_keys": ["customer_id"],
        "not_null": ["customer_id"],
        "business_rules": {},
    },
    "employees": {
        "columns": ["employee_id", "first_name", "middle_initial", "last_name",
                    "birth_date", "gender", "city_id", "hire_date"],
        "unique_keys": ["employee_id"],
        "not_null": ["employee_id"],
        "business_rules": {
            "hire_after_birth": (lambda: col("hire_date") > col("birth_date"), "WARNING", 5.0),
        },
    },
    "products": {
        "columns": ["product_id", "product_name", "price", "category_id", "class",
                    "modify_date", "resistant", "is_allergic", "vitality_days"],
        "unique_keys": ["product_id"],
        "not_null": ["product_id", "product_name", "price"],
        "business_rules": {
            "price_positive": (lambda: col("price") > 0, "BLOCKING", 1.0),
            "vitality_days_non_negative": (lambda: col("vitality_days") >= 0, "WARNING", 1.0),
        },
    },
    "sales": {
        "columns": ["sales_id", "sales_person_id", "customer_id", "product_id", "quantity",
                    "discount", "total_price", "sales_date", "transaction_number"],
        "unique_keys": ["sales_id"],
        "not_null": ["sales_id", "sales_person_id", "customer_id", "product_id",
                     "quantity", "sales_date", "transaction_number"],
        "business_rules": {
            "quantity_positive": (lambda: col("quantity") > 0, "BLOCKING", 1.0),
            "discount_in_range": (lambda: (col("discount") >= 0) & (col("discount") <= 1), "BLOCKING", 1.0),
        },
    },
}


def create_spark_session():
    return SparkSession.builder.appName("TransformSilver").enableHiveSupport().getOrCreate()


def read_incremental_bronze(spark, table_name):
    last_watermark = get_watermark(spark, layer="silver", table_name=table_name)
    df = spark.read.table(f"bronze.bronze_{table_name}")
    if last_watermark is None:
        print(f"[Silver] {table_name}: FIRST RUN")
        return df, None
    print(f"[Silver] {table_name}: Previous watermark = {last_watermark}")
    return df.filter(col("ingest_at") > lit(last_watermark)), last_watermark


def clean(df: DataFrame, cfg: dict) -> DataFrame:
    """trim chuỗi -> bỏ dòng thiếu khoá/trường bắt buộc -> dedupe giữ bản ingest mới nhất -> chọn cột."""
    for field in df.schema.fields:
        if field.dataType.simpleString() == "string":
            df = df.withColumn(field.name, F.trim(col(field.name)))
    df = df.na.drop(subset=cfg["not_null"])
    df = dedupe_by_key(df, cfg["unique_keys"], order_column="ingest_at")
    return df.select(*cfg["columns"])


def process_table(spark, table_name):
    layer = "silver"
    run_id = generate_run_id(layer, table_name)
    started_at = datetime.now()
    rundate = str(date.today())
    silver_table = f"silver.silver_{table_name}"
    cfg = SILVER_CONFIG[table_name]
    keys = cfg["unique_keys"]
    input_rows = output_rows = rejected_rows = 0
    df_raw = df_clean = df_out = None

    print(f"\n========== SILVER: {table_name} ==========")
    try:
        df_raw, last_watermark = read_incremental_bronze(spark, table_name)

        if df_raw.limit(1).count() == 0:
            print(f"[Silver] {table_name}: No new data")
            insert_audit(spark=spark, run_id=run_id, layer=layer, table_name=table_name, status="PASS",
                         started_at=started_at, finished_at=datetime.now(),
                         input_rows=0, output_rows=0, rejected_rows=0, rundate=rundate)
            return

        df_raw = df_raw.persist(StorageLevel.MEMORY_AND_DISK)

        # Gộp count và lấy watermark vào 1 action duy nhất
        stats = df_raw.agg(F.count("*").alias("cnt"), spark_max("ingest_at").alias("w")).first()
        input_rows = stats["cnt"]
        new_watermark = stats["w"]

        # Persist df_clean NGAY SAU clean() để tránh chạy lại shuffle dedupe_by_key nhiều lần
        df_clean = clean(df_raw, cfg).persist(StorageLevel.MEMORY_AND_DISK)

        # ---- Completeness: mất dòng khi làm sạch + quarantine phần bị loại ----
        df_clean = validate_row_count(
            spark, run_id, df_raw, df_clean, layer=layer, table_name=table_name, rundate=rundate,
            max_loss_pct=10.0, key_columns=keys, severity="BLOCKING",
        )

        # Giải phóng df_raw ngay sau khi dùng xong để nhường RAM cho business rules & Delta merge
        df_raw.unpersist()
        df_raw = None

        # ---- Validity: business rules ----
        for rule_name, (cond_fn, severity, max_pct) in cfg["business_rules"].items():
            df_clean = validate_business_rule(
                spark, run_id, df_clean, rule_name, condition=cond_fn(), key_columns=keys,
                layer=layer, table_name=table_name, rundate=rundate,
                max_violation_pct=max_pct, severity=severity,
            )

        ingest_time = spark.sql("SELECT current_timestamp()").first()[0]
        df_out = (df_clean.withColumn("ingest_at", lit(ingest_time))
                  .withColumn("source", lit("bronze"))
                  .persist(StorageLevel.MEMORY_AND_DISK))

        if not spark.catalog.tableExists(silver_table):
            df_out.write.format("delta").mode("overwrite").saveAsTable(silver_table)
            print(f"[Silver] Created {silver_table}")
        else:
            cond = " AND ".join(f"tgt.{k} = src.{k}" for k in keys)
            (DeltaTable.forName(spark, silver_table).alias("tgt")
             .merge(df_out.alias("src"), cond)
             .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
            print(f"[Silver] MERGE completed: {silver_table}")

        output_rows = df_out.count()
        rejected_rows = input_rows - output_rows

        insert_log(spark=spark, layer=layer, table_name=table_name,
                   watermark_column="ingest_at", watermark_value=new_watermark, rundate=rundate)

        status = "PARTIAL_FAIL" if rejected_rows > 0 else "PASS"
        insert_audit(spark=spark, run_id=run_id, layer=layer, table_name=table_name, status=status,
                     started_at=started_at, finished_at=datetime.now(),
                     input_rows=input_rows, output_rows=output_rows, rejected_rows=rejected_rows, rundate=rundate)
        print(f"[AUDIT] {status} run_id={run_id}: {input_rows} -> {output_rows} rows")

    except Exception as e:
        insert_audit(spark=spark, run_id=run_id, layer=layer, table_name=table_name, status="FAIL",
                     started_at=started_at, finished_at=datetime.now(),
                     input_rows=input_rows, output_rows=output_rows, rejected_rows=rejected_rows,
                     error_message=str(e)[:2000], rundate=rundate)
        print(f"[AUDIT] FAILED run_id={run_id}: {e}")
        raise
    finally:
        for d in (df_raw, df_clean, df_out):
            if d is not None:
                d.unpersist()


def parse_args():
    parser = argparse.ArgumentParser(description="Transform một bảng từ Bronze sang Silver")
    parser.add_argument("--table", required=True, choices=list(SILVER_CONFIG.keys()))
    return parser.parse_args()


def main():
    table_name = parse_args().table
    spark = create_spark_session()
    try:
        process_table(spark, table_name)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
