"""Gold: star schema (dim_* snapshot SCD1 + fact_sales incremental), kiểm tra grain / orphan / số tiền."""
import argparse
from datetime import datetime, date

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, sha2, concat_ws, broadcast, max as spark_max

from job_control import generate_run_id, get_watermark, insert_log, insert_audit
from validation import validate_referential_integrity, validate_grain, validate_amount_consistency


def create_spark_session():
    return SparkSession.builder.appName("BuildGold").enableHiveSupport().getOrCreate()


def sk(*cols) -> "Column":
    """Surrogate key tất định (hash của natural key) -> ổn định giữa các lần chạy, không cần bảng sequence."""
    return sha2(concat_ws("||", *[col(c).cast("string") for c in cols]), 256)


def silver(spark, name) -> DataFrame:
    return spark.read.table(f"silver.silver_{name}")


# ---------------------------------------------------------------- DIMENSIONS (SCD1 = ghi đè snapshot)
def build_dim_date(spark, run_id) -> DataFrame:
    df = spark.sql("SELECT explode(sequence(to_date('2017-01-01'), to_date('2019-12-31'), interval 1 day)) AS date")
    return df.select(
        sha2(col("date").cast("string"), 256).alias("date_sk"),
        col("date"),
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "yyyy-MM").alias("year_month"),
        F.date_format("date", "MMM").alias("month_name"),
        F.dayofmonth("date").alias("day"),
        F.dayofweek("date").alias("day_of_week"),            # 1 = Chủ nhật ... 7 = Thứ bảy
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )


def build_dim_category(spark, run_id) -> DataFrame:
    return silver(spark, "categories").select(sk("category_id").alias("category_sk"), "category_id", "category_name")


def build_dim_geography(spark, run_id) -> DataFrame:
    cities, countries = silver(spark, "cities"), silver(spark, "countries")
    # Integrity: city -> country
    validate_referential_integrity(
        spark, run_id, df_fact=cities, df_dim=countries.select("country_id"), fact_key="country_id",
        dim_key="country_id", layer="gold", table_name="dim_geography", rundate=str(date.today()),
        max_orphan_pct=5.0, severity="WARNING")
    df = cities.join(countries, "country_id", "left")
    return df.select(
        sk("city_id").alias("geography_sk"), "city_id", "city_name", "zipcode", "country_id",
        F.coalesce(col("country_name"), lit("Unknown")).alias("country_name"), "country_code")


def build_dim_product(spark, run_id) -> DataFrame:
    dim_cat = spark.read.table("gold.dim_category").select("category_id", "category_name")
    df = silver(spark, "products").join(dim_cat, "category_id", "left")
    return df.select(
        sk("product_id").alias("product_sk"), "product_id", "product_name", "price", "category_id",
        F.coalesce(col("category_name"), lit("Unknown")).alias("category_name"),
        F.coalesce(col("class"), lit("Unknown")).alias("class"),
        "resistant", "is_allergic", "vitality_days", "modify_date")


def build_dim_customer(spark, run_id) -> DataFrame:
    geo = spark.read.table("gold.dim_geography").select("geography_sk", "city_id")
    df = silver(spark, "customers").join(geo, "city_id", "left")
    return df.select(
        sk("customer_id").alias("customer_sk"), "customer_id",
        concat_ws(" ", "first_name", "middle_initial", "last_name").alias("full_name"),
        "address", "city_id", "geography_sk")


def build_dim_employee(spark, run_id) -> DataFrame:
    geo = spark.read.table("gold.dim_geography").select("geography_sk", "city_id")
    df = silver(spark, "employees").join(geo, "city_id", "left")
    return df.select(
        sk("employee_id").alias("employee_sk"), "employee_id",
        concat_ws(" ", "first_name", "middle_initial", "last_name").alias("full_name"),
        "gender", "birth_date", "hire_date", "city_id", "geography_sk")


DIM_CONFIG = {
    "dim_date":      {"fn": build_dim_date,      "grain": "date_sk"},
    "dim_category":  {"fn": build_dim_category,  "grain": "category_sk"},
    "dim_geography": {"fn": build_dim_geography, "grain": "geography_sk"},
    "dim_product":   {"fn": build_dim_product,   "grain": "product_sk"},
    "dim_customer":  {"fn": build_dim_customer,  "grain": "customer_sk"},
    "dim_employee":  {"fn": build_dim_employee,  "grain": "employee_sk"},
}

def build_dimension(spark, name):
    layer = "gold"
    run_id = generate_run_id(layer, name)
    started_at, rundate = datetime.now(), str(date.today())
    print(f"\n========== GOLD: {name} ==========")
    rows = 0
    try:
        cfg = DIM_CONFIG[name]
        df = cfg["fn"](spark, run_id).withColumn("ingest_at", F.current_timestamp()).withColumn("source", lit("silver"))
        df = df.persist(StorageLevel.MEMORY_AND_DISK)
        validate_grain(spark, run_id, df, [cfg["grain"]], layer=layer, table_name=name, rundate=rundate)
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"gold.{name}")
        rows = df.count()
        df.unpersist()
        insert_audit(spark=spark, run_id=run_id, layer=layer, table_name=name, status="PASS",
                     started_at=started_at, finished_at=datetime.now(),
                     input_rows=rows, output_rows=rows, rejected_rows=0, rundate=rundate)
        print(f"[Gold] gold.{name}: {rows} rows")
    except Exception as e:
        insert_audit(spark=spark, run_id=run_id, layer=layer, table_name=name, status="FAIL",
                     started_at=started_at, finished_at=datetime.now(), input_rows=rows, output_rows=0,
                     rejected_rows=None, error_message=str(e)[:2000], rundate=rundate)
        raise


# ---------------------------------------------------------------- FACT (incremental theo ingest_at của Silver)
FK_CHECKS = {   # cột khoá ở silver_sales -> (bảng dim, cột natural key trong dim)
    "customer_id":     ("gold.dim_customer", "customer_id"),
    "product_id":      ("gold.dim_product", "product_id"),
    "sales_person_id": ("gold.dim_employee", "employee_id"),
}


def build_fact_sales_df(spark, df: DataFrame) -> DataFrame:
    dim_product = spark.read.table("gold.dim_product").select("product_sk", "product_id", col("price").alias("unit_price"))
    dim_customer = spark.read.table("gold.dim_customer").select("customer_sk", "customer_id")
    dim_employee = spark.read.table("gold.dim_employee").select("employee_sk", col("employee_id").alias("sales_person_id"))
    dim_date = spark.read.table("gold.dim_date").select("date_sk", "date")

    f = (df.join(broadcast(dim_product), "product_id", "left")
           .join(broadcast(dim_customer), "customer_id", "left")
           .join(broadcast(dim_employee), "sales_person_id", "left")
           .join(broadcast(dim_date), F.to_date(col("sales_date")) == col("date"), "left")
           .withColumn("discount", F.coalesce(col("discount"), lit(0)))
           # Doanh thu thực tế TỰ TÍNH, không tin cột total_price của nguồn
           .withColumn("gross_amount", (col("quantity") * col("unit_price")).cast("decimal(18,4)"))
           .withColumn("net_amount", (col("quantity") * col("unit_price") * (lit(1) - col("discount"))).cast("decimal(18,4)")))
    return f.select(
        "sales_id", "transaction_number", "customer_sk", "product_sk", "employee_sk", "date_sk",
        "quantity", "discount", "unit_price", "gross_amount", "net_amount",
        col("total_price").alias("source_total_price"), "sales_date")


def build_fact_sales(spark):
    layer, name = "gold", "fact_sales"
    run_id = generate_run_id(layer, name)
    started_at, rundate = datetime.now(), str(date.today())
    input_rows = output_rows = 0
    print(f"\n========== GOLD: {name} ==========")
    df_inc = fact = None
    try:
        last_wm = get_watermark(spark, layer, name)
        df_inc = silver(spark, "sales")
        if last_wm is not None:
            print(f"[Gold] Previous watermark = {last_wm}")
            df_inc = df_inc.filter(col("ingest_at") > lit(last_wm))
        else:
            print("[Gold] FIRST RUN")

        if df_inc.limit(1).count() == 0:
            print("[Gold] fact_sales: No new data")
            insert_audit(spark=spark, run_id=run_id, layer=layer, table_name=name, status="PASS",
                         started_at=started_at, finished_at=datetime.now(),
                         input_rows=0, output_rows=0, rejected_rows=0, rundate=rundate)
            return

        df_inc = df_inc.persist(StorageLevel.MEMORY_AND_DISK)
        stats = df_inc.agg(F.count("*").alias("cnt"), spark_max("ingest_at").alias("w")).first()
        input_rows, new_wm = stats["cnt"], stats["w"]

        # ---- Integrity: khoá ngoại mồ côi (WARNING — dim có thể đến trễ) ----
        for fk, (dim_table, dim_col) in FK_CHECKS.items():
            validate_referential_integrity(
                spark, run_id, df_fact=df_inc, df_dim=broadcast(spark.read.table(dim_table).select(dim_col).distinct()),
                fact_key=fk, dim_key=dim_col, layer=layer, table_name=name, rundate=rundate,
                max_orphan_pct=5.0, severity="WARNING")

        fact = build_fact_sales_df(spark, df_inc).persist(StorageLevel.MEMORY_AND_DISK)
        output_rows = fact.count()  # materialize fact xong rồi xả df_inc ngay
        df_inc.unpersist()
        df_inc = None

        # ---- Accuracy: total_price nguồn vs công thức tự tính ----
        validate_amount_consistency(
            spark, run_id, fact, stated_col="source_total_price", computed_col="net_amount",
            layer=layer, table_name=name, rundate=rundate, tolerance=0.01, max_mismatch_pct=1.0, severity="WARNING")

        # ---- Integrity: grain ----
        validate_grain(spark, run_id, fact, ["sales_id"], layer=layer, table_name=name, rundate=rundate)

        out = fact.withColumn("ingest_at", F.current_timestamp()).withColumn("source", lit("silver"))
        target = f"gold.{name}"
        if not spark.catalog.tableExists(target):
            out.write.format("delta").mode("overwrite").saveAsTable(target)
        else:
            (DeltaTable.forName(spark, target).alias("tgt")
             .merge(out.alias("src"), "tgt.sales_id = src.sales_id")
             .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

        insert_log(spark=spark, layer=layer, table_name=name, watermark_column="ingest_at",
                   watermark_value=new_wm, rundate=rundate)
        insert_audit(spark=spark, run_id=run_id, layer=layer, table_name=name, status="PASS",
                     started_at=started_at, finished_at=datetime.now(),
                     input_rows=input_rows, output_rows=output_rows, rejected_rows=0, rundate=rundate)
        print(f"[AUDIT] SUCCESS run_id={run_id}: {output_rows} rows")
    except Exception as e:
        insert_audit(spark=spark, run_id=run_id, layer=layer, table_name=name, status="FAIL",
                     started_at=started_at, finished_at=datetime.now(), input_rows=input_rows,
                     output_rows=output_rows, rejected_rows=None, error_message=str(e)[:2000], rundate=rundate)
        raise
    finally:
        for d in (df_inc, fact):
            if d is not None:
                d.unpersist()


ALL_GOLD_TABLES = list(DIM_CONFIG.keys()) + ["fact_sales"]


def parse_args():
    parser = argparse.ArgumentParser(description="Build một bảng Gold")
    parser.add_argument("--table", required=True, choices=ALL_GOLD_TABLES)
    return parser.parse_args()


def main():
    table = parse_args().table
    spark = create_spark_session()
    try:
        build_fact_sales(spark) if table == "fact_sales" else build_dimension(spark, table)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
