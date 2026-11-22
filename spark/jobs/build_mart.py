"""Mart: các bảng tổng hợp phục vụ 5 tình huống phân tích. Validate (grain + đối soát với Gold) TRƯỚC khi ghi."""
import argparse
from datetime import datetime, date

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit

from job_control import generate_run_id, insert_audit
from validation import validate_grain, validate_reconciliation


def create_spark_session():
    return SparkSession.builder.appName("BuildMart").enableHiveSupport().getOrCreate()


def gold(spark, name) -> DataFrame:
    return spark.read.table(f"gold.{name}")


def pct(num, den):
    return F.when(den != 0, (num / den * 100).cast("double")).otherwise(lit(None).cast("double"))


def with_month(spark, fact: DataFrame) -> DataFrame:
    dim_date = gold(spark, "dim_date").select("date_sk", "year_month")
    return fact.join(dim_date, "date_sk", "left").withColumn("year_month", F.coalesce(col("year_month"), lit("Unknown")))


def _sum(df, c):
    return df.agg(F.sum(c)).first()[0]


# Mỗi build_* trả về (DataFrame mart, tổng doanh thu "nguồn đối soát" ở Gold tương ứng phạm vi của mart)

# ---- Tình huống 1.1 + 3.3: KPI theo tháng (doanh thu, AOV, basket size)
def build_sales_monthly(spark):
    fact = with_month(spark, gold(spark, "fact_sales"))
    w = Window.orderBy("year_month")
    df = (fact.groupBy("year_month").agg(
            F.sum("net_amount").alias("revenue"),
            F.sum("gross_amount").alias("gross_revenue"),
            F.countDistinct("transaction_number").alias("total_orders"),
            F.count("*").alias("total_lines"),
            F.sum("quantity").alias("total_units"),
            F.countDistinct("customer_sk").alias("unique_customers"))
          .withColumn("aov", (col("revenue") / col("total_orders")).cast("decimal(18,4)"))
          .withColumn("basket_size_lines", (col("total_lines") / col("total_orders")).cast("double"))
          .withColumn("basket_size_units", (col("total_units") / col("total_orders")).cast("double"))
          .withColumn("prev_revenue", F.lag("revenue").over(w))
          .withColumn("mom_growth_pct", pct(col("revenue") - col("prev_revenue"), col("prev_revenue")))
          .drop("prev_revenue"))
    return df, _sum(gold(spark, "fact_sales"), "net_amount")


# ---- Tình huống 1.2: doanh thu & tỷ trọng theo danh mục x tháng
def build_category_monthly(spark):
    fact = with_month(spark, gold(spark, "fact_sales"))
    prod = gold(spark, "dim_product").select("product_sk", "category_name")
    f = fact.join(prod, "product_sk", "left").withColumn("category_name", F.coalesce(col("category_name"), lit("Unknown")))
    wm = Window.partitionBy("year_month")
    wc = Window.partitionBy("category_name").orderBy("year_month")
    df = (f.groupBy("year_month", "category_name").agg(
            F.sum("net_amount").alias("revenue"), F.sum("quantity").alias("total_units"),
            F.countDistinct("transaction_number").alias("total_orders"))
          .withColumn("month_total_revenue", F.sum("revenue").over(wm))
          .withColumn("share_pct", pct(col("revenue"), col("month_total_revenue")))
          .withColumn("prev_revenue", F.lag("revenue").over(wc))
          .withColumn("mom_growth_pct", pct(col("revenue") - col("prev_revenue"), col("prev_revenue")))
          .drop("prev_revenue"))
    return df, _sum(gold(spark, "fact_sales"), "net_amount")


# ---- Tình huống 2.1 + 2.2: xếp hạng sản phẩm, top/bottom 5, nhu cầu cao - giá trị thấp
def build_product_performance(spark):
    fact = gold(spark, "fact_sales")
    agg = fact.groupBy("product_sk").agg(
        F.sum("quantity").alias("units_sold"), F.sum("net_amount").alias("revenue"),
        F.sum("gross_amount").alias("gross_revenue"),
        F.countDistinct("transaction_number").alias("total_orders"),
        F.countDistinct("customer_sk").alias("unique_customers"))
    base = gold(spark, "dim_product").select("product_sk", "product_id", "product_name", "category_name", "class", "price")
    df = base.join(agg, "product_sk", "left")
    for c in ["units_sold", "total_orders", "unique_customers"]:
        df = df.withColumn(c, F.coalesce(col(c), lit(0)))
    for c in ["revenue", "gross_revenue"]:
        df = df.withColumn(c, F.coalesce(col(c), lit(0).cast("decimal(18,4)")))
    df = (df.withColumn("discount_amount", col("gross_revenue") - col("revenue"))
            .withColumn("avg_discount_pct", pct(col("discount_amount"), col("gross_revenue")))
            .withColumn("revenue_per_unit", F.when(col("units_sold") > 0, (col("revenue") / col("units_sold")).cast("double")).otherwise(lit(0.0))))
    w_desc, w_asc = Window.orderBy(col("revenue").desc()), Window.orderBy(col("revenue").asc())
    df = (df.withColumn("revenue_rank", F.rank().over(w_desc))
            .withColumn("bottom_rank", F.rank().over(w_asc))
            .withColumn("units_rank", F.rank().over(Window.orderBy(col("units_sold").desc())))
            .withColumn("rank_gap", col("revenue_rank") - col("units_rank"))   # >0: bán nhiều đơn vị nhưng thứ hạng doanh thu thấp hơn
            .withColumn("is_top5", col("revenue_rank") <= 5)
            .withColumn("is_bottom5", col("bottom_rank") <= 5)
            .withColumn("units_pctile", F.percent_rank().over(Window.orderBy("units_sold")))
            .withColumn("value_pctile", F.percent_rank().over(Window.orderBy("revenue_per_unit")))
            .withColumn("demand_value_segment",
                F.when((col("units_pctile") >= 0.75) & (col("value_pctile") <= 0.25), "High demand / Low value")
                 .when((col("units_pctile") >= 0.75) & (col("value_pctile") >= 0.75), "Star")
                 .when((col("units_pctile") <= 0.25) & (col("value_pctile") >= 0.75), "Niche premium")
                 .when((col("units_pctile") <= 0.25) & (col("value_pctile") <= 0.25), "Underperformer")
                 .otherwise("Core")))
    return df, _sum(fact, "net_amount")


# ---- Tình huống 2.3: ảnh hưởng của Class
def build_class_performance(spark):
    fact = gold(spark, "fact_sales")
    prod = gold(spark, "dim_product").select("product_sk", "class")
    n_prod = prod.groupBy("class").agg(F.count("*").alias("total_products"))
    agg = (fact.join(prod, "product_sk", "left").withColumn("class", F.coalesce(col("class"), lit("Unknown")))
           .groupBy("class").agg(F.sum("quantity").alias("total_units"), F.sum("net_amount").alias("revenue"),
                                 F.countDistinct("transaction_number").alias("total_orders")))
    w = Window.partitionBy()
    df = (agg.join(n_prod, "class", "left")
          .withColumn("revenue_share_pct", pct(col("revenue"), F.sum("revenue").over(w)))
          .withColumn("units_share_pct", pct(col("total_units"), F.sum("total_units").over(w)))
          .withColumn("revenue_per_unit", (col("revenue") / col("total_units")).cast("double"))
          .withColumn("revenue_per_product", (col("revenue") / col("total_products")).cast("double")))
    return df, _sum(fact, "net_amount")


# ---- Tình huống 3.1 + 3.2: phân khúc Frequency x Monetary, Repeat vs One-time
def build_customer_360(spark):
    fact = gold(spark, "fact_sales").filter(col("customer_sk").isNotNull())
    m = fact.groupBy("customer_sk").agg(
        F.countDistinct("transaction_number").alias("total_orders"), F.sum("quantity").alias("total_units"),
        F.sum("net_amount").alias("total_spend"), F.countDistinct("product_sk").alias("distinct_products"),
        F.min("sales_date").alias("first_purchase_date"), F.max("sales_date").alias("last_purchase_date"))
    geo = gold(spark, "dim_geography").select("geography_sk", "city_name", "country_name")
    cust = gold(spark, "dim_customer").select("customer_sk", "customer_id", "full_name", "geography_sk").join(geo, "geography_sk", "left").drop("geography_sk")
    df = cust.join(m, "customer_sk", "left")
    df = (df.withColumn("total_orders", F.coalesce(col("total_orders"), lit(0)))
            .withColumn("total_units", F.coalesce(col("total_units"), lit(0)))
            .withColumn("total_spend", F.coalesce(col("total_spend"), lit(0).cast("decimal(18,4)")))
            .withColumn("avg_order_value", F.when(col("total_orders") > 0, (col("total_spend") / col("total_orders")).cast("double")))
            .withColumn("customer_type", F.when(col("total_orders") == 0, "No purchase")
                                          .when(col("total_orders") == 1, "One-time").otherwise("Repeat")))
    # Ngưỡng tertile của Frequency và Monetary, chỉ tính trên khách đã có mua hàng
    active = df.filter(col("total_orders") > 0)
    qf = active.approxQuantile("total_orders", [0.33, 0.67], 0.001) or [1, 2]
    qm = active.approxQuantile("total_spend", [0.33, 0.67], 0.001) or [0, 0]
    score = lambda c, q: F.when(col(c) <= q[0], 1).when(col(c) <= q[1], 2).otherwise(3)
    df = (df.withColumn("frequency_score", F.when(col("total_orders") > 0, score("total_orders", qf)))
            .withColumn("monetary_score", F.when(col("total_orders") > 0, score("total_spend", qm)))
            .withColumn("segment",
                F.when(col("total_orders") == 0, "No purchase")
                 .when((col("frequency_score") == 3) & (col("monetary_score") == 3), "Champions")
                 .when((col("frequency_score") >= 2) & (col("monetary_score") >= 2), "Loyal")
                 .when((col("frequency_score") == 1) & (col("monetary_score") == 3), "Big spenders")
                 .when((col("frequency_score") == 3) & (col("monetary_score") == 1), "Frequent small baskets")
                 .when((col("frequency_score") == 1) & (col("monetary_score") == 1), "Occasional low value")
                 .otherwise("Potential")))
    return df, _sum(fact, "net_amount")      # đối soát trên các dòng bán có customer_sk hợp lệ


# ---- Tình huống 4.1 + 4.2: hiệu suất nhân viên
def build_employee_performance(spark):
    fact = gold(spark, "fact_sales").filter(col("employee_sk").isNotNull())
    agg = fact.groupBy("employee_sk").agg(
        F.sum("net_amount").alias("revenue"), F.countDistinct("transaction_number").alias("total_orders"),
        F.sum("quantity").alias("total_units"), F.countDistinct("customer_sk").alias("unique_customers"))
    emp = gold(spark, "dim_employee").select("employee_sk", "employee_id", "full_name", "gender", "hire_date")
    df = emp.join(agg, "employee_sk", "left")
    df = (df.withColumn("revenue", F.coalesce(col("revenue"), lit(0).cast("decimal(18,4)")))
            .withColumn("total_orders", F.coalesce(col("total_orders"), lit(0)))
            .withColumn("total_units", F.coalesce(col("total_units"), lit(0)))
            .withColumn("unique_customers", F.coalesce(col("unique_customers"), lit(0)))
            .withColumn("avg_order_value", F.when(col("total_orders") > 0, (col("revenue") / col("total_orders")).cast("double")))
            .withColumn("revenue_share_pct", pct(col("revenue"), F.sum("revenue").over(Window.partitionBy())))
            .withColumn("revenue_vs_avg_pct", pct(col("revenue") - F.avg("revenue").over(Window.partitionBy()), F.avg("revenue").over(Window.partitionBy())))
            .withColumn("revenue_rank", F.rank().over(Window.orderBy(col("revenue").desc())))
            .withColumn("revenue_pctile", F.percent_rank().over(Window.orderBy("revenue")))
            .withColumn("performance_tier",
                F.when(col("revenue_pctile") >= 0.8, "Top performer")
                 .when(col("revenue_pctile") <= 0.2, "Underperformer").otherwise("Average")))
    return df, _sum(fact, "net_amount")


# ---- Tình huống 4.3: xu hướng & độ ổn định theo tháng (điền 0 cho tháng không có doanh số)
def build_employee_monthly(spark):
    fact = with_month(spark, gold(spark, "fact_sales").filter(col("employee_sk").isNotNull())).filter(col("year_month") != "Unknown")
    months = fact.select("year_month").distinct()
    emp = gold(spark, "dim_employee").select("employee_sk", "employee_id", "full_name")
    grid = emp.crossJoin(months)
    agg = fact.groupBy("employee_sk", "year_month").agg(
        F.sum("net_amount").alias("revenue"), F.countDistinct("transaction_number").alias("total_orders"),
        F.sum("quantity").alias("total_units"))
    df = grid.join(agg, ["employee_sk", "year_month"], "left")
    df = (df.withColumn("revenue", F.coalesce(col("revenue"), lit(0).cast("decimal(18,4)")))
            .withColumn("total_orders", F.coalesce(col("total_orders"), lit(0)))
            .withColumn("total_units", F.coalesce(col("total_units"), lit(0))))
    w_t = Window.partitionBy("employee_sk").orderBy("year_month")
    w_e = Window.partitionBy("employee_sk")
    df = (df.withColumn("prev_revenue", F.lag("revenue").over(w_t))
            .withColumn("mom_growth_pct", pct(col("revenue") - col("prev_revenue"), col("prev_revenue")))
            .withColumn("avg_monthly_revenue", F.avg("revenue").over(w_e).cast("double"))
            .withColumn("stddev_monthly_revenue", F.stddev_pop(col("revenue").cast("double")).over(w_e))
            .withColumn("cv_pct", pct(col("stddev_monthly_revenue"), col("avg_monthly_revenue")))   # càng thấp càng ổn định
            .drop("prev_revenue"))
    return df, _sum(fact, "net_amount")


# ---- Tình huống 5: địa giới (thành phố / quốc gia) theo địa chỉ khách hàng
def build_geo_performance(spark):
    fact = gold(spark, "fact_sales")
    geo = gold(spark, "dim_geography").select("geography_sk", "city_name", "country_name", "country_code")
    cust = gold(spark, "dim_customer").select("customer_sk", "geography_sk")
    key = ["country_name", "country_code", "city_name"]
    f = (fact.join(cust, "customer_sk", "left").join(geo, "geography_sk", "left")
             .withColumn("country_name", F.coalesce(col("country_name"), lit("Unknown")))
             .withColumn("city_name", F.coalesce(col("city_name"), lit("Unknown"))))
    agg = f.groupBy(*key).agg(
        F.sum("net_amount").alias("revenue"), F.sum("quantity").alias("total_units"),
        F.countDistinct("transaction_number").alias("total_orders"), F.countDistinct("customer_sk").alias("buying_customers"))
    n_cust = (cust.join(geo, "geography_sk", "left")
              .withColumn("country_name", F.coalesce(col("country_name"), lit("Unknown")))
              .withColumn("city_name", F.coalesce(col("city_name"), lit("Unknown")))
              .groupBy(*key).agg(F.count("*").alias("registered_customers")))
    wg, wc = Window.partitionBy(), Window.partitionBy("country_name")
    df = (agg.join(n_cust, key, "left")
          .withColumn("registered_customers", F.coalesce(col("registered_customers"), lit(0)))
          .withColumn("avg_order_value", (col("revenue") / col("total_orders")).cast("double"))
          .withColumn("revenue_per_customer", F.when(col("buying_customers") > 0, (col("revenue") / col("buying_customers")).cast("double")))
          .withColumn("penetration_pct", pct(col("buying_customers"), col("registered_customers")))
          .withColumn("revenue_share_pct", pct(col("revenue"), F.sum("revenue").over(wg)))
          .withColumn("country_revenue", F.sum("revenue").over(wc))
          .withColumn("city_share_in_country_pct", pct(col("revenue"), col("country_revenue")))
          .withColumn("units_vs_avg_pct", pct(col("total_units") - F.avg("total_units").over(wg), F.avg("total_units").over(wg)))
          .withColumn("global_revenue_rank", F.rank().over(Window.orderBy(col("revenue").desc())))
          .withColumn("rank_in_country", F.rank().over(Window.partitionBy("country_name").orderBy(col("revenue").desc()))))
    return df, _sum(fact, "net_amount")


MART_CONFIG = {
    "sales_monthly":        {"fn": build_sales_monthly,        "grain": ["year_month"],                 "revenue_col": "revenue"},
    "category_monthly":     {"fn": build_category_monthly,     "grain": ["year_month", "category_name"], "revenue_col": "revenue"},
    "product_performance":  {"fn": build_product_performance,  "grain": ["product_sk"],                 "revenue_col": "revenue"},
    "class_performance":    {"fn": build_class_performance,    "grain": ["class"],                      "revenue_col": "revenue"},
    "customer_360":         {"fn": build_customer_360,         "grain": ["customer_sk"],                "revenue_col": "total_spend"},
    "employee_performance": {"fn": build_employee_performance, "grain": ["employee_sk"],                "revenue_col": "revenue"},
    "employee_monthly":     {"fn": build_employee_monthly,     "grain": ["employee_sk", "year_month"],  "revenue_col": "revenue"},
    "geo_performance":      {"fn": build_geo_performance,      "grain": ["country_name", "city_name"],  "revenue_col": "revenue"},
}


def validate_mart(spark, run_id, df, table_name, rundate, source_revenue, cfg):
    """Mọi check ở Mart là BLOCKING: số sai không được phép âm thầm lên dashboard."""
    validate_grain(spark, run_id, df, cfg["grain"], layer="mart", table_name=table_name, rundate=rundate)
    mart_total = df.agg(F.sum(cfg["revenue_col"])).first()[0]
    validate_reconciliation(spark, run_id, mart_total, source_revenue, "revenue_reconciliation_diff",
                            layer="mart", table_name=table_name, rundate=rundate)


def parse_args():
    parser = argparse.ArgumentParser(description="Build một bảng Mart")
    parser.add_argument("--table", required=True, choices=list(MART_CONFIG.keys()))
    return parser.parse_args()


def main():
    table_name = parse_args().table
    layer = "mart"
    run_id = generate_run_id(layer, table_name)
    started_at, rundate = datetime.now(), str(date.today())
    spark = create_spark_session()
    row_count = 0
    try:
        cfg = MART_CONFIG[table_name]
        df, source_revenue = cfg["fn"](spark)
        df = df.withColumn("source", lit("gold")).withColumn("ingest_at", F.current_timestamp()).cache()

        validate_mart(spark, run_id, df, table_name, rundate, source_revenue, cfg)   # TRƯỚC khi ghi

        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"mart.{table_name}")
        row_count = df.count()
        insert_audit(spark=spark, run_id=run_id, layer=layer, table_name=table_name, status="PASS",
                     started_at=started_at, finished_at=datetime.now(),
                     input_rows=row_count, output_rows=row_count, rejected_rows=0, rundate=rundate)
        print(f"[AUDIT] SUCCESS run_id={run_id}: mart.{table_name} {row_count} rows")
    except Exception as e:
        insert_audit(spark=spark, run_id=run_id, layer=layer, table_name=table_name, status="FAIL",
                     started_at=started_at, finished_at=datetime.now(), input_rows=row_count,
                     output_rows=0, rejected_rows=None, error_message=str(e)[:2000], rundate=rundate)
        print(f"[AUDIT] FAILED run_id={run_id}: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
