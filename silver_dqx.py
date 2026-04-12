import dlt
from pyspark.sql.functions import (
    col, when, lit, to_date, regexp_extract, array, size, filter as array_filter,
    monotonically_increasing_id
)

@dlt.table(name="silver_chicago")
@dlt.expect_or_drop("valid_business_name",    "business_name IS NOT NULL")
@dlt.expect_or_drop("valid_inspection_date",  "inspection_date IS NOT NULL")
@dlt.expect_or_drop("valid_inspection_type",  "inspection_type IS NOT NULL")
@dlt.expect_or_drop("valid_zip",              "zip_code RLIKE '^[0-9]{5}$'")
@dlt.expect_or_drop("valid_result",           "inspection_result IS NOT NULL")
@dlt.expect_or_drop("no_future_date",         "inspection_date <= current_date()")
@dlt.expect_or_drop("has_violations",         "violations IS NOT NULL")
@dlt.expect_or_drop(
    "no_false_pass",
    "NOT (inspection_result = 'Pass' AND ("
    "  violations LIKE '%PRIORITY VIOLATION%' OR "
    "  violations LIKE '%CRITICAL%'"
    "))"
)
def silver_chicago():
    df = spark.read.table("bronze_chicago")
    df = (df
        .withColumnRenamed("dba_name",   "business_name")
        .withColumnRenamed("license_num", "license_number")
        .withColumnRenamed("results",     "inspection_result")
        .withColumnRenamed("aka_name",    "aka_name")
    )
    df = df.withColumn("zip_code", col("zip").cast("string")).drop("zip")
    df = df.withColumn("inspection_date", to_date(col("inspection_date"), "MM/dd/yyyy"))
    df = df.withColumn(
        "inspection_score",
        when(col("inspection_result") == "Pass",             90)
        .when(col("inspection_result") == "Pass w/ Conditions", 80)
        .when(col("inspection_result") == "Fail",            70)
        .when(col("inspection_result") == "No Entry",         0)
    )
    df = df.withColumn("source_city", lit("Chicago"))
    df = df.drop("location")

    return df


# ============================================================
# SILVER - DALLAS
# ============================================================
_DALLAS_VIOL_DESC_COLS = ", ".join(
    [f"`violation_description_-_{i}`" for i in range(1, 26)]
)
_HIGH_SCORE_RULE = (
    "inspection_score < 90 OR "
    f"size(filter(array({_DALLAS_VIOL_DESC_COLS}), x -> x IS NOT NULL)) <= 3"
)

@dlt.table(name="silver_dallas")
@dlt.expect_or_drop("valid_business_name",    "business_name IS NOT NULL")
@dlt.expect_or_drop("valid_inspection_date",  "inspection_date IS NOT NULL")
@dlt.expect_or_drop("valid_inspection_type",  "inspection_type IS NOT NULL")
@dlt.expect_or_drop("valid_zip",              "zip_code RLIKE '^[0-9]{5}$'")
@dlt.expect_or_drop("score_limit",            "inspection_score <= 100")
@dlt.expect_or_drop("no_future_date",         "inspection_date <= current_date()")
@dlt.expect_or_drop("has_violations",         "`violation_description_-_1` IS NOT NULL")
@dlt.expect_or_drop("high_score_viol_limit",  _HIGH_SCORE_RULE)
def silver_dallas():
    df = spark.read.table("bronze_dallas")
    df = df.withColumnRenamed("restaurant_name", "business_name")
    df = df.withColumn("zip_code", col("zip_code").cast("string"))
    df = df.withColumn("inspection_date", to_date(col("inspection_date"), "MM/dd/yyyy"))
    df = (df
        .withColumn("latitude",  regexp_extract(col("lat_long_location"), r"\((-?[\d.]+),",     1).cast("double"))
        .withColumn("longitude", regexp_extract(col("lat_long_location"), r",\s*(-?[\d.]+)\)",  1).cast("double"))
        .drop("lat_long_location")
    )
    df = df.withColumn(
        "inspection_result",
        when(col("inspection_score") >= 90, "Pass")
        .when(col("inspection_score") >= 80, "Pass w/ Conditions")
        .otherwise("Fail")
    )
    df = df.withColumn(
        "inspection_id",
        monotonically_increasing_id().cast("string")
    )
    df = df.withColumn("source_city", lit("Dallas"))
    df = df.drop("inspection_month", "inspection_year")
    df = df.withColumnRenamed("violation__memo_-_20", "violation_memo_-_20")

    return df