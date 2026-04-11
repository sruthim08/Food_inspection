import dlt
from pyspark.sql.functions import col, when, current_date

# ======================================================
# SILVER - CHICAGO
# ======================================================

@dlt.table(name="silver_chicago")

@dlt.expect_or_drop("valid_business_name", "business_name IS NOT NULL")
@dlt.expect_or_drop("valid_inspection_date", "inspection_date IS NOT NULL")
@dlt.expect_or_drop("valid_zip", "zip_code RLIKE '^[0-9]{5}$'")
@dlt.expect_or_drop("valid_result", "inspection_result IS NOT NULL")

@dlt.expect_or_drop("no_future_date", "inspection_date <= current_date()")
@dlt.expect_or_drop("has_violation", "violations IS NOT NULL")

@dlt.expect_or_drop(
    "no_false_pass",
    "NOT (inspection_result = 'Pass' AND violations LIKE '%CRITICAL%')"
)

def silver_chicago():

    df = spark.read.table("bronze_chicago")

    # rename FIRST
    df = df.withColumnRenamed("dba_name", "business_name") \
           .withColumnRenamed("license_num", "license_number") \
           .withColumnRenamed("zip", "zip_code") \
           .withColumnRenamed("results", "inspection_result")

    # derive score
    df = df.withColumn(
        "inspection_score",
        when(col("inspection_result") == "Pass", 90)
        .when(col("inspection_result") == "Pass w/ Conditions", 80)
        .when(col("inspection_result") == "Fail", 70)
        .when(col("inspection_result") == "No Entry", 0)
    )

    return df


# ======================================================
# SILVER - DALLAS
# ======================================================



@dlt.table(name="silver_dallas")

@dlt.expect_or_drop("valid_business_name", "business_name IS NOT NULL")
@dlt.expect_or_drop("valid_inspection_date", "inspection_date IS NOT NULL")
@dlt.expect_or_drop("valid_zip", "zip_code RLIKE '^[0-9]{5}$'")
@dlt.expect_or_drop("score_limit", "inspection_score <= 100")

@dlt.expect_or_drop("no_future_date", "inspection_date <= current_date()")

def silver_dallas():

    df = spark.read.table("bronze_dallas")

    # rename FIRST
    df = df.withColumnRenamed("restaurant_name", "business_name")

    return df