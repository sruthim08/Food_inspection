import dlt
from pyspark.sql.functions import (
    col, when, lit, to_date, regexp_extract, array, size, filter as array_filter, monotonically_increasing_id
)

# ============================================================
# SILVER - CHICAGO
# ============================================================
# BUG FIXES from original:
#   1. no_false_pass: was checking '%CRITICAL%' only.
#      Chicago uses 'PRIORITY VIOLATION' — both terms now checked.
#   2. valid_inspection_type added (was missing — required by spec).
#   3. zip cast to STRING inside function before RLIKE can work.
#   4. inspection_date cast to DATE type.
#   5. source_city column added for cross-city filtering in Gold.
#
# DECORATOR ORDER NOTE:
#   @dlt.table must be OUTERMOST (written first / top).
#   Expectations sit between @dlt.table and def. This is correct.
# ============================================================

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
    # FIX: Chicago uses 'PRIORITY VIOLATION', not 'CRITICAL'
    "NOT (inspection_result = 'Pass' AND ("
    "  violations LIKE '%PRIORITY VIOLATION%' OR "
    "  violations LIKE '%CRITICAL%'"
    "))"
)
def silver_chicago():
    df = spark.read.table("bronze_chicago")

    # --- Renames ---
    df = (df
        .withColumnRenamed("dba_name",   "business_name")
        .withColumnRenamed("license_num", "license_number")
        .withColumnRenamed("results",     "inspection_result")
        .withColumnRenamed("aka_name",    "aka_name")
    )

    # FIX: Cast zip (inferred as Integer in Bronze) to STRING
    # so the RLIKE DQX expectation above can evaluate correctly.
    df = df.withColumn("zip_code", col("zip").cast("string")).drop("zip")

    # FIX: Cast date string to proper DATE type
    df = df.withColumn("inspection_date", to_date(col("inspection_date"), "MM/dd/yyyy"))

    # Derive numeric inspection score from categorical result
    df = df.withColumn(
        "inspection_score",
        when(col("inspection_result") == "Pass",             90)
        .when(col("inspection_result") == "Pass w/ Conditions", 80)
        .when(col("inspection_result") == "Fail",            70)
        .when(col("inspection_result") == "No Entry",         0)
        # All other values (Out of Business, Business Not Located, etc.) → NULL
    )

    # Tag source city for cross-city joins in Gold
    df = df.withColumn("source_city", lit("Chicago"))

    # Drop redundant combined location string
    df = df.drop("location")

    return df


# ============================================================
# SILVER - DALLAS
# ============================================================
# BUG FIXES from original:
#   1. Function body was nearly empty — only did one rename.
#      Now includes all required transformations.
#   2. zip_code cast to STRING (inferred as Integer in Bronze).
#   3. inspection_date cast to DATE type.
#   4. Lat/Lon parsed from combined '(lat, lon)' string.
#   5. inspection_result derived from score (≥90=Pass, 80-89=
#      Pass w/ Conditions, <80=Fail) — required for BI dashboard.
#   6. Redundant columns dropped (inspection_month, inspection_year).
#   7. Double-space column 'violation__memo_-_20' renamed to
#      'violation_memo_-_20' for consistency.
#   8. inspection_id generated HERE (stable within pipeline run).
#   9. valid_inspection_type added (was missing).
#  10. has_violations added (was missing).
#  11. high_score_viol_limit: proper array expression on wide
#      columns before unpivot — was listed in spec but missing.
#  12. source_city column added.
# ============================================================

# Build the array expression for counting non-null violations
# Evaluated on wide-format columns BEFORE the unpivot in Gold.
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
@dlt.expect_or_drop("score_limit",            "inspection_score <= 100 AND inspection_score >= 0")
@dlt.expect_or_drop("no_future_date",         "inspection_date <= current_date()")
@dlt.expect_or_drop("has_violations",         "`violation_description_-_1` IS NOT NULL")
@dlt.expect_or_drop("high_score_viol_limit",  _HIGH_SCORE_RULE)
def silver_dallas():
    df = spark.read.table("bronze_dallas")

    # --- Rename primary business name ---
    df = df.withColumnRenamed("restaurant_name", "business_name")

    # FIX: Cast zip_code (Integer in Bronze) to STRING for RLIKE
    df = df.withColumn("zip_code", col("zip_code").cast("string"))

    # FIX: Cast inspection_date string to DATE
    df = df.withColumn("inspection_date", to_date(col("inspection_date"), "MM/dd/yyyy"))

    # FIX: Parse latitude and longitude from combined '(lat, lon)' string
    df = (df
        .withColumn("latitude",  regexp_extract(col("lat_long_location"), r"\((-?[\d.]+),",     1).cast("double"))
        .withColumn("longitude", regexp_extract(col("lat_long_location"), r",\s*(-?[\d.]+)\)",  1).cast("double"))
        .drop("lat_long_location")
    )

    # FIX: Derive inspection_result from score for BI dashboard
    # Without this Dallas rows are invisible in 'by result' visuals.
    df = df.withColumn(
        "inspection_result",
        when(col("inspection_score") >= 90, "Pass")
        .when(col("inspection_score") >= 80, "Pass w/ Conditions")
        .otherwise("Fail")
    )

    # FIX: Generate stable inspection_id for Dallas
    # Must be done in Silver so Gold tables can reference it consistently.
    df = df.withColumn(
        "inspection_id",
        monotonically_increasing_id().cast("string")
    )

    # Tag source city
    df = df.withColumn("source_city", lit("Dallas"))

    # FIX: Drop redundant temporal columns (derivable from inspection_date)
    df = df.drop("inspection_month", "inspection_year")

    # FIX: Fix the double-space column name produced by clean_column_names
    # 'Violation  Memo - 20' → 'violation__memo_-_20' (double underscore)
    df = df.withColumnRenamed("violation__memo_-_20", "violation_memo_-_20")

    return df