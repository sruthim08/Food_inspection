from pyspark.sql.functions import (
    col, explode, split, regexp_extract, lit,
    lpad, trim, monotonically_increasing_id, expr
)

# ============================================================
# GOLD - STEP A: Parse violations + build dim_violation
#
# Run order:
#   1. gold_dim_supporting.py       (dim_date, dim_inspection_type, dim_risk)
#   2. gold_dim_restaurant.py       (dim_restaurant SCD2)
#   3. THIS FILE                    (dim_violation + staging_all_violations)
#   4. gold_fact_inspection.py      (fact_inspection)
#   5. gold_fact_violation.py       (fact_inspection_violation — needs fact_inspection)
# ============================================================

chicago = spark.table("silver_chicago")
dallas  = spark.table("silver_dallas")


# ──────────────────────────────────────────────────────────────
# STEP 1 — Parse Chicago violations from pipe-delimited blob
# ──────────────────────────────────────────────────────────────
CHICAGO_CODE_PATTERN    = r"^\s*(\d+)\."
CHICAGO_DESC_PATTERN    = r"^\s*\d+\.\s+(.+?)\s+-\s+Comments:"
CHICAGO_COMMENT_PATTERN = r"Comments:\s*(.+)$"

chicago_violations = (
    chicago
    .withColumn("violation_segment", explode(split(col("violations"), r"\|")))
    .withColumn("violation_segment", trim(col("violation_segment")))
    .filter(col("violation_segment") != "")
    .withColumn("violation_code",
        lpad(regexp_extract(col("violation_segment"), CHICAGO_CODE_PATTERN, 1), 2, "0"))
    .withColumn("violation_description",
        trim(regexp_extract(col("violation_segment"), CHICAGO_DESC_PATTERN, 1)))
    .withColumn("inspector_comment",
        trim(regexp_extract(col("violation_segment"), CHICAGO_COMMENT_PATTERN, 1)))
    .withColumn("violation_detail",  lit(None).cast("string"))
    .withColumn("violation_points",  lit(None).cast("integer"))
    .withColumn("source_city",       lit("Chicago"))
    .filter(col("violation_code") != "")
    .select("inspection_id", "violation_code", "violation_description",
            "violation_detail", "inspector_comment", "violation_points", "source_city")
    .dropDuplicates(["inspection_id", "violation_code"])
)

print("Chicago violations parsed:", chicago_violations.count())


# ──────────────────────────────────────────────────────────────
# STEP 2 — Unpivot Dallas violations
# ──────────────────────────────────────────────────────────────

def build_dallas_stack_expr(n=25):
    args = []
    for i in range(1, n + 1):
        desc = f"`violation_description_-_{i}`"
        pts  = f"CAST(`violation_points_-_{i}` AS STRING)"
        det  = f"`violation_detail_-_{i}`"
        memo = f"`violation_memo_-_{i}`"
        args.append(f"{desc}, {pts}, {det}, {memo}")
    return (
        f"stack({n}, {', '.join(args)}) "
        f"as (raw_description, violation_points_str, violation_detail, inspector_comment)"
    )

DALLAS_CODE_PATTERN = r"^\*?(\d+)\s"
DALLAS_DESC_PATTERN = r"^\*?\d+\s+(.+)$"

dallas_violations = (
    dallas
    .selectExpr("inspection_id", "source_city", build_dallas_stack_expr(25))
    .filter(col("raw_description").isNotNull())
    .withColumn("violation_points", expr("try_cast(violation_points_str as int)"))
    .drop("violation_points_str")
    .withColumn("violation_code",
        lpad(regexp_extract(col("raw_description"), DALLAS_CODE_PATTERN, 1), 2, "0"))
    .withColumn("violation_description",
        trim(regexp_extract(col("raw_description"), DALLAS_DESC_PATTERN, 1)))
    .withColumn("violation_detail", col("violation_detail"))
    .drop("raw_description")
    .filter(col("violation_code") != "")
)

print("Dallas violations unpivoted:", dallas_violations.count())


# ──────────────────────────────────────────────────────────────
# STEP 3 — Save staging table
# ──────────────────────────────────────────────────────────────

all_violations = chicago_violations.unionByName(dallas_violations)

all_violations.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("staging_all_violations")

print("staging_all_violations rows:", all_violations.count())


# ──────────────────────────────────────────────────────────────
# STEP 4 — Build dim_violation
# ──────────────────────────────────────────────────────────────
# Grain: one row per (violation_code, violation_description, source_city)

dim_violation = (
    all_violations
    .select("violation_code", "violation_description", "violation_detail", "source_city")
    .dropDuplicates(["violation_code", "violation_description", "source_city"])
    .withColumn("violation_sk", monotonically_increasing_id())
    .select("violation_sk", "violation_code", "violation_description", "violation_detail", "source_city")
)

dim_violation.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_violation")

print("dim_violation rows:", spark.table("dim_violation").count())
spark.table("dim_violation").show(10, truncate=False)

print("\n--- Next step: run gold_fact_inspection.py ---")