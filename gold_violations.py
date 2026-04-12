from pyspark.sql.functions import (
    col, explode, split, regexp_extract, lit,
    lpad, trim, filter as array_filter, array,
    expr, monotonically_increasing_id
)
from pyspark.sql import functions as F

# ============================================================
# GOLD - VIOLATION PARSING
# ============================================================

chicago = spark.table("silver_chicago")
dallas  = spark.table("silver_dallas")


# ──────────────────────────────────────────────────────────────
# STEP 1 — Parse Chicago violations from pipe-delimited blob
# ──────────────────────────────────────────────────────────────
CHICAGO_CODE_PATTERN  = r"^\s*(\d+)\."
CHICAGO_DESC_PATTERN  = r"^\s*\d+\.\s+(.+?)\s+-\s+Comments:"
CHICAGO_COMMENT_PATTERN = r"Comments:\s*(.+)$"

chicago_violations_raw = (
    chicago
    .withColumn("violation_segment", explode(split(col("violations"), r"\|")))
    .withColumn("violation_segment", trim(col("violation_segment")))
    .filter(col("violation_segment") != "")
)

chicago_violations = (
    chicago_violations_raw
    .withColumn("violation_code",
        lpad(regexp_extract(col("violation_segment"), CHICAGO_CODE_PATTERN, 1), 2, "0"))
    .withColumn("violation_description",
        trim(regexp_extract(col("violation_segment"), CHICAGO_DESC_PATTERN, 1)))
    .withColumn("inspector_comment",
        trim(regexp_extract(col("violation_segment"), CHICAGO_COMMENT_PATTERN, 1)))
    .withColumn("violation_detail", lit(None).cast("string"))
    .withColumn("violation_points", lit(None).cast("integer"))
    .withColumn("source_city", lit("Chicago"))
    .filter(col("violation_code") != "")
    .select(
        "inspection_id",
        "violation_code",
        "violation_description",
        "violation_detail",
        "inspector_comment",
        "violation_points",
        "source_city"
    )
    .dropDuplicates(["inspection_id", "violation_code"])
)

print("Chicago violations parsed:", chicago_violations.count())
chicago_violations.show(5, truncate=False)


# ──────────────────────────────────────────────────────────────
# STEP 2 — Unpivot Dallas violations from wide to long format
# ──────────────────────────────────────────────────────────────
def build_dallas_stack_expr(n=25):
    """Build a selectExpr stack() call for n violation groups."""
    args = []
    for i in range(1, n + 1):
        desc = f"`violation_description_-_{i}`"
        pts  = f"`violation_points_-_{i}`"
        det  = f"`violation_detail_-_{i}`"
        memo = f"`violation_memo_-_{i}`"
        args.append(f"{desc}, {pts}, {det}, {memo}")
    return (
        f"stack({n}, {', '.join(args)}) "
        f"as (raw_description, violation_points, violation_detail, inspector_comment)"
    )
dallas_long = (
    dallas
    .selectExpr("inspection_id", "source_city", build_dallas_stack_expr(25))
    .filter(col("raw_description").isNotNull())
)
DALLAS_CODE_PATTERN = r"^\*?(\d+)\s"
DALLAS_DESC_PATTERN = r"^\*?\d+\s+(.+)$"

dallas_violations = (
    dallas_long
    .withColumn("violation_code",
        lpad(regexp_extract(col("raw_description"), DALLAS_CODE_PATTERN, 1), 2, "0"))
    .withColumn("violation_description",
        trim(regexp_extract(col("raw_description"), DALLAS_DESC_PATTERN, 1)))
    .drop("raw_description")
    .filter(col("violation_code") != "")
)

print("Dallas violations unpivoted:", dallas_violations.count())
dallas_violations.show(5, truncate=False)


# ──────────────────────────────────────────────────────────────
# STEP 3 — Build dim_violation
# ──────────────────────────────────────────────────────────────
all_violations = chicago_violations.unionByName(dallas_violations)

dim_violation = (
    all_violations
    .select("violation_code", "violation_description", "violation_detail", "source_city")
    .dropDuplicates(["violation_code", "violation_description", "source_city"])
    .withColumn("violation_sk", monotonically_increasing_id())
    .select("violation_sk", "violation_code", "violation_description", "violation_detail", "source_city")
)

dim_violation.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_violation")
print("dim_violation rows:", dim_violation.count())
spark.table("dim_violation").show(10, truncate=False)


# ──────────────────────────────────────────────────────────────
# STEP 4 — Build fact_inspection_violation (bridge table)
# ──────────────────────────────────────────────────────────────
dim_viol_lookup = spark.table("dim_violation").select(
    "violation_sk", "violation_code", "violation_description", "source_city"
)

fact_inspection_violation = (
    all_violations
    .join(
        dim_viol_lookup,
        on=["violation_code", "violation_description", "source_city"],
        how="left"
    )
    .select(
        "inspection_id",
        "violation_sk",
        "violation_points",
        "inspector_comment",
        "source_city"
    )
    .withColumn("insp_violation_sk", monotonically_increasing_id())
)

fact_inspection_violation.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("fact_inspection_violation")
print("fact_inspection_violation rows:", fact_inspection_violation.count())
spark.table("fact_inspection_violation").show(10, truncate=False)