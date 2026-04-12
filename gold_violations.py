from pyspark.sql.functions import (
    col, explode, split, regexp_extract, lit,
    lpad, trim, expr, monotonically_increasing_id
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
    .select(
        "inspection_id", "violation_code", "violation_description",
        "violation_detail", "inspector_comment", "violation_points", "source_city"
    )
    .dropDuplicates(["inspection_id", "violation_code"])
)

print("Chicago violations parsed:", chicago_violations.count())
chicago_violations.show(5, truncate=False)


# ──────────────────────────────────────────────────────────────
# STEP 2 — Unpivot Dallas violations from wide to long format
# ──────────────────────────────────────────────────────────────

# MUST cast all violation_points columns to STRING before stack()
# because Bronze ingested them with mixed types (some INT, some STRING)
for i in range(1, 26):
    dallas = dallas.withColumn(
        f"violation_points_-_{i}",
        col(f"violation_points_-_{i}").cast("string")
    )

# Build stack expression with backtick-quoted names (hyphens require this)
stack_expr = ", ".join([
    f"`violation_description_-_{i}`, `violation_points_-_{i}`, `violation_detail_-_{i}`, `violation_memo_-_{i}`"
    for i in range(1, 26)
])

dallas_violations = (
    dallas
    .selectExpr(
        "inspection_id",
        "source_city",
        f"stack(25, {stack_expr}) as (raw_description, violation_points, violation_detail, inspector_comment)"
    )
    .filter(col("raw_description").isNotNull())
    .withColumn("violation_points",   expr("try_cast(violation_points AS INT)"))
    .withColumn("violation_code",     expr(r"lpad(regexp_extract(raw_description, '^\*?(\d+)\s', 1), 2, '0')"))
    .withColumn("violation_description", expr(r"trim(regexp_extract(raw_description, '^\*?\d+\s+(.+)$', 1))"))
    .drop("raw_description")
    .select(
        "inspection_id", "violation_code", "violation_description",
        "violation_detail", "inspector_comment", "violation_points", "source_city"
    )
    .dropDuplicates(["inspection_id", "violation_code"])
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
        "inspection_id", "violation_sk", "violation_points",
        "inspector_comment", "source_city"
    )
    .withColumn("insp_violation_sk", monotonically_increasing_id())
)
