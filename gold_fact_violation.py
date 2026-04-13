from pyspark.sql.functions import col, monotonically_increasing_id

# ============================================================
# GOLD - STEP B: Build fact_inspection_violation
#
# Run order:
#   1. gold_dim_supporting.py
#   2. gold_dim_restaurant.py
#   3. gold_dim_violation.py       <- dim_violation + staging_all_violations
#   4. gold_fact_inspection.py     <- fact_inspection
#   5. THIS FILE
# ============================================================

# ── SET YOUR SCHEMA HERE ─────────────────────────────────────
# Find the right value by running in a separate cell:
#   spark.sql("SELECT current_catalog(), current_schema()").show()
#   spark.sql("SHOW TABLES IN default").filter("tableName='fact_inspection'").show()
#
# Then set SCHEMA below to wherever fact_inspection lives, e.g.:
#   "workspace.default"   or   "hive_metastore.default"   or just "default"

SCHEMA = "default"   # <-- change this if needed

def tbl(name):
    """Return a fully qualified table reference."""
    return f"{SCHEMA}.{name}"

# ── Dependency check ─────────────────────────────────────────
print(f"Using schema: {SCHEMA}")

required = ["staging_all_violations", "dim_violation", "fact_inspection"]
missing  = []
for t in required:
    try:
        spark.table(tbl(t)).limit(1).count()
    except Exception:
        missing.append(t)

if missing:
    raise Exception(
        f"\n\nMissing tables in schema '{SCHEMA}': {missing}\n"
        "Either update SCHEMA above to the correct schema, or run the "
        "upstream notebooks first:\n"
        "  gold_dim_violation.py   -> staging_all_violations, dim_violation\n"
        "  gold_fact_inspection.py -> fact_inspection\n\n"
        "To find the right schema run:\n"
        "  spark.sql('SHOW TABLES IN default').show(50, truncate=False)\n"
        "  spark.sql('SELECT current_catalog(), current_schema()').show()\n"
    )

print("All required tables confirmed. Proceeding...")

# ── Load dependencies ────────────────────────────────────────
all_violations = spark.table(tbl("staging_all_violations"))

dim_viol_lookup = spark.table(tbl("dim_violation")).select(
    "violation_sk", "violation_code", "violation_description", "source_city"
)

# Resolve inspection_sk from inspection_id via fact_inspection
fact_insp_keys = spark.table(tbl("fact_inspection")).select(
    "inspection_sk", "inspection_id"
)

# Cast inspection_id in staging to match fact_inspection type (long)
# Chicago was integer, Dallas was string — both land as long after unionByName
all_violations = all_violations.withColumn(
    "inspection_id", col("inspection_id").cast("long")
)

# ── Build bridge table ───────────────────────────────────────
fact_inspection_violation = (
    all_violations
    .join(
        dim_viol_lookup,
        on=["violation_code", "violation_description", "source_city"],
        how="left"
    )
    .join(
        fact_insp_keys,
        on="inspection_id",
        how="left"
    )
    .withColumn("insp_violation_sk", monotonically_increasing_id())
    .select(
        "insp_violation_sk",
        "inspection_sk",
        "violation_sk",
        "violation_points",
        "inspector_comment",
        "source_city"
    )
)

fact_inspection_violation.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(tbl("fact_inspection_violation"))

total = fact_inspection_violation.count()
print(f"fact_inspection_violation rows: {total}")

# ── FK integrity checks ──────────────────────────────────────
null_insp_sk = fact_inspection_violation.filter(col("inspection_sk").isNull()).count()
null_viol_sk = fact_inspection_violation.filter(col("violation_sk").isNull()).count()
print(f"NULL inspection_sk (should be 0): {null_insp_sk}")
print(f"NULL violation_sk  (should be 0): {null_viol_sk}")

spark.table(tbl("fact_inspection_violation")).show(10, truncate=False)

# ── Drop staging table ───────────────────────────────────────
spark.sql(f"DROP TABLE IF EXISTS {tbl('staging_all_violations')}")
print("staging_all_violations dropped.")