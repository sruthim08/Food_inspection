from pyspark.sql.functions import (
    col, lit, upper, to_date, date_format,
    monotonically_increasing_id, concat_ws, coalesce
)
from pyspark.sql.types import IntegerType

# ============================================================
# GOLD - fact_inspection
# ============================================================

SCHEMA = "default"
def tbl(name): return f"{SCHEMA}.{name}"

chicago = spark.table(tbl("silver_chicago"))
dallas  = spark.table(tbl("silver_dallas"))

# ── Load dimensions ──────────────────────────────────────────
dim_restaurant      = spark.table(tbl("dim_restaurant")).filter(col("is_current") == True)
dim_location        = spark.table(tbl("dim_location"))
dim_date            = spark.table(tbl("dim_date"))
dim_inspection_type = spark.table(tbl("dim_inspection_type"))
dim_risk            = spark.table(tbl("dim_risk"))

print("Dimensions loaded.")

# ── Chicago base ─────────────────────────────────────────────
chicago_base = (
    chicago
    .withColumn("source_city",    lit("Chicago"))
    .withColumn("street_address", upper(col("address")))
    .withColumn("city",           upper(col("city")))
    # FIX: use concat_ws() function — Column.concat() does not exist in PySpark
    .withColumn("restaurant_nk",  concat_ws("|", lit("Chicago"), col("license_number")))
    .select(
        col("inspection_id").cast("string").alias("inspection_id"),
        "inspection_date",
        "inspection_type",
        "inspection_score",
        "inspection_result",
        "license_number",
        "business_name",
        "restaurant_nk",
        "street_address",
        "city",
        "zip_code",
        col("risk").alias("risk_label"),
        "source_city"
    )
)
print("Chicago base rows:", chicago_base.count())

# ── Dallas base ──────────────────────────────────────────────
dallas_base = (
    dallas
    .withColumn("source_city",   lit("Dallas"))
    .withColumn("city",          lit("DALLAS"))
    .withColumn("restaurant_nk", concat_ws("|", lit("Dallas"), col("business_name")))
    .select(
        col("inspection_id").cast("string").alias("inspection_id"),
        "inspection_date",
        "inspection_type",
        "inspection_score",
        "inspection_result",
        lit(None).cast("string").alias("license_number"),
        "business_name",
        "restaurant_nk",
        "street_address",
        "city",
        "zip_code",
        lit(None).cast("string").alias("risk_label"),
        "source_city"
    )
)
print("Dallas base rows:", dallas_base.count())

# ── Union ────────────────────────────────────────────────────
combined = chicago_base.unionByName(dallas_base)
print("Combined rows:", combined.count())

# ── Join: restaurant_sk ──────────────────────────────────────
dim_rest_lookup = dim_restaurant.select("restaurant_sk", "restaurant_nk")
combined = (
    combined
    .join(dim_rest_lookup, on="restaurant_nk", how="left")
    .drop("restaurant_nk")
)

# ── Join: location_sk ────────────────────────────────────────
# Drop location cols from combined after join to avoid ambiguity
# FIX: uppercase street_address and city in the dim_location lookup to match
# chicago_base which uses upper(col("address")). dim_location was built without
# uppercasing, so the join was silently failing for all ~68k Chicago rows.
# Dallas street_address is already consistent between both sides.
dim_loc_lookup = dim_location.select(
    "location_sk",
    upper(col("street_address")).alias("street_address"),
    upper(col("city")).alias("city"),
    col("zip_code")
)
combined = (
    combined
    .join(dim_loc_lookup, on=["street_address", "zip_code", "city"], how="left")
    .drop("street_address", "zip_code", "city")
)

# ── Join: date_sk ────────────────────────────────────────────
dim_date_lookup = dim_date.select(
    "date_sk",
    col("full_date").alias("inspection_date_key")
)
combined = (
    combined
    .join(
        dim_date_lookup,
        combined["inspection_date"] == dim_date_lookup["inspection_date_key"],
        how="left"
    )
    .drop("inspection_date_key")
)

# ── Join: inspection_type_sk ─────────────────────────────────
dim_type_lookup = dim_inspection_type.select(
    "inspection_type_sk",
    col("inspection_type_name").alias("inspection_type"),
    "source_city"
)
combined = (
    combined
    .join(dim_type_lookup, on=["inspection_type", "source_city"], how="left")
    .drop("inspection_type")
)

# ── Join: risk_sk (nullable — Dallas rows will be NULL) ──────
dim_risk_lookup = dim_risk.select("risk_sk", col("risk_label"))
combined = (
    combined
    .join(dim_risk_lookup, on="risk_label", how="left")
    .drop("risk_label")
)

# ── Assemble final fact table ────────────────────────────────
fact_inspection = (
    combined
    .withColumn("inspection_sk", monotonically_increasing_id())
    .select(
        "inspection_sk",
        "inspection_id",
        "restaurant_sk",
        "location_sk",
        "date_sk",
        "inspection_type_sk",
        "risk_sk",
        "inspection_score",
        "inspection_result",
        "license_number",
        "source_city"
    )
)

fact_inspection.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(tbl("fact_inspection"))

total   = spark.table(tbl("fact_inspection")).count()
chicago_count = spark.table(tbl("fact_inspection")).filter(col("source_city") == "Chicago").count()
dallas_count  = spark.table(tbl("fact_inspection")).filter(col("source_city") == "Dallas").count()
print(f"fact_inspection rows: {total}")
print(f"  Chicago: {chicago_count}")
print(f"  Dallas:  {dallas_count}")

# ── Validation ───────────────────────────────────────────────
print("\n--- Score range by city ---")
spark.sql(f"""
    SELECT source_city,
           MIN(inspection_score) AS min_score,
           MAX(inspection_score) AS max_score,
           COUNT(*) AS total_rows,
           SUM(CASE WHEN inspection_result IS NULL THEN 1 ELSE 0 END) AS null_results
    FROM {tbl('fact_inspection')}
    GROUP BY source_city
""").show()

print("\n--- NULL FK counts (should all be 0 except risk_sk) ---")
spark.sql(f"""
    SELECT
        SUM(CASE WHEN restaurant_sk     IS NULL THEN 1 ELSE 0 END) AS null_restaurant_sk,
        SUM(CASE WHEN location_sk       IS NULL THEN 1 ELSE 0 END) AS null_location_sk,
        SUM(CASE WHEN date_sk           IS NULL THEN 1 ELSE 0 END) AS null_date_sk,
        SUM(CASE WHEN inspection_type_sk IS NULL THEN 1 ELSE 0 END) AS null_type_sk,
        SUM(CASE WHEN risk_sk           IS NULL THEN 1 ELSE 0 END) AS null_risk_sk
    FROM {tbl('fact_inspection')}
""").show()