from pyspark.sql.functions import (
    col, lit, to_date, monotonically_increasing_id,
    date_format, upper, concat
)
from pyspark.sql.types import IntegerType

# ============================================================
# GOLD - fact_inspection
# ============================================================

chicago = spark.table("silver_chicago")
dallas  = spark.table("silver_dallas")

# ── Load dimension lookup tables ──────────────────────────────
dim_restaurant    = spark.table("dim_restaurant").filter(col("is_current") == True)
dim_location      = spark.table("dim_location")
dim_date          = spark.table("dim_date")
dim_inspection_type = spark.table("dim_inspection_type")
dim_risk          = spark.table("dim_risk")


# ──────────────────────────────────────────────────────────────
# STEP 1 — Prepare Chicago base columns
# ──────────────────────────────────────────────────────────────

chicago_base = (
    chicago
    .withColumn("source_city", lit("Chicago"))
    .withColumn("street_address", upper(col("address")))
    .withColumn("city_upper", upper(col("city")))
    .withColumn("restaurant_nk",
        concat(col("source_city"), lit("|"), col("license_number").cast("string")))
    .select(
        "inspection_id",
        "inspection_date",
        "inspection_type",
        "inspection_score",
        "inspection_result",
        "license_number",
        "business_name",
        "restaurant_nk",
        "street_address",
        col("city_upper").alias("city"),
        "zip_code",
        "risk",
        "source_city"
    )
)


# ──────────────────────────────────────────────────────────────
# STEP 2 — Prepare Dallas base columns
# ──────────────────────────────────────────────────────────────

dallas_base = (
    dallas
    .withColumn("source_city", lit("Dallas"))
    .withColumn("city_upper", lit("DALLAS"))
    .withColumn("restaurant_nk",
        concat(col("source_city"), lit("|"), col("business_name")))
    .select(
        "inspection_id",
        "inspection_date",
        "inspection_type",
        "inspection_score",
        "inspection_result",
        lit(None).cast("string").alias("license_number"),
        "business_name",
        "restaurant_nk",
        col("street_address"),
        col("city_upper").alias("city"),
        "zip_code",
        lit(None).cast("string").alias("risk"),
        "source_city"
    )
)


# ──────────────────────────────────────────────────────────────
# STEP 3 — Union and join all FKs
# ──────────────────────────────────────────────────────────────

combined = chicago_base.unionByName(dallas_base)
combined = combined.withColumn(
    "date_sk_join", date_format(col("inspection_date"), "yyyyMMdd").cast(IntegerType())
)
combined = combined.join(
    dim_restaurant.select("restaurant_sk", "restaurant_nk"),
    on="restaurant_nk",
    how="left"
)
combined = combined.join(
    dim_location.select("location_sk", "street_address", "zip_code", "city"),
    on=["street_address", "zip_code", "city"],
    how="left"
)
combined = combined.join(
    dim_date.select("date_sk", col("date_sk").alias("date_sk_lookup")),
    col("date_sk_join") == col("date_sk"),
    how="left"
).drop("date_sk_join", "date_sk_lookup")

combined = combined.join(
    dim_inspection_type.select(
        "inspection_type_sk",
        col("inspection_type_name").alias("inspection_type"),
        "source_city"
    ),
    on=["inspection_type", "source_city"],
    how="left"
)
combined = combined.join(
    dim_risk.select("risk_sk", col("risk_label").alias("risk")),
    on="risk",
    how="left"
)

# ──────────────────────────────────────────────────────────────
# STEP 4 — Assemble final fact table
# ──────────────────────────────────────────────────────────────

fact_inspection = (
    combined
    .withColumn("inspection_sk", monotonically_increasing_id())
    .select(
        "inspection_sk",          # surrogate PK
        "inspection_id",          # natural key (Chicago: city ID, Dallas: surrogate string)
        "restaurant_sk",          # FK → dim_restaurant
        "location_sk",            # FK → dim_location
        "date_sk",                # FK → dim_date
        "inspection_type_sk",     # FK → dim_inspection_type
        "risk_sk",                # FK → dim_risk (NULL for Dallas)
        "inspection_score",       # numeric 0-100 (Chicago derived, Dallas native)
        "inspection_result",      # categorical (Chicago native, Dallas derived from score)
        "license_number",         # denormalized for drill-through (NULL for Dallas)
        "source_city",            # 'Chicago' or 'Dallas'
    )
)
