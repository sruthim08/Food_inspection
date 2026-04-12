from pyspark.sql.functions import (
    col, lit, current_date, concat_ws, coalesce, monotonically_increasing_id, when
)
from delta.tables import DeltaTable

# ============================================================
# GOLD - dim_restaurant (SCD Type 2)
# ============================================================
chicago = spark.table("silver_chicago")
dallas  = spark.table("silver_dallas")


# ──────────────────────────────────────────────────────────────
# STEP 1 — Build incoming (source) restaurant records
# ──────────────────────────────────────────────────────────────
chicago_restaurants = (
    chicago
    .select(
        "business_name",
        col("aka_name"),
        col("license_number"),
        col("facility_type"),
        col("risk").alias("risk_label"),
        lit("Chicago").alias("source_city")
    )
    .withColumn(
        "restaurant_nk",
        concat_ws("|", lit("Chicago"), col("license_number"))
    )
    .dropDuplicates(["restaurant_nk"])
)

dallas_restaurants = (
    dallas
    .select(
        "business_name",
        lit(None).cast("string").alias("aka_name"),          # Dallas has no AKA field
        lit(None).cast("string").alias("license_number"),    # Dallas has no license number
        lit(None).cast("string").alias("facility_type"),     # Dallas has no facility type
        lit(None).cast("string").alias("risk_label"),        # Dallas has no risk level
        lit("Dallas").alias("source_city")
    )
    # Dallas NK uses business_name — no stable alternative exists.
    # Limitation: a name change creates a new restaurant record.
    .withColumn(
        "restaurant_nk",
        concat_ws("|", lit("Dallas"), col("business_name"))
    )
    .dropDuplicates(["restaurant_nk"])
)

incoming = chicago_restaurants.unionByName(dallas_restaurants)


# ──────────────────────────────────────────────────────────────
# STEP 2 — Initial load OR SCD2 merge into dim_restaurant
# ──────────────────────────────────────────────────────────────

TABLE_NAME = "dim_restaurant"

def initial_load(df):
    """First-time load: assign surrogate keys and SCD2 timestamps."""
    return (
        df
        .withColumn("restaurant_sk", monotonically_increasing_id())
        .withColumn("start_date",    current_date())
        .withColumn("end_date",      lit(None).cast("date"))
        .withColumn("is_current",    lit(True))
        .select(
            "restaurant_sk", "restaurant_nk", "business_name", "aka_name",
            "license_number", "facility_type", "risk_label", "source_city",
            "start_date", "end_date", "is_current"
        )
    )

def scd2_merge(incoming_df):
    """
    SCD2 merge pattern using Delta MERGE:
    - New records (NK not in dim) → INSERT as current
    - Changed records (NK in dim, attributes differ) → CLOSE old, INSERT new
    - Unchanged records → no action
    """
    dim_table = DeltaTable.forName(spark, TABLE_NAME)

    existing_current = (
        spark.table(TABLE_NAME)
        .filter(col("is_current") == True)
        .select("restaurant_nk", "business_name", "aka_name", "facility_type", "risk_label")
    )

    changed = (
        incoming_df
        .join(existing_current, on="restaurant_nk", how="left")
        .filter(
            existing_current["business_name"].isNull()  # new record
            | (incoming_df["business_name"] != existing_current["business_name"])
            | (incoming_df["facility_type"] != existing_current["facility_type"])
        )
    )

    dim_table.alias("dim").merge(
        changed.alias("src"),
        "dim.restaurant_nk = src.restaurant_nk AND dim.is_current = true"
    ).whenMatchedUpdate(set={
        "is_current": lit(False),
        "end_date":   current_date()
    }).execute()

    new_versions = (
        changed
        .withColumn("restaurant_sk", monotonically_increasing_id())
        .withColumn("start_date",    current_date())
        .withColumn("end_date",      lit(None).cast("date"))
        .withColumn("is_current",    lit(True))
        .select(
            "restaurant_sk", "restaurant_nk", "business_name", "aka_name",
            "license_number", "facility_type", "risk_label", "source_city",
            "start_date", "end_date", "is_current"
        )
    )
    new_versions.write.mode("append").saveAsTable(TABLE_NAME)
    print(f"SCD2 merge complete — {new_versions.count()} new/updated records inserted.")

try:
    spark.table(TABLE_NAME)
    print(f"{TABLE_NAME} exists — running SCD2 merge.")
    scd2_merge(incoming)
except Exception:
    print(f"{TABLE_NAME} does not exist — running initial load.")
    initial_load(incoming).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE_NAME)

spark.table(TABLE_NAME).show(10, truncate=False)
print("Total dim_restaurant rows:", spark.table(TABLE_NAME).count())
print("Current records:",          spark.table(TABLE_NAME).filter(col("is_current") == True).count())