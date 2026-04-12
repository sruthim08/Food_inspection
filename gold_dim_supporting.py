from pyspark.sql.functions import (
    col, lit, upper, to_date, date_format, dayofmonth, month, quarter,
    year, dayofweek, monotonically_increasing_id, when, concat_ws,
    regexp_extract, trim
)
from pyspark.sql.types import IntegerType

# ============================================================
# GOLD - Supporting Dimensions
# ============================================================


# ──────────────────────────────────────────────────────────────
# dim_location
# ──────────────────────────────────────────────────────────────
chicago = spark.table("silver_chicago")
dallas  = spark.table("silver_dallas")

chicago_locations = (
    chicago
    .select(
        col("address").alias("street_address"),
        upper(col("city")).alias("city"),
        col("state"),
        col("zip_code"),
        col("latitude"),
        col("longitude")
    )
)

dallas_locations = (
    dallas
    .select(
        col("street_address"),
        lit("DALLAS").alias("city"),
        lit("TX").alias("state"),
        col("zip_code"),
        col("latitude"),
        col("longitude")
    )
)

dim_location = (
    chicago_locations.unionByName(dallas_locations)
    .dropDuplicates(["street_address", "zip_code", "city"])
    .withColumn("location_sk", monotonically_increasing_id())
    .select("location_sk", "street_address", "city", "state", "zip_code", "latitude", "longitude")
)

dim_location.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_location")
print("dim_location rows:", dim_location.count())
spark.table("dim_location").show(5)


# ──────────────────────────────────────────────────────────────
# dim_date
# ──────────────────────────────────────────────────────────────

from pyspark.sql import Row
import datetime

min_date = min(
    chicago.selectExpr("min(inspection_date)").collect()[0][0],
    dallas.selectExpr("min(inspection_date)").collect()[0][0]
)
max_date = max(
    chicago.selectExpr("max(inspection_date)").collect()[0][0],
    dallas.selectExpr("max(inspection_date)").collect()[0][0]
)

print(f"Date spine range: {min_date} → {max_date}")

date_rows = []
current = min_date
while current <= max_date:
    date_rows.append(Row(full_date=current))
    current += datetime.timedelta(days=1)

date_spine = spark.createDataFrame(date_rows)

dim_date = (
    date_spine
    .withColumn("date_sk",      date_format(col("full_date"), "yyyyMMdd").cast(IntegerType()))
    .withColumn("day_of_month", dayofmonth(col("full_date")))
    .withColumn("month",        month(col("full_date")))
    .withColumn("month_name",   date_format(col("full_date"), "MMMM"))
    .withColumn("quarter",      quarter(col("full_date")))
    .withColumn("year",         year(col("full_date")))
    .withColumn("day_of_week",  date_format(col("full_date"), "EEEE"))
    .withColumn("is_weekend",   dayofweek(col("full_date")).isin([1, 7]))
    .select("date_sk", "full_date", "day_of_month", "month", "month_name",
            "quarter", "year", "day_of_week", "is_weekend")
)

dim_date.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_date")
print("dim_date rows:", dim_date.count())
spark.table("dim_date").show(3)


# ──────────────────────────────────────────────────────────────
# dim_inspection_type
# ──────────────────────────────────────────────────────────────

from pyspark.sql.functions import countDistinct

chicago_types = (
    chicago.select("inspection_type", lit("Chicago").alias("source_city")).distinct()
)
dallas_types = (
    dallas.select("inspection_type", lit("Dallas").alias("source_city")).distinct()
)

dim_inspection_type = (
    chicago_types.unionByName(dallas_types)
    .dropDuplicates(["inspection_type", "source_city"])
    .withColumn("inspection_type_sk", monotonically_increasing_id())
    .select("inspection_type_sk", col("inspection_type").alias("inspection_type_name"), "source_city")
)

dim_inspection_type.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_inspection_type")
print("dim_inspection_type rows:", dim_inspection_type.count())
spark.table("dim_inspection_type").show(10)


# ──────────────────────────────────────────────────────────────
# dim_risk
# ──────────────────────────────────────────────────────────────

from pyspark.sql import Row

risk_rows = [
    Row(risk_sk=1, risk_code=1, risk_label="Risk 1 (High)"),
    Row(risk_sk=2, risk_code=2, risk_label="Risk 2 (Medium)"),
    Row(risk_sk=3, risk_code=3, risk_label="Risk 3 (Low)"),
]

dim_risk = spark.createDataFrame(risk_rows)
dim_risk.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_risk")
print("dim_risk rows:", dim_risk.count())
spark.table("dim_risk").show()