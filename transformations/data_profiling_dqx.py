import dlt
from pyspark.sql.functions import length

@dlt.table(name="profile_chicago")
@dlt.expect("business_name_not_null", "dba_name IS NOT NULL")
@dlt.expect("inspection_date_not_null", "inspection_date IS NOT NULL")
@dlt.expect("valid_zip", "length(zip) = 5")
@dlt.expect("results_not_null", "results IS NOT NULL")
def profile_chicago():
    return spark.read.table("bronze_chicago")


@dlt.table(name="profile_dallas")
@dlt.expect("business_name_not_null", "restaurant_name IS NOT NULL")
@dlt.expect("inspection_date_not_null", "inspection_date IS NOT NULL")
@dlt.expect("valid_zip", "length(zip_code) = 5")
@dlt.expect("score_valid", "inspection_score <= 100")
def profile_dallas():
    return spark.read.table("bronze_dallas")