from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format, dayofweek, dayofmonth, month, year, weekofyear, quarter, when
from pyspark.sql.types import DateType
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("Load Airbnb fact and dim tables to BigQuery") \
    .getOrCreate()

# Read your existing parquet data
listings_df = spark.read.parquet("gs://airbnb-data-bct/processed-data/listings/")
reviews_df = spark.read.parquet("gs://airbnb-data-bct/processed-data/reviews/")
calendar_df = spark.read.parquet("gs://airbnb-data-bct/processed-data/calendar/")

# Set your temporary GCS bucket here (must exist and be writable by your service account)
temporary_bucket = "temp-biqquery-bucket" 

# ===== Create dim_date =====
# Use dates from calendar & reviews
calendar_dates = calendar_df.select(to_date("date").alias("date"))
review_dates = reviews_df.select(to_date("date").alias("date"))

all_dates_df = calendar_dates.union(review_dates).distinct().dropna()

dim_date = all_dates_df.withColumn("date_key", date_format("date", "yyyyMMdd").cast("int")) \
    .withColumn("year", year("date")) \
    .withColumn("month", month("date")) \
    .withColumn("day", dayofmonth("date")) \
    .withColumn("day_of_week", date_format("date", "EEEE")) \
    .withColumn("week_of_year", weekofyear("date")) \
    .withColumn("quarter", quarter("date")) \
    .withColumn("is_weekend", when(dayofweek("date").isin(1,7), True).otherwise(False)) \
    .select("date_key", "date", "year", "month", "day", "day_of_week", "week_of_year", "quarter", "is_weekend")

# ===== Write dim_date to BigQuery =====
dim_date.write.format("bigquery") \
    .option("table", "airbnb-465211.airbnb.dim_date") \
    .option("temporaryGcsBucket", temporary_bucket) \
    .mode("overwrite") \
    .save()

# ===== Continue writing other tables =====
# dim_listings
dim_listings = listings_df.select(
    "id","host_id", "name", "description", "neighbourhood", "neighbourhood_cleansed",
    "neighbourhood_group_cleansed", "latitude", "longitude",
    "property_type", "room_type", "accommodates", "bathrooms",
    "bedrooms", "beds", "amenities", "price", "minimum_nights",
    "maximum_nights", "number_of_reviews", "review_scores_rating",
    "review_scores_accuracy", "review_scores_cleanliness",
    "review_scores_checkin", "review_scores_communication",
    "review_scores_location", "review_scores_value", "instant_bookable"
).withColumnRenamed("id", "listing_id")

dim_listings.write.format("bigquery") \
    .option("table", "airbnb-465211.airbnb.dim_listings") \
    .option("temporaryGcsBucket", temporary_bucket) \
    .mode("overwrite").save()

# dim_hosts
dim_hosts = listings_df.select(
    "host_id", "host_name", "host_since", "host_location",
    "host_about", "host_response_time", "host_response_rate",
    "host_acceptance_rate", "host_is_superhost", "host_thumbnail_url",
    "host_picture_url", "host_listings_count", "host_total_listings_count",
    "host_verifications", "host_has_profile_pic", "host_identity_verified"
).dropDuplicates(["host_id"])

dim_hosts.write.format("bigquery") \
    .option("table", "airbnb-465211.airbnb.dim_hosts") \
    .option("temporaryGcsBucket", temporary_bucket) \
    .mode("overwrite").save()

# fact_reviews
reviews_df.write.format("bigquery") \
    .option("table", "airbnb-465211.airbnb.fact_reviews") \
    .option("temporaryGcsBucket", temporary_bucket) \
    .mode("overwrite").save()



calendar_df = calendar_df \
    .withColumn("rental_category", when(col("minimum_nights") <= 7, "short_term")
                                     .when((col("minimum_nights") > 7) & (col("minimum_nights") <= 30), "medium_term")
                                     .otherwise("long_term")) \
    .dropna(subset=["listing_id", "date"])


# fact_calendar
calendar_df.write.format("bigquery") \
    .option("table", "airbnb-465211.airbnb.fact_calendar") \
    .option("temporaryGcsBucket", temporary_bucket) \
    .mode("overwrite").save()

spark.stop()
