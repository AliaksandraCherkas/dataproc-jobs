from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Transform Airbnb Raw Data") \
    .getOrCreate()

datasets = {
    "listings": {
        "input": "gs://airbnb-data-bct/raw-data/listings/listings.csv",
        "output": "gs://airbnb-data-bct/processed-data/listings/"
    },
    "calendar": {
        "input": "gs://airbnb-data-bct/raw-data/calendar/calendar.csv",
        "output": "gs://airbnb-data-bct/processed-data/calendar/"
    },
    "reviews": {
        "input": "gs://airbnb-data-bct/raw-data/reviews/reviews.csv",
        "output": "gs://airbnb-data-bct/processed-data/reviews/"
    }
}

for name, paths in datasets.items():
    df = spark.read.option("header", "true").csv(paths["input"])
    df.write.mode("overwrite").parquet(paths["output"])

spark.stop()
