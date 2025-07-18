from pyspark.sql import SparkSession

# Инициализация SparkSession
spark = SparkSession.builder \
    .appName("Transform Airbnb Raw Data") \
    .getOrCreate()

# Параметры входных и выходных путей
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

# Функция обработки одного датасета
def process_dataset(input_path, output_path):
    df = spark.read.options(
        header=True,
        inferSchema=True,
        sep=",",
        quote='"',
        escape='"',
        mode="PERMISSIVE"
    ).csv(input_path)
    
    df.write.mode("overwrite").parquet(output_path)

# Обработка всех датасетов
for paths in datasets.values():
    process_dataset(paths["input"], paths["output"])

spark.stop()

