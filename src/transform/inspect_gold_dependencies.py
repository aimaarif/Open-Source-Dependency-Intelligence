
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("InspectGoldDependencies")
    .getOrCreate()
)

path = "/opt/project/data/gold/pypi/package_dependencies"

dependencies = spark.read.parquet(path)

print("\n=== SCHEMA ===")
dependencies.printSchema()

print("\n=== TOTAL ROWS ===")
print(dependencies.count())

print("\n=== DISTINCT SOURCE PACKAGES ===")
print(dependencies.select("package_name").distinct().count())

print("\n=== SAMPLE RECORDS ===")
dependencies.show(10, truncate=False)

spark.stop()
