
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("ValidateDependencyGraph")
    .getOrCreate()
)

graph_path = "/opt/project/data/gold/pypi/dependency_graph"
reach_path = "/opt/project/data/gold/pypi/dependency_reach"

edges = spark.read.parquet(graph_path)
reach = spark.read.parquet(reach_path)

edge_count = edges.count()
reach_count = reach.count()

duplicate_edges = (
    edges
    .groupBy("package_name", "normalized_dependency_name")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

duplicate_targets = (
    reach
    .groupBy("normalized_dependency_name")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

invalid_counts = reach.filter(
    (F.col("direct_dependent_count") < 0)
    | (F.col("tracked_source_dependent_count") < 0)
).count()

invalid_tracking_flags = edges.filter(
    F.col("source_is_tracked").isNull()
    | F.col("dependency_is_tracked").isNull()
).count()

print(f"Graph edges: {edge_count}")
print(f"Dependency targets: {reach_count}")
print(f"Duplicate source-target pairs: {duplicate_edges}")
print(f"Duplicate reach targets: {duplicate_targets}")
print(f"Invalid counts: {invalid_counts}")
print(f"Missing tracking flags: {invalid_tracking_flags}")

print("\nTracking flag distribution:")
edges.groupBy(
    "source_is_tracked",
    "dependency_is_tracked"
).count().show()

if (
    edge_count == 0
    or reach_count == 0
    or duplicate_edges > 0
    or duplicate_targets > 0
    or invalid_counts > 0
    or invalid_tracking_flags > 0
):
    raise RuntimeError("Dependency graph validation FAILED")

print("Dependency graph validation PASSED")

spark.stop()
