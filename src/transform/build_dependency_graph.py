
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


spark = (
    SparkSession.builder
    .appName("BuildDependencyGraph")
    .getOrCreate()
)

dependencies_path = (
    "/opt/project/data/gold/pypi/package_dependencies"
)

package_health_path = (
    "/opt/project/data/gold/pypi/package_health"
)

graph_output_path = (
    "/opt/project/data/gold/pypi/dependency_graph"
)

reach_output_path = (
    "/opt/project/data/gold/pypi/dependency_reach"
)


# 1. Read existing Gold datasets

dependencies = spark.read.parquet(dependencies_path)
packages = spark.read.parquet(package_health_path)


# 2. Select the latest dependency snapshot for each source package

window = (
    Window
    .partitionBy("package_name")
    .orderBy(
        F.col("snapshot_date").desc(),
        F.col("run_id").desc()
    )
)

latest_dependencies = (
    dependencies
    .withColumn(
        "snapshot_rank",
        F.row_number().over(window)
    )
    .filter(F.col("snapshot_rank") == 1)
    .drop("snapshot_rank")
)

# Normalize tracked package names

tracked_packages = (
    packages
    .select("package_name")
    .withColumn(
        "normalized_package_name",
        F.lower(
            F.regexp_replace(
                F.trim(F.col("package_name")),
                r"[-_.]+",
                "-"
            )
        )
    )
    .dropDuplicates(["normalized_package_name"])
)


# Build directed dependency edges
# Example:
# aiohttp -> attrs
# means aiohttp declares attrs as a dependency.


# Build separate lookup tables for source and target packages.
tracked_sources = tracked_packages.select(
    F.col("normalized_package_name").alias(
        "normalized_source_package"
    ),
    F.lit(True).alias("source_is_tracked"),
)

tracked_targets = tracked_packages.select(
    F.col("normalized_package_name").alias(
        "normalized_dependency_name"
    ),
    F.lit(True).alias("dependency_is_tracked"),
)

# Build directed dependency edges.
edges = (
    latest_dependencies
    .filter(
        F.col("normalized_dependency_name").isNotNull()
        & (F.trim(F.col("normalized_dependency_name")) != "")
    )
    .withColumn(
        "normalized_source_package",
        F.lower(
            F.regexp_replace(
                F.trim(F.col("package_name")),
                r"[-_.]+",
                "-"
            )
        )
    )
    .join(
        tracked_sources,
        on="normalized_source_package",
        how="left"
    )
    .join(
        tracked_targets,
        on="normalized_dependency_name",
        how="left"
    )
    .withColumn(
        "source_is_tracked",
        F.coalesce(F.col("source_is_tracked"), F.lit(False))
    )
    .withColumn(
        "dependency_is_tracked",
        F.coalesce(F.col("dependency_is_tracked"), F.lit(False))
    )
    .select(
        "package_name",
        "version",
        "snapshot_date",
        "run_id",
        "dependency_name",
        "normalized_dependency_name",
        "dependency_extras",
        "dependency_requirement",
        "environment_marker",
        "dependency_declaration",
        "source_is_tracked",
        "dependency_is_tracked"
    )
    .dropDuplicates()
)

# Save the graph.
(
    edges.write
    .mode("overwrite")
    .parquet(graph_output_path)
)

# Aggregate direct dependency reach.
reach = (
    edges
    .groupBy("normalized_dependency_name")
    .agg(
        F.first(
            "dependency_name",
            ignorenulls=True
        ).alias("dependency_name"),

        F.countDistinct(
            "package_name"
        ).alias("direct_dependent_count"),

        F.countDistinct(
            F.when(
                F.col("source_is_tracked"),
                F.col("package_name")
            )
        ).alias("tracked_source_dependent_count"),

        F.max(
            F.when(
                F.col("dependency_is_tracked"),
                1
            ).otherwise(0)
        ).cast("boolean").alias("dependency_is_tracked")
    )
)

(
    reach.write
    .mode("overwrite")
    .parquet(reach_output_path)
)

# Report results

print("\n=== DEPENDENCY GRAPH ===")
print(f"Dependency edges: {edges.count()}")
print(
    "Distinct source packages:",
    edges.select("package_name").distinct().count()
)
print(
    "Distinct dependency targets:",
    edges.select("normalized_dependency_name").distinct().count()
)

print("\n=== TRACKED VS UNTRACKED TARGETS ===")
edges.groupBy("dependency_is_tracked").count().show()

print("\n=== MOST DECLARED DEPENDENCIES ===")
reach.orderBy(
    F.col("direct_dependent_count").desc()
).show(20, truncate=False)

print("\n=== OUTPUT PATHS ===")
print(graph_output_path)
print(reach_output_path)

spark.stop()
