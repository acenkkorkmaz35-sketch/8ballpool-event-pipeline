"""Shared SparkSession factory for the Spark jobs (streaming aggregator and daily batch)."""

from pyspark.sql import SparkSession


def build_spark_session(app_name: str, shuffle_partitions: int = 4) -> SparkSession:
    """Create (or reuse) the small local-mode session used by every job.

    UTC is essential: window boundaries and the daily job's day filter are UTC. The Spark UI
    and console progress bar are off to keep container logs readable, and the driver binds to
    127.0.0.1 so no hostname resolution is needed inside the container.
    """
    spark = (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.driver.memory", "1g")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
