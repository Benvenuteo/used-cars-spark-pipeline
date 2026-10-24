"""Spark session lifecycle without requiring a separate cluster."""

import os
import sys

from pyspark.sql import SparkSession

from used_cars.config import Settings


def create_spark(settings: Settings) -> SparkSession:
    """Use the same Python interpreter on driver and workers, and stable SQL settings."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    session = (
        SparkSession.builder.appName(f"UsedCars-{settings.run_id}")
        .master(settings.master)
        .config("spark.sql.shuffle.partitions", str(settings.partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.csv.parser.columnPruning.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    return session
