"""A small, repeated projection workload; never manufacture a claimed Parquet speedup."""

import csv
import statistics
import time
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from used_cars.etl import read_source
from used_cars.io import environment, write_json


def benchmark(
    spark: SparkSession, source: Path, bronze: Path, output: Path, repeats: int = 3
) -> dict[str, Any]:
    """Alternate CSV and equivalent bronze Parquet scans; OS filesystem caches remain uncontrolled."""
    if repeats < 2:
        raise ValueError("At least two repetitions are required")
    timings = []
    reference = None
    for repeat in range(repeats):
        formats = ("csv", "parquet") if repeat % 2 == 0 else ("parquet", "csv")
        for format_name in formats:
            spark.catalog.clearCache()
            started = time.perf_counter()
            data = (
                read_source(spark, source)
                if format_name == "csv"
                else spark.read.parquet(str(bronze))
            )
            row = (
                data.select(F.col("price").cast("double").alias("price"))
                .agg(F.count("*").alias("rows"), F.sum("price").alias("sum_price"))
                .first()
            )
            seconds = time.perf_counter() - started
            if reference is None:
                reference = row.asDict()
            if (
                row["rows"] != reference["rows"]
                or abs(row["sum_price"] - reference["sum_price"]) > 1
            ):
                raise RuntimeError("Benchmark inputs do not contain equivalent rows/prices")
            timings.append(
                {
                    "repeat": repeat + 1,
                    "format": format_name,
                    "seconds": seconds,
                    "rows": row["rows"],
                    "sum_price": row["sum_price"],
                }
            )
    medians = {
        name: statistics.median(row["seconds"] for row in timings if row["format"] == name)
        for name in ("csv", "parquet")
    }
    result = {
        "workload": "count all rows and sum cast price; CSV is multiline and Parquet column-pruned",
        "method": "alternating order; Spark cache cleared; OS cache not cleared; no warmup excluded",
        "environment": environment(),
        "measurements": timings,
        "median_seconds": medians,
        "csv_over_parquet_ratio": medians["csv"] / medians["parquet"],
    }
    write_json(output / "benchmark.json", result)
    with (output / "benchmark.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(timings[0]))
        writer.writeheader()
        writer.writerows(timings)
    return result
