"""Auditable CSV -> bronze -> classified -> deduplicated silver transformations."""

import csv
import json
import logging
import time
from pathlib import Path
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from used_cars.config import Settings
from used_cars.io import environment, fingerprint, utc_now, write_json
from used_cars.schema import RAW_COLUMNS, RAW_SCHEMA

LOGGER = logging.getLogger(__name__)
SELECTED_COLUMNS = (
    "vin",
    "listing_id",
    "listed_date",
    "make_name",
    "model_name",
    "body_type",
    "fuel_type",
    "transmission",
    "year",
    "price",
    "mileage",
    "horsepower",
    "is_new",
    "_corrupt_record",
)


def read_source(spark: SparkSession, path: Path) -> DataFrame:
    """Validate positional schema, then parse RFC-style quoting and multiline fields."""
    if not path.is_file():
        raise FileNotFoundError(f"Input CSV does not exist: {path}")
    with path.open(encoding="utf-8-sig", newline="") as stream:
        header = next(csv.reader(stream), [])
    if header != list(RAW_COLUMNS):
        raise ValueError("CSV header does not match the 66-column Kaggle v1 schema")
    return (
        spark.read.schema(RAW_SCHEMA)
        .option("header", True)
        .option("encoding", "UTF-8")
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("maxCharsPerColumn", 8_000_000)
        .csv(str(path.resolve()))
    )


def _finite(name: str) -> F.Column:
    """Reject missing, NaN and infinite numeric values."""
    column = F.col(name)
    return column.isNotNull() & ~F.isnan(column) & (F.abs(column) < F.lit(1e308))


def classify(bronze: DataFrame, settings: Settings) -> DataFrame:
    """Separate invalid data from valid listings outside the declared study population."""
    selected = bronze.select(*SELECTED_COLUMNS)
    raw_names = ("year", "price", "mileage", "horsepower", "is_new")
    for name in raw_names:
        selected = selected.withColumnRenamed(name, f"{name}_raw")
    prepared = (
        selected.withColumn("record_hash", F.sha2(F.to_json(F.struct(*selected.columns)), 256))
        .withColumn("vin", F.upper(F.trim("vin")))
        .withColumn("listing_id", F.trim("listing_id"))
        .withColumn("make_name", F.upper(F.trim("make_name")))
        .withColumn("model_name", F.trim("model_name"))
        .withColumn("year_number", F.col("year_raw").cast("double"))
        .withColumn("year", F.col("year_number").cast("int"))
        .withColumn("price", F.col("price_raw").cast("double"))
        .withColumn("mileage", F.col("mileage_raw").cast("double"))
        .withColumn("horsepower_number", F.col("horsepower_raw").cast("double"))
        .withColumn("listed_date", F.to_date("listed_date", "yyyy-MM-dd"))
        .withColumn("new_text", F.lower(F.trim("is_new_raw")))
    )
    invalid = {
        "csv_malformed": F.col("_corrupt_record").isNotNull(),
        "invalid_price": ~_finite("price"),
        "invalid_year": ~_finite("year_number") | (F.col("year_number") != F.col("year")),
        "invalid_mileage": ~_finite("mileage") | (F.col("mileage") < 0),
        "missing_make": F.col("make_name").isNull() | (F.col("make_name") == ""),
        "unknown_new_flag": F.col("new_text").isNull() | ~F.col("new_text").isin("true", "false"),
    }
    excluded = {
        "new_vehicle": F.col("new_text") == "true",
        "price_below_scope": F.col("price") <= settings.min_price,
        "year_outside_scope": ~F.col("year").between(settings.min_year, settings.max_year),
    }

    def reasons(conditions: dict[str, F.Column]) -> F.Column:
        return F.filter(
            F.array(*[F.when(condition, F.lit(name)) for name, condition in conditions.items()]),
            lambda value: value.isNotNull(),
        )

    valid_vin = F.col("vin").rlike("^[A-HJ-NPR-Z0-9]{17}$")
    valid_listing = F.col("listing_id").isNotNull() & (F.col("listing_id") != "")
    prepared = (
        prepared.withColumn("invalid_reasons", reasons(invalid))
        .withColumn("excluded_reasons", reasons(excluded))
        .withColumn("reasons", F.concat("invalid_reasons", "excluded_reasons"))
        .withColumn(
            "route",
            F.when(F.size("invalid_reasons") > 0, "invalid")
            .when(F.size("excluded_reasons") > 0, "out_of_scope")
            .otherwise("accepted"),
        )
        .withColumn("primary_reason", F.element_at("reasons", 1))
        .withColumn(
            "listing_key",
            F.when(valid_listing, F.concat(F.lit("listing:"), F.col("listing_id")))
            .when(valid_vin, F.concat(F.lit("vin:"), F.col("vin")))
            .otherwise(F.concat(F.lit("row:"), F.col("record_hash"))),
        )
        .withColumn(
            "vehicle_key",
            F.when(valid_vin, F.concat(F.lit("vin:"), F.col("vin"))).otherwise(
                F.col("listing_key")
            ),
        )
        .withColumn("vin_usable", F.coalesce(valid_vin, F.lit(False)))
        .withColumn(
            "horsepower",
            F.when(
                _finite("horsepower_number") & (F.col("horsepower_number") > 0),
                F.col("horsepower_number"),
            ),
        )
        .withColumn("horsepower_missing", F.col("horsepower").isNull())
        .withColumn(
            "extreme_value",
            (F.col("price") > 150_000)
            | (F.col("mileage") > 300_000)
            | F.coalesce(F.col("horsepower") > 1000, F.lit(False)),
        )
    )
    return prepared.drop(
        "invalid_reasons",
        "excluded_reasons",
        "year_number",
        "new_text",
        "horsepower_number",
        "_corrupt_record",
    )


def deduplicate(classified: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Keep the latest dated row per listing, with a stable hash as the final tie-breaker."""
    window = Window.partitionBy("listing_key").orderBy(
        F.col("listed_date").desc_nulls_last(), F.col("record_hash").desc()
    )
    ranked = classified.filter(F.col("route") == "accepted").withColumn(
        "duplicate_rank", F.row_number().over(window)
    )
    silver = ranked.filter("duplicate_rank = 1").drop("duplicate_rank")
    duplicates = (
        ranked.filter("duplicate_rank > 1")
        .drop("duplicate_rank")
        .withColumn("route", F.lit("duplicate"))
        .withColumn("primary_reason", F.lit("duplicate_listing"))
        .withColumn("reasons", F.array(F.lit("duplicate_listing")))
    )
    quarantine = classified.filter(F.col("route") != "accepted").unionByName(duplicates)
    return silver, quarantine


def ingest(spark: SparkSession, source: Path, settings: Settings) -> dict[str, Any]:
    """Persist restartable layers and reconcile every input row with a final disposition."""
    root = settings.output
    if root.exists() and any(root.iterdir()):
        raise ValueError(f"Output is not empty: {root}. Choose a new run directory")
    root.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    manifest: dict[str, Any] = {
        "run_id": settings.run_id,
        "dataset_kind": settings.dataset_kind,
        "settings": settings.to_dict(),
        "started_at": utc_now(),
        "source": fingerprint(source),
        "environment": environment(),
        "stages": {},
    }
    write_json(root / "manifest.json", manifest)
    try:
        LOGGER.info("Writing bronze Parquet; quoted multiline CSV is parsed as one input stream")
        bronze_start = time.perf_counter()
        # Do not shuffle the 10 GB string-heavy CSV before column pruning. A single
        # input task streams bounded Parquet files; subsequent reads are parallel.
        read_source(spark, source).write.option("maxRecordsPerFile", 250_000).mode("error").parquet(
            str(root / "bronze")
        )
        bronze_seconds = time.perf_counter() - bronze_start
        classified = classify(spark.read.parquet(str(root / "bronze")), settings).persist(
            StorageLevel.DISK_ONLY
        )
        try:
            routes = {
                row["route"]: row["count"] for row in classified.groupBy("route").count().collect()
            }
            silver, quarantine = deduplicate(classified)
            silver.repartition(settings.partitions).write.mode("error").parquet(
                str(root / "silver")
            )
            quarantine.repartition(settings.partitions).write.mode("error").parquet(
                str(root / "quarantine")
            )
        finally:
            classified.unpersist()
        stored_silver = spark.read.parquet(str(root / "silver"))
        stored_quarantine = spark.read.parquet(str(root / "quarantine"))
        silver_count = stored_silver.count()
        dispositions = [
            row.asDict()
            for row in stored_quarantine.groupBy("route", "primary_reason")
            .count()
            .orderBy("route", "primary_reason")
            .collect()
        ]
        quarantine_count = sum(row["count"] for row in dispositions)
        input_count = sum(routes.values())
        if input_count != silver_count + quarantine_count:
            raise RuntimeError("Quality reconciliation failed: input != silver + quarantine")
        counts = {
            "input_rows": input_count,
            "invalid_rows": routes.get("invalid", 0),
            "out_of_scope_rows": routes.get("out_of_scope", 0),
            "duplicate_rows": routes.get("accepted", 0) - silver_count,
            "silver_rows": silver_count,
            "quarantine_rows": quarantine_count,
        }
        quality = {
            **counts,
            "reconciled": True,
            "primary_dispositions": dispositions,
            "all_reason_counts_overlap": [
                row.asDict()
                for row in stored_quarantine.select(F.explode("reasons").alias("reason"))
                .groupBy("reason")
                .count()
                .orderBy("reason")
                .collect()
            ],
            "silver_flags": stored_silver.agg(
                F.sum(F.col("horsepower_missing").cast("int")).alias("horsepower_missing"),
                F.sum((~F.col("vin_usable")).cast("int")).alias("vin_unusable"),
                F.sum(F.col("extreme_value").cast("int")).alias("extreme_value"),
            )
            .first()
            .asDict(),
        }
        write_json(root / "quality.json", quality)
        manifest["stages"]["ingest"] = {
            "status": "complete",
            "seconds": round(time.perf_counter() - started, 3),
            "csv_to_bronze_seconds": round(bronze_seconds, 3),
            **counts,
        }
        write_json(root / "manifest.json", manifest)
        LOGGER.info("Ingestion complete: %s", json.dumps(counts))
        if silver_count == 0:
            raise ValueError(
                "No in-scope valid listings remain. Inspect quality.json and quarantine"
            )
        return manifest
    except Exception as exc:
        manifest["stages"]["ingest"] = {"status": "failed", "error_type": type(exc).__name__}
        write_json(root / "manifest.json", manifest)
        raise
