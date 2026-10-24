"""Run explicit Spark SQL, optional ML, aggregate publication and report generation."""

import json
import logging
import time
from importlib.resources import files
from typing import Any

from pyspark.sql import SparkSession

from used_cars.config import Settings
from used_cars.io import utc_now, write_json
from used_cars.ml import run_models
from used_cars.mongo import publish
from used_cars.report import render_report

LOGGER = logging.getLogger(__name__)


def analyze(
    spark: SparkSession, settings: Settings, skip_ml: bool = False, mongo_uri: str | None = None
) -> dict[str, Any]:
    """Restart from silver without reparsing the large CSV or recomputing quality rules."""
    root = settings.output
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    if manifest["stages"].get("ingest", {}).get("status") != "complete":
        raise ValueError("A completed ingestion stage is required before analysis")
    if (root / "gold").exists() or (root / "models").exists():
        raise ValueError(
            "Analysis artifacts already exist. Use a fresh run directory for another model run"
        )
    started = time.perf_counter()
    try:
        silver = spark.read.parquet(str(root / "silver"))
        silver.createOrReplaceTempView("cars")
        aggregates = {}
        for name in ("overview", "by_make", "by_year", "by_mileage"):
            query = files("used_cars").joinpath(f"sql/{name}.sql").read_text(encoding="utf-8")
            result = spark.sql(query)
            result.write.parquet(str(root / "gold" / name))
            aggregates[name] = [
                row.asDict() for row in spark.read.parquet(str(root / "gold" / name)).collect()
            ]
        # File reads do not guarantee ordering, so restore display order explicitly.
        aggregates["by_make"].sort(key=lambda row: (-row["listings"], row["make_name"]))
        aggregates["by_year"].sort(key=lambda row: row["year"])
        aggregates["by_mileage"].sort(key=lambda row: row["mileage_band_miles"])
        payload: dict[str, Any] = {
            "quality": json.loads((root / "quality.json").read_text(encoding="utf-8")),
            "market": aggregates,
        }
        if not skip_ml:
            payload["models"] = run_models(silver, settings)
        manifest["stages"]["analysis"] = {
            "status": "complete",
            "seconds": round(time.perf_counter() - started, 3),
            "ml_skipped": skip_ml,
            "completed_at": utc_now(),
        }
        write_json(root / "manifest.json", manifest)
        write_json(root / "results.json", payload)
        render_report(root, payload)
    except Exception as exc:
        manifest["stages"]["analysis"] = {"status": "failed", "error_type": type(exc).__name__}
        write_json(root / "manifest.json", manifest)
        raise
    if mongo_uri:
        try:
            documents = publish(payload, settings.run_id, settings.dataset_kind, mongo_uri)
            manifest["stages"]["mongo"] = {"status": "complete", "documents": documents}
        except Exception as exc:
            manifest["stages"]["mongo"] = {"status": "failed", "error_type": type(exc).__name__}
            write_json(root / "manifest.json", manifest)
            raise RuntimeError(
                "Analysis artifacts are complete, but MongoDB publication failed. "
                "Check connectivity and retry with cars publish."
            ) from None
        write_json(root / "manifest.json", manifest)
    LOGGER.info("Analysis complete. Report: %s", root / "report" / "index.html")
    return payload
