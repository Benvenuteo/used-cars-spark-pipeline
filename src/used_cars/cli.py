"""Command-line entry points for demonstration and full-data execution."""

import argparse
import json
import logging
import os
from pathlib import Path

from used_cars.config import Settings, settings_from_manifest


def parser() -> argparse.ArgumentParser:
    """Describe commands without starting a JVM for --help, demo generation or download."""
    root = argparse.ArgumentParser(description="Used-car PySpark pipeline")
    commands = root.add_subparsers(dest="command", required=True)
    demo = commands.add_parser("demo", help="Generate synthetic data, not a Kaggle sample")
    demo.add_argument("--output", type=Path, default=Path("data/demo/used_cars_data.csv"))
    demo.add_argument("--rows", type=int, default=1200)
    demo.add_argument("--seed", type=int, default=42)
    download = commands.add_parser("download", help="Download Kaggle dataset version 1")
    download.add_argument("--destination", type=Path, default=Path("data/raw"))
    for command in ("run", "ingest"):
        sub = commands.add_parser(
            command, help="ETL and analysis" if command == "run" else "ETL only"
        )
        sub.add_argument("--input", type=Path, required=True)
        sub.add_argument("--output", type=Path, required=True)
        sub.add_argument("--run-id", required=True)
        sub.add_argument("--dataset-kind", choices=("synthetic", "kaggle"), required=True)
        sub.add_argument("--master", default="local[2]")
        sub.add_argument("--partitions", type=int, default=4)
        sub.add_argument("--seed", type=int, default=42)
        sub.add_argument("--model-fraction", type=float, default=1.0)
        sub.add_argument("--cluster-k", type=int, nargs="+", default=[3, 4, 5])
        if command == "run":
            sub.add_argument("--skip-ml", action="store_true")
            sub.add_argument("--publish-mongo", action="store_true")
    analyze = commands.add_parser("analyze", help="Start analysis from existing silver Parquet")
    analyze.add_argument("--output", type=Path, required=True)
    analyze.add_argument("--skip-ml", action="store_true")
    analyze.add_argument("--publish-mongo", action="store_true")
    publish = commands.add_parser(
        "publish", help="Idempotently publish existing JSON results to MongoDB"
    )
    publish.add_argument("--output", type=Path, required=True)
    bench = commands.add_parser("benchmark", help="Measure equivalent CSV and bronze Parquet scans")
    bench.add_argument("--input", type=Path, required=True)
    bench.add_argument("--output", type=Path, required=True, help="Existing run directory")
    bench.add_argument("--repeats", type=int, default=3)
    return root


def main() -> None:
    """Dispatch with actionable failures and a clean Spark shutdown."""
    arguments = parser().parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    # Import heavier components only after parsing the command.
    if arguments.command == "demo":
        from used_cars.demo import generate_demo

        print(generate_demo(arguments.output, arguments.rows, arguments.seed))
        return
    if arguments.command == "download":
        from used_cars.download import download_dataset

        print(download_dataset(arguments.destination))
        return
    if arguments.command in {"analyze", "publish", "benchmark"}:
        settings = settings_from_manifest(arguments.output)
    else:
        settings = Settings(
            output=arguments.output,
            dataset_kind=arguments.dataset_kind,
            run_id=arguments.run_id,
            master=arguments.master,
            partitions=arguments.partitions,
            seed=arguments.seed,
            model_fraction=arguments.model_fraction,
            cluster_candidates=tuple(arguments.cluster_k),
        )
    uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
    if arguments.command == "publish":
        from used_cars.io import utc_now, write_json
        from used_cars.mongo import publish

        payload = json.loads((settings.output / "results.json").read_text(encoding="utf-8"))
        count = publish(payload, settings.run_id, settings.dataset_kind, uri)
        manifest_path = settings.output / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["stages"]["mongo"] = {
            "status": "complete",
            "documents": count,
            "published_at": utc_now(),
        }
        write_json(manifest_path, manifest)
        print(f"Published {count} documents")
        return
    from used_cars.spark import create_spark

    spark = create_spark(settings)
    try:
        if arguments.command == "benchmark":
            from used_cars.benchmark import benchmark

            benchmark(
                spark,
                arguments.input,
                settings.output / "bronze",
                settings.output,
                arguments.repeats,
            )
            return
        if arguments.command in {"ingest", "run"}:
            from used_cars.etl import ingest

            ingest(spark, arguments.input, settings)
        if arguments.command in {"analyze", "run"}:
            from used_cars.analysis import analyze

            analyze(spark, settings, arguments.skip_ml, uri if arguments.publish_mongo else None)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
