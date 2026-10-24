"""Export only small aggregate artifacts; keep local data and model rows outside Git."""

import argparse
import json
import shutil
from pathlib import Path

from used_cars.io import write_json


def export(source: Path, destination: Path) -> None:
    """Copy reviewable results and normalize only the machine-specific output directory."""
    manifest = json.loads((source / "manifest.json").read_text(encoding="utf-8"))
    if manifest["stages"].get("analysis", {}).get("status") != "complete":
        raise ValueError("Only completed analyses can be exported")
    destination.mkdir(parents=True, exist_ok=True)
    manifest["settings"]["output"] = f"outputs/{manifest['run_id']}"
    manifest["export_note"] = "Machine-specific output path normalized; metrics are unchanged"
    write_json(destination / "manifest.json", manifest)
    for name in (
        "quality.json",
        "results.json",
        "regression.json",
        "clustering.json",
        "benchmark.json",
        "benchmark.csv",
    ):
        if (source / name).exists():
            shutil.copyfile(source / name, destination / name)
    shutil.copytree(source / "report", destination / "report", dirs_exist_ok=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    export(args.source, args.destination)
