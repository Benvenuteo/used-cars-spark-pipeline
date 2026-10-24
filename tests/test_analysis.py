"""A database outage must not invalidate successfully computed local artifacts."""

import json

import pytest

from used_cars.analysis import analyze
from used_cars.demo import generate_demo
from used_cars.etl import ingest


def test_database_failure_keeps_completed_analysis(spark, tmp_path, settings, monkeypatch):
    source = generate_demo(tmp_path / "cars.csv", rows=100)
    ingest(spark, source, settings)

    def unavailable(*args, **kwargs):
        raise RuntimeError("Database unavailable")

    monkeypatch.setattr("used_cars.analysis.publish", unavailable)
    with pytest.raises(RuntimeError, match="artifacts are complete"):
        analyze(spark, settings, skip_ml=True, mongo_uri="mongodb://unused")
    manifest = json.loads((settings.output / "manifest.json").read_text())
    assert manifest["stages"]["analysis"]["status"] == "complete"
    assert manifest["stages"]["mongo"]["status"] == "failed"
    assert (settings.output / "results.json").is_file()
    assert (settings.output / "report/index.html").is_file()
