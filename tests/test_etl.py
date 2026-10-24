"""Exercise data contracts and adverse records rather than duplicating implementation."""

import csv
import json

import pytest
from pyspark.sql import functions as F

from used_cars.config import Settings
from used_cars.demo import generate_demo
from used_cars.etl import classify, deduplicate, ingest, read_source
from used_cars.schema import RAW_COLUMNS


def test_csv_quotes_multiline_and_postal_code(spark, tmp_path, raw_row):
    path = tmp_path / "cars.csv"
    record = raw_row(description='Comma, quote "inside"\nand newline', dealer_zip="00960")
    record.pop("_corrupt_record")
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=RAW_COLUMNS)
        writer.writeheader()
        writer.writerow(record)
    result = read_source(spark, path).first()
    assert result.description == record["description"]
    assert result.dealer_zip == "00960"
    assert result._corrupt_record is None


def test_rejects_reordered_header(spark, tmp_path):
    path = tmp_path / "wrong.csv"
    path.write_text(",".join(reversed(RAW_COLUMNS)) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="header"):
        read_source(spark, path)


@pytest.mark.parametrize(
    ("changes", "route", "reason"),
    [
        ({"price": "broken"}, "invalid", "invalid_price"),
        ({"price": "NaN"}, "invalid", "invalid_price"),
        ({"price": "Infinity"}, "invalid", "invalid_price"),
        ({"mileage": "-1"}, "invalid", "invalid_mileage"),
        ({"year": "2015.5"}, "invalid", "invalid_year"),
        ({"is_new": "maybe"}, "invalid", "unknown_new_flag"),
        ({"is_new": "True"}, "out_of_scope", "new_vehicle"),
        ({"year": "1980"}, "out_of_scope", "year_outside_scope"),
        ({"price": "500"}, "out_of_scope", "price_below_scope"),
        ({"_corrupt_record": "malformed"}, "invalid", "csv_malformed"),
    ],
)
def test_quality_rules(frame, raw_row, settings, changes, route, reason):
    row = classify(frame([raw_row(**changes)]), settings).first()
    assert row.route == route
    assert reason in row.reasons


def test_missing_hp_and_extremes_are_retained(frame, raw_row, settings):
    result = classify(frame([raw_row(horsepower="unknown", price="250000")]), settings).first()
    assert result.route == "accepted"
    assert result.horsepower is None
    assert result.horsepower_missing
    assert result.extreme_value


def test_latest_duplicate_is_kept_deterministically(frame, raw_row, settings):
    source = frame(
        [
            raw_row(listed_date="2020-09-10", price="14000"),
            raw_row(listed_date="2020-09-11", price="14500"),
        ]
    )
    for partitions in (1, 2):
        silver, quarantine = deduplicate(classify(source.repartition(partitions), settings))
        assert silver.first().price == 14500
        assert quarantine.first().primary_reason == "duplicate_listing"
        assert silver.count() + quarantine.count() == 2


def test_end_to_end_reconciliation(spark, settings, tmp_path):
    path = generate_demo(tmp_path / "demo.csv", rows=200)
    ingest(spark, path, settings)
    quality = json.loads((settings.output / "quality.json").read_text())
    assert quality["input_rows"] == 200
    assert quality["reconciled"]
    assert quality["input_rows"] == sum(
        quality[key]
        for key in ("invalid_rows", "out_of_scope_rows", "duplicate_rows", "silver_rows")
    )
    silver = spark.read.parquet(str(settings.output / "silver"))
    assert silver.filter(F.col("price").isNull()).count() == 0
    assert silver.select("listing_key").distinct().count() == silver.count()
    with pytest.raises(ValueError, match="not empty"):
        ingest(spark, path, settings)


def test_empty_scope_is_reported(spark, tmp_path, raw_row):
    path = tmp_path / "empty.csv"
    row = raw_row(is_new="True")
    row.pop("_corrupt_record")
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=RAW_COLUMNS)
        writer.writeheader()
        writer.writerow(row)
    settings = Settings(tmp_path / "empty-run", "synthetic", "empty-test")
    with pytest.raises(ValueError, match="No in-scope"):
        ingest(spark, path, settings)
    assert json.loads((settings.output / "quality.json").read_text())["silver_rows"] == 0
