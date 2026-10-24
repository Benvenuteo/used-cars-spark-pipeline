"""Shared small Spark fixtures; production data is never required by tests."""

import os
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from used_cars.config import Settings
from used_cars.schema import RAW_COLUMNS, RAW_SCHEMA
from used_cars.spark import create_spark


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> SparkSession:
    os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "--driver-memory 1g pyspark-shell")
    session = create_spark(Settings(tmp_path_factory.mktemp("spark"), "synthetic", "tests"))
    yield session
    session.stop()


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(tmp_path / "run", "synthetic", "tests")


@pytest.fixture
def raw_row():
    def build(**overrides: str | None) -> dict[str, str | None]:
        values = dict.fromkeys(RAW_COLUMNS)
        values.update(
            {
                "vin": "1HGCM82633A004352",
                "listing_id": "123",
                "listed_date": "2020-09-10",
                "make_name": "Honda",
                "model_name": "Accord",
                "year": "2015",
                "price": "15000",
                "mileage": "50000",
                "horsepower": "180",
                "is_new": "False",
                "body_type": "Sedan",
                "fuel_type": "Gasoline",
                "transmission": "A",
                "_corrupt_record": None,
            }
        )
        values.update(overrides)
        return values

    return build


@pytest.fixture
def frame(spark):
    def build(rows):
        return spark.createDataFrame(rows, RAW_SCHEMA)

    return build
