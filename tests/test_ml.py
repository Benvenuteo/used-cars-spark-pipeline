"""Check leakage controls, unknown categories, and metric definitions."""

import pytest
from pyspark.ml.feature import ImputerModel
from pyspark.ml.regression import LinearRegression
from pyspark.sql import functions as F

from used_cars.ml import assign_split, price_pipeline, regression_metrics


def test_vehicle_groups_never_cross_splits(spark):
    rows = [(f"listing-{index}-{copy}", f"vin-{index}") for index in range(80) for copy in range(3)]
    data = spark.createDataFrame(rows, ["listing_key", "vehicle_key"])
    result = assign_split(data, 42)
    assert (
        result.groupBy("vehicle_key")
        .agg(F.countDistinct("split").alias("n"))
        .filter("n > 1")
        .count()
        == 0
    )
    first = {row.listing_key: row.split for row in result.collect()}
    second = {row.listing_key: row.split for row in assign_split(data.repartition(3), 42).collect()}
    assert first == second


def test_metrics_have_usd_units(spark):
    data = spark.createDataFrame([(10.0, 8.0), (20.0, 24.0)], ["price", "prediction"])
    result = regression_metrics(data)
    assert result["mae_usd"] == 3
    assert result["rmse_usd"] == pytest.approx(10**0.5)
    assert result["r2"] == pytest.approx(0.6)


def test_preprocessing_uses_train_only_and_handles_unknown_make(spark):
    columns = [
        "price",
        "year",
        "mileage",
        "horsepower",
        "make_name",
        "body_type",
        "fuel_type",
        "transmission",
    ]
    train = spark.createDataFrame(
        [
            (
                10000.0 + i * 500,
                2010 + i % 8,
                float(i * 10000),
                float(100 + i * 10),
                "FORD" if i % 2 else "HONDA",
                "Sedan",
                "Gasoline",
                "A",
            )
            for i in range(12)
        ],
        columns,
    )
    test = spark.createDataFrame(
        [(25000.0, 2019, 10000.0, None, "UNSEEN_MAKE", "UnknownBody", "Gasoline", "A")],
        train.schema,
    )
    model = price_pipeline(LinearRegression(labelCol="price", regParam=0.1, maxIter=10)).fit(train)
    imputer = next(stage for stage in model.stages if isinstance(stage, ImputerModel))
    median = imputer.surrogateDF.first()["horsepower"]
    assert 100 <= median <= 210
    result = model.transform(test).first()
    assert result.horsepower_imputed == median
    assert result.prediction is not None
    # The encoder expands categories into nominal feature dimensions, not one ordinal make index.
    assert len(result.features) > 3 + 1
