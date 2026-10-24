"""Train-only preprocessing, vehicle-group splits, validation selection and honest metrics."""

import logging
from typing import Any

from pyspark import StorageLevel
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import (
    Imputer,
    OneHotEncoder,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)
from pyspark.ml.regression import GBTRegressor, LinearRegression
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from used_cars.config import Settings
from used_cars.io import write_json

LOGGER = logging.getLogger(__name__)
CATEGORICAL = ("make_name", "body_type", "fuel_type", "transmission")


def assign_split(data: DataFrame, seed: int) -> DataFrame:
    """Assign all rows of a vehicle to a deterministic 70/15/15 hash bucket."""
    bucket = F.pmod(F.xxhash64("vehicle_key", F.lit(seed)), F.lit(100))
    return data.withColumn(
        "split", F.when(bucket < 70, "train").when(bucket < 85, "validation").otherwise("test")
    )


def regression_metrics(predictions: DataFrame) -> dict[str, float | int | None]:
    """Compute USD MAE/RMSE and R-squared in a single aggregation."""
    error = F.col("price") - F.col("prediction")
    row = predictions.agg(
        F.count("price").alias("n"),
        F.avg(F.abs(error)).alias("mae_usd"),
        F.avg(error * error).alias("mse"),
        F.sum(error * error).alias("sse"),
        F.sum("price").alias("sum_y"),
        F.sum(F.col("price") ** 2).alias("sum_y2"),
    ).first()
    if not row["n"]:
        raise ValueError("Cannot evaluate an empty partition")
    sst = row["sum_y2"] - row["sum_y"] ** 2 / row["n"]
    return {
        "rows": row["n"],
        "mae_usd": row["mae_usd"],
        "rmse_usd": row["mse"] ** 0.5,
        "r2": 1 - row["sse"] / sst if sst > 0 else None,
    }


def price_pipeline(regressor: Any) -> Pipeline:
    """Use nominal one-hot encoding and a horsepower median learned only from train."""
    indexers = [
        StringIndexer(inputCol=name, outputCol=f"{name}_index", handleInvalid="keep")
        for name in CATEGORICAL
    ]
    encoder = OneHotEncoder(
        inputCols=[f"{name}_index" for name in CATEGORICAL],
        outputCols=[f"{name}_encoded" for name in CATEGORICAL],
        handleInvalid="keep",
    )
    imputer = Imputer(
        inputCols=["horsepower"], outputCols=["horsepower_imputed"], strategy="median"
    )
    assembler = VectorAssembler(
        inputCols=["year", "mileage", "horsepower_imputed"]
        + [f"{name}_encoded" for name in CATEGORICAL],
        outputCol="features",
    )
    return Pipeline(stages=[*indexers, encoder, imputer, assembler, regressor])


def train_prices(data: DataFrame, settings: Settings) -> dict[str, Any]:
    """Select between ridge and two small GBT configurations using validation RMSE."""
    split = assign_split(data, settings.seed).persist(StorageLevel.DISK_ONLY)
    try:
        counts = {row["split"]: row["count"] for row in split.groupBy("split").count().collect()}
        if any(counts.get(name, 0) < 10 for name in ("train", "validation", "test")):
            raise ValueError(f"Need at least 10 rows in each split; observed {counts}")
        train = split.filter("split = 'train'")
        validation = split.filter("split = 'validation'")
        test = split.filter("split = 'test'")
        train_summary = train.agg(
            F.avg("price").alias("mean_price"), F.count("horsepower").alias("hp_count")
        ).first()
        if not train_summary["hp_count"]:
            raise ValueError("No usable horsepower values in train; cannot fit median imputation")
        mean_price = train_summary["mean_price"]
        baseline = {
            "train_mean_usd": mean_price,
            "validation": regression_metrics(
                validation.withColumn("prediction", F.lit(mean_price))
            ),
            "test": regression_metrics(test.withColumn("prediction", F.lit(mean_price))),
        }
        candidates = [
            ("ridge", LinearRegression(labelCol="price", maxIter=60, regParam=0.1)),
            (
                "gbt_depth3",
                GBTRegressor(labelCol="price", maxIter=20, maxDepth=3, seed=settings.seed),
            ),
            (
                "gbt_depth5",
                GBTRegressor(labelCol="price", maxIter=20, maxDepth=5, seed=settings.seed),
            ),
        ]
        scores = []
        best_model: PipelineModel | None = None
        best_name = ""
        best_rmse = float("inf")
        for name, estimator in candidates:
            LOGGER.info("Fitting asking-price candidate %s", name)
            model = price_pipeline(estimator).fit(train)
            metrics = regression_metrics(model.transform(validation))
            scores.append({"model": name, "validation": metrics})
            if metrics["rmse_usd"] < best_rmse:
                best_rmse = metrics["rmse_usd"]
                best_model = model
                best_name = name
        assert best_model is not None
        metrics = regression_metrics(best_model.transform(test))
        best_model.write().save(str(settings.output / "models" / "price_pipeline"))
        split.select("listing_key", "vehicle_key", "split").write.parquet(
            str(settings.output / "models" / "split_assignments")
        )
        result = {
            "split_policy": "VIN group hash 70/15/15; listing/hash fallback when VIN is unusable",
            "split_rows": counts,
            "baseline": baseline,
            "candidates": scores,
            "selected_model": best_name,
            "selection_metric": "validation RMSE in USD",
            "test": metrics,
            "target": "advertised asking price; not transaction price",
        }
        write_json(settings.output / "regression.json", result)
        return result
    finally:
        split.unpersist()


def cluster_listings(data: DataFrame, settings: Settings) -> dict[str, Any]:
    """Compare a few k values on standardized numeric features; report descriptive segments."""
    assembler = VectorAssembler(inputCols=["price", "mileage", "year"], outputCol="cluster_raw")
    scaling = Pipeline(
        stages=[
            assembler,
            StandardScaler(
                inputCol="cluster_raw", outputCol="cluster_features", withMean=True, withStd=True
            ),
        ]
    ).fit(data)
    features = scaling.transform(data).persist(StorageLevel.DISK_ONLY)
    try:
        unique = features.select("price", "mileage", "year").distinct().count()
        evaluator = ClusteringEvaluator(
            featuresCol="cluster_features",
            predictionCol="cluster",
            metricName="silhouette",
            distanceMeasure="squaredEuclidean",
        )
        candidates = []
        best_model = None
        best_score = -float("inf")
        for k in settings.cluster_candidates:
            if k >= unique:
                continue
            model = KMeans(
                k=k,
                seed=settings.seed,
                maxIter=30,
                featuresCol="cluster_features",
                predictionCol="cluster",
            ).fit(features)
            predictions = model.transform(features)
            actual_clusters = predictions.select("cluster").distinct().count()
            if actual_clusters < 2:
                continue
            score = evaluator.evaluate(predictions)
            candidates.append({"k": k, "silhouette_squared_euclidean": score})
            if score > best_score:
                best_score, best_model = score, model
        if best_model is None:
            raise ValueError("Not enough distinct observations to form at least two clusters")
        selected = best_model.transform(features)
        stats = (
            selected.groupBy("cluster")
            .agg(
                F.count("*").alias("listings"),
                F.avg("price").alias("mean_price_usd"),
                F.avg("mileage").alias("mean_mileage_miles"),
                F.avg("year").alias("mean_model_year"),
            )
            .orderBy("cluster")
        )
        rows = [row.asDict() for row in stats.collect()]
        stats.write.parquet(str(settings.output / "gold" / "clusters"))
        best_model.write().save(str(settings.output / "models" / "kmeans"))
        scaling.write().save(str(settings.output / "models" / "cluster_scaler"))
        result = {
            "features": ["asking_price_usd", "mileage_miles", "model_year"],
            "selected_k": best_model.getK(),
            "seed": settings.seed,
            "candidates": candidates,
            "clusters": rows,
            "limitation": "Exploratory in-sample silhouette; not an independent validation of segments",
        }
        write_json(settings.output / "clustering.json", result)
        return result
    finally:
        features.unpersist()


def run_models(silver: DataFrame, settings: Settings) -> dict[str, Any]:
    """Optionally sample complete vehicle groups before splitting or clustering."""
    threshold = int(settings.model_fraction * 1_000_000)
    sampled = silver.filter(
        F.pmod(F.xxhash64("vehicle_key", F.lit(settings.seed + 1)), F.lit(1_000_000)) < threshold
    ).persist(StorageLevel.DISK_ONLY)
    try:
        rows = sampled.count()
        LOGGER.info("Model input: %d rows; requested fraction %.4f", rows, settings.model_fraction)
        return {
            "input_rows": rows,
            "requested_fraction": settings.model_fraction,
            "regression": train_prices(sampled, settings),
            "clustering": cluster_listings(sampled, settings),
        }
    finally:
        sampled.unpersist()
