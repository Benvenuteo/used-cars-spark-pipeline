# Optional Azure / Databricks extension

This is a plan, not a claim that the current project has already run on Azure.

1. Upload the personally downloaded source to an authorized ADLS Gen2 location. Preserve the source version and SHA-256. Keep credentials out of notebooks and Git.
2. Package `used_cars` as a wheel and install it on a compatible Databricks runtime. Use the platform Spark session instead of the local `create_spark` helper.
3. Replace local `pathlib`/Python file I/O in source validation, manifests, reports and downloads with volume/cloud-aware adapters. The existing CLI is intentionally local; passing an `abfss://` string to it is insufficient.
4. Store bronze, silver and quarantine as Delta tables in the configured catalog/schema. Preserve the documented reason codes and disposition reconciliation.
5. Parameterize source path, target catalog, run ID, model fraction and snapshot boundaries in a Databricks Workflow with separate ingest and analysis tasks.
6. Track candidate metrics, parameters and artifacts in MLflow; record the grouped split and data version. Register a model only if required by a concrete serving use case.
7. Replace direct local MongoDB access with an approved secret-backed destination if aggregate publication is needed. The HTML/JSON export can remain a simpler delivery option.
8. Add a genuine remote job run, execution evidence and cost/volume measurements before writing “Azure/Databricks experience” in the project description.

The domain transformations, Spark SQL queries, quality rules and ML validation strategy are reusable. Storage, session creation, orchestration and credentials require platform-specific work.
