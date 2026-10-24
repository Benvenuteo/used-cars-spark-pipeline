# Engineering decisions

## Scope and business interpretation

This project analyzes a historical inventory snapshot, not actual sales. Prices are advertised USD amounts, distance is in miles and the target population is explicitly used vehicles. New listings are quarantined as out-of-scope, not labelled data errors. Model year 2021 is permitted because vehicles for the next model year can appear in a September 2020 snapshot.

The original notebook's lower price/year boundaries are retained and made explicit. We do not quietly delete expensive or high-mileage vehicles. They receive flags; the report and squared-error metrics expose their influence. A future sensitivity analysis can compare a separately documented restricted population.

## CSV and storage

The v1 header was checked directly in the archive. Bronze types are deliberately strings: identifiers, postal codes and unit-bearing text must survive ingestion unchanged. Silver casts only analyzed numeric fields. Header validation prevents accidental positional schema misalignment, and `inferSchema` does not add a scan.

The source contains quoted commas and descriptions. `quote='"'`, `escape='"'` and `multiLine=True` correctly parse CSV double-quote escaping and embedded newlines. A single multiline CSV is not split into independent input tasks. Initial ingestion is therefore a throughput bottleneck; bronze Parquet provides parallel, column-pruned reads thereafter. Initial writes stream files of at most 250,000 records without a whole-row repartition: shuffling all description strings before storage created unnecessary disk pressure in the first full-data attempt. Only pruned analytical fields are shuffled downstream. No claim is made that a local two-core session is a large distributed deployment.

The parser's corrupt-record column is only one diagnostic. Spark PERMISSIVE mode can accept token-count mismatches without flagging them. Required numeric/category domain checks add coverage; a complete lexical CSV anomaly detector remains out of scope. The raw file's hash and retained bronze make follow-up inspection possible.

All 66 columns are kept in bronze. Downstream tables keep only the selected analytical fields, quality flags, raw numeric/new-flag strings and identifiers. Record hashes use selected raw fields, not descriptions, so fallback identity is an explicit analytical identity rather than a proof that original CSV bytes were identical.

## Quality and identity

Each record has a single final disposition: invalid, out-of-scope, duplicate or silver. Multiple reasons can apply, but invalid takes priority, and counts of primary reasons reconcile. Missing horsepower is retained for train-only median imputation. Invalid or negative core measurements are quarantined rather than silently coalesced to zero.

Deduplication is per listing ID, with VIN and selected-field-hash fallbacks. Latest valid listed date wins; record hash is the tie-breaker. Listed date represents inventory metadata, so it is not assumed to be a reliable timestamp of a price update. This policy is reproducible, but it cannot reconstruct a full historical sequence. Multiple listing IDs for a vehicle can remain; they share a VIN-group ML split. Missing VINs are reported as a limitation.

## Learning and validation

The split is based on a deterministic vehicle-key hash, not random row splitting. A second independent seed controls optional model sampling by vehicle group. Data preparation learned from values (category frequencies, encoding and horsepower median) is fitted on train only. One-hot encoding avoids assigning an ordinal meaning to makes. Unknown categories are retained in a supported extra category.

Features are deliberately small: year, mileage, horsepower, make, body type, fuel type and transmission. A train-mean baseline, ridge regression and two modest GBT depths provide interpretable comparisons without an expensive parameter search. Validation RMSE chooses a candidate; only the selected candidate receives a final test score. The baseline test score remains a reference. There is no repeated tuning against the test partition and no performance threshold chosen after seeing results.

K-Means is descriptive and uses price as one of its features. The standardized three-dimensional vectors are dense, so mean-centering is appropriate here. Candidate k values and in-sample silhouette are recorded; stability over different seeds and external business validation remain future work.

## Execution and publication

Intermediate classified data and sampled ML rows use disk persistence to limit heap use; these caches are released explicitly. Raw datasets are never converted to pandas or collected at the driver. Only small, bounded SQL results and segment summaries are collected. `.count()` is used to materialize required audit and sample counts, not repeatedly for notebook exploration.

MongoDB stores only quality, market and model summaries. A MongoDB Spark connector is unnecessary for these few documents. Stable IDs plus a unique `(run_id, section)` index make upserts safe to retry. Different runs coexist. No `delete_many({})` is used. A database failure does not justify erasing local computed results; publication can be retried with `cars publish`.

Secrets are read from environment variables and are not written into manifests. MongoDB has no host port in Compose. Its unauthenticated local demo service stays inside the Compose network. A real hosted database needs its own authentication and network policy. The notebook server retains token authentication and binds its published host port to localhost.

## Reproducibility boundaries

The manifest records the input SHA-256, settings, Python/Spark/Java versions, source size, stage timing, seed and split counts. Primary and transitive Python dependencies are pinned. OS packages are installed from security-updated Debian repositories; the Dockerfile does not claim immutable binary reproducibility. Floating-point reductions and clustering can vary slightly with partitioning/platform. These limits belong beside measured results, not hidden behind a generic “reproducible” claim.

To refresh compatible lock files with `uv`, compile the core first, then constrain extras to the preceding lock. This prevents development and notebook extras from independently pinning conflicting versions of shared dependencies:

```bash
uv pip compile pyproject.toml -o requirements.txt
uv pip compile pyproject.toml --extra dev -c requirements.txt -o requirements-dev.txt
uv pip compile pyproject.toml --extra notebooks -c requirements-dev.txt -o requirements-notebooks.txt
```

The committed benchmark used a requested 2 GB driver heap, `local[2]`, eight shuffle partitions and an 8 GiB container memory limit. Its record includes the individual timings and uncontrollable OS-cache caveat. The initial shuffle-heavy attempt was interrupted and is not included in reported successful ETL timings.
