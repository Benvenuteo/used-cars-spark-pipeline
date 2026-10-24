# Audit of the university notebook

The starting material was a text export of a PySpark notebook, a Dockerfile, a Compose file, unpinned Python requirements and the project README. The original work demonstrated SQL/DataFrame operations, visual exploration, linear regression, K-Means and writing aggregate cluster statistics to MongoDB.

## Historical output — not results from this pipeline

The supplied notebook export contained these historical outputs. They have not been used as acceptance targets:

| Output | Notebook export |
| --- | ---: |
| Input rows | 3,000,507 |
| Rows after price/year filter and dropping missing required fields | 1,652,776 |
| Removed rows | 1,347,731 (44.92%) |
| Linear regression RMSE | 13,197.77 USD |
| Linear regression R² | 0.5511 |
| K-Means k | 5 |

The historical population did not explicitly exclude `is_new=True` and dropped rows missing horsepower. The rewritten pipeline excludes new vehicles and imputes missing horsepower from train. Counts and model scores are therefore not directly comparable.

The checked v1 file contains 3,000,040 parsed records in the rewritten multiline-aware reader. The original export reported 3,000,507 with its different parser settings. That historical count is not treated as the correct record count or copied into current results. Physical line counts are not CSV record counts when fields contain newlines.

## Material corrections

- Schema inference was replaced with header-verified explicit raw strings and typed silver columns. Nearly all string types in historical output alone were not proof of malformed source rows.
- The original `StringIndexer` was inside a pipeline fitted on train; no preprocessing leakage was established by that code. The actual regression problem was feeding the make index directly as a numeric ordinal feature. The replacement uses one-hot encoding.
- Random row splitting was replaced with vehicle-group hashing to reduce repeated-vehicle overlap. The old export alone could not establish whether identical VINs actually crossed its split.
- RMSE is now described as root mean squared error, not mean absolute price error. MAE and a train-mean baseline are reported alongside it.
- K=5 was replaced with a small recorded k comparison. Centering was not inherently wrong for the original dense three-field vectors; it remains appropriate here.
- Small aggregate collection for MongoDB was already a reasonable pattern. Collection-wide `delete_many({})` was replaced with run-scoped upserts, a unique index and bounded connection retries.
- Notebook-only execution was replaced with a package/CLI and presentation notebooks that consume package outputs.
- Floating `latest` images and unpinned requirements were replaced with documented runtime versions and Python lock files.

The original text export remains in the user's supplied materials. This repository focuses on the maintained implementation and documents its provenance without copying stale notebook outputs into the current report.
