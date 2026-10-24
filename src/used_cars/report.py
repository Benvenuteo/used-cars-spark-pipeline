"""Portable HTML report generated from measured run artifacts and bounded aggregates."""

import html
import json
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402


def _table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "<p>No records.</p>"
    names = list(rows[0])
    head = "".join(f"<th>{html.escape(str(name))}</th>" for name in names)
    body = "".join(
        "<tr>"
        + "".join(
            f"<td>{html.escape(f'{row[name]:,.2f}' if isinstance(row[name], float) else str(row[name]))}</td>"
            for name in names
        )
        + "</tr>"
        for row in rows
    )
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def render_report(root: Path, payload: dict[str, Any]) -> Path:
    """Render figures and a readable report without collecting raw records to pandas."""
    report = root / "report"
    report.mkdir(parents=True, exist_ok=True)
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    is_synthetic = manifest["dataset_kind"] == "synthetic"
    label = (
        "SYNTHETIC DEMONSTRATION — not Kaggle results"
        if is_synthetic
        else "Kaggle v1 listings, September 2020"
    )
    by_make = payload["market"]["by_make"][:10]
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10})
    fig, axis = plt.subplots(figsize=(10, 5), layout="constrained")
    axis.barh(
        [row["make_name"] for row in reversed(by_make)],
        [row["listings"] for row in reversed(by_make)],
        color="#246b8e",
    )
    axis.set(xlabel="Valid used-car listings", title="Most frequent makes")
    axis.spines[["top", "right"]].set_visible(False)
    fig.savefig(report / "top_makes.png", dpi=150)
    plt.close(fig)
    years = payload["market"]["by_year"]
    fig, axis = plt.subplots(figsize=(10, 4), layout="constrained")
    axis.plot(
        [row["year"] for row in years],
        [row["median_price_usd"] for row in years],
        color="#246b8e",
        marker=".",
    )
    axis.set(
        xlabel="Model year",
        ylabel="Median asking price (USD)",
        title="Median asking price by model year",
    )
    axis.spines[["top", "right"]].set_visible(False)
    fig.savefig(report / "prices_by_year.png", dpi=150)
    plt.close(fig)
    quality = payload["quality"]
    stages = [
        {"disposition": key, "rows": quality[key]}
        for key in (
            "input_rows",
            "invalid_rows",
            "out_of_scope_rows",
            "duplicate_rows",
            "silver_rows",
        )
    ]
    model_html = "<p>Model training was skipped for this run.</p>"
    if "models" in payload:
        regression = payload["models"]["regression"]
        model_html = (
            f"<p>Selected using validation RMSE: <strong>{html.escape(regression['selected_model'])}</strong>. "
            f"Models use {payload['models']['input_rows']:,} vehicle-group-sampled listings.</p>"
            + _table(
                [
                    {"model": "Train-mean baseline", **regression["baseline"]["test"]},
                    {"model": regression["selected_model"], **regression["test"]},
                ]
            )
            + "<p>Target: advertised asking price. MAE and RMSE use USD. These are not sale-price estimates.</p>"
            + "<h3>Exploratory K-Means segments</h3>"
            + _table(payload["models"]["clustering"]["clusters"])
            + "<p>k selected by in-sample squared-Euclidean silhouette; segment labels have no natural order.</p>"
        )
    content = f"""<!doctype html>
<html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Used Cars — {html.escape(manifest["run_id"])}</title>
<style>
body{{font:16px/1.6 system-ui,sans-serif;color:#163247;background:#f2f6f8;margin:0}}
main{{max-width:1050px;margin:auto;padding:32px}}section{{background:white;padding:24px;margin:20px 0;border-radius:12px}}
h1,h2,h3{{line-height:1.25}}.label{{color:#824300;font-weight:700}}img{{width:100%;height:auto}}
table{{border-collapse:collapse;width:100%;font-size:14px;display:block;overflow-x:auto}}
th,td{{padding:9px 12px;border-bottom:1px solid #dce5ea;text-align:left}}th{{background:#eaf1f5}}
code{{overflow-wrap:anywhere}}a{{color:#246b8e}}
</style><main><h1>US used-car listing analysis</h1><p class="label">{html.escape(label)}</p>
<p>Run: <code>{html.escape(manifest["run_id"])}</code> · Seed: {manifest["settings"]["seed"]}</p>
<section><h2>Population and data quality</h2>
<p>Used listings only, price &gt; {manifest["settings"]["min_price"]:g} USD, model years
{manifest["settings"]["min_year"]}–{manifest["settings"]["max_year"]}. Missing horsepower is imputed from train.
Plausible extreme prices and mileage are flagged and retained.</p>
{_table(stages)}<h3>Exclusive primary rejection reasons</h3>{_table(quality["primary_dispositions"])}</section>
<section><h2>Market overview</h2>{_table(payload["market"]["overview"])}
<img src="top_makes.png" alt="Top ten vehicle makes by valid listing count">
<img src="prices_by_year.png" alt="Median asking price in USD by model year"></section>
<section><h2>Models</h2>{model_html}</section>
<section><h2>Scope and limitations</h2><p>Historical advertised inventory is not a representative sample of
all US transactions. Source collection: CarGurus inventory, September 2020. No temporal generalization
claim is made. VIN grouping reduces repeated-vehicle leakage; missing VINs use listing/hash fallback.
Descriptions, dealer details and URLs are excluded from predictive features.</p>
<p>Execution: local Spark, {html.escape(manifest["environment"]["spark"])};
Python {html.escape(manifest["environment"]["python"])}.
Input SHA-256: <code>{manifest["source"]["sha256"]}</code>.</p>
<p><a href="https://www.kaggle.com/datasets/ananaymital/us-used-cars-dataset">Dataset and usage notice</a></p></section>
</main></html>"""
    path = report / "index.html"
    path.write_text(content, encoding="utf-8")
    return path
