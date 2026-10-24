"""Stream the public Kaggle version-1 archive without loading it into memory."""

import argparse
import logging
import shutil
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

from used_cars.io import fingerprint, utc_now, write_json

LOGGER = logging.getLogger(__name__)
DATASET_REF = "ananaymital/us-used-cars-dataset"
SOURCE_URL = f"https://www.kaggle.com/datasets/{DATASET_REF}"
DOWNLOAD_URL = (
    f"https://www.kaggle.com/api/v1/datasets/download/{DATASET_REF}?datasetVersionNumber=1"
)
CSV_NAME = "used_cars_data.csv"
EXPECTED_CSV_BYTES = 9_980_208_148


def download_dataset(destination: Path) -> Path:
    """Download/extract exactly one known file; never redistribute the source data."""
    destination.mkdir(parents=True, exist_ok=True)
    csv_path = destination / CSV_NAME
    if csv_path.exists():
        if csv_path.stat().st_size != EXPECTED_CSV_BYTES:
            raise ValueError(f"Existing {csv_path} has an unexpected size; use another directory")
        LOGGER.info("Source CSV already exists: %s", csv_path)
        return csv_path
    if shutil.disk_usage(destination).free < 16_000_000_000:
        raise ValueError(
            "At least 16 GB of free disk space is required for download and extraction"
        )
    archive = destination / "us-used-cars-v1.zip"
    if not archive.exists():
        partial = archive.with_suffix(".zip.part")
        request = urllib.request.Request(
            DOWNLOAD_URL, headers={"User-Agent": "used-cars-pipeline/0.1"}
        )
        try:
            with (
                urllib.request.urlopen(request, timeout=60) as response,
                partial.open("wb") as output,
            ):
                total = 0
                last_report = 0
                while chunk := response.read(8 * 1024 * 1024):
                    output.write(chunk)
                    total += len(chunk)
                    if total - last_report >= 256 * 1024 * 1024:
                        LOGGER.info("Downloaded %.2f GB", total / 1e9)
                        last_report = total
        except urllib.error.HTTPError as exc:
            raise RuntimeError(
                f"Kaggle returned HTTP {exc.code}. Download version 1 manually from {SOURCE_URL} "
                f"and extract {CSV_NAME} to {destination}. Do not commit Kaggle credentials."
            ) from exc
        partial.replace(archive)
    temporary = csv_path.with_suffix(".csv.part")
    with zipfile.ZipFile(archive) as bundle:
        entry = bundle.getinfo(CSV_NAME)
        if entry.file_size != EXPECTED_CSV_BYTES:
            raise ValueError(
                "Unexpected Kaggle file size; verify dataset version before proceeding"
            )
        LOGGER.info("Extracting %.2f GB CSV", entry.file_size / 1e9)
        with bundle.open(entry) as source, temporary.open("wb") as output:
            shutil.copyfileobj(source, output, length=8 * 1024 * 1024)
    temporary.replace(csv_path)
    write_json(
        destination / "source.json",
        {
            "source_url": SOURCE_URL,
            "dataset_version": 1,
            "downloaded_at": utc_now(),
            "archive": fingerprint(archive),
            "license_notice": "Data files © Original Authors; academic/research/individual experimentation only",
        },
    )
    LOGGER.info("Dataset ready: %s", csv_path)
    return csv_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--destination", type=Path, default=Path("data/raw"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    download_dataset(args.destination)
