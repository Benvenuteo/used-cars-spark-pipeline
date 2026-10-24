"""Small local artifacts; bulk records always stay in Spark/Parquet."""

import hashlib
import json
import platform
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyspark


def utc_now() -> str:
    """Return an unambiguous timestamp for run metadata."""
    return datetime.now(UTC).isoformat()


def write_json(path: Path, payload: Any) -> None:
    """Atomically replace a small JSON document and reject NaN/Infinity."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def fingerprint(path: Path) -> dict[str, Any]:
    """Hash source bytes in bounded memory for reproducible input identity."""
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return {"name": path.name, "bytes": path.stat().st_size, "sha256": digest.hexdigest()}


def environment() -> dict[str, Any]:
    """Record measured software and host facts without reading credentials."""
    java = subprocess.run(["java", "-version"], capture_output=True, text=True, check=False)
    return {
        "python": platform.python_version(),
        "spark": pyspark.__version__,
        "java": (java.stderr or java.stdout).splitlines()[0],
        "platform": platform.platform(),
    }
