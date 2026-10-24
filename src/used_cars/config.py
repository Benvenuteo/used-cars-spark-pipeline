"""Validated configuration shared by the CLI and notebooks."""

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Settings:
    """Keep execution choices explicit in each run's manifest."""

    output: Path
    dataset_kind: str
    run_id: str
    master: str = "local[2]"
    partitions: int = 4
    seed: int = 42
    model_fraction: float = 1.0
    cluster_candidates: tuple[int, ...] = (3, 4, 5)
    min_price: float = 500.0
    min_year: int = 1991
    max_year: int = 2021

    def __post_init__(self) -> None:
        if self.dataset_kind not in {"synthetic", "kaggle"}:
            raise ValueError("dataset_kind must be synthetic or kaggle")
        if not self.run_id or any(char in self.run_id for char in "/\\\x00"):
            raise ValueError("run_id must be a nonempty identifier without path separators")
        if self.partitions < 1:
            raise ValueError("partitions must be positive")
        if not 0 < self.model_fraction <= 1:
            raise ValueError("model_fraction must be in (0, 1]")
        if not self.cluster_candidates or min(self.cluster_candidates) < 2:
            raise ValueError("cluster candidates must be at least 2")
        if self.min_year > self.max_year:
            raise ValueError("min_year must not exceed max_year")

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe configuration snapshot."""
        return {**asdict(self), "output": str(self.output.resolve())}


def settings_from_manifest(output: Path) -> Settings:
    """Restore settings for an analysis-only restart from persisted silver data."""
    import json

    payload = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    values = payload["settings"]
    values["output"] = output
    values["cluster_candidates"] = tuple(values["cluster_candidates"])
    return Settings(**values)
