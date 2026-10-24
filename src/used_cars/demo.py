"""Generate explicitly synthetic, deterministic listings with intentional quality cases."""

import csv
import random
from pathlib import Path

from used_cars.schema import RAW_COLUMNS


def generate_demo(destination: Path, rows: int = 1200, seed: int = 42) -> Path:
    """Write a small schema-compatible CSV; these records are never Kaggle samples."""
    if rows < 100:
        raise ValueError("Use at least 100 demo rows for meaningful train/validation/test splits")
    if destination.exists():
        raise FileExistsError(f"Refusing to replace existing input: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    generator = random.Random(seed)
    brands = (
        ("Toyota", "Camry", 3000),
        ("Ford", "Focus", 0),
        ("BMW", "320i", 9000),
        ("Honda", "Civic", 2000),
        ("Chevrolet", "Malibu", 500),
    )
    with destination.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=RAW_COLUMNS)
        writer.writeheader()
        previous: dict[str, str] | None = None
        for index in range(rows):
            make, model, premium = generator.choice(brands)
            year = generator.randint(2002, 2021)
            mileage = max(0, int((2021 - year) * generator.uniform(6000, 13000)))
            horsepower = generator.randint(110, 350)
            price = max(
                900,
                int(
                    26000
                    + premium
                    + 550 * horsepower / 10
                    - (2021 - year) * 700
                    - mileage * 0.055
                    + generator.gauss(0, 1700)
                ),
            )
            row = dict.fromkeys(RAW_COLUMNS, "")
            row.update(
                {
                    "vin": f"1DEM0{index:012d}",
                    "listing_id": str(100000 + index),
                    "listed_date": f"2020-09-{1 + index % 20:02d}",
                    "make_name": make,
                    "model_name": model,
                    "body_type": "Sedan",
                    "fuel_type": "Gasoline",
                    "transmission": "A",
                    "year": str(year),
                    "mileage": str(mileage),
                    "horsepower": str(horsepower),
                    "price": str(price),
                    "is_new": "False",
                    "dealer_zip": "00960",
                    "description": 'Synthetic listing, quoted "text"\nwith a second line.',
                }
            )
            case = index % 100
            if case == 0:
                row["price"] = "not-a-number"
            elif case == 1:
                row["mileage"] = "-10"
            elif case == 2:
                row["is_new"] = "True"
            elif case == 3:
                row["year"] = "1985"
            elif case == 4:
                row["price"] = "400"
            elif case == 5:
                row["horsepower"] = ""
            elif case == 6:
                row["make_name"] = ""
            elif case == 7:
                row["is_new"] = "unknown"
            elif case == 8:
                row["vin"] = ""
            elif case == 9 and previous:
                row = previous.copy()
            elif case == 10 and previous:
                row["vin"] = previous["vin"]
            writer.writerow(row)
            previous = row
    return destination
