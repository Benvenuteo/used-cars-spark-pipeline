"""Publish only bounded aggregate documents with idempotent, run-scoped keys."""

import hashlib
import time
from typing import Any

from pymongo import MongoClient, UpdateOne
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError


def make_documents(payload: dict[str, Any], run_id: str, dataset_kind: str) -> list[dict[str, Any]]:
    """Use stable IDs so retrying one run cannot duplicate its records or erase other runs."""
    documents = []
    for section, content in payload.items():
        identifier = hashlib.sha256(f"{run_id}:{section}".encode()).hexdigest()
        documents.append(
            {
                "_id": identifier,
                "run_id": run_id,
                "dataset_kind": dataset_kind,
                "section": section,
                "payload": content,
            }
        )
    return documents


def upsert_documents(collection: Any, documents: list[dict[str, Any]]) -> int:
    """Replace the complete payload of each section without delete_many or collection-wide writes."""
    collection.create_index([("run_id", 1), ("section", 1)], unique=True)
    if documents:
        collection.bulk_write(
            [
                UpdateOne(
                    {"_id": document["_id"]},
                    {"$set": {key: value for key, value in document.items() if key != "_id"}},
                    upsert=True,
                )
                for document in documents
            ],
            ordered=False,
        )
    return len(documents)


def publish(payload: dict[str, Any], run_id: str, dataset_kind: str, uri: str) -> int:
    """Ping with bounded retries; keep URI credentials out of logs and reports."""
    with MongoClient(uri, serverSelectionTimeoutMS=3000, connectTimeoutMS=3000) as client:
        for attempt in range(5):
            try:
                client.admin.command("ping")
                break
            except (ConnectionFailure, ServerSelectionTimeoutError):
                if attempt == 4:
                    raise RuntimeError(
                        "MongoDB is unavailable after five connection attempts"
                    ) from None
                time.sleep(min(2**attempt, 8))
        return upsert_documents(
            client["car_market"]["run_results"], make_documents(payload, run_id, dataset_kind)
        )
