"""Verify deterministic document identity and real database retry behavior when available."""

import os
import uuid

import pytest
from pymongo import MongoClient

from used_cars.mongo import make_documents, upsert_documents


def test_stable_keys_isolate_runs():
    payload = {"quality": {"rows": 12}, "market": {"listings": 10}}
    first = make_documents(payload, "run-one", "synthetic")
    repeated = make_documents(payload, "run-one", "synthetic")
    second = make_documents(payload, "run-two", "synthetic")
    assert first == repeated
    assert {row["_id"] for row in first}.isdisjoint(row["_id"] for row in second)


@pytest.mark.mongo
def test_mongo_upsert_is_idempotent_and_keeps_other_runs():
    uri = os.environ.get("TEST_MONGO_URI")
    if not uri:
        pytest.skip("Set TEST_MONGO_URI for live MongoDB integration")
    with MongoClient(uri, serverSelectionTimeoutMS=5000) as client:
        client.admin.command("ping")
        name = f"test_used_cars_{uuid.uuid4().hex}"
        collection = client[name]["results"]
        try:
            first = make_documents({"quality": {"rows": 12}}, "first", "synthetic")
            other = make_documents({"quality": {"rows": 99}}, "other", "synthetic")
            upsert_documents(collection, first)
            upsert_documents(collection, other)
            upsert_documents(
                collection, make_documents({"quality": {"rows": 13}}, "first", "synthetic")
            )
            assert collection.count_documents({}) == 2
            assert collection.find_one({"run_id": "first"})["payload"]["rows"] == 13
            assert collection.find_one({"run_id": "other"})["payload"]["rows"] == 99
        finally:
            client.drop_database(name)
