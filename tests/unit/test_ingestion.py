"""
tests/unit/test_ingestion.py — Unit tests for ingestion layer.

Tests schema validation, Bronze key building, NDJSON serialization,
and ingestion context metadata — all without Kafka or MinIO.
"""

import sys, os, json, uuid
from datetime import datetime, timezone

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src/ingestion"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src/simulators"))

from ingestion.schemas.bronze_schemas import validate_record, SCHEMA_REGISTRY, REQUIRED_FIELDS
from ingestion.utils.ingestion_utils import (
    IngestionContext,
    build_bronze_key,
    records_to_ndjson,
    chunk_records,
)


# ── Schema validation tests ────────────────────────────────────────────────────

class TestSchemaValidation:

    def _valid_tweet(self) -> dict:
        return {
            "tweet_id":  str(uuid.uuid4()),
            "user_id":   str(uuid.uuid4()),
            "platform":  "twitter",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

    def test_valid_tweet_passes(self):
        record = self._valid_tweet()
        is_valid, errors = validate_record(record, "twitter.tweets")
        assert is_valid
        assert errors == []

    def test_missing_tweet_id_fails(self):
        record = self._valid_tweet()
        del record["tweet_id"]
        is_valid, errors = validate_record(record, "twitter.tweets")
        assert not is_valid
        assert any("tweet_id" in e for e in errors)

    def test_null_user_id_fails(self):
        record = self._valid_tweet()
        record["user_id"] = None
        is_valid, errors = validate_record(record, "twitter.tweets")
        assert not is_valid
        assert any("user_id" in e for e in errors)

    def test_all_schema_keys_in_registry(self):
        for key in REQUIRED_FIELDS:
            assert key in SCHEMA_REGISTRY, f"{key} missing from SCHEMA_REGISTRY"

    def test_unknown_schema_key_returns_valid(self):
        """Bronze is permissive — unknown schema keys pass validation."""
        record = {"some_field": "value"}
        is_valid, errors = validate_record(record, "unknown.schema")
        assert is_valid

    def test_valid_youtube_video_passes(self):
        record = {
            "video_id":   str(uuid.uuid4()),
            "channel_id": str(uuid.uuid4()),
            "platform":   "youtube",
            "published_at": datetime.now(timezone.utc).isoformat(),
        }
        is_valid, errors = validate_record(record, "youtube.videos")
        assert is_valid

    def test_valid_reddit_post_passes(self):
        record = {
            "post_id":        str(uuid.uuid4()),
            "author_user_id": str(uuid.uuid4()),
            "subreddit":      "r/technology",
            "posted_at":      datetime.now(timezone.utc).isoformat(),
        }
        is_valid, errors = validate_record(record, "reddit.posts")
        assert is_valid

    def test_engagement_missing_type_fails(self):
        record = {
            "engagement_id":    str(uuid.uuid4()),
            "post_id":          str(uuid.uuid4()),
            "engaging_user_id": str(uuid.uuid4()),
            # engagement_type is missing
        }
        is_valid, errors = validate_record(record, "twitter.engagements")
        assert not is_valid
        assert any("engagement_type" in e for e in errors)


# ── Bronze key builder tests ───────────────────────────────────────────────────

class TestBronzeKeyBuilder:

    def test_key_has_correct_structure(self):
        key = build_bronze_key("twitter", "tweets", "abc123def456")
        parts = key.split("/")
        assert parts[0] == "twitter"
        assert parts[1] == "tweets"
        assert parts[2].startswith("year=")
        assert parts[3].startswith("month=")
        assert parts[4].startswith("day=")
        assert parts[5].startswith("hour=")

    def test_key_ends_with_json(self):
        key = build_bronze_key("youtube", "videos", str(uuid.uuid4()))
        assert key.endswith(".json")

    def test_key_has_parquet_extension(self):
        key = build_bronze_key("reddit", "posts", str(uuid.uuid4()), extension="parquet")
        assert key.endswith(".parquet")

    def test_chunk_index_in_filename(self):
        key_0 = build_bronze_key("twitter", "tweets", "run123", chunk_idx=0)
        key_5 = build_bronze_key("twitter", "tweets", "run123", chunk_idx=5)
        filename_0 = key_0.split("/")[-1]
        filename_5 = key_5.split("/")[-1]
        assert "_000." in filename_0
        assert "_005." in filename_5

    def test_same_source_same_day_same_prefix(self):
        run_id = str(uuid.uuid4())
        key1 = build_bronze_key("twitter", "tweets", run_id, chunk_idx=0)
        key2 = build_bronze_key("twitter", "tweets", run_id, chunk_idx=1)
        prefix1 = "/".join(key1.split("/")[:-1])
        prefix2 = "/".join(key2.split("/")[:-1])
        assert prefix1 == prefix2, "Same run should use same prefix"


# ── NDJSON serialization tests ─────────────────────────────────────────────────

class TestNDJSON:

    def test_ndjson_one_line_per_record(self):
        records = [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}, {"id": 3, "val": "c"}]
        ndjson = records_to_ndjson(records)
        lines = ndjson.strip().split("\n")
        assert len(lines) == 3

    def test_each_line_is_valid_json(self):
        records = [{"id": i, "name": f"record_{i}"} for i in range(10)]
        ndjson = records_to_ndjson(records)
        for line in ndjson.strip().split("\n"):
            obj = json.loads(line)
            assert "id" in obj

    def test_handles_nested_structures(self):
        records = [{"hashtags": ["#AI", "#Data"], "nested": {"key": "value"}}]
        ndjson = records_to_ndjson(records)
        parsed = json.loads(ndjson.strip())
        assert parsed["hashtags"] == ["#AI", "#Data"]

    def test_handles_none_values(self):
        records = [{"id": 1, "optional": None}]
        ndjson = records_to_ndjson(records)
        parsed = json.loads(ndjson.strip())
        assert parsed["optional"] is None


# ── Chunking tests ─────────────────────────────────────────────────────────────

class TestChunkRecords:

    def test_exact_chunk_size(self):
        records = list(range(100))
        chunks = list(chunk_records(records, chunk_size=25))
        assert len(chunks) == 4
        for idx, chunk in chunks:
            assert len(chunk) == 25

    def test_uneven_chunks(self):
        records = list(range(105))
        chunks = list(chunk_records(records, chunk_size=25))
        assert len(chunks) == 5
        assert len(chunks[-1][1]) == 5   # Last chunk has remainder

    def test_chunk_idx_sequential(self):
        records = list(range(300))
        chunks = list(chunk_records(records, chunk_size=100))
        indices = [idx for idx, _ in chunks]
        assert indices == [0, 1, 2]

    def test_empty_input(self):
        chunks = list(chunk_records([], chunk_size=100))
        assert chunks == []

    def test_single_record(self):
        chunks = list(chunk_records([{"id": 1}], chunk_size=100))
        assert len(chunks) == 1
        assert chunks[0][1] == [{"id": 1}]


# ── Ingestion context tests ────────────────────────────────────────────────────

class TestIngestionContext:

    def test_run_id_is_uuid(self):
        ctx = IngestionContext("kafka", "test_job")
        uuid.UUID(ctx.run_id)   # Raises ValueError if invalid

    def test_tag_record_adds_batch_id(self):
        ctx = IngestionContext("kafka", "test_job")
        record = {"tweet_id": "123"}
        tagged = ctx.tag_record(record, "test_file.json")
        assert tagged["batch_id"] == ctx.run_id

    def test_tag_record_adds_ingested_at(self):
        ctx = IngestionContext("kafka", "test_job")
        record = {"tweet_id": "123"}
        tagged = ctx.tag_record(record)
        assert "ingested_at" in tagged
        datetime.fromisoformat(tagged["ingested_at"])  # Valid ISO format

    def test_tag_record_adds_source_file(self):
        ctx = IngestionContext("batch", "test_job")
        record = {}
        tagged = ctx.tag_record(record, "youtube/videos/test.json")
        assert tagged["source_file"] == "youtube/videos/test.json"

    def test_summary_has_required_fields(self):
        ctx = IngestionContext("batch", "batch_ingestion")
        ctx.records_read    = 100
        ctx.records_written = 95
        ctx.records_rejected = 5
        summary = ctx.summary()
        assert summary["records_read"]    == 100
        assert summary["records_written"] == 95
        assert summary["records_rejected"] == 5
        assert "run_id" in summary
        assert "started_at" in summary
        assert "finished_at" in summary
