import base64
import io
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from flask import Flask, request
from fastavro import parse_schema, schemaless_reader
from google.cloud import storage
import pyarrow as pa
import pyarrow.parquet as pq

app = Flask(__name__)

RAW_BUCKET = os.environ["RAW_BUCKET"]
PROCESSED_BUCKET = os.environ["PROCESSED_BUCKET"]
PREFIX = os.getenv("PREFIX", "topic")
TIME_FIELD = os.getenv("TIME_FIELD", "creation_date")

AVRO_SCHEMA = {
    "type": "record",
    "name": "StackOverflowQuestion",
    "namespace": "tpiuo.lab2",
    "fields": [
        {"name": "question_id", "type": "long"},
        {"name": "title", "type": "string"},
        {"name": "link", "type": "string"},
        {"name": "creation_date", "type": "long"},
        {"name": "last_activity_date", "type": "long"},
        {"name": "is_answered", "type": "boolean"},
        {"name": "score", "type": "int"},
        {"name": "answer_count", "type": "int"},
        {"name": "view_count", "type": "int"},
        {"name": "content_license", "type": ["null", "string"], "default": None},
        {"name": "closed_date", "type": ["null", "long"], "default": None},
        {"name": "closed_reason", "type": ["null", "string"], "default": None},
        {"name": "owner_user_id", "type": ["null", "long"], "default": None},
        {"name": "owner_display_name", "type": ["null", "string"], "default": None},
    ],
}
PARSED_SCHEMA = parse_schema(AVRO_SCHEMA)

PARQUET_SCHEMA = pa.schema([
    ("question_id", pa.int64()),
    ("title", pa.string()),
    ("link", pa.string()),
    ("creation_date", pa.int64()),
    ("last_activity_date", pa.int64()),
    ("is_answered", pa.bool_()),
    ("score", pa.int64()),
    ("answer_count", pa.int64()),
    ("view_count", pa.int64()),
    ("content_license", pa.string()),      # ← UVIJEK STRING
    ("closed_date", pa.int64()),
    ("closed_reason", pa.string()),
    ("owner_user_id", pa.int64()),
    ("owner_display_name", pa.string()),
])

gcs = storage.Client()

@app.route("/listening", methods=["GET"])
def listening_check():
    return "Consumer service is listening", 200


def avro_decode(payload: bytes) -> Dict[str, Any]:
    bio = io.BytesIO(payload)
    return schemaless_reader(bio, PARSED_SCHEMA)


def record_datetime_utc(record: Dict[str, Any]) -> datetime:
    ts = record.get(TIME_FIELD)
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(int(ts), tz=timezone.utc)
    return datetime.now(tz=timezone.utc)


def build_path(kind: str, dt: datetime, filename: str) -> str:
    return (
        f"{PREFIX}/"
        f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/hour={dt.hour:02d}/"
        f"{kind}/{filename}"
    )


def upload_bytes(bucket_name: str, object_name: str, data: bytes, content_type: str) -> None:
    bucket = gcs.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_string(data, content_type=content_type)


def normalize_for_parquet(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Osigurava da svi tipovi odgovaraju PARQUET_SCHEMA.
    """
    rec = dict(record)

    # critical: uvijek STRING ili None
    cl = rec.get("content_license")
    rec["content_license"] = None if cl is None else str(cl)

    # int/long normalizacija
    for k in ("question_id", "creation_date", "last_activity_date",
              "score", "answer_count", "view_count"):
        if rec.get(k) is not None:
            rec[k] = int(rec[k])

    return rec


def save_raw_json(record: Dict[str, Any], dt: datetime, message_id: str) -> None:
    filename = f"part-{message_id}.json"
    object_name = build_path("raw", dt, filename)

    payload = (json.dumps(record, ensure_ascii=False) + "\n").encode("utf-8")
    upload_bytes(RAW_BUCKET, object_name, payload, "application/json")
    print(f"[RAW] gs://{RAW_BUCKET}/{object_name}")


def save_parquet(record: Dict[str, Any], dt: datetime, message_id: str) -> None:
    filename = f"part-{message_id}.parquet"
    object_name = build_path("processed", dt, filename)

    rec = normalize_for_parquet(record)

    table = pa.Table.from_pylist([rec], schema=PARQUET_SCHEMA)

    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    upload_bytes(PROCESSED_BUCKET, object_name, buf.getvalue(), "application/octet-stream")
    print(f"[PARQUET] gs://{PROCESSED_BUCKET}/{object_name}")


def get_pubsub_message_id(envelope: Dict[str, Any]) -> str:
    msg = envelope.get("message", {})
    message_id = msg.get("messageId")
    if message_id:
        return str(message_id)
    return f"no-id-{int(datetime.now(tz=timezone.utc).timestamp() * 1000)}"


@app.route("/", methods=["POST"])
def receive_pubsub_message():
    envelope: Optional[Dict[str, Any]] = request.get_json(silent=True)
    if not envelope or "message" not in envelope:
        return "No Pub/Sub message", 200

    msg = envelope["message"]
    data_b64 = msg.get("data")
    if not data_b64:
        return "No data", 200

    message_id = get_pubsub_message_id(envelope)

    try:
        payload = base64.b64decode(data_b64)
        record = avro_decode(payload)

        dt = record_datetime_utc(record)

        save_raw_json(record, dt, message_id)
        save_parquet(record, dt, message_id)

        return ("", 204)

    except Exception as e:
        print(f"Processing failed for messageId={message_id}: {e}")
        return (f"Processing failed: {e}", 500)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
