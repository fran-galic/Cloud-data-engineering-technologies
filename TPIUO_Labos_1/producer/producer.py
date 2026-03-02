import os
import io
import time
import json
from typing import Dict, List

import requests
from dotenv import load_dotenv
from fastavro import parse_schema, schemaless_writer
from google.cloud import pubsub_v1

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC")  # npr. reddit-topic-0036546889 (glavni topic sa schemom)
DLQ_TOPIC = os.getenv("DLQ_TOPIC")        # npr. stack-overflow-dead-letter-topic (topic bez sheme)

STACK_TAG = os.getenv("STACK_TAG", "python")
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "10"))
PUBLISH_BAD_MESSAGE = os.getenv("PUBLISH_BAD_MESSAGE", "false").lower() == "true"

# NEW: ukupno koliko poruka želimo (default: PAGE_SIZE -> ponašanje kao prije)
TOTAL_MESSAGES = int(os.getenv("TOTAL_MESSAGES", str(PAGE_SIZE)))
START_PAGE = int(os.getenv("START_PAGE", "1"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "1000"))
SLEEP_BETWEEN_PAGES_SEC = float(os.getenv("SLEEP_BETWEEN_PAGES_SEC", "0"))

if not PROJECT_ID or not PUBSUB_TOPIC or not DLQ_TOPIC:
    raise RuntimeError(
        "Missing env vars. Required: PROJECT_ID, PUBSUB_TOPIC, DLQ_TOPIC. "
        "Example DLQ_TOPIC=stack-overflow-dead-letter-topic"
    )

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


def fetch_stackoverflow_questions(tag: str, pagesize: int, page: int) -> List[Dict]:
    url = "https://api.stackexchange.com/2.3/questions"
    params = {
        "order": "desc",
        "sort": "creation",
        "site": "stackoverflow",
        "pagesize": pagesize,
        "page": page,
        "tagged": tag,
    }
    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json().get("items", [])


def fetch_stackoverflow_questions_many(tag: str, pagesize: int, total: int) -> List[Dict]:
    # minimal safety
    if total <= 0:
        return []
    if pagesize <= 0:
        raise ValueError("PAGE_SIZE must be > 0")

    items_all: List[Dict] = []
    page = START_PAGE
    pages_fetched = 0

    while len(items_all) < total and pages_fetched < MAX_PAGES:
        items = fetch_stackoverflow_questions(tag=tag, pagesize=pagesize, page=page)
        if not items:
            break

        items_all.extend(items)
        pages_fetched += 1
        page += 1

        if SLEEP_BETWEEN_PAGES_SEC > 0:
            time.sleep(SLEEP_BETWEEN_PAGES_SEC)

    return items_all[:total]


def normalize_question(q: Dict) -> Dict:
    owner = q.get("owner") or {}
    return {
        "question_id": int(q["question_id"]),
        "title": str(q["title"]),
        "link": str(q["link"]),

        "creation_date": int(q["creation_date"]),
        "last_activity_date": int(q.get("last_activity_date", q["creation_date"])),

        "is_answered": bool(q["is_answered"]),
        "score": int(q.get("score", 0)),
        "answer_count": int(q.get("answer_count", 0)),
        "view_count": int(q.get("view_count", 0)),

        "content_license": q.get("content_license"),

        "closed_date": q.get("closed_date"),
        "closed_reason": q.get("closed_reason"),

        "owner_user_id": owner.get("user_id"),
        "owner_display_name": owner.get("display_name"),
    }


def avro_encode(record: Dict) -> bytes:
    buf = io.BytesIO()
    schemaless_writer(buf, PARSED_SCHEMA, record)
    return buf.getvalue()


def looks_like_schema_rejection(exc: Exception) -> bool:
    s = str(exc).lower()
    return ("schema" in s and "validation" in s) or ("invalid_binary_avro_message" in s) or ("failed schema" in s)


def publish_json_to_dlq(
    publisher: pubsub_v1.PublisherClient,
    dlq_topic_path: str,
    original_record: Dict,
    reason: str,
    error: str,
) -> str:
    payload = json.dumps(
        {
            "reason": reason,
            "error": error,
            "record": original_record,
        },
        ensure_ascii=False,
    ).encode("utf-8")

    attrs = {
        "reason": reason,
        "source_topic": PUBSUB_TOPIC,
        "stack_tag": STACK_TAG,
    }

    return publisher.publish(dlq_topic_path, data=payload, **attrs).result()


def publish_messages(items: List[Dict]) -> None:
    publisher = pubsub_v1.PublisherClient()
    main_topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC)
    dlq_topic_path = publisher.topic_path(PROJECT_ID, DLQ_TOPIC)

    print(f"Main topic: {main_topic_path}")
    print(f"DLQ topic:  {dlq_topic_path}")

    ok = 0
    dlq = 0

    for q in items:
        normalized = normalize_question(q)

        try:
            payload = avro_encode(normalized)
        except Exception as e:
            dlq_msg_id = publish_json_to_dlq(
                publisher,
                dlq_topic_path,
                original_record=normalized,
                reason="avro_encode_failed",
                error=str(e),
            )
            dlq += 1
            print(f"[DLQ {dlq}] encode failed -> dlq_message_id={dlq_msg_id} question_id={normalized.get('question_id')}")
            continue

        try:
            msg_id = publisher.publish(main_topic_path, data=payload).result()
            ok += 1
            print(f"[OK {ok}] message_id={msg_id} question_id={normalized['question_id']} title={normalized['title']!r}")
        except Exception as e:
            reason = "publish_schema_rejected" if looks_like_schema_rejection(e) else "publish_failed"
            dlq_msg_id = publish_json_to_dlq(
                publisher,
                dlq_topic_path,
                original_record=normalized,
                reason=reason,
                error=str(e),
            )
            dlq += 1
            print(f"[DLQ {dlq}] publish failed -> dlq_message_id={dlq_msg_id} question_id={normalized.get('question_id')} reason={reason}")

    if PUBLISH_BAD_MESSAGE:
        bad = {"title": "bad message"}
        try:
            payload = avro_encode(bad)
            publisher.publish(main_topic_path, data=payload).result()
        except Exception as e:
            dlq_msg_id = publish_json_to_dlq(
                publisher,
                dlq_topic_path,
                original_record=bad,
                reason="manual_bad_message",
                error=str(e),
            )
            dlq += 1
            print(f"[DLQ {dlq}] manual bad message -> dlq_message_id={dlq_msg_id}")

    time.sleep(3)
    print(f"Done. Published OK={ok}, sent to DLQ={dlq}")


def main():
    items = fetch_stackoverflow_questions_many(tag=STACK_TAG, pagesize=PAGE_SIZE, total=TOTAL_MESSAGES)
    print(
        f"Fetched {len(items)} questions (tag={STACK_TAG}, page_size={PAGE_SIZE}, total={TOTAL_MESSAGES}, start_page={START_PAGE})."
    )
    if not items:
        return
    publish_messages(items)


if __name__ == "__main__":
    main()
