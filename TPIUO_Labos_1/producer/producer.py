import os
import io
import time
from typing import Dict, List, Optional

import requests
from google.cloud import pubsub_v1
from fastavro import parse_schema, schemaless_writer
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC")

STACK_TAG = os.getenv("STACK_TAG", "data-engineering")
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "10"))
PUBLISH_BAD_MESSAGE = os.getenv("PUBLISH_BAD_MESSAGE", "false").lower() == "true"

if not PROJECT_ID or not PUBSUB_TOPIC:
    raise RuntimeError("Missing PROJECT_ID or PUBSUB_TOPIC in environment (.env or Cloud Run env vars).")


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


def fetch_stackoverflow_questions(tag: str, pagesize: int) -> List[Dict]:
    url = "https://api.stackexchange.com/2.3/questions"
    params = {
        "order": "desc",
        "sort": "creation",
        "site": "stackoverflow",
        "pagesize": pagesize,
        "tagged": tag,
    }

    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    return data.get("items", [])


def normalize_question(q: Dict) -> Dict:
    # SVE što je "možda ne postoji" ide preko get(...), da ne puca KeyError
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


def publish_messages(items: List[Dict]) -> None:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC)

    print(f"Publishing to: {topic_path}")
    published = 0

    for q in items:
        normalized = normalize_question(q)
        payload = avro_encode(normalized)

        msg_id = publisher.publish(topic_path, data=payload).result()
        published += 1
        print(f"[{published}] message_id={msg_id} question_id={normalized['question_id']} title={normalized['title']!r}")

    # opcionalno: namjerno pošalji "lošu" poruku (npr. fali obavezno polje)
    if PUBLISH_BAD_MESSAGE:
        try:
            bad = {"title": "bad message"}  # fali question_id, link, itd.
            payload = avro_encode(bad)  # ovo će puknuti već na encode (što je OK za test)
            publisher.publish(topic_path, data=payload).result()
        except Exception as e:
            print(f"[EXPECTED] Bad message failed: {e}")

    time.sleep(2)
    print("Done.")


def main():
    items = fetch_stackoverflow_questions(tag=STACK_TAG, pagesize=PAGE_SIZE)
    print(f"Fetched {len(items)} questions.")
    if not items:
        return
    publish_messages(items)


if __name__ == "__main__":
    main()
