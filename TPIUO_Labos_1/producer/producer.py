import os
import json
from typing import List, Dict

import requests
from google.cloud import pubsub_v1
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC")

if not PROJECT_ID or not PUBSUB_TOPIC:
    raise RuntimeError("PROJECT_ID or PUBSUB_TOPIC are missing in .env")


def fetch_stackoverflow_questions(
    tag: str = "data-engineering",
    pagesize: int = 10,
) -> List[Dict]:
    """
    Fetch StackOverflow questions filtered by tag.
    Using StackExchange API (no API key required).
    """
    url = "https://api.stackexchange.com/2.3/questions"
    params = {
        "order": "desc",
        "sort": "votes",     # highest voted questions first
        "site": "stackoverflow",
        "pagesize": pagesize,
        "tagged": tag,
    }

    print(f"Fetching StackOverflow questions (tag={tag}, pagesize={pagesize})")

    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()  # fail immediately on error

    data = resp.json()
    items = data.get("items", [])
    print(f"Fetched {len(items)} questions.")
    return items


def publish_to_pubsub(questions: List[Dict]) -> None:
    """
    Publish each question from the API into Pub/Sub as a separate message.
    The entire JSON object is sent as message payload.
    """
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC)

    print(f"Publishing messages to topic: {topic_path}")

    for index, question in enumerate(questions, start=1):
        payload = json.dumps(question).encode("utf-8")

        future = publisher.publish(topic_path, data=payload)
        message_id = future.result()  # wait for Pub/Sub confirmation

        print(f"[{index}] Published message_id={message_id}, title={question.get('title')!r}")

    print("All questions successfully published.")


def main():
    questions = fetch_stackoverflow_questions(tag="data-engineering", pagesize=10)
    if not questions:
        print("No questions fetched from StackOverflow.")
        return

    publish_to_pubsub(questions)


if __name__ == "__main__":
    main()
    # Cloud Run Job will exit automatically — no infinite loop needed
