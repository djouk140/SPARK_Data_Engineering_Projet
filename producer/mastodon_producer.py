
import json
import logging
import os
import re
from datetime import datetime
from typing import TYPE_CHECKING, Any

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from mastodon import Mastodon, StreamListener

if TYPE_CHECKING:
    from kafka import KafkaProducer

load_dotenv()
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
LOGGER = logging.getLogger(__name__)
TOPIC = os.getenv("KAFKA_TOPIC", "mastodon_stream")
KEYWORDS = tuple(
    keyword.strip().lstrip("#").casefold()
    for keyword in os.getenv(
        "MASTODON_KEYWORDS", "DataScience,AI,MachineLearning"
    ).split(",")
    if keyword.strip()
)


def clean_html(content: str | None) -> str:
    """Convert Mastodon's HTML content into readable plain text."""
    return BeautifulSoup(content or "", "html.parser").get_text(
        " ", strip=True
    )


def status_to_record(status: dict[str, Any]) -> dict[str, Any] | None:
    """Normalize a Mastodon status and return it only when it matches."""
    if status.get("reblog") or not status.get("id"):
        return None

    text = clean_html(status.get("content"))
    hashtags = [
        tag["name"]
        for tag in status.get("tags") or []
        if tag.get("name")
    ]
    searchable = f"{text} {' '.join(hashtags)}".casefold()
    if not any(
        re.search(rf"(?<!\w){re.escape(keyword)}(?!\w)", searchable)
        for keyword in KEYWORDS
    ):
        return None

    account = status.get("account") or {}
    created_at = status.get("created_at")
    if isinstance(created_at, datetime):
        created_at = created_at.isoformat()
    return {
        "id": str(status["id"]),
        "created_at": created_at,
        "username": account.get("acct") or "unknown",
        "language": status.get("language"),
        "text": text,
        "hashtags": hashtags,
        "favourites_count": status.get("favourites_count", 0),
        "reblogs_count": status.get("reblogs_count", 0),
    }


class MastodonKafkaListener(StreamListener):
    def __init__(self, producer: "KafkaProducer") -> None:
        super().__init__()
        self.producer = producer

    def on_update(self, status: dict[str, Any]) -> None:
        record = status_to_record(status)
        if record is None:
            return

        self.producer.send(
            TOPIC,
            key=record["id"].encode("utf-8"),
            value=record,
        )
        LOGGER.info("Toot queued for Kafka: %s", record["id"])

    def on_abort(self, error: Exception) -> None:
        LOGGER.error("Mastodon stream interrupted: %s", error)


def main() -> None:
    from kafka import KafkaProducer

    access_token = os.getenv("MASTODON_ACCESS_TOKEN")
    if not access_token:
        raise RuntimeError(
            "Set MASTODON_ACCESS_TOKEN in the project .env file."
        )

    producer = KafkaProducer(
        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"
        ),
        value_serializer=lambda value: json.dumps(
            value, ensure_ascii=False
        ).encode("utf-8"),
        acks="all",
        retries=5,
    )
    api = Mastodon(
        access_token=access_token,
        api_base_url=os.getenv(
            "MASTODON_INSTANCE", "https://mastodon.social"
        ),
    )

    try:
        LOGGER.info("Connecting to the Mastodon public stream...")
        api.stream_public(
            MastodonKafkaListener(producer),
            run_async=False,
        )
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
