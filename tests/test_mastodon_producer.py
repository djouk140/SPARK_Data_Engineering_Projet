import json
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from producer.mastodon_producer import (
    MastodonKafkaListener,
    TOPIC,
    clean_html,
    status_to_record,
)


class RecordingProducer:
    def __init__(self):
        self.messages = []

    def send(self, topic, key, value):
        self.messages.append((topic, key, value))


class MastodonProducerTests(unittest.TestCase):
    def test_clean_html_removes_markup_and_decodes_entities(self):
        self.assertEqual(
            clean_html("<p>Hello <strong>AI</strong> &amp; Spark</p>"),
            "Hello AI & Spark",
        )

    def test_status_to_record_normalizes_matching_status(self):
        with patch(
            "producer.mastodon_producer.KEYWORDS", ("ai", "datascience")
        ):
            record = status_to_record(
                {
                    "id": 42,
                    "created_at": datetime(
                        2026, 1, 1, tzinfo=timezone.utc
                    ),
                    "content": "<p>Learning with <a>AI</a></p>",
                    "tags": [{"name": "DataScience"}],
                    "account": {"acct": "student@example.org"},
                    "language": "en",
                    "favourites_count": 2,
                    "reblogs_count": 1,
                }
            )

        self.assertIsNotNone(record)
        self.assertEqual(record["id"], "42")
        self.assertEqual(record["text"], "Learning with AI")
        self.assertEqual(
            record["created_at"], "2026-01-01T00:00:00+00:00"
        )
        self.assertEqual(record["username"], "student@example.org")
        self.assertEqual(record["hashtags"], ["DataScience"])
        json.dumps(record)

    def test_keyword_match_uses_word_boundaries(self):
        with patch("producer.mastodon_producer.KEYWORDS", ("ai",)):
            record = status_to_record(
                {
                    "id": 1,
                    "content": "<p>Daily updates are useful.</p>",
                    "tags": [],
                    "account": {},
                }
            )
        self.assertIsNone(record)

    def test_reblogs_are_ignored(self):
        self.assertIsNone(
            status_to_record(
                {
                    "id": 2,
                    "content": "<p>AI DataScience</p>",
                    "reblog": {"id": 1},
                    "tags": [],
                }
            )
        )

    def test_listener_sends_normalized_records_to_kafka(self):
        producer = RecordingProducer()
        listener = MastodonKafkaListener(producer)
        with patch("producer.mastodon_producer.KEYWORDS", ("ai",)):
            listener.on_update(
                {
                    "id": 7,
                    "content": "<p>AI on Mastodon</p>",
                    "tags": [],
                    "account": {"acct": "analyst"},
                }
            )

        self.assertEqual(len(producer.messages), 1)
        topic, key, record = producer.messages[0]
        self.assertEqual(topic, TOPIC)
        self.assertEqual(key, b"7")
        self.assertEqual(record["text"], "AI on Mastodon")


if __name__ == "__main__":
    unittest.main()
