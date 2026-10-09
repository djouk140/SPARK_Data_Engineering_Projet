import unittest
from unittest.mock import MagicMock, patch

from spark.postgres import upsert_toots


class PostgresPersistenceTests(unittest.TestCase):
    @patch("spark.postgres.execute_values")
    @patch("spark.postgres.connect")
    def test_toot_batch_uses_idempotent_upsert_and_closes_connection(
        self, connect, execute_values
    ):
        connection = MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.__enter__.return_value = (
            connection.cursor.return_value
        )
        connect.return_value = connection

        upsert_toots(
            [
                {
                    "id": "123",
                    "created_at": "2026-01-01T12:00:00Z",
                    "username": "alice",
                    "language": "en",
                    "text": "AI is useful",
                    "hashtags": ["AI"],
                    "favourites_count": 2,
                    "reblogs_count": 1,
                    "toot_length": 13,
                }
            ]
        )

        sql = execute_values.call_args.args[1]
        rows = list(execute_values.call_args.args[2])
        self.assertIn("ON CONFLICT (id) DO UPDATE", sql)
        self.assertEqual(rows[0][0], "123")
        self.assertEqual(rows[0][5], ["AI"])
        connection.__exit__.assert_called_once()
        connection.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
