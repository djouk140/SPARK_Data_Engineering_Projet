"""PostgreSQL persistence helpers shared by the Spark jobs."""

import os
from collections.abc import Iterable, Mapping
from contextlib import closing
from typing import Any

import psycopg2
from psycopg2.extensions import connection as PostgresConnection
from psycopg2.extras import execute_values


def connect() -> PostgresConnection:
    """Open a PostgreSQL connection using the project environment."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "mastodon_db"),
        user=os.getenv("POSTGRES_USER", "mastodon"),
        password=os.getenv("POSTGRES_PASSWORD", "change_me_local"),
    )


def _execute_values(
    sql: str, rows: Iterable[tuple[Any, ...]], page_size: int = 500
) -> None:
    with closing(connect()) as connection:
        with connection, connection.cursor() as cursor:
            execute_values(cursor, sql, rows, page_size=page_size)


def upsert_toots(rows: Iterable[Mapping[str, Any]]) -> None:
    """Idempotently save a micro-batch of normalized toots."""
    values = (
        (
            row["id"],
            row["created_at"],
            row["username"],
            row["language"],
            row["text"],
            row["hashtags"] or [],
            row["favourites_count"] or 0,
            row["reblogs_count"] or 0,
            row["toot_length"] or 0,
        )
        for row in rows
    )
    _execute_values(
        """
        INSERT INTO toots (
            id, created_at, username, language, text, hashtags,
            favourites_count, reblogs_count, toot_length
        ) VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            created_at = EXCLUDED.created_at,
            username = EXCLUDED.username,
            language = EXCLUDED.language,
            text = EXCLUDED.text,
            hashtags = EXCLUDED.hashtags,
            favourites_count = EXCLUDED.favourites_count,
            reblogs_count = EXCLUDED.reblogs_count,
            toot_length = EXCLUDED.toot_length
        """,
        values,
    )


def upsert_hourly_stats(rows: Iterable[Mapping[str, Any]]) -> None:
    values = (
        (
            row["window_start"],
            row["window_end"],
            row["toot_count"],
            row["avg_length"],
        )
        for row in rows
    )
    _execute_values(
        """
        INSERT INTO toot_hourly_stats (
            window_start, window_end, toot_count, avg_length
        ) VALUES %s
        ON CONFLICT (window_start, window_end) DO UPDATE SET
            toot_count = EXCLUDED.toot_count,
            avg_length = EXCLUDED.avg_length
        """,
        values,
    )


def upsert_user_hourly_stats(rows: Iterable[Mapping[str, Any]]) -> None:
    values = (
        (
            row["window_start"],
            row["window_end"],
            row["username"],
            row["toot_count"],
            row["avg_length"],
        )
        for row in rows
    )
    _execute_values(
        """
        INSERT INTO user_hourly_stats (
            window_start, window_end, username, toot_count, avg_length
        ) VALUES %s
        ON CONFLICT (window_start, window_end, username) DO UPDATE SET
            toot_count = EXCLUDED.toot_count,
            avg_length = EXCLUDED.avg_length
        """,
        values,
    )


def upsert_sentiments(rows: Iterable[Mapping[str, Any]]) -> None:
    values = (
        (
            row["toot_id"],
            row["text"],
            row["sentiment"],
            row["confidence"],
        )
        for row in rows
    )
    _execute_values(
        """
        INSERT INTO toot_sentiments (
            toot_id, text, sentiment, confidence
        ) VALUES %s
        ON CONFLICT (toot_id) DO UPDATE SET
            text = EXCLUDED.text,
            sentiment = EXCLUDED.sentiment,
            confidence = EXCLUDED.confidence,
            processed_at = NOW()
        """,
        values,
    )
