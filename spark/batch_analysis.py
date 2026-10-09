"""Analyze stored Mastodon history and persist English sentiment results."""

import os
import time
from pathlib import Path

from dotenv import load_dotenv
from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    explode,
    lower,
    to_date,
    when,
)

from spark.postgres import upsert_sentiments
from spark.session import configure_python_worker
from spark.sentiment_model import normalize_text

load_dotenv()


def _persist_sentiment_predictions(
    predictions: DataFrame, neutral_confidence: float
) -> None:
    def rows():
        for row in predictions.select(
            "id", "text", "probability", "prediction"
        ).toLocalIterator():
            probabilities = row["probability"].toArray()
            confidence = float(max(probabilities))
            if confidence < neutral_confidence:
                sentiment = "neutral"
            else:
                sentiment = (
                    "positive" if float(row["prediction"]) == 1.0 else "negative"
                )
            yield {
                "toot_id": row["id"],
                "text": row["text"],
                "sentiment": sentiment,
                "confidence": confidence,
            }

    upsert_sentiments(rows())


def main() -> None:
    configure_python_worker()
    model_path = Path(
        os.getenv("SENTIMENT_MODEL_PATH", "spark/models/sentiment_pipeline")
    )
    if not model_path.exists():
        raise FileNotFoundError(
            f"Sentiment model not found: {model_path}. "
            "Train it first with `python -m spark.sentiment_model`."
        )

    spark = (
        SparkSession.builder.appName("MastodonHistoricalAnalysis")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.5",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    jdbc_url = (
        f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'localhost')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'mastodon_db')}"
    )
    toots = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "toots")
        .option("user", os.getenv("POSTGRES_USER", "mastodon"))
        .option(
            "password",
            os.getenv("POSTGRES_PASSWORD", "change_me_local"),
        )
        .option("driver", "org.postgresql.Driver")
        .load()
        .filter(col("created_at").isNotNull())
    )

    user_threshold = int(os.getenv("MIN_USER_TOOTS", "10"))
    active_users = (
        toots.groupBy("username")
        .agg(count("*").alias("toot_count"))
        .filter(col("toot_count") > user_threshold)
        .orderBy(col("toot_count").desc())
    )
    active_user_toots = toots.join(
        active_users.select("username"), "username", "inner"
    )
    active_user_daily_counts = (
        active_user_toots.groupBy(to_date("created_at").alias("day"))
        .agg(count("*").alias("toot_count"))
        .orderBy("day")
    )
    daily_counts = (
        toots.groupBy(to_date("created_at").alias("day"))
        .agg(count("*").alias("toot_count"))
        .orderBy("day")
    )
    hashtag_counts = (
        toots.select(explode("hashtags").alias("hashtag"))
        .filter(col("hashtag").isNotNull())
        .groupBy(lower("hashtag").alias("hashtag"))
        .agg(count("*").alias("toot_count"))
        .orderBy(col("toot_count").desc())
    )
    print(f"Stored toots: {toots.count()}")
    print(f"Active-user threshold: more than {user_threshold} toots")
    active_users.show(20, truncate=False)
    print("Daily publications from active users:")
    active_user_daily_counts.show(30, truncate=False)
    daily_counts.show(30, truncate=False)
    hashtag_counts.show(20, truncate=False)

    # Compare the same aggregation once without and once with a cached shuffle.
    baseline_start = time.perf_counter()
    toots.groupBy("username").count().count()
    baseline_seconds = time.perf_counter() - baseline_start
    partitions = max(1, int(os.getenv("BATCH_PARTITIONS", "4")))
    optimized = toots.repartition(partitions).cache()
    optimized.count()
    optimized_start = time.perf_counter()
    optimized.groupBy("username").count().count()
    optimized_seconds = time.perf_counter() - optimized_start
    print(f"Baseline aggregation: {baseline_seconds:.3f}s")
    print(
        f"Cached/repartitioned aggregation ({partitions} partitions): "
        f"{optimized_seconds:.3f}s"
    )

    output_dir = Path(os.getenv("BATCH_OUTPUT_DIR", "reports/batch"))
    output_dir.mkdir(parents=True, exist_ok=True)
    daily_counts.coalesce(1).write.mode("overwrite").option(
        "header", "true"
    ).csv(str(output_dir / "daily_counts"))
    hashtag_counts.coalesce(1).write.mode("overwrite").option(
        "header", "true"
    ).csv(str(output_dir / "hashtag_counts"))
    active_users.coalesce(1).write.mode("overwrite").option(
        "header", "true"
    ).csv(str(output_dir / "active_users"))
    active_user_daily_counts.coalesce(1).write.mode("overwrite").option(
        "header", "true"
    ).csv(str(output_dir / "active_user_daily_counts"))

    model = PipelineModel.load(str(model_path))
    english_toots = normalize_text(
        toots.filter(col("language") == "en").select("id", "text")
    ).filter(
        col("text") != ""
    )
    predictions = model.transform(english_toots).select(
        "id",
        "text",
        "probability",
        "prediction",
    )
    neutral_confidence = float(
        os.getenv("SENTIMENT_NEUTRAL_CONFIDENCE", "0.60")
    )
    _persist_sentiment_predictions(predictions, neutral_confidence)

    sentiment_counts = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "toot_sentiments")
        .option("user", os.getenv("POSTGRES_USER", "mastodon"))
        .option(
            "password",
            os.getenv("POSTGRES_PASSWORD", "change_me_local"),
        )
        .option("driver", "org.postgresql.Driver")
        .load()
        .groupBy("sentiment")
        .agg(count("*").alias("toot_count"))
        .orderBy("sentiment")
    )
    sentiment_counts.show(truncate=False)
    sentiment_counts.coalesce(1).write.mode("overwrite").option(
        "header", "true"
    ).csv(str(output_dir / "sentiment_counts"))
    optimized.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
