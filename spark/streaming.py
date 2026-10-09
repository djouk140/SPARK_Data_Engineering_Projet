"""Consume the Mastodon Kafka topic and persist live Spark aggregates."""

import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    from_json,
    length,
    regexp_replace,
    to_timestamp,
    window,
)
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from spark.postgres import (
    upsert_hourly_stats,
    upsert_toots,
    upsert_user_hourly_stats,
)
from spark.session import configure_python_worker

load_dotenv()

TOOT_COLUMNS = [
    "id",
    "created_at",
    "username",
    "language",
    "text",
    "hashtags",
    "favourites_count",
    "reblogs_count",
    "toot_length",
]
TOOT_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("created_at", StringType()),
        StructField("username", StringType()),
        StructField("language", StringType()),
        StructField("text", StringType()),
        StructField("hashtags", ArrayType(StringType())),
        StructField("favourites_count", IntegerType()),
        StructField("reblogs_count", IntegerType()),
    ]
)


def _persist_batch(batch_df, batch_id: int) -> None:
    if batch_df.isEmpty():
        return
    rows = (
        row.asDict(recursive=True)
        for row in batch_df.select(*TOOT_COLUMNS)
        .dropDuplicates(["id"])
        .toLocalIterator()
    )
    upsert_toots(rows)


def _persist_hourly_stats(batch_df, batch_id: int) -> None:
    if not batch_df.isEmpty():
        upsert_hourly_stats(batch_df.toLocalIterator())


def _persist_user_hourly_stats(batch_df, batch_id: int) -> None:
    if not batch_df.isEmpty():
        upsert_user_hourly_stats(batch_df.toLocalIterator())


def main() -> None:
    configure_python_worker()
    kafka_connector_version = os.getenv(
        "SPARK_KAFKA_CONNECTOR_VERSION", "4.2.0"
    )
    spark = (
        SparkSession.builder.appName("MastodonStreaming")
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:"
                    f"{kafka_connector_version}",
                    "org.postgresql:postgresql:42.7.5",
                ]
            ),
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.host", os.getenv("SPARK_UI_HOST", "127.0.0.1"))
        .config(
            "spark.ui.bindAddress",
            os.getenv("SPARK_UI_BIND_ADDRESS", "127.0.0.1"),
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    messages = (
        spark.readStream.format("kafka")
        .option(
            "kafka.bootstrap.servers",
            os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
        )
        .option("subscribe", os.getenv("KAFKA_TOPIC", "mastodon_stream"))
        .option("startingOffsets", "latest")
        .load()
    )

    toots = (
        messages.selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), TOOT_SCHEMA).alias("data"))
        .select("data.*")
        .filter(col("id").isNotNull() & col("text").isNotNull())
        .withColumn("created_at", to_timestamp("created_at"))
        .withColumn("text", regexp_replace("text", r"\s+", " "))
        .withColumn("text", regexp_replace("text", r"^\s+|\s+$", ""))
        .withColumn("toot_length", length("text"))
        .filter((col("text") != "") & col("created_at").isNotNull())
        .filter(col("language").isin("en", "fr"))
    )

    checkpoint_root = os.getenv("SPARK_CHECKPOINT_DIR", "checkpoints")
    toot_query = (
        toots.writeStream.foreachBatch(_persist_batch)
        .option(
            "checkpointLocation",
            os.path.join(checkpoint_root, "toots"),
        )
        .start()
    )

    watermarked = toots.withWatermark("created_at", "2 hours")
    hourly = (
        watermarked.groupBy(window("created_at", "1 hour"))
        .agg(
            count("*").alias("toot_count"),
            avg("toot_length").alias("avg_length"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "toot_count",
            "avg_length",
        )
    )
    hourly_query = (
        hourly.writeStream.outputMode("update")
        .foreachBatch(_persist_hourly_stats)
        .option(
            "checkpointLocation",
            os.path.join(checkpoint_root, "hourly"),
        )
        .start()
    )

    user_hourly = (
        watermarked.groupBy(window("created_at", "1 hour"), "username")
        .agg(
            count("*").alias("toot_count"),
            avg("toot_length").alias("avg_length"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "username",
            "toot_count",
            "avg_length",
        )
    )
    user_hourly_query = (
        user_hourly.writeStream.outputMode("update")
        .foreachBatch(_persist_user_hourly_stats)
        .option(
            "checkpointLocation",
            os.path.join(checkpoint_root, "user-hourly"),
        )
        .start()
    )

    try:
        spark.streams.awaitAnyTermination()
    finally:
        for query in (toot_query, hourly_query, user_hourly_query):
            if query.isActive:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
