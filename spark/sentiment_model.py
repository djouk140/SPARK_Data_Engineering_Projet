"""Train and evaluate an English sentiment model with Spark MLlib."""

import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import CountVectorizer, StopWordsRemover, Tokenizer
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, trim, when
from pyspark.sql.types import StringType, StructField, StructType

from spark.session import configure_python_worker

load_dotenv()


def build_training_pipeline() -> Pipeline:
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    stopwords = StopWordsRemover(
        inputCol="words", outputCol="filtered_words"
    )
    vectorizer = CountVectorizer(
        inputCol="filtered_words",
        outputCol="features",
        vocabSize=20000,
        minDF=2,
    )
    classifier = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        maxIter=20,
        regParam=0.01,
    )
    return Pipeline(stages=[tokenizer, stopwords, vectorizer, classifier])


def normalize_text(data: DataFrame) -> DataFrame:
    return (
        data.withColumn("text", lower(col("text")))
        .withColumn("text", regexp_replace("text", r"https?://\S+", " "))
        .withColumn("text", regexp_replace("text", r"@\w+", " "))
        .withColumn("text", regexp_replace("text", r"[^a-z\s]", " "))
        .withColumn("text", regexp_replace("text", r"\s+", " "))
        .withColumn("text", trim("text"))
    )


def main() -> None:
    configure_python_worker()
    data_path = Path(os.getenv("SENTIMENT140_PATH", "data/sentiment140.csv"))
    if not data_path.is_file():
        raise FileNotFoundError(
            f"Sentiment140 CSV not found: {data_path}. "
            "Set SENTIMENT140_PATH or place the six-column CSV there."
        )

    spark = (
        SparkSession.builder.appName("MastodonSentimentTraining")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    schema = StructType(
        [
            StructField(f"_c{index}", StringType(), True)
            for index in range(6)
        ]
    )
    raw = (
        spark.read.option("header", "false")
        .option("quote", '"')
        .schema(schema)
        .csv(str(data_path))
    )
    data = (
        raw.select(
            col("_c0").cast("int").alias("original_label"),
            col("_c5").alias("text"),
        )
        .filter(col("text").isNotNull())
        .filter(col("original_label").isin(0, 4))
        .withColumn(
            "label",
            when(col("original_label") == 4, 1.0).otherwise(0.0),
        )
    )
    data = normalize_text(data.select("text", "label")).filter(
        col("text") != ""
    )

    if data.select("label").distinct().count() != 2:
        raise ValueError(
            "Training data must contain both Sentiment140 labels 0 and 4."
        )

    train, test = data.randomSplit([0.8, 0.2], seed=42)
    model = build_training_pipeline().fit(train)
    predictions = model.transform(test)

    accuracy = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="accuracy",
    ).evaluate(predictions)
    f1 = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="f1",
    ).evaluate(predictions)
    print(f"Test accuracy: {accuracy:.4f}")
    print(f"Test weighted F1: {f1:.4f}")
    predictions.select("text", "label", "prediction").show(
        20, truncate=False
    )

    model_path = Path(
        os.getenv("SENTIMENT_MODEL_PATH", "spark/models/sentiment_pipeline")
    )
    model.write().overwrite().save(str(model_path))
    print(f"Saved trained Spark ML pipeline to {model_path}")
    spark.stop()


if __name__ == "__main__":
    main()
