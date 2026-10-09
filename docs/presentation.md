# Project presentation: Mastodon stream analysis with Spark

Use the English slide text for the final presentation; the Japanese paragraphs
are speaker notes, covering the Japanese-language objective in the brief.
Replace bracketed result fields only with measurements produced by a real run.

## Slide 1 — Goal

**English:** We built a reproducible pipeline for collecting public Mastodon
posts, processing a live stream and analyzing historical data with Apache
Spark.

**日本語メモ:** Mastodonの公開投稿を収集し、Apache Sparkを使ってリアルタイム処理と
過去データ分析を行う、再現可能なデータパイプラインを構築しました。

## Slide 2 — Architecture

**English:** A Python Mastodon listener cleans post HTML and filters configured
keywords. Kafka transports JSON records. Spark Structured Streaming parses
and filters events, then PostgreSQL stores posts and hourly aggregates.

**日本語メモ:** PythonのリスナーがHTMLをテキストに変換し、指定キーワードで絞り込みます。
KafkaがJSONを運び、Spark Structured Streamingが解析・集計し、PostgreSQLに保存します。

## Slide 3 — Stream and storage

**English:** The stream stores English and French posts, counts posts in
watermarked hourly windows, and measures average post length per user. Database
upserts make Kafka replays safe. Public Mastodon posts do not provide precise
location, so we do not infer or store it.

**日本語メモ:** 英語とフランス語の投稿を保存し、ウォーターマーク付きの時間窓で投稿数を数え、
ユーザーごとの平均文字数を計算します。再処理時の重複を防ぐため、DBではupsertを使います。
公開投稿に正確な位置情報はないため、推測して保存することはありません。

## Slide 4 — Historical processing and sentiment

**English:** Batch Spark reads PostgreSQL, finds daily volume, frequent
hashtags and active users, and benchmarks a repeated aggregation with and
without repartitioning/caching. A CountVectorizer and Logistic Regression
pipeline is trained and evaluated on labeled Sentiment140 English data. We
score English Mastodon posts; low-confidence predictions are marked neutral.

**日本語メモ:** バッチ処理では日別件数、人気ハッシュタグ、活発なユーザーを集計し、
repartitionとcacheの効果を測定します。ラベル付きの英語Sentiment140データで
CountVectorizerとロジスティック回帰を学習・評価します。英語投稿だけを分類し、
信頼度が低い予測は中立とします。

## Slide 5 — Findings and limitations

**English:** From our actual run: **[toots collected]**, **[top hashtag]**,
**[sentiment distribution]**, and **[measured baseline/optimized times]**.
The neutral label is a confidence threshold, not a class learned from the
binary training labels. Results depend on the selected Mastodon instance,
collection period, keywords and language.

**日本語メモ:** 実測結果は、収集投稿数が**[件数]**、最多ハッシュタグが**[タグ]**、
感情分布が**[分布]**、処理時間が**[比較結果]**です。中立は二値学習データから
学習したクラスではなく、信頼度の閾値で決めています。結果はインスタンス、
収集期間、キーワード、言語に依存します。

## Slide 6 — Live demo

**English:** We start Kafka and PostgreSQL with Docker Compose, run the Mastodon
producer and Spark stream, then query the newest rows in `toots`. We finish by
running the historical batch and showing the notebook visualizations.

**日本語メモ:** Docker ComposeでKafkaとPostgreSQLを起動し、Mastodonプロデューサーと
Sparkストリーミングを実行します。`toots`テーブルの最新データを確認した後、
バッチ分析を実行し、Notebookのグラフを紹介します。
