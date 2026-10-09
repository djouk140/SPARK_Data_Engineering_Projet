# Mastodon + Kafka + Spark

Pipeline de bout en bout pour collecter des publications publiques Mastodon, les
transporter avec Kafka, les traiter en streaming avec Spark Structured
Streaming, les historiser dans PostgreSQL, puis produire des analyses batch,
une classification de sentiment MLlib et des visualisations Jupyter.

## Architecture

```text
Mastodon public stream
        | producer/mastodon_producer.py (nettoyage HTML + mots-clés)
        v
Kafka: mastodon_stream (JSON)
        | spark/streaming.py (Spark Structured Streaming)
        +--> toots (PostgreSQL; upsert idempotent)
        +--> toot_hourly_stats (fenêtres horaires)
        +--> user_hourly_stats (activité et longueur moyenne par utilisateur)
                 |
                 +--> spark/batch_analysis.py
                       +-- agrégats quotidiens / hashtags / utilisateurs actifs
                       +-- scoring positif/négatif/neutre des toots en anglais
                       +-- exports CSV dans reports/batch/
                 |
                 +--> notebooks/analysis.ipynb (graphiques)
```

La localisation précise n'est pas disponible dans les statuts publics Mastodon
et n'est donc pas inventée ni stockée. Les boosts sont écartés afin d'éviter les
doublons. Les toots en anglais et en français sont stockés; le modèle
Sentiment140, entraîné sur des textes anglais, ne score que l'anglais.

## Pré-requis

- Python 3.10+ et Java 17 ou ultérieur (Spark 4.2.x; `JAVA_HOME` configuré).
- Docker Desktop / Docker Compose.
- Un jeton Mastodon avec accès au flux public de l'instance.
- Pour l'entraînement MLlib : le CSV Sentiment140 (6 colonnes, sans en-tête),
  à télécharger depuis
  [Kaggle](https://www.kaggle.com/datasets/kazanova/sentiment140) et à placer
  sous `data/sentiment140.csv`.

## Installation et démarrage (PowerShell)

Depuis la racine du dépôt :

```powershell
if (-not (Test-Path .venv\Scripts\python.exe)) { py -3.14 -m venv .venv }
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
# Modifier .env : renseigner MASTODON_ACCESS_TOKEN et choisir un mot de passe.
.\.venv\Scripts\Activate.ps1
pip install -r mastodon-spark-project\requirements.txt
docker compose --env-file .env -f mastodon-spark-project\docker-compose.yml up -d --build
```

`sql/init.sql` est monté dans PostgreSQL et crée les tables au premier
démarrage de son volume. Kafka publie son port `9092` et PostgreSQL son port
`15432` sur la machine hôte, uniquement sur l'interface locale; PostgreSQL
reste sur le port `5432` à l'intérieur du réseau Docker. Kafka, PostgreSQL et
le consommateur Spark sont démarrés par Compose; `kafka-init` termine
normalement après avoir créé le topic. Les ports `9092` et `15432` sont des
ports de protocoles applicatifs, pas des pages web à ouvrir dans un navigateur.
Vérifier les services et suivre les logs Spark avec :

```powershell
docker compose --env-file .env -f mastodon-spark-project\docker-compose.yml ps
docker compose --env-file .env -f mastodon-spark-project\docker-compose.yml logs -f spark-streaming
```

L'interface Spark UI est disponible sur `http://127.0.0.1:4050` lorsque le
consommateur est actif. N'ouvrez pas `9092` ou `15432` dans un navigateur :
ils servent respectivement aux protocoles Kafka et PostgreSQL.

Le producteur Mastodon n'est pas lancé par défaut dans Docker : il nécessite
un jeton API secret. Après avoir renseigné `MASTODON_ACCESS_TOKEN` dans `.env`,
lancer le producteur sur l'hôte depuis la racine du dépôt :

```powershell
.\.venv\Scripts\python.exe -m producer.mastodon_producer
```

Le service `kafka-init` crée le topic configuré au démarrage de Compose.
Le consommateur Spark est géré par Compose; son premier démarrage télécharge
les connecteurs Kafka/JDBC depuis Maven Central.
L'instance Mastodon doit autoriser l'accès à son flux public; une instance
peut limiter ou désactiver cet endpoint. Les erreurs de connexion et
d'authentification s'affichent dans le terminal.

## Historique, MLlib et notebook

1. Télécharger/décompresser le CSV Sentiment140 vers `data/sentiment140.csv`.
2. Entraîner et évaluer le pipeline MLlib (split reproductible 80/20,
   accuracy et F1 pondéré) :

   ```powershell
   python -m spark.sentiment_model
   ```

   Le modèle est enregistré dans `spark/models/sentiment_pipeline/`.
3. Lancer les agrégations historiques, le benchmark et le scoring PostgreSQL :

   ```powershell
   python -m spark.batch_analysis
   ```

   Le seuil d'activité est `MIN_USER_TOOTS` (strictement supérieur); les
   exports Spark (CSV) sont déposés dans `reports/batch/`. Le paramètre
   `SENTIMENT_NEUTRAL_CONFIDENCE` (par défaut `0.60`) définit le seuil de
   confiance maximal sous lequel un toot est classé `neutral`. Ces résultats
   sont heuristiques, pas une classe « neutre » apprise par Sentiment140.
4. Ouvrir `notebooks/analysis.ipynb` avec le noyau Python du `.venv` après avoir
   démarré le flux et le batch. Les figures affichent la distribution des
   sentiments, les toots par jour, les hashtags et l'activité horaire des
   utilisateurs. Démarrer Jupyter avec `jupyter lab` depuis la racine du dépôt.

Les tables `toots`, `toot_hourly_stats`, `user_hourly_stats` et
`toot_sentiments` permettent de vérifier le flux en direct, par exemple dans
`psql` :

```sql
SELECT id, created_at, username, text FROM toots
ORDER BY ingested_at DESC LIMIT 10;

SELECT window_start, toot_count, avg_length FROM toot_hourly_stats
ORDER BY window_start DESC LIMIT 10;
```

## Validation

```powershell
python -m unittest discover -s tests -v
```

Pour évaluer le benchmark correctement, le même agrégat est exécuté avant et
après un `repartition` et un cache matérialisé. Le cache peut accélérer des
requêtes répétées; le shuffle de repartition peut au contraire ralentir un
petit jeu de données. Les temps sont mesurés à l'exécution, ils ne sont pas
présentés comme un gain garanti.
Lors d'une exécution directe de Spark sur l'hôte, l'interface Spark UI est
accessible sur `http://localhost:4040`. Avec Docker Compose, utilisez
`http://127.0.0.1:4050`.

## Présentation

Le sujet mentionne une présentation en japonais dans les objectifs mais
demande l'anglais dans les livrables finaux. `docs/presentation.md` propose un
support court en anglais avec notes orales japonaises, afin de couvrir les
deux consignes sans inventer de résultats. Compléter les emplacements de
résultats après la collecte réelle.
