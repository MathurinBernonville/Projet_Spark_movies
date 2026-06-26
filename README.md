# Projet Spark - Pipeline MovieLens

Projet réalisé dans le cadre du cours Apache Spark (Hetic MD4, jour 4).
Pipeline PySpark de bout en bout sur le dataset MovieLens small : ingestion, nettoyage, analyses et exploration des performances.

## Contenu

- `pipeline.py` - pipeline principal (bronze -> silver -> gold)
- `exploration.py` - mesure du partition pruning sur la couche silver
- `spark_session.py` - helper SparkSession
- `data/` - couche silver (Parquet) et résultats des analyses (gold)
- `rapport.md` - rapport écrit avec captures Spark UI

## Prérequis

- Python 3.9+
- Java 17 ou 21
- PySpark 4.x (`pip install pyspark`)
- Dataset MovieLens small dans `data/datasets/ml-latest-small/`

## Lancement

```bash
python pipeline.py
python exploration.py
```

Spark UI disponible sur http://localhost:4040 pendant l'exécution.
