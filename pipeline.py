"""Pipeline data MovieLens - Projet Jour 4.

Architecture :
    brut (bronze) -> nettoyé (silver, Parquet) -> agrégé (gold, résultats)

Lancement, depuis la racine du projet :
    python starter-code/pipeline.py
"""

import sys
import time

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, LongType, StringType, StructField, StructType
from pyspark.sql.window import Window

from spark_session import get_spark

# Chemins MovieLens
RATINGS_CSV   = "data/datasets/ml-latest-small/ratings.csv"
MOVIES_CSV    = "data/datasets/ml-latest-small/movies.csv"
SORTIE_SILVER = "data/output/silver_ratings"
SORTIE_GOLD   = "data/output/analyses"


# ---------------------------------------------------------------------------
# Schémas explicites (pas inferSchema)
# ---------------------------------------------------------------------------

SCHEMA_RATINGS = StructType([
    StructField("userId",    IntegerType(), nullable=False),
    StructField("movieId",   IntegerType(), nullable=False),
    StructField("rating",    FloatType(),   nullable=False),
    StructField("timestamp", LongType(),    nullable=False),
])

SCHEMA_MOVIES = StructType([
    StructField("movieId", IntegerType(), nullable=False),
    StructField("title",   StringType(),  nullable=True),
    StructField("genres",  StringType(),  nullable=True),
])


# ---------------------------------------------------------------------------
# Étape 1a : ingestion
# ---------------------------------------------------------------------------

def ingestion(spark):
    """Lire ratings.csv avec schéma explicite, inspecter."""
    df = (
        spark.read
        .option("header", "true")
        .schema(SCHEMA_RATINGS)
        .csv(RATINGS_CSV)
    )
    df.printSchema()
    print("Lignes brutes :", df.count())
    return df


# ---------------------------------------------------------------------------
# Étape 1b : nettoyage (bronze -> silver)
# ---------------------------------------------------------------------------

def nettoyage(df):
    """Typer, dériver des colonnes, nettoyer."""
    # Convertir le timestamp Unix en date lisible
    df = df.withColumn("date_note", F.to_timestamp(F.col("timestamp")))
    df = df.withColumn("annee",     F.year(F.col("date_note")))

    # Supprimer la colonne timestamp brut (redondant)
    df = df.drop("timestamp")

    # Filtrer les notes hors bornes (MovieLens : 0.5 à 5.0, pas de 0)
    df = df.filter((F.col("rating") >= 0.5) & (F.col("rating") <= 5.0))

    # Supprimer les doublons (même userId + movieId)
    df = df.dropDuplicates(["userId", "movieId"])

    # Supprimer les lignes avec valeurs manquantes
    df = df.na.drop()

    print("Lignes après nettoyage :", df.count())
    return df


# ---------------------------------------------------------------------------
# Étape 1c : écrire la couche silver
# ---------------------------------------------------------------------------

def ecrire_silver(df):
    """Écrire en Parquet, partitionné par année (faible cardinalité)."""
    df.write.mode("overwrite").partitionBy("annee").parquet(SORTIE_SILVER)
    print("Couche silver écrite dans", SORTIE_SILVER)


# ---------------------------------------------------------------------------
# Étape 2 : transformation et analyses (silver -> gold)
# ---------------------------------------------------------------------------

def transformation_et_analyses(spark):
    """3 analyses : agrégation, jointure, window function."""

    df_ratings = spark.read.parquet(SORTIE_SILVER)

    # Optimisation : cache car df_ratings est réutilisé par les 3 analyses
    df_ratings = df_ratings.cache()
    df_ratings.count()  # matérialise le cache
    print("Cache ratings matérialisé.")

    # Charger movies (petite table) avec schéma explicite
    df_movies = (
        spark.read
        .option("header", "true")
        .schema(SCHEMA_MOVIES)
        .csv(MOVIES_CSV)
    )

    # ------------------------------------------------------------------
    # Analyse 1 : agrégation
    # Films les mieux notés (avec seuil minimum de 50 notes)
    # ------------------------------------------------------------------
    analyse_1 = (
        df_ratings
        .groupBy("movieId")
        .agg(
            F.count("rating").alias("nb_notes"),
            F.avg("rating").alias("note_moyenne"),
            F.stddev("rating").alias("ecart_type"),
        )
        .filter(F.col("nb_notes") >= 50)
        .orderBy(F.desc("note_moyenne"))
    )
    print("Analyse 1 — films les mieux notés (min 50 notes) :")
    analyse_1.show(10, truncate=False)

    # ------------------------------------------------------------------
    # Analyse 2 : jointure avec broadcast
    # Joindre ratings + movies pour avoir le titre et le genre
    # F.broadcast sur movies (petite table ~9 700 lignes) évite un shuffle
    # ------------------------------------------------------------------
    t0 = time.time()
    df_joint = df_ratings.join(F.broadcast(df_movies), on="movieId", how="inner")

    analyse_2 = (
        df_joint
        .groupBy("genres")
        .agg(
            F.count("rating").alias("nb_notes"),
            F.avg("rating").alias("note_moyenne"),
        )
        .orderBy(F.desc("note_moyenne"))
    )
    analyse_2.count()  # déclenche le calcul pour mesure
    t1 = time.time()
    print(f"Analyse 2 — note moyenne par genre (broadcast join) : {t1 - t0:.2f}s")
    analyse_2.show(15, truncate=False)

    # ------------------------------------------------------------------
    # Analyse 3 : window function
    # Top 5 films les mieux notés par genre (parmi ceux avec >= 20 notes)
    # ------------------------------------------------------------------
    df_film_genre = (
        df_joint
        .groupBy("movieId", "title", "genres")
        .agg(
            F.count("rating").alias("nb_notes"),
            F.avg("rating").alias("note_moyenne"),
        )
        .filter(F.col("nb_notes") >= 20)
    )

    fenetre = Window.partitionBy("genres").orderBy(F.desc("note_moyenne"))

    analyse_3 = (
        df_film_genre
        .withColumn("rang", F.row_number().over(fenetre))
        .filter(F.col("rang") <= 5)
        .orderBy("genres", "rang")
    )
    print("Analyse 3 — top 5 par genre (window function) :")
    analyse_3.show(30, truncate=False)

    return {
        "analyse_1_top_films":     analyse_1,
        "analyse_2_notes_genre":   analyse_2,
        "analyse_3_top5_par_genre": analyse_3,
    }


# ---------------------------------------------------------------------------
# Étape 3 : écrire les résultats gold
# ---------------------------------------------------------------------------

def ecrire_gold(resultats):
    """Écrire chaque résultat agrégé en Parquet (coalesce(1) acceptable ici)."""
    for nom, df in resultats.items():
        chemin = f"{SORTIE_GOLD}/{nom}"
        df.coalesce(1).write.mode("overwrite").parquet(chemin)
        print("Résultat écrit :", chemin)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    spark = get_spark("Projet Jour 4 - MovieLens")
    print("Spark UI disponible sur http://localhost:4040")

    # Bronze -> Silver
    brut   = ingestion(spark)
    propre = nettoyage(brut)
    ecrire_silver(propre)

    # Silver -> Gold
    resultats = transformation_et_analyses(spark)
    ecrire_gold(resultats)

    input("Spark UI sur http://localhost:4040 - Entree pour quitter...")
    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except NotImplementedError as e:
        print()
        print("Pipeline incomplet :", e)
        print("Complétez les sections TODO dans starter-code/pipeline.py.")
        sys.exit(1)
