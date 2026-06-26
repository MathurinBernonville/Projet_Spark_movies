"""Exploration : partition pruning et predicate pushdown sur la couche silver.

On mesure combien de données Spark lit selon qu'on filtre ou non sur 'annee',
la colonne de partitionnement de la couche silver.

Lancement depuis la racine du projet :
    python starter-code/exploration.py
"""

import time
from pyspark.sql import functions as F
from spark_session import get_spark

SORTIE_SILVER = "data/output/silver_ratings"

spark = get_spark("Exploration - Partition Pruning")
print("Spark UI disponible sur http://localhost:4040\n")

# --------------------------------------------------------------------------
# Mesure 1 : SANS filtre — Spark lit toutes les partitions
# --------------------------------------------------------------------------
df = spark.read.parquet(SORTIE_SILVER)

t0 = time.time()
nb_sans_filtre = df.agg(F.avg("rating")).collect()
t1 = time.time()
duree_sans_filtre = t1 - t0

print(f"Sans filtre  — note moyenne : {nb_sans_filtre[0][0]:.4f} | durée : {duree_sans_filtre:.3f}s")

# --------------------------------------------------------------------------
# Mesure 2 : AVEC filtre sur annee=2015 — Spark ne lit qu'une partition
# --------------------------------------------------------------------------
df_filtre = spark.read.parquet(SORTIE_SILVER).filter(F.col("annee") == 2015)

t0 = time.time()
nb_avec_filtre = df_filtre.agg(F.avg("rating")).collect()
t1 = time.time()
duree_avec_filtre = t1 - t0

print(f"Avec filtre  — note moyenne : {nb_avec_filtre[0][0]:.4f} | durée : {duree_avec_filtre:.3f}s")

# --------------------------------------------------------------------------
# Résumé
# --------------------------------------------------------------------------
print()
print("=== Résumé partition pruning ===")
print(f"Sans filtre (toutes les années) : {duree_sans_filtre:.3f}s")
print(f"Avec filtre (annee=2015)        : {duree_avec_filtre:.3f}s")
gain = ((duree_sans_filtre - duree_avec_filtre) / duree_sans_filtre) * 100
print(f"Gain estimé                     : {gain:.1f}%")

# --------------------------------------------------------------------------
# Vérifier le plan d'exécution : PartitionFilters confirme le pruning
# --------------------------------------------------------------------------
print()
print("=== Plan avec filtre (chercher PartitionFilters) ===")
df_filtre.explain(mode="formatted")

input("\nSpark UI sur http://localhost:4040 - Entree pour quitter...")
spark.stop()
