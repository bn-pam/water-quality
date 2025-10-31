import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window # Nécessaire pour la Tâche 4

TARGET_DB = "gold" # Schéma des tables Silver (à ajuster si besoin)

# --- 1. Table GOLD : Conformité par Commune ---
@dlt.table(
    name="gold_conformite_commune",
    comment="Taux de non-conformité global par commune, agrégé sur l'année."
)
def gold_conformite_commune_agg():
    # Source: silver_plv_summary (Synthèse des conclusions)
    df_plv_silver = dlt.read("silver.silver_plv_summary")

    return (
        df_plv_silver
        .groupBy(
            col("annee_prel").alias("annee"),
            col("departement_code"),
            col("insee_commune_code_principal").alias("insee_commune_code")
        )
        .agg(
            count(col("prelevement_date")).alias("nb_prelevements_total"),
            sum(
                when(col("conclusion_prelevement").rlike("Non conforme"), 1)
                .otherwise(0)
            ).alias("nb_non_conforme"),
            round(
                (col("nb_non_conforme") / col("nb_prelevements_total") * 100), 2
            ).alias("taux_non_conformite_percent")
        )
        .filter(col("nb_prelevements_total") > 0)
        .withColumn("date_agregation", current_date())
    )


# --- Table GOLD : Non-Conformité par Paramètre ---
@dlt.table(
    name="gold_non_conformite_param",
    comment="Analyse des non-conformités par paramètre et par année, basé sur le dépassement de la limite légale."
)
def gold_non_conformite_param_agg():
    # Source: silver_detailed_results (Analyses détaillées)
    df_results_silver = dlt.read("silver.silver_detailed_results")

    return (
        df_results_silver
        # On ne garde que les lignes où la limite est définie (pour le calcul)
        .filter(col("limite_qualite_reglementaire").isNotNull())
        .groupBy(
            col("annee_prel").alias("annee"),
            col("departement_code"),
            col("parametre_code"),
            col("parametre_name_major")
        )
        .agg(
            count(col("resultat_mesure")).alias("nb_analyses_total"),

            # Compte le nombre de dépassements
            sum(
                when(col("resultat_mesure") > col("limite_qualite_reglementaire"), 1)
                .otherwise(0)
            ).alias("nb_depassements"),

            # Calcule le taux de non-conformité par paramètre (arrondi)
            round(
                (col("nb_depassements") / col("nb_analyses_total") * 100), 2
            ).alias("taux_depassement_percent")
        )
        .filter(col("nb_analyses_total") > 0)
        .withColumn("date_agregation", current_date())
    )


# --- Table GOLD : Top 10 Communes les Moins/Plus Conformes ---
@dlt.table(
    name="gold_top_communes",
    comment="Classement annuel des 10 communes les plus/moins conformes, basé sur la conformité globale."
)
def gold_top_communes_agg():
    # Source 1: gold_conformite_commune (Résultats agrégés déjà calculés)
    df_conformite = dlt.read("gold_conformite_commune")

    # Source 2: silver_udi_classification (Pour obtenir le nom de la commune)
    # Assurez-vous d'utiliser le bon schéma pour dlt.read (ici, bronze)
    df_udi_silver = dlt.read("silver.silver_udi_classification")

    # Joindre les deux pour avoir les noms
    df_joined = df_conformite.join(
        df_udi_silver.select("insee_commune_code", "commune_name"),
        on="insee_commune_code",
        how="left"
    )

    # Création de la fonction de fenêtre pour le classement
    window_spec = Window.partitionBy("annee").orderBy(
        col("taux_non_conformite_percent").desc()  # Taux DESC pour le classement (1er = pire)
    )

    return (
        df_joined
        .withColumn("rank_non_conforme", rank().over(window_spec))
        .filter(col("nb_prelevements_total") >= 5)  # Filtre pour la pertinence statistique (min 5 prélèvements)
        .filter(col("rank_non_conforme") <= 10)
        .select(
            "annee",
            "rank_non_conforme",
            "departement_code",
            "commune_name",
            "taux_non_conformite_percent"
        )
        .withColumn("date_agregation", current_date())
    )


# --- Table GOLD : Carte de Qualité par Commune (pour visualisation) ---
@dlt.table(
    name="gold_carte_qualite",
    comment="Agrégation de la conformité globale et des coordonnées géographiques pour la cartographie."
)
def gold_carte_qualite_agg():
    # Source 1: gold_conformite_commune (Résultats agrégés)
    df_conformite = dlt.read("gold_conformite_commune")

    # Source 2: silver_detailed_results (Pour récupérer les coordonnées enrichies)
    df_results_silver = dlt.read("silver.silver_detailed_results")

    # Jointure pour récupérer la dernière géolocalisation connue
    df_geo = (df_results_silver
              .select("insee_commune_code", "longitude", "latitude")
              .filter(col("longitude").isNotNull())
              .distinct()
              )

    return (
        df_conformite.alias("conf")
        .join(
            df_geo.alias("geo"),
            on=(col("conf.insee_commune_code") == col("geo.insee_commune_code")),
            how="left"
        )
        .select(
            col("annee"),
            col("departement_code"),
            col("conf.insee_commune_code"),
            col("taux_non_conformite_percent"),
            col("geo.longitude"),
            col("geo.latitude")
        )
        .withColumn("date_agregation", current_date())
    )