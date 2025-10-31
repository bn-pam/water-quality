import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from azure.storage.blob import BlobServiceClient

# Configuration ADLS
STORAGE_ACCOUNT_NAME = "mondatalake" # nom datalake ADLS
CONTAINER_NAME = "landingzone" # Conteneur de la source
BRONZE_CONTAINER = "bronze" # Conteneur de destination

STORAGE_ACCOUNT_KEY = "" # clé datalake ADLS, meilleur pratique : utiliser un keyvault ou secret scope
LANDING_ZONE_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/"

# variable propre à DBS
TARGET_DB = "qualite_eau_db_bronze" # Le schéma cible de DLT/Hive

# Connexion à ADLS
try:
    connect_str = f"DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT_NAME};AccountKey={STORAGE_ACCOUNT_KEY};EndpointSuffix=core.windows.net"
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    print(f"Connexion ADLS réussie pour le container '{CONTAINER_NAME}'.")
except Exception as e:
    print(f"Erreur de connexion ADLS : {e}")

# 1. Configuration par Clé (méthode de secours)
spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", STORAGE_ACCOUNT_KEY)

# pipeline DLT - Ingestion SILVER avec transformations


# --- 1. Table SILVER : Classification UDI/Commune (Basé sur : inseecommune, nomcommune, cdreseau, nomreseau, debutalim) ---
@dlt.table(
    name="silver_udi_classification",
    comment="Classification UDI et Communes: Nettoyée et typée."
)
def silver_udi_classification_pipeline():
    # Source : bronze.bronze_udi_classification_tbl
    df_bronze = dlt.read("bronze.bronze_udi_classification_tbl")

    return (
        df_bronze
        .select(
            # Clés et Renommage
            trim(col("inseecommune")).cast(StringType()).alias("insee_commune_code"),
            trim(col("nomcommune")).alias("commune_name"),
            trim(col("quartier")).alias("quartier_name"),
            trim(col("cdreseau")).cast(StringType()).alias("reseau_code"),
            trim(col("nomreseau")).alias("reseau_name"),

            # Conversion de la date
            to_date(col("debutalim"), "yyyy-MM-dd").alias("debut_alimentation_date"),

            # Colonne _rescued_data (utile pour le debug)
            col("_rescued_data").alias("malformed_data_udi")
        )
        .filter(col("insee_commune_code").isNotNull())
    )


# --- 2. Table SILVER : Synthèse PLV (Basé sur : cddept, referenceprel, conclusionprel, plvconformite...) ---
@dlt.table(
    name="silver_plv_summary",
    comment="Synthèse des Points de Vente: Conversion des dates, typage des conclusions et partitionnement.",
    table_properties={
        "quality": "silver",  # Optionnel, mais bonne pratique
        # Ligne CORRECTE : Utilisation du paramètre DLT natif pour le partitionnement
        "pipelines.partition_by": "annee_prel, departement_code"
    }
)
def silver_plv_summary_pipeline():
    # Source : bronze.bronze_plv_summary_tbl
    df_bronze = dlt.read("bronze.bronze_plv_summary_tbl")

    return (
        df_bronze
        .select(
            # Clés
            trim(col("cddept")).alias("departement_code"),
            trim(col("inseecommuneprinc")).cast(StringType()).alias("insee_commune_code_principal"),
            trim(col("referenceprel")).alias("prelevement_reference"),

            # Conversion de la date et extraction de l'année pour partitionnement
            to_date(col("dateprel"), "yyyy-MM-dd").alias("prelevement_date"),
            year(to_date(col("dateprel"), "yyyy-MM-dd")).alias("annee_prel"),  # Pour le PARTITIONNEMENT

            # Conversion du débit (pourcentdebit)
            col("pourcentdebit").cast(DoubleType()).alias("pourcentage_debit"),

            # Données de conformité
            trim(col("conclusionprel")).alias("conclusion_prelevement"),
            trim(col("plvconformitebacterio")).alias("conformite_bacterio"),
            trim(col("plvconformitechimique")).alias("conformite_chimique"),

            # Colonne _rescued_data
            col("_rescued_data").alias("malformed_data_plv"),

            current_timestamp().alias("processing_time")
        )
        # Filtre les lignes où l'on ne peut pas extraire de date
        .filter(col("prelevement_date").isNotNull())
    )


# --- 3. Table SILVER : Résultats d'Analyse Détaillés (FINAL avec ENRICHISSEMENT GEO) ---
@dlt.table(
    name="silver_detailed_results",
    comment="Résultats détaillés enrichis avec les coordonnées du référentiel et partitionnés.",
    table_properties={
        "quality": "silver",
        "pipelines.partition_by": "annee_prel, departement_code"
    }
)
def silver_detailed_results_pipeline():
    # Source 1: Résultats détaillés (contient les valeurs mesurées)
    df_results_bronze = dlt.read("bronze.bronze_detailed_results_tbl").alias("res")

    # Source 2: Synthèse PLV (contient la clé d'enrichissement : Code INSEE et Référence du prélèvement)
    df_plv_bronze = dlt.read("bronze.bronze_plv_summary_tbl").alias("plv")

    # Source 3: RÉFÉRENTIEL GÉOGRAPHIQUE ENRICHI (Pour les Longitude/Latitude)
    df_ref_geo = dlt.read("silver.referentiel_commune_geo").alias("geo")

    # 1. Joindre Résultat (res) avec Synthèse PLV (plv) sur la référence du prélèvement
    df_step1 = df_results_bronze.join(
        df_plv_bronze,
        on=(col("res.referenceprel") == col("plv.referenceprel")),
        how="left"
    )

    # 2. Joindre le résultat (Step 1) avec le Référentiel Géo (geo) sur le Code INSEE
    df_final = df_step1.join(
        df_ref_geo,
        on=(col("plv.inseecommuneprinc") == col("geo.insee_commune_code")),
        how="left"  # Jointure gauche pour garder toutes les analyses, même sans géocodage
    )

    return (
        df_final
        .select(
            # CLÉS D'IDENTIFICATION
            trim(col("plv.inseecommuneprinc")).cast(StringType()).alias("insee_commune_code"),
            trim(col("plv.cddept")).alias("departement_code"),

            # Paramètres de prélèvement
            col("res.referenceprel").alias("prelevement_reference"),
            col("limitequal").cast(DoubleType()).alias("limite_qualite_reglementaire"),

            # Date et Partitionnement
            to_date(col("plv.dateprel"), "yyyy-MM-dd").alias("prelevement_date"),
            year(to_date(col("plv.dateprel"), "yyyy-MM-dd")).alias("annee_prel"),

            # Paramètres d'Analyse (colonnes du log d'erreur)
            trim(col("res.cdparametre")).alias("parametre_code"),
            trim(col("res.libmajparametre")).alias("parametre_name_major"),

            # CONVERSIONS NUMÉRIQUES
            col("res.valtraduite").cast(DoubleType()).alias("resultat_mesure"),

            # ENRICHISSEMENT GÉOGRAPHIQUE
            col("geo.longitude").alias("longitude"),  # Récupéré du référentiel
            col("geo.latitude").alias("latitude"),  # Récupéré du référentiel

            col("res._rescued_data").alias("malformed_data_detailed"),
            current_timestamp().alias("processing_time")
        )
        # Filtre final pour ne garder que les lignes avec des données d'analyse principales
        .filter(col("resultat_mesure").isNotNull() & col("insee_commune_code").isNotNull())
    )