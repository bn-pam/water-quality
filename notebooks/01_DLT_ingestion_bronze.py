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


# pipeline DLT - Ingestion BRONZE avec Auto Loader

def _read_data_stream(file_pattern, table_name):
    """
    Configure Auto Loader pour un motif de fichier spécifique et retourne le DataFrame.
    """

    read_path = f"{LANDING_ZONE_PATH}**/{file_pattern}.csv"

    # Chemin de checkpoint indépendant pour chaque flux
    checkpoint_loc = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/checkpoints/{table_name}"

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("cloudFiles.schemaLocation", checkpoint_loc)

        # Options d'ingestion robuste (tout en string par défaut)
        .option("mode", "PERMISSIVE")
        .option("multiLine", "true")
        .option("escape", '"')
        .option("quote", '"')

        .load(read_path)
    )


# --- 1. Table BRONZE : Classification (UDI) ---
@dlt.table(
    name="bronze_udi_classification_tbl",
    comment="Classification des unités de distribution (UDI) et lien Commune/UDI",
    table_properties={"qualite_eau.quality": "bronze"}
)
def bronze_udi_classification_pipeline():
    # Cible: *dis_com_udi_*.csv
    return _read_data_stream("dis_com_udi_*", "bronze_udi_classification")


# --- 2. Table BRONZE : Synthèse (PLV - Points de Vente) ---
@dlt.table(
    name="bronze_plv_summary_tbl",
    comment="Synthèse des conclusions sur les points de vente (conformité générale)",
    table_properties={"qualite_eau.quality": "bronze"}
)
def bronze_plv_summary_pipeline():
    # Cible: *dis_plv_*_*.csv
    return _read_data_stream("dis_plv_*", "bronze_plv_summary")


# --- 3. Table BRONZE : Résultats d'Analyse (Analyses détaillées) ---
@dlt.table(
    name="bronze_detailed_results_tbl",
    comment="Résultats détaillés des analyses par paramètre (la table la plus volumineuse)",
    table_properties={"qualite_eau.quality": "bronze"}
)
def bronze_detailed_results_pipeline():
    # Cible: *dis_result_*_*.csv
    return _read_data_stream("dis_result_*", "bronze_detailed_results")