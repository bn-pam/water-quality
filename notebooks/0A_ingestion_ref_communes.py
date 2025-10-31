import requests
from pyspark.sql.functions import *
from pyspark.sql.types import *
from azure.storage.blob import BlobServiceClient

# Configuration ADLS
STORAGE_ACCOUNT_NAME = "stpbowaterqualitynq2txl"
BRONZE_CONTAINER = "bronze" # Conteneur de la source
BRONZE_DB = "bronze"
SILVER_CONTAINER = "silver" # Conteneur de destination



# variable propre à DBS
TARGET_DB = "silver" # Le schéma cible de DLT/Hive

# URL d'un référentiel de communes publiques (INSEE/COG)
URL_REFERENTIEL = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/georef-france-commune/exports/csv/?delimiters=%3B&lang=fr&timezone=Europe%2FParis&use_labels=true" # Exemple d'URL directe
REF_PATH = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/referentiel/communes_georef.csv"

# 1. Téléchargement sécurisé et Upload vers ADLS (Méthode Python/requests)
try:
    print(f"Téléchargement du référentiel depuis {URL_REFERENTIEL}...")

    # 1a. Téléchargement du contenu
    response = requests.get(URL_REFERENTIEL)
    response.raise_for_status()
    print(f"✅ Téléchargement réussi. Taille du fichier : {len(response.content)} octets")

    # 1b. Connexion et Upload
    STORAGE_ACCOUNT_KEY = "" # clé datalake ADLS, meilleur pratique : utiliser un keyvault ou secret scope
    connect_str = f"DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT_NAME};AccountKey={STORAGE_ACCOUNT_KEY};EndpointSuffix=core.windows.net"
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # Upload direct du contenu dans le conteneur BRONZE
    blob_client = blob_service_client.get_blob_client(container=BRONZE_CONTAINER,
                                                      blob="referentiel/communes_georef.csv")
    blob_client.upload_blob(response.content, overwrite=True)

    print(f"✅ Fichier téléchargé et sauvegardé dans ADLS à : {REF_PATH}")

except Exception as e:
    print(f"❌ ERREUR lors du téléchargement ou de l'upload du référentiel : {e}")
    # dbutils.notebook.exit("Échec du chargement du référentiel.")

# Configuration Spark pour l'accès (par clé si non configuré précédemment)
spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", STORAGE_ACCOUNT_KEY)

# 2. Lecture Spark depuis ADLS (Méthode sécurisée)
print("\nLecture Spark du fichier depuis ADLS et création de la table Silver...")

df_ref_bronze = (spark.read
                 .format("csv")
                 .option("header", "true")
                 .option("delimiter", ";")
                 .load(REF_PATH)
                 )

df_temp = df_ref_bronze.withColumn(
    "coordonnees_split",
    # Utilisation de la virgule comme séparateur interne pour le Geo Point
    split(col("Geo Point"), ",")
)

df_ref_silver = (df_temp
                 .select(
    col("Code Officiel Commune").alias("insee_commune_code"),
    col("Nom Officiel Commune").alias("commune_name_ref"),

    # Étape B : On extrait le 1er élément (Latitude) et on le type
    trim(col("coordonnees_split").getItem(0)).cast(DoubleType()).alias("latitude"),

    # Étape C : On extrait le 2e élément (Longitude) et on le type
    trim(col("coordonnees_split").getItem(1)).cast(DoubleType()).alias("longitude")
)
                 .filter(col("insee_commune_code").isNotNull())
                 .distinct()
                 )

# 3. Écrire le Référentiel dans une Table Silver
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
df_ref_silver.write.format("delta").mode("overwrite").saveAsTable("silver.referentiel_commune_geo")

print("✅ Table silver.referentiel_commune_geo créée et peuplée.")