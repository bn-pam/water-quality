import requests
import zipfile
import io
import re
import time
from urllib.parse import urljoin
from azure.storage.blob import BlobServiceClient

# --- 1. Configuration ---

# Termes de recherche pour trouver les bons datasets via l'API
SEARCH_QUERY = "resultats controle sanitaire eau"
# Filtre pour les noms de fichiers (comme vous l'avez demandé)
FILE_FILTER = "-dept"

# URL de l'API de recherche de data.gouv.fr
DATA_GOV_API_URL = "https://www.data.gouv.fr/api/1/search/"
SOURCE_PAGE_URL = "https://www.data.gouv.fr/datasets/resultats-du-controle-sanitaire-de-leau-distribuee-commune-par-commune/"

# Configuration ADLS
STORAGE_ACCOUNT_NAME = "stpbowaterqualitynq2txl"
CONTAINER_NAME = "landingzone"

# Utiliser un secret scope est la MEILLEURE pratique
STORAGE_ACCOUNT_KEY = "" # clé datalake ADLS, meilleur pratique : utiliser un keyvault ou secret scope
ZIP_URL = "https://www.data.gouv.fr/api/1/datasets/r/3c5ebbd9-f6b5-4837-a194-12bfeda7f38e"
OUTPUT_FILENAME = "dis-2021-dept.zip" # Nom du fichier à la source

# --- 2. Connexion à ADLS ---
try:
    connect_str = f"DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT_NAME};AccountKey={STORAGE_ACCOUNT_KEY};EndpointSuffix=core.windows.net"
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    print(f"Connexion ADLS réussie pour le container '{CONTAINER_NAME}'.")
except Exception as e:
    print(f"Erreur de connexion ADLS : {e}")

# --- 3. Téléchargement, Unzip, et Upload (Logique de boucle ajustée) ---
print(f"\nTentative de téléchargement du ZIP : {ZIP_URL}")
try:
    # 1. Télécharger le contenu du .zip
    print("  > 1. Téléchargement du .zip...")
    zip_response = requests.get(ZIP_URL)
    zip_response.raise_for_status()

    print(f"  > Téléchargement réussi (Statut {zip_response.status_code}).")

    # 2. Ouvrir le .zip en mémoire
    print("  > 2. Ouverture du .zip en mémoire...")
    zip_in_memory = io.BytesIO(zip_response.content)

    files_uploaded_count = 0

    with zipfile.ZipFile(zip_in_memory, 'r') as zf:

        # 3. Trouver TOUS les .TXT à l'intérieur
        txt_filenames = [name for name in zf.namelist() if name.lower().endswith('.txt')]

        if txt_filenames:
            print(f"  > 3. {len(txt_filenames)} fichier(s) TXT trouvé(s) dans le zip.")

            # Boucle sur TOUS les fichiers .txt
            for txt_filename in txt_filenames:
                # 4. Extraire et Uploader le TXT
                with zf.open(txt_filename) as txt_file:
                    # --- MODIFICATIONS POUR LE DOSSIER ANNUEL ---

                    # a) Extraction de l'année (recherche d'un nombre à 4 chiffres)
                    year_match = re.search(r'(\d{4})', txt_filename)
                    year_folder = year_match.group(
                        1) if year_match else "unknown_year"  # Si non trouvé, utilise 'unknown_year'

                    # b) Renommage en .csv
                    clean_blob_name = re.sub(r'[^a-zA-Z0-9-.]', '_', txt_filename.lower()).replace(".txt", ".csv")

                    # c) Construction du chemin final: ANNEE/NOM_FICHIER.csv
                    final_blob_path = f"{year_folder}/{clean_blob_name}"

                    # --- FIN DES MODIFICATIONS ---

                    print(f"  > 4. Upload vers le dossier : {year_folder}...")

                    # Uploader vers le chemin final
                    blob_client = container_client.get_blob_client(final_blob_path)
                    blob_client.upload_blob(txt_file.read(), overwrite=True)

                    files_uploaded_count += 1

            print(f"--- Succès. {files_uploaded_count} fichier(s) uploadé(s) dans landingzone/ANNEE/. ---")

        else:
            print("  > ERREUR : Aucun fichier .txt trouvé dans ce .zip.")
            dbutils.notebook.exit("Échec Ingestion : Pas de TXT dans le ZIP")

except requests.exceptions.HTTPError as errh:
    print(f"--- ERREUR HTTP --- Statut: {errh.response.status_code}")
    dbutils.notebook.exit("Échec Ingestion : HTTP Error")

except Exception as e:
    print("--- ERREUR INCONNUE / DÉCOMPRESSION ---")
    print(e)
    dbutils.notebook.exit("Échec Ingestion : Autre")

