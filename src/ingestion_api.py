import os
import logging
import requests
import pandas as pd
import time
import signal
import sys
from datetime import datetime
from sqlalchemy import create_engine

# ==========================================
# 0. CONFIGURATION
# ==========================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logging.info("🚀 Démarrage du Moteur Hybride (Parquet + PostgreSQL)...")

SEUIL_BALEINE = float(os.getenv("SEUIL_BALEINE", 0.5))
INTERVALLE_SECONDES = int(os.getenv("INTERVALLE_SECONDES", 10))
TAILLE_MAX_BUFFER = int(os.getenv("TAILLE_MAX_BUFFER", 500))
RETENTION_JOURS = int(os.getenv("RETENTION_JOURS", 7))
SYMBOLE_CRYPTO = os.getenv("SYMBOLE_CRYPTO", "BTCUSDT")

# ==========================================
# 1. CONNEXION BDD (Le Stockage Chaud)
# ==========================================
DB_USER = os.getenv("DB_USER", "admin")
DB_PASS = os.getenv("DB_PASS", "password_secret_123")
DB_HOST = os.getenv("DB_HOST", "localhost")  # En prod, ce sera "db_postgres"
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "alertes_crypto")

try:
    # On crée le tunnel de communication vers la base de données
    moteur_sql = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    logging.info(f"🔌 Tunnel SQL préparé vers {DB_HOST}:{DB_PORT}.")
except Exception as e:
    logging.critical(f"❌ ERREUR FATALE BDD : Impossible de préparer le tunnel -> {e}")
    exit(1)

# ==========================================
# 2. PRÉPARATION FICHIERS (Le Stockage Froid)
# ==========================================
output_dir = "data/clean"
os.makedirs(output_dir, exist_ok=True)
buffer_df = []  # Buffer pour les DataFrames nettoyés


# --- INTERCEPTION DE L'ARRÊT DOCKER (GRACEFUL SHUTDOWN) ---
def sauvegarde_urgence_et_quitter(signum, frame):
    logging.warning("🛑 Signal d'arrêt reçu de Docker. Sauvegarde du buffer en cours...")
    if buffer_df:
        df_complet = pd.concat(buffer_df, ignore_index=True)
        fichier_sortie = f"{output_dir}/trades_{SYMBOLE_CRYPTO}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df_complet.to_parquet(fichier_sortie, engine="pyarrow")
        logging.info(f"💾 Sauvegarde d'urgence réussie : {len(df_complet)} transactions archivées.")
    sys.exit(0)

signal.signal(signal.SIGTERM, sauvegarde_urgence_et_quitter)
signal.signal(signal.SIGINT, sauvegarde_urgence_et_quitter)

def nettoyer_vieux_fichiers():
    maintenant = time.time()
    limite_age = RETENTION_JOURS * 86400
    for fichier in os.listdir(output_dir):
        chemin = os.path.join(output_dir, fichier)
        if os.path.isfile(chemin) and (maintenant - os.path.getctime(chemin)) > limite_age:
            os.remove(chemin)
            logging.info(f"🗑️ Fichier expiré supprimé : {fichier}")


# ==========================================
# 3. LA BOUCLE DE PRODUCTION
# ==========================================
while True:
    try:
        url = f"https://api.binance.com/api/v3/trades?symbol={SYMBOLE_CRYPTO}&limit=100"
        reponse = requests.get(url, timeout=5)
        reponse.raise_for_status()  # Lève une exception pour les codes d'erreur HTTP (4xx ou 5xx)

        donnees_json = reponse.json()
        if not donnees_json:
            logging.info("Aucune nouvelle transaction reçue, cycle suivant.")
            time.sleep(INTERVALLE_SECONDES)
            continue

        # --- TRANSFORMATION À LA VOLÉE (UNE SEULE FOIS) ---
        df_brut = pd.DataFrame(donnees_json)
        df_brut["price"] = pd.to_numeric(df_brut["price"])
        df_brut["qty"] = pd.to_numeric(df_brut["qty"])
        df_brut["montant_usd"] = df_brut["price"] * df_brut["qty"]
        df_brut["time"] = pd.to_datetime(df_brut["time"], unit="ms")
        df_clean = df_brut[["id", "time", "price", "qty", "montant_usd", "isBuyerMaker"]]

        # --- LOGIQUE MÉTIER & STOCKAGE CHAUD ---
        baleines = df_clean[df_clean["qty"] >= SEUIL_BALEINE]
        if not baleines.empty:
            baleines.to_sql("table_alertes_baleines", moteur_sql, if_exists="append", index=False)
            logging.warning(f"🔥 ALERTE : {len(baleines)} baleine(s) injectée(s) dans PostgreSQL !")

        # --- MISE EN BUFFER POUR STOCKAGE FROID ---
        buffer_df.append(df_clean)

        # --- ÉCRITURE DU BUFFER SI ASSEZ GRAND ---
        taille_actuelle_buffer = sum(len(df) for df in buffer_df)
        if taille_actuelle_buffer >= TAILLE_MAX_BUFFER:
            df_complet = pd.concat(buffer_df, ignore_index=True)
            fichier_sortie = (
                f"{output_dir}/trades_{SYMBOLE_CRYPTO}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            )
            df_complet.to_parquet(fichier_sortie, engine="pyarrow")

            logging.info(f"💾 Froid : {len(df_complet)} transactions archivées en Parquet.")
            buffer_df = []  # On vide le buffer
            nettoyer_vieux_fichiers()

    except requests.exceptions.RequestException as e:
        logging.error(f"⚠️ ERREUR RÉSEAU : {e}")
    except Exception as e:
        logging.error(f"⚠️ ERREUR DE CYCLE INATTENDUE : {e}")

    time.sleep(INTERVALLE_SECONDES)
