import os
import logging
import requests
import pandas as pd
import time
from datetime import datetime
from sqlalchemy import create_engine

# ==========================================
# 0. CONFIGURATION
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("🚀 Démarrage du Moteur Hybride (Parquet + PostgreSQL)...")

SEUIL_BALEINE = float(os.getenv("SEUIL_BALEINE", 0.5))
INTERVALLE_SECONDES = int(os.getenv("INTERVALLE_SECONDES", 10))
TAILLE_MAX_BUFFER = int(os.getenv("TAILLE_MAX_BUFFER", 500))
RETENTION_JOURS = int(os.getenv("RETENTION_JOURS", 7))

# ==========================================
# 1. CONNEXION BDD (Le Stockage Chaud)
# ==========================================
DB_USER = os.getenv("DB_USER", "admin")
DB_PASS = os.getenv("DB_PASS", "password_secret_123")
DB_HOST = os.getenv("DB_HOST", "localhost") # En prod, ce sera "db_postgres"
DB_NAME = os.getenv("DB_NAME", "alertes_crypto")

try:
    # On crée le tunnel de communication vers la base de données
    moteur_sql = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}")
    logging.info(f"🔌 Tunnel SQL préparé vers {DB_HOST}.")
except Exception as e:
    logging.critical(f"❌ ERREUR FATALE BDD : Impossible de préparer le tunnel -> {e}")
    exit(1)

# ==========================================
# 2. PRÉPARATION FICHIERS (Le Stockage Froid)
# ==========================================
output_dir = "data/clean"
os.makedirs(output_dir, exist_ok=True)
buffer_transactions = []

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
        url = "https://api.binance.com/api/v3/trades?symbol=BTCUSDT&limit=100"
        reponse = requests.get(url, timeout=5)
        
        if reponse.status_code == 200:
            donnees_json = reponse.json()
            buffer_transactions.extend(donnees_json)
            
            # --- TRANSFORMATION À LA VOLÉE ---
            df_temp = pd.DataFrame(donnees_json)
            df_temp['price'] = pd.to_numeric(df_temp['price'])
            df_temp['qty'] = pd.to_numeric(df_temp['qty'])
            df_temp['montant_usd'] = df_temp['price'] * df_temp['qty']
            df_temp['time'] = pd.to_datetime(df_temp['time'], unit='ms')
            df_clean = df_temp[['id', 'time', 'price', 'qty', 'montant_usd', 'isBuyerMaker']]
            
            # --- LOGIQUE MÉTIER & STOCKAGE CHAUD ---
            baleines = df_clean[df_clean['qty'] >= SEUIL_BALEINE]
            if not baleines.empty:
                # Magie Pandas : to_sql crée la table automatiquement et insère les lignes
                baleines.to_sql('table_alertes_baleines', moteur_sql, if_exists='append', index=False)
                logging.warning(f"🔥 ALERTE : {len(baleines)} baleine(s) injectée(s) dans PostgreSQL !")
            
            # --- STOCKAGE FROID ---
            if len(buffer_transactions) >= TAILLE_MAX_BUFFER:
                df_buffer = pd.DataFrame(buffer_transactions)
                # ... (Le formatage habituel pour le parquet)
                df_buffer['price'] = pd.to_numeric(df_buffer['price'])
                df_buffer['qty'] = pd.to_numeric(df_buffer['qty'])
                df_buffer['montant_usd'] = df_buffer['price'] * df_buffer['qty']
                df_buffer['time'] = pd.to_datetime(df_buffer['time'], unit='ms')
                
                fichier_sortie = f"{output_dir}/trades_btc_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
                df_buffer[['id', 'time', 'price', 'qty', 'montant_usd', 'isBuyerMaker']].to_parquet(fichier_sortie, engine='pyarrow')
                
                logging.info(f"💾 Froid : {len(buffer_transactions)} transactions archivées en Parquet.")
                buffer_transactions = []
                nettoyer_vieux_fichiers()

    except Exception as e:
         logging.error(f"⚠️ ERREUR DE CYCLE : {e}")
    
    time.sleep(INTERVALLE_SECONDES)