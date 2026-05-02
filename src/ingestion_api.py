import os
import logging
import requests
import pandas as pd
import time
from datetime import datetime, timedelta

# ==========================================
# 0. CONFIGURATION DE PRODUCTION
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("🚀 Démarrage du Moteur de Streaming avec Buffer et Rétention...")

SEUIL_BALEINE = float(os.getenv("SEUIL_BALEINE", 0.5))
INTERVALLE_SECONDES = int(os.getenv("INTERVALLE_SECONDES", 10))
# Nouvelles règles métier
TAILLE_MAX_BUFFER = int(os.getenv("TAILLE_MAX_BUFFER", 500)) # On sauvegarde tous les 500 trades
RETENTION_JOURS = int(os.getenv("RETENTION_JOURS", 7))       # On supprime après 7 jours

output_dir = "data/clean"
os.makedirs(output_dir, exist_ok=True)

# Le Panier en RAM
buffer_transactions = []

# ==========================================
# FONCTION DE NETTOYAGE (L'idée de l'Ingénieur)
# ==========================================
def nettoyer_vieux_fichiers():
    maintenant = time.time()
    limite_age = RETENTION_JOURS * 86400 # 86400 secondes dans un jour
    
    for fichier in os.listdir(output_dir):
        chemin = os.path.join(output_dir, fichier)
        if os.path.isfile(chemin):
            age_fichier = maintenant - os.path.getctime(chemin)
            if age_fichier > limite_age:
                os.remove(chemin)
                logging.info(f"🗑️ NETTOYAGE : Fichier expiré supprimé -> {fichier}")

# ==========================================
# LA BOUCLE INFINIE BLINDÉE
# ==========================================
while True:
    try:
        url = "https://api.binance.com/api/v3/trades?symbol=BTCUSDT&limit=100"
        reponse = requests.get(url, timeout=5)
        
        if reponse.status_code == 200:
            donnees_json = reponse.json()
            
            # On ajoute les données brutes dans notre panier en RAM
            buffer_transactions.extend(donnees_json)
            logging.info(f"📥 API lue. Panier actuel : {len(buffer_transactions)}/{TAILLE_MAX_BUFFER} trades.")
            
            # LA PORTE DE SÉCURITÉ : On vide le panier seulement s'il déborde
            if len(buffer_transactions) >= TAILLE_MAX_BUFFER:
                logging.info("💾 Panier plein ! Lancement du pipeline ETL et écriture disque...")
                
                df = pd.DataFrame(buffer_transactions)
                df['price'] = pd.to_numeric(df['price'])
                df['qty'] = pd.to_numeric(df['qty'])
                df['montant_usd'] = df['price'] * df['qty']
                df['time'] = pd.to_datetime(df['time'], unit='ms')
                df_clean = df[['id', 'time', 'price', 'qty', 'montant_usd', 'isBuyerMaker']]
                
                # Sauvegarde massive (1 seul gros fichier au lieu de 5 petits)
                fichier_sortie = f"{output_dir}/trades_btc_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
                df_clean.to_parquet(fichier_sortie, engine='pyarrow')
                
                # On vide le panier en RAM pour le prochain cycle
                buffer_transactions = []
                
                # On lance un coup de balai sur le disque
                nettoyer_vieux_fichiers()

        else:
            logging.error(f"❌ ERREUR API : {reponse.status_code}")

    except requests.exceptions.RequestException as e:
         logging.critical(f"⚠️ DÉFAILLANCE RÉSEAU : {e}")
    except Exception as e:
         logging.critical(f"⚠️ ERREUR FATALE : {e}")
    
    time.sleep(INTERVALLE_SECONDES)