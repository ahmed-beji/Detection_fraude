import json
import os
import time
import psycopg2
from confluent_kafka import Consumer, KafkaError

# --- Configuration Kafka ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
TOPIC_NAME = "binance-trades"
GROUP_ID = "whale-detectors"
# On force le type float pour éviter les erreurs de typage
WHALE_THRESHOLD = float(os.getenv("WHALE_THRESHOLD", 10.0)) 

# --- Configuration PostgreSQL ---
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "crypto_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")

# --- 1. Initialisation et Sécurisation de la Base de Données ---
try:
    db_conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    db_cursor = db_conn.cursor()
    print("🟢 Connecté à PostgreSQL avec succès.")

    # CRÉATION AUTOMATIQUE DE LA TABLE (La faille est corrigée ici)
    db_cursor.execute("""
        CREATE TABLE IF NOT EXISTS whale_transactions (
            id SERIAL PRIMARY KEY,
            trade_timestamp BIGINT,
            price NUMERIC,
            quantity NUMERIC,
            value_usdt NUMERIC
        );
    """)
    db_conn.commit()
    print("✅ Table 'whale_transactions' vérifiée et prête.")

except Exception as e:
    print(f"🔴 Échec critique de connexion à PostgreSQL : {e}")
    exit(1)

# --- 2. Initialisation du Consommateur Kafka ---
consumer = Consumer({
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})
consumer.subscribe([TOPIC_NAME])

def insert_into_postgres(whale_data):
    """Insère la transaction et valide l'opération."""
    query = """
        INSERT INTO whale_transactions (trade_timestamp, price, quantity, value_usdt)
        VALUES (%s, %s, %s, %s)
    """
    try:
        db_cursor.execute(query, (
            whale_data["timestamp"],
            whale_data["price"],
            whale_data["quantity"],
            whale_data["value_usdt"],
        ))
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        print(f"❌ Erreur d'insertion SQL : {e}")
        raise e

# --- 3. Boucle Principale d'Écoute ---
try:
    print(f"🎧 En écoute sur le topic '{TOPIC_NAME}' (Seuil : {WHALE_THRESHOLD}$)...")

    while True:
        msg = consumer.poll(1.0)
        
        if msg is None:
            continue
            
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            elif msg.error().code() in (KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError._UNKNOWN_TOPIC):
                print("⏳ Topic introuvable. En attente du producteur (2s)...")
                time.sleep(2)
                continue
            
            print(f"⚠️ Erreur Broker critique : {msg.error()}")
            break

        try:
            raw_data = json.loads(msg.value().decode("utf-8"))
            price = float(raw_data["p"])
            quantity = float(raw_data["q"])
            trade_value = price * quantity

            # Filtre de détection des baleines
            if trade_value >= WHALE_THRESHOLD:
                whale_record = {
                    "timestamp": int(raw_data["T"]),
                    "price": price,
                    "quantity": quantity,
                    "value_usdt": trade_value,
                }

                # Insertion et validation
                insert_into_postgres(whale_record)
                consumer.commit(asynchronous=False)
                
                print(f"🐳 BALEINE ENREGISTRÉE : {trade_value:,.2f} $ (Prix: {price}$, Qte: {quantity})")

        except json.JSONDecodeError:
            pass  # On ignore les messages qui ne sont pas du JSON valide
        except KeyError as e:
            print(f"⚠️ Format de données inattendu (clé manquante) : {e}")

except KeyboardInterrupt:
    print("\n🔴 Interruption manuelle demandée.")
finally:
    db_cursor.close()
    db_conn.close()
    consumer.close()
    print("✅ Ressources libérées proprement. Arrêt du consommateur.")