import json
import os
import psycopg2
from psycopg2.extras import execute_values
from confluent_kafka import Consumer, KafkaError

# --- Configuration Kafka ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
TOPIC_NAME = "binance-trades"
GROUP_ID = "whale-detectors"
WHALE_THRESHOLD = float(os.getenv("WHALE_THRESHOLD", 50000.0))

# --- Configuration PostgreSQL ---
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "crypto_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")

# Établissement de la connexion persistante
try:
    db_conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    db_cursor = db_conn.cursor()
    print("🟢 Connecté à PostgreSQL avec succès.")
except Exception as e:
    print(f"🔴 Échec critique de connexion à PostgreSQL : {e}")
    exit(1)  # Si la DB n'est pas là, le conteneur doit crasher pour que Docker le redémarre

consumer = Consumer(
    {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
)
consumer.subscribe([TOPIC_NAME])


def insert_into_postgres(whale_data):
    """Insère la transaction et valide l'opération."""
    query = """
        INSERT INTO whale_transactions (trade_timestamp, price, quantity, value_usdt)
        VALUES (%s, %s, %s, %s)
    """
    try:
        db_cursor.execute(
            query,
            (
                whale_data["timestamp"],
                whale_data["price"],
                whale_data["quantity"],
                whale_data["value_usdt"],
            ),
        )
        db_conn.commit()  # Validation transactionnelle côté base de données
    except Exception as e:
        db_conn.rollback()  # Annulation en cas d'erreur pour ne pas bloquer la connexion
        print(f"❌ Erreur d'insertion SQL : {e}")
        raise e  # On relance l'erreur pour empêcher le commit Kafka


try:
    print(f"🎧 En écoute sur le topic '{TOPIC_NAME}' (Seuil : {WHALE_THRESHOLD}$)...")

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"⚠️ Erreur Broker : {msg.error()}")
            break

        try:
            raw_data = json.loads(msg.value().decode("utf-8"))
            price, quantity = float(raw_data["p"]), float(raw_data["q"])
            trade_value = price * quantity

            if trade_value >= WHALE_THRESHOLD:
                whale_record = {
                    "timestamp": int(raw_data["T"]),
                    "price": price,
                    "quantity": quantity,
                    "value_usdt": trade_value,
                }

                # Étape 1 : Sauvegarde en Base de Données
                insert_into_postgres(whale_record)

                # Étape 2 : Confirmation à Kafka (Seulement si l'Étape 1 réussit)
                consumer.commit(asynchronous=False)
                print(f"🐳 BALEINE ENREGISTRÉE : {trade_value:,.2f} $")

        except json.JSONDecodeError:
            pass  # Ignorer silencieusement les données corrompues

except KeyboardInterrupt:
    print("🔴 Interruption manuelle demandée.")
finally:
    db_cursor.close()
    db_conn.close()
    consumer.close()
    print("✅ Ressources libérées proprement.")
