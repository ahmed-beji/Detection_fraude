import json
import os
import time
import websocket
from confluent_kafka import Producer

# Configuration du Broker (Redpanda) via variable d'environnement
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
TOPIC_NAME = "binance-trades"

# Initialisation du Producteur Kafka
producer = Producer({
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'binance-producer',
    'linger.ms': 10  # Micro-batching : attend 10ms pour grouper les messages et optimiser le réseau
})

def delivery_report(err, msg):
    """Callback pour confirmer que Redpanda a bien reçu la donnée."""
    if err is not None:
        print(f"❌ Échec d'envoi Kafka : {err}")
    # En production, on évite de print chaque succès pour ne pas saturer les logs
    
def on_message(ws, message):
    """Déclenché à chaque transaction reçue de Binance."""
    # On pousse le JSON brut directement dans le topic sans le décoder lourdement
    producer.produce(TOPIC_NAME, value=message, callback=delivery_report)
    producer.poll(0) # Vide le buffer interne de Kafka (Évite les fuites de RAM)

def on_error(ws, error):
    print(f"⚠️ Erreur WebSocket : {error}")

def on_close(ws, close_status_code, close_msg):
    print("🔴 Déconnexion de Binance. Le conteneur va redémarrer automatiquement.")
    producer.flush() # Force l'envoi des derniers messages en attente avant de fermer

def on_open(ws):
    print("🟢 Connecté au flux Binance. Ingestion en cours...")

if __name__ == "__main__":
    # URL du flux WebSocket public de Binance (Paire BTC/USDT)
    SOCKET = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    
    # Boucle de reconnexion infinie (Résilience)
    while True:
        ws = websocket.WebSocketApp(SOCKET, 
                                    on_open=on_open,
                                    on_message=on_message,
                                    on_error=on_error,
                                    on_close=on_close)
        ws.run_forever()
        time.sleep(5) # Backoff de 5 secondes avant de retenter la connexion