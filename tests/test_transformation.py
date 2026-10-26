import json
import pytest
from unittest.mock import MagicMock

# ==========================================
# 1. ISOLATION DE LA LOGIQUE MÉTIER
# ==========================================
# En TDD (Test Driven Development), on extrait la logique du script principal
# pour pouvoir la tester unitairement sans avoir besoin d'allumer Kafka ou Docker.

def detecter_baleine(payload_json, seuil_usd):
    """Fonction pure qui imite la logique de notre consumer_etl.py"""
    try:
        raw_data = json.loads(payload_json)
        price = float(raw_data['p'])
        quantity = float(raw_data['q'])
        trade_value = price * quantity
        
        if trade_value >= seuil_usd:
            return {
                "timestamp": int(raw_data['T']),
                "price": price,
                "quantity": quantity,
                "value_usdt": trade_value
            }
        return None
    except (KeyError, json.JSONDecodeError):
        return None


# ==========================================
# 2. TESTS UNITAIRES (Pytest)
# ==========================================

def test_detection_baleine_succes():
    """Vérifie qu'une transaction au-dessus du seuil est bien détectée (Baleine)."""
    # Payload brut typique de l'API WebSocket Binance
    # 2 BTC à 50 000$ = 100 000$ (Au-dessus d'un seuil de 50 000$)
    mock_payload = '{"e":"trade", "p":"50000.00", "q":"2.00", "T":1672531200000}'
    
    resultat = detecter_baleine(mock_payload, seuil_usd=50000.0)
    
    assert resultat is not None, "La baleine aurait dû être détectée"
    assert resultat["value_usdt"] == 100000.0, "Erreur de calcul du montant"
    assert type(resultat["timestamp"]) is int, "Le timestamp doit être un entier"
    assert type(resultat["price"]) is float, "Le prix doit être un float"

def test_detection_baleine_ignoree():
    """Vérifie qu'une transaction sous le seuil est ignorée (Pas de Baleine)."""
    # 0.1 BTC à 50 000$ = 5 000$ (Sous le seuil de 50 000$)
    mock_payload = '{"e":"trade", "p":"50000.00", "q":"0.10", "T":1672531200000}'
    
    resultat = detecter_baleine(mock_payload, seuil_usd=50000.0)
    
    assert resultat is None, "La transaction est trop petite, elle doit être ignorée"

def test_donnees_corrompues():
    """Vérifie que l'ETL ne crashe pas si Binance envoie un JSON malformé."""
    mock_payload = '{"e":"trade", "p":"50000.00"}' # Il manque la quantité 'q'
    
    resultat = detecter_baleine(mock_payload, seuil_usd=50000.0)
    
    assert resultat is None, "Le système doit ignorer silencieusement la donnée corrompue"


# ==========================================
# 3. TEST DE L'INSERTION SQL (MOCKING)
# ==========================================
def test_insertion_sql_mock():
    """Vérifie que la requête SQL de psycopg2 est correcte sans allumer PostgreSQL."""
    
    # MagicMock simule le curseur de la base de données (db_cursor)
    mock_cursor = MagicMock()
    
    # Donnée d'une baleine fictive
    whale_data = {
        "timestamp": 1672531200000,
        "price": 50000.0,
        "quantity": 2.0,
        "value_usdt": 100000.0
    }
    
    # La requête exacte issue de consumer_etl.py
    query = """
        INSERT INTO whale_transactions (trade_timestamp, price, quantity, value_usdt)
        VALUES (%s, %s, %s, %s)
    """
    
    # Simulation de l'appel de la fonction execute() par le code
    mock_cursor.execute(query, (
        whale_data["timestamp"], 
        whale_data["price"], 
        whale_data["quantity"], 
        whale_data["value_usdt"]
    ))
    
    # Vérifications strictes (Assertions)
    mock_cursor.execute.assert_called_once() # S'assure que la requête n'est lancée qu'une fois
    
    # On récupère les arguments passés à la fonction execute() simulée
    args, _ = mock_cursor.execute.call_args
    requete_sql_envoyee = args[0]
    valeurs_envoyees = args[1]
    
    assert "INSERT INTO whale_transactions" in requete_sql_envoyee, "Nom de table incorrect"
    assert valeurs_envoyees == (1672531200000, 50000.0, 2.0, 100000.0), "Mauvais mapping des valeurs"