import pytest

@pytest.fixture
def mock_payload_binance():
    """Fixture contenant des données simulées de l'API Binance pour les tests."""
    return [
        {
            "id": 1001,
            "price": "50000.00",
            "qty": "2.0",
            "time": 1672531200000,
            "isBuyerMaker": True,
            "isBestMatch": True
        },
        {
            "id": 1002,
            "price": "50000.00",
            "qty": "0.5",
            "time": 1672531201000,
            "isBuyerMaker": False,
            "isBestMatch": True
        }
    ]