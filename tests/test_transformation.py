import os
import pandas as pd
import sqlalchemy
import pytest
from src.ingestion_api import transformer_donnees


def test_transformer_donnees_types_et_calculs(mock_payload_binance):
    """Vérifie que l'ETL nettoie les colonnes, transforme les types et calcule le montant USD."""

    df_resultat = transformer_donnees(mock_payload_binance)

    # 1. Vérification globale et nettoyage
    assert len(df_resultat) == 2, "Le DataFrame devrait contenir exactement deux lignes"
    assert "isBestMatch" not in df_resultat.columns, "Le filtrage des colonnes n'a pas fonctionné"
    assert list(df_resultat.columns) == [
        "id",
        "time",
        "price",
        "qty",
        "montant_usd",
        "isBuyerMaker",
    ]

    # 2. Vérification des Types (La clé d'un ETL solide)
    assert pd.api.types.is_datetime64_any_dtype(
        df_resultat["time"]
    ), "La colonne 'time' doit être un datetime"
    assert pd.api.types.is_float_dtype(
        df_resultat["price"]
    ), "La colonne 'price' doit être convertie en float"
    assert pd.api.types.is_float_dtype(
        df_resultat["qty"]
    ), "La colonne 'qty' doit être convertie en float"

    # 3. Vérification des calculs métier
    assert df_resultat.iloc[0]["montant_usd"] == 100000.00, "Erreur mathématique sur la ligne 1"
    assert df_resultat.iloc[1]["montant_usd"] == 25000.00, "Erreur mathématique sur la ligne 2"
    assert df_resultat.iloc[0]["time"] == pd.to_datetime(
        1672531200000, unit="ms"
    ), "Erreur de conversion temporelle"


def test_ecriture_bdd_mock(mock_payload_binance):
    """Test unitaire : Utilise une BDD fictive en mémoire pour tester l'insertion SQL."""
    # sqlite:///:memory: crée une base de données temporaire directement dans la RAM.
    # Elle n'écrit RIEN sur le disque dur et disparaît à la fin du test !
    moteur_mock = sqlalchemy.create_engine("sqlite:///:memory:")

    # 1. On récupère nos données nettoyées et on teste l'insertion SQL (Pandas)
    df_resultat = transformer_donnees(mock_payload_binance)
    df_resultat.to_sql("table_alertes_baleines", moteur_mock, index=False)

    # 2. On vérifie que la table a bien été créée dans le Mock et contient nos 2 lignes
    with moteur_mock.connect() as connection:
        resultat = connection.execute(
            sqlalchemy.text("SELECT COUNT(*) FROM table_alertes_baleines")
        )
        assert resultat.scalar() == 2, "Les 2 baleines auraient dû être insérées dans la BDD mock"
