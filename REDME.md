# 🐋 ETL de Détection d'Anomalies Financières (Temps Réel)

Ce projet est un pipeline de données (ETL) robuste et conteneurisé, conçu pour ingérer un flux de transactions en temps réel depuis l'API publique de Binance, appliquer des règles métier de détection d'anomalies ("Baleines"), et stocker l'historique de manière optimisée.

## 🏗️ Architecture du Système

- **Extract (Temps Réel) :** Ingestion continue via `requests` sur l'API Binance (Paire BTC/USDT).
- **Transform (Pandas) :** Nettoyage à la volée, typage strict et calculs de montants en USD.
- **Logique Métier :** Détection d'anomalies transactionnelles basées sur un seuil paramétrable.
- **Load (Optimisation Disque) :** Utilisation d'un buffer en mémoire vive (RAM) pour agréger les données avant écriture sur disque au format compressé `.parquet` (PyArrow).
- **Maintenance (Auto-nettoyage) :** Rétention automatique des fichiers pour éviter la saturation du disque (Data Lifecycle Management).

## 📂 Structure du Projet
```text
projet_fraude_etl/
├── src/
│   └── ingestion_api.py      # Moteur principal de streaming
├── data/
│   ├── raw/                  # (Ignoré par Git) Données brutes
│   └── clean/                # (Ignoré par Git) Fichiers .parquet
├── Dockerfile                # Recette de construction de l'image
├── requirements.txt          # Dépendances Python
└── .gitignore                # Règles de sécurité Gitgi