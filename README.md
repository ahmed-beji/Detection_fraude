#  Crypto Whale Detector - Real-Time ETL Pipeline

[![CI/CD Pipeline](https://github.com/ahmed-beji/Detection_fraude/actions/workflows/deploy.yml/badge.svg)](https://github.com/ahmed-beji/Detection_fraude/actions)
[![Python Version](https://img.shields.io/badge/python-3.11-blue.svg)](https://python.org)
[![Infrastructure](https://img.shields.io/badge/IaC-Terraform-7B42BC.svg)](https://terraform.io)

Ce projet est un pipeline de données (ETL) robuste, asynchrone et conteneurisé. Il est conçu pour ingérer un flux continu de transactions en temps réel depuis l'API WebSocket de Binance, détecter les anomalies financières ("Baleines" - transactions au-dessus d'un seuil critique), et stocker ces données pour une visualisation instantanée.

L'infrastructure est provisionnée dynamiquement sur AWS via Terraform, et le déploiement est automatisé par un pipeline CI/CD GitHub Actions.

##  Architecture du Système

L'architecture abandonne les requêtes HTTP synchrones classiques au profit d'une connexion WebSocket pour garantir un véritable temps réel sans subir de limitations d'API (Rate Limits).

1. **Extract (WebSockets) :** Connexion asynchrone au flux WebSocket public de Binance (Paire BTC/USDT).
2. **Transform (Buffer in-memory) :** Nettoyage à la volée des payloads JSON et typage strict sans instanciation lourde de DataFrames.
3. **Logique Métier :** Évaluation instantanée du volume de la transaction par rapport à un seuil paramétrable via variables d'environnement.
4. **Load (PostgreSQL / Hot Storage) :** Insertion des transactions suspectes dans une base de données relationnelle pour la visualisation en temps réel.
5. **Dataviz (Metabase) :** Tableau de bord branché directement sur PostgreSQL pour le suivi des "Baleines".
6. **Infrastructure (AWS & Terraform) :** Déploiement automatisé sur une instance Amazon EC2 sécurisée.

```mermaid
flowchart LR
    subgraph AWS [Infrastructure AWS EC2]
        direction TB
        subgraph Docker [Stack Docker Compose]
            A[Moteur Ingestion Python\nWebSocket Consumer]
            B[(PostgreSQL\nHot Storage)]
            C[Fichiers Parquet\nCold Storage]
            D[Metabase\nDashboard Dataviz]
        end
    end

    API((API Binance)) == Flux BTC/USDT\nTemps Réel ==> A
    A -- Filtre Baleines\nÉcriture instantanée --> B
    A -- Purge Buffer RAM\nCompression PyArrow --> C
    B -. Requêtes SQL .-> D
    User((Data Engineer)) == Port 3000\nHTTP ==> D

    classDef aws fill:#FF9900,stroke:#232F3E,stroke-width:2px,color:black;
    classDef docker fill:#2496ED,stroke:#fff,stroke-width:2px,color:white;
    class AWS aws;
    class Docker docker;