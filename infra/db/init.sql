-- 1. Création de la base dédiée aux métadonnées internes de Metabase
CREATE DATABASE metabase_app;

-- 2. Connexion explicite à la base métier (créée par défaut via POSTGRES_DB dans le .env)
\connect alertes_crypto;

-- 3. Création de la table des baleines en dur
CREATE TABLE IF NOT EXISTS whale_transactions (
    id SERIAL PRIMARY KEY,
    trade_timestamp BIGINT NOT NULL,
    price NUMERIC(18, 8) NOT NULL,
    quantity NUMERIC(18, 8) NOT NULL,
    value_usdt NUMERIC(18, 2) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 4. Indexation sur le timestamp pour accélérer les futures requêtes de Metabase
CREATE INDEX IF NOT EXISTS idx_whale_timestamp ON whale_transactions(trade_timestamp DESC);