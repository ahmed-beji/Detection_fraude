-- Création de la table des anomalies
CREATE TABLE IF NOT EXISTS whale_transactions (
    id SERIAL PRIMARY KEY,
    trade_timestamp BIGINT NOT NULL,
    price NUMERIC(20, 8) NOT NULL,
    quantity NUMERIC(20, 8) NOT NULL,
    value_usdt NUMERIC(20, 8) NOT NULL,
    detected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexation stricte pour Metabase (Crucial pour les performances)
-- Sans ces index, Metabase fera un "Sequential Scan" (lecture de toute la table) à chaque rafraîchissement
CREATE INDEX IF NOT EXISTS idx_whale_value ON whale_transactions(value_usdt DESC);
CREATE INDEX IF NOT EXISTS idx_whale_time ON whale_transactions(trade_timestamp DESC);