-- TABLE: merchants
-- Stores reference data about merchants (stores/businesses).
-- Used to enrich transactions with merchant category risk info.
-- Populated once at startup; queried by dbt mart models.
CREATE TABLE IF NOT EXISTS merchants (
    merchant_id     VARCHAR(50) PRIMARY KEY,
    merchant_name   VARCHAR(255),
    category_code   INT,
    category_name   VARCHAR(100),
    risk_tier       VARCHAR(20)  -- 'low', 'medium', 'high'
);

-- TABLE: transactions
-- The core fact table. Every payment event processed by the
-- risk engine is written here — whether approved or flagged.
-- This is the single source of truth for all pipeline activity
-- and the primary input for dbt transformations in Phase 4.
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id  UUID PRIMARY KEY,
    card_id         VARCHAR(100),
    amount          NUMERIC(12, 2),
    merchant_id     VARCHAR(50),
    merchant_category_code INT,
    latitude        FLOAT,
    longitude       FLOAT,
    timestamp       TIMESTAMPTZ,
    is_fraud        BOOLEAN,        -- ground truth label from generator
    risk_decision   VARCHAR(20),    -- 'approved' or 'flagged'
    triggered_rules TEXT[],         -- e.g. ['velocity', 'impossible_travel']
    scoring_latency_ms FLOAT,       -- time taken to score in ms
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Stores a dedicated record for every transaction the risk
-- engine flags as suspicious. Separated from the main
-- transactions table to mirror how real fraud operations teams
-- work — analysts query this table to review flagged cases
-- without scanning millions of approved transactions.
CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id        SERIAL PRIMARY KEY,
    transaction_id  UUID REFERENCES transactions(transaction_id),
    triggered_rules TEXT[],
    alerted_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_transactions_card_id ON transactions(card_id);
CREATE INDEX idx_transactions_timestamp ON transactions(timestamp);