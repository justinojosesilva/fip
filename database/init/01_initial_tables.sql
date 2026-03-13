-- =========================================
-- EXTENSÕES
-- =========================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- =========================================
-- SCHEMAS
-- =========================================

CREATE SCHEMA IF NOT EXISTS market_data;
CREATE SCHEMA IF NOT EXISTS betting_data;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS signals;
CREATE SCHEMA IF NOT EXISTS portfolio;
CREATE SCHEMA IF NOT EXISTS risk;
CREATE SCHEMA IF NOT EXISTS events;
CREATE SCHEMA IF NOT EXISTS ml_models;
CREATE SCHEMA IF NOT EXISTS system;

-- =========================================
-- MARKET DATA (CRYPTO)
-- =========================================

CREATE TABLE market_data.crypto_prices (
    time TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    price NUMERIC,
    volume NUMERIC,
    exchange TEXT,
    PRIMARY KEY (time, symbol)
);

SELECT create_hypertable('market_data.crypto_prices', 'time', if_not_exists => TRUE);

ALTER TABLE market_data.crypto_prices
SET (timescaledb.compress);

SELECT add_compression_policy('market_data.crypto_prices', INTERVAL '7 days');

SELECT add_retention_policy('market_data.crypto_prices', INTERVAL '3 years');

-- =========================================

CREATE TABLE market_data.crypto_trades_raw (
    time TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    trade_id BIGINT,
    price NUMERIC,
    quantity NUMERIC,
    side TEXT,
    exchange TEXT,
    PRIMARY KEY (time, symbol, trade_id)
);

SELECT create_hypertable('market_data.crypto_trades_raw', 'time', if_not_exists => TRUE);

ALTER TABLE market_data.crypto_trades_raw
SET (timescaledb.compress);

SELECT add_compression_policy('market_data.crypto_trades_raw', INTERVAL '3 days');

SELECT add_retention_policy('market_data.crypto_trades_raw', INTERVAL '1 year');

-- =========================================

CREATE TABLE market_data.crypto_candles (
    time TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC,
    PRIMARY KEY (time, symbol, timeframe)
);

SELECT create_hypertable('market_data.crypto_candles', 'time', if_not_exists => TRUE);

ALTER TABLE market_data.crypto_candles
SET (timescaledb.compress);

SELECT add_compression_policy('market_data.crypto_candles', INTERVAL '30 days');

SELECT add_retention_policy('market_data.crypto_candles', INTERVAL '5 years');

-- =========================================

CREATE TABLE market_data.crypto_liquidations (
    time TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    price NUMERIC,
    size NUMERIC,
    side TEXT,
    exchange TEXT,
    PRIMARY KEY (time, symbol)
);

SELECT create_hypertable('market_data.crypto_liquidations', 'time', if_not_exists => TRUE);

ALTER TABLE market_data.crypto_liquidations
SET (timescaledb.compress);

SELECT add_compression_policy('market_data.crypto_liquidations', INTERVAL '7 days');

SELECT add_retention_policy('market_data.crypto_liquidations', INTERVAL '2 years');

-- =========================================
-- BETTING DATA
-- =========================================

CREATE TABLE betting_data.sports (
    id SERIAL PRIMARY KEY,
    name TEXT
);

CREATE TABLE betting_data.leagues (
    id SERIAL PRIMARY KEY,
    sport_id INTEGER,
    name TEXT,
    country TEXT
);

CREATE TABLE betting_data.teams (
    id SERIAL PRIMARY KEY,
    league_id INTEGER,
    name TEXT
);

CREATE TABLE betting_data.matches (
    id SERIAL PRIMARY KEY,
    league_id INTEGER,
    home_team INTEGER,
    away_team INTEGER,
    start_time TIMESTAMPTZ,
    status TEXT
);

CREATE TABLE betting_data.bookmakers (
    id SERIAL PRIMARY KEY,
    name TEXT
);

-- =========================================

CREATE TABLE betting_data.odds_history (
    time TIMESTAMPTZ NOT NULL,
    match_id INTEGER,
    bookmaker_id INTEGER,
    market TEXT,
    selection TEXT,
    odd NUMERIC,
    PRIMARY KEY (time, match_id, bookmaker_id, selection)
);

SELECT create_hypertable('betting_data.odds_history', 'time', if_not_exists => TRUE);

ALTER TABLE betting_data.odds_history
SET (timescaledb.compress);

SELECT add_compression_policy('betting_data.odds_history', INTERVAL '7 days');

SELECT add_retention_policy('betting_data.odds_history', INTERVAL '3 years');

-- =========================================
-- ANALYTICS
-- =========================================

CREATE TABLE analytics.crypto_indicators (
    time TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    price NUMERIC,
    rsi NUMERIC,
    ema NUMERIC,
    vwap NUMERIC,
    volume NUMERIC,
    PRIMARY KEY (time, symbol)
);

SELECT create_hypertable('analytics.crypto_indicators', 'time', if_not_exists => TRUE);

ALTER TABLE analytics.crypto_indicators
SET (timescaledb.compress);

SELECT add_compression_policy('analytics.crypto_indicators', INTERVAL '30 days');

SELECT add_retention_policy('analytics.crypto_indicators', INTERVAL '5 years');

-- =========================================

CREATE TABLE analytics.arbitrage_opportunities (
    time TIMESTAMPTZ NOT NULL,
    market_type TEXT,
    asset TEXT,
    source_a TEXT,
    source_b TEXT,
    profit_percent NUMERIC,
    PRIMARY KEY (time, asset)
);

SELECT create_hypertable('analytics.arbitrage_opportunities', 'time', if_not_exists => TRUE);

-- =========================================
-- SIGNALS
-- =========================================

CREATE TABLE signals.trading_signals (
    time TIMESTAMPTZ NOT NULL,
    symbol TEXT,
    signal_type TEXT,
    confidence NUMERIC,
    price_target NUMERIC,
    stop_loss NUMERIC,
    PRIMARY KEY (time, symbol)
);

SELECT create_hypertable('signals.trading_signals', 'time', if_not_exists => TRUE);

CREATE TABLE signals.betting_signals (
    time TIMESTAMPTZ NOT NULL,
    match_id INTEGER,
    market TEXT,
    selection TEXT,
    confidence NUMERIC,
    expected_value NUMERIC,
    PRIMARY KEY (time, match_id)
);

SELECT create_hypertable('signals.betting_signals', 'time', if_not_exists => TRUE);

-- =========================================
-- MACHINE LEARNING
-- =========================================

CREATE TABLE ml_models.models (
    id SERIAL PRIMARY KEY,
    name TEXT,
    version TEXT,
    created_at TIMESTAMPTZ
);

CREATE TABLE ml_models.predictions (
    time TIMESTAMPTZ NOT NULL,
    model_id INTEGER,
    target TEXT,
    prediction NUMERIC,
    confidence NUMERIC,
    PRIMARY KEY (time, model_id)
);

SELECT create_hypertable('ml_models.predictions', 'time', if_not_exists => TRUE);

-- =========================================
-- SYSTEM
-- =========================================

CREATE TABLE system.users (
    id SERIAL PRIMARY KEY,
    email TEXT,
    password_hash TEXT,
    created_at TIMESTAMPTZ
);

CREATE TABLE system.api_keys (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    api_key TEXT,
    created_at TIMESTAMPTZ
);

CREATE TABLE system.ingestion_logs (
    time TIMESTAMPTZ NOT NULL,
    service TEXT,
    status TEXT,
    message TEXT,
    PRIMARY KEY (time, service)
);

SELECT create_hypertable('system.ingestion_logs', 'time', if_not_exists => TRUE);