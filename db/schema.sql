-- Pokemon Analytics Database Schema
-- SQLite with WAL mode for concurrent reads

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- ============================================================
-- Core reference tables
-- ============================================================

CREATE TABLE IF NOT EXISTS sets (
    set_code       TEXT PRIMARY KEY,
    set_name       TEXT NOT NULL,
    release_date   TEXT,
    psa_pop_url    TEXT,
    logo_url       TEXT,
    created_at     TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS rarities (
    set_rarity     TEXT PRIMARY KEY,          -- 'PRE_SIR'
    set_code       TEXT NOT NULL REFERENCES sets(set_code),
    rarity_code    TEXT NOT NULL,
    rarity_name    TEXT NOT NULL,
    card_count     INTEGER NOT NULL,
    pull_rate      REAL NOT NULL,
    pull_rate_odds TEXT,
    created_at     TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS cards (
    id                TEXT PRIMARY KEY,
    product_name      TEXT NOT NULL,
    set_code          TEXT NOT NULL REFERENCES sets(set_code),
    card_number       INTEGER,
    set_count         INTEGER,
    card_unique       TEXT,
    rarity_code       TEXT,
    rarity_name       TEXT,
    tcg_id            TEXT,
    image_url         TEXT,
    tcgplayer_image_url TEXT,
    set_value_include TEXT DEFAULT 'Y',
    sealed_product    TEXT DEFAULT 'N',
    sealed_type       TEXT,
    ebay_q_phrase     TEXT,
    ebay_q_num        TEXT,
    ebay_category_id  TEXT DEFAULT '183454',
    search_text       TEXT,
    created_at        TEXT DEFAULT (datetime('now'))
);

-- ============================================================
-- Price history tables (7 history arrays per card)
-- ============================================================

-- 1. PriceCharting prices (primary source)
CREATE TABLE IF NOT EXISTS price_history (
    card_id        TEXT NOT NULL REFERENCES cards(id),
    date           TEXT NOT NULL,
    raw_price      REAL,
    psa_7_price    REAL,
    psa_8_price    REAL,
    psa_9_price    REAL,
    psa_10_price   REAL,
    psa_10_vs_raw  REAL,
    psa_10_vs_raw_pct REAL,
    sales_volume   INTEGER,
    interpolated   INTEGER DEFAULT 0,
    interpolation_source TEXT,
    PRIMARY KEY (card_id, date)
);

-- 2. PSA Pop Report snapshots
CREATE TABLE IF NOT EXISTS psa_pop_history (
    card_id        TEXT NOT NULL REFERENCES cards(id),
    date           TEXT NOT NULL,
    psa_8_base     INTEGER,
    psa_9_base     INTEGER,
    psa_10_base    INTEGER,
    total_base     INTEGER,
    gem_pct        REAL,
    PRIMARY KEY (card_id, date)
);

-- 3. eBay listing snapshots
CREATE TABLE IF NOT EXISTS ebay_history (
    card_id        TEXT NOT NULL REFERENCES cards(id),
    date           TEXT NOT NULL,
    from_date      TEXT,
    active_from    REAL,
    active_to      REAL,
    ended          REAL,
    new            REAL,
    ended_rate     REAL,
    ended_raw      REAL,
    new_raw        REAL,
    ended_graded   REAL,
    new_graded     REAL,
    ended_psa_10   REAL,
    new_psa_10     REAL,
    ended_psa_9    REAL,
    new_psa_9      REAL,
    ended_other_10 REAL,
    new_other_10   REAL,
    ended_avg_raw_price    REAL,
    ended_avg_psa_10_price REAL,
    ended_avg_psa_9_price  REAL,
    ended_avg_other_10_price REAL,
    interpolated   INTEGER DEFAULT 0,
    ended_adj      REAL,
    ended_raw_adj  REAL,
    ended_graded_adj REAL,
    new_adj        REAL,
    new_raw_adj    REAL,
    new_graded_adj REAL,
    ended_avg_raw_price_adj    REAL,
    ended_avg_psa_10_price_adj REAL,
    ended_avg_psa_9_price_adj  REAL,
    PRIMARY KEY (card_id, date)
);

-- 4. eBay market analysis
CREATE TABLE IF NOT EXISTS ebay_market_history (
    card_id        TEXT NOT NULL REFERENCES cards(id),
    date           TEXT NOT NULL,
    from_date      TEXT,
    active_from    REAL,
    active_to      REAL,
    ended          REAL,
    new            REAL,
    ended_raw      REAL,
    ended_psa_9    REAL,
    ended_psa_10   REAL,
    interpolated   INTEGER DEFAULT 0,
    demand_pressure_observed REAL,
    demand_pressure_est      REAL,
    sold_rate_est  REAL,
    sold_est       REAL,
    PRIMARY KEY (card_id, date)
);

-- 5. eBay-derived pricing
CREATE TABLE IF NOT EXISTS ebay_derived_history (
    card_id        TEXT NOT NULL REFERENCES cards(id),
    date           TEXT NOT NULL,
    d_raw_price    REAL,
    d_psa_9_price  REAL,
    d_psa_10_price REAL,
    PRIMARY KEY (card_id, date)
);

-- 6. JustTCG pricing
CREATE TABLE IF NOT EXISTS justtcg_history (
    card_id        TEXT NOT NULL REFERENCES cards(id),
    date           TEXT NOT NULL,
    j_raw_price    REAL,
    PRIMARY KEY (card_id, date)
);

-- 7. Collectrics Composite Price
CREATE TABLE IF NOT EXISTS composite_history (
    card_id        TEXT NOT NULL REFERENCES cards(id),
    date           TEXT NOT NULL,
    c_raw_price    REAL,
    c_psa_9_price  REAL,
    c_psa_10_price REAL,
    PRIMARY KEY (card_id, date)
);

-- ============================================================
-- Computed / aggregate tables
-- ============================================================

CREATE TABLE IF NOT EXISTS market_pressure (
    card_id        TEXT NOT NULL REFERENCES cards(id),
    window_days    INTEGER NOT NULL,
    mode           TEXT NOT NULL,
    as_of          TEXT NOT NULL,
    sample_days    INTEGER,
    interpolated_days INTEGER,
    avg_active     REAL,
    avg_existing   REAL,
    avg_ended      REAL,
    avg_new        REAL,
    demand_pressure REAL,
    supply_pressure REAL,
    net_flow       REAL,
    net_flow_pct   REAL,
    state_label    TEXT,
    PRIMARY KEY (card_id, window_days, mode, as_of)
);

CREATE TABLE IF NOT EXISTS supply_saturation (
    card_id        TEXT NOT NULL REFERENCES cards(id),
    mode           TEXT NOT NULL,
    as_of          TEXT NOT NULL,
    supply_saturation_index REAL,
    supply_saturation_label TEXT,
    trend          TEXT,
    active_listings_delta_pct REAL,
    demand_delta_pct REAL,
    supply_delta_pct REAL,
    PRIMARY KEY (card_id, mode, as_of)
);

CREATE TABLE IF NOT EXISTS set_daily (
    set_code       TEXT NOT NULL REFERENCES sets(set_code),
    date           TEXT NOT NULL,
    ev_raw_per_pack    REAL,
    ev_psa_10_per_pack REAL,
    avg_pack_cost      REAL,
    avg_gain_loss      REAL,
    total_set_raw_value REAL,
    PRIMARY KEY (set_code, date)
);

CREATE TABLE IF NOT EXISTS set_rarity_snapshot (
    set_rarity     TEXT NOT NULL,
    date           TEXT NOT NULL,
    avg_raw_price  REAL,
    avg_psa_10_price REAL,
    ev_raw_per_pack  REAL,
    ev_psa_10_per_pack REAL,
    psa_pop_10_base  INTEGER,
    psa_pop_total_base INTEGER,
    psa_avg_gem_pct  REAL,
    PRIMARY KEY (set_rarity, date)
);

CREATE TABLE IF NOT EXISTS pack_cost (
    set_code       TEXT NOT NULL REFERENCES sets(set_code),
    date           TEXT NOT NULL,
    avg_booster_pack         REAL,
    avg_sleeved_booster_pack REAL,
    avg_booster_bundle_per_pack REAL,
    avg_pack_cost            REAL,
    booster_pack_count    INTEGER,
    sleeved_booster_count INTEGER,
    booster_bundle_count  INTEGER,
    PRIMARY KEY (set_code, date)
);

CREATE TABLE IF NOT EXISTS leaderboard (
    set_code       TEXT NOT NULL REFERENCES sets(set_code),
    date           TEXT NOT NULL,
    rarity_buckets     INTEGER,
    cards_counted      INTEGER,
    avg_pack_cost      REAL,
    ev_raw_per_pack    REAL,
    ev_psa_10_per_pack REAL,
    avg_gain_loss      REAL,
    total_set_raw_value REAL,
    psa_pop_10_base    INTEGER,
    psa_pop_total_base INTEGER,
    psa_avg_gem_pct    REAL,
    rank_avg_gain_loss      INTEGER,
    rank_ev_raw_per_pack    INTEGER,
    rank_total_set_raw_value INTEGER,
    rank_psa_avg_gem_pct    INTEGER,
    PRIMARY KEY (set_code, date)
);

-- ============================================================
-- Pipeline metadata
-- ============================================================

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at  TEXT NOT NULL,
    finished_at TEXT,
    status      TEXT DEFAULT 'running',
    stage       TEXT,
    cards_processed INTEGER DEFAULT 0,
    errors      INTEGER DEFAULT 0,
    notes       TEXT
);

-- ============================================================
-- Model prediction tables (PokeDelta)
-- ============================================================

CREATE TABLE IF NOT EXISTS model_projections (
    card_id         TEXT NOT NULL REFERENCES cards(id),
    as_of           TEXT NOT NULL,
    horizon_days    INTEGER NOT NULL,
    projected_return REAL,
    confidence_low  REAL,
    confidence_high REAL,
    confidence_width REAL,
    feature_contributions TEXT,
    model_version   TEXT,
    PRIMARY KEY (card_id, as_of, horizon_days)
);

CREATE TABLE IF NOT EXISTS model_report_card (
    model_version       TEXT NOT NULL,
    as_of               TEXT NOT NULL,
    horizon_days        INTEGER NOT NULL,
    total_samples       INTEGER,
    r_squared_oos       REAL,
    spearman_oos        REAL,
    mean_return_top_decile    REAL,
    mean_return_bottom_decile REAL,
    decile_spread       REAL,
    hit_rate_positive   REAL,
    calibration_json    TEXT,
    feature_importance_json TEXT,
    PRIMARY KEY (model_version, as_of, horizon_days)
);

-- ============================================================
-- Indexes
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_price_history_date ON price_history(date);
CREATE INDEX IF NOT EXISTS idx_cards_set_code ON cards(set_code);
CREATE INDEX IF NOT EXISTS idx_cards_set_value ON cards(set_code, set_value_include);
CREATE INDEX IF NOT EXISTS idx_cards_sealed ON cards(sealed_product, sealed_type);
CREATE INDEX IF NOT EXISTS idx_ebay_history_date ON ebay_history(date);
CREATE INDEX IF NOT EXISTS idx_composite_date ON composite_history(date);
CREATE INDEX IF NOT EXISTS idx_set_daily_date ON set_daily(date);
CREATE INDEX IF NOT EXISTS idx_leaderboard_date ON leaderboard(date);
CREATE INDEX IF NOT EXISTS idx_model_projections_asof ON model_projections(as_of);
CREATE INDEX IF NOT EXISTS idx_model_projections_card ON model_projections(card_id, horizon_days);
