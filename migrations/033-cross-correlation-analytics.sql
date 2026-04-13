BEGIN;

-- Cross-metric daily values (normalized from all data sources)
CREATE TABLE IF NOT EXISTS aurora_cross_metrics (
    date DATE NOT NULL,
    metric_name TEXT NOT NULL,
    value REAL NOT NULL,
    source TEXT NOT NULL,
    PRIMARY KEY (date, metric_name)
);
CREATE INDEX idx_cross_metrics_source ON aurora_cross_metrics(source);
CREATE INDEX idx_cross_metrics_date ON aurora_cross_metrics(date DESC);

-- Time-lagged correlations between metric pairs
CREATE TABLE IF NOT EXISTS aurora_lag_correlations (
    id SERIAL PRIMARY KEY,
    metric_a TEXT NOT NULL,
    metric_b TEXT NOT NULL,
    optimal_lag_days INT NOT NULL,
    coefficient REAL NOT NULL,
    sample_size INT NOT NULL,
    summary TEXT,
    calculated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(metric_a, metric_b)
);

-- Event-triggered impact windows
CREATE TABLE IF NOT EXISTS aurora_event_impacts (
    id SERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    affected_metric TEXT NOT NULL,
    before_avg REAL,
    during_avg REAL,
    after_avg REAL,
    impact_pct REAL,
    event_count INT,
    confidence REAL,
    summary TEXT,
    calculated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(event_type, affected_metric)
);

-- Predictive signals derived from correlations and event impacts
CREATE TABLE IF NOT EXISTS aurora_predictive_signals (
    id SERIAL PRIMARY KEY,
    trigger_condition JSONB NOT NULL,
    predicted_outcome JSONB NOT NULL,
    accuracy REAL,
    sample_size INT,
    human_summary TEXT,
    last_triggered_at TIMESTAMPTZ,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Artist genre cache for music enrichment
CREATE TABLE IF NOT EXISTS artist_genres (
    artist_name TEXT PRIMARY KEY,
    genres TEXT[],
    primary_genre TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Job routing
INSERT INTO nexus_job_routing (job_type) VALUES ('cross-metrics-compute'), ('advanced-correlate')
ON CONFLICT DO NOTHING;

INSERT INTO nexus_schema_version (version, description)
VALUES (32, 'Cross-correlation analytics: daily metrics, lag correlations, event impacts, predictive signals, artist genres')
ON CONFLICT (version) DO NOTHING;

COMMIT;
