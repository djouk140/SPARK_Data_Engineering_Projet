
CREATE TABLE IF NOT EXISTS toots (
    id TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL,
    username TEXT NOT NULL,
    language TEXT,
    text TEXT NOT NULL,
    hashtags TEXT[] NOT NULL DEFAULT '{}',
    favourites_count INTEGER DEFAULT 0,
    reblogs_count INTEGER DEFAULT 0,
    toot_length INTEGER DEFAULT 0,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS toot_hourly_stats (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    toot_count BIGINT NOT NULL,
    avg_length DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (window_start, window_end)
);

CREATE TABLE IF NOT EXISTS user_hourly_stats (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    username TEXT NOT NULL,
    toot_count BIGINT NOT NULL,
    avg_length DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (window_start, window_end, username)
);

CREATE TABLE IF NOT EXISTS toot_sentiments (
    toot_id TEXT PRIMARY KEY REFERENCES toots(id) ON DELETE CASCADE,
    text TEXT NOT NULL,
    sentiment TEXT NOT NULL CHECK (
        sentiment IN ('positive', 'negative', 'neutral')
    ),
    confidence DOUBLE PRECISION NOT NULL CHECK (
        confidence >= 0.0 AND confidence <= 1.0
    ),
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_toots_created_at
    ON toots(created_at);

CREATE INDEX IF NOT EXISTS idx_toots_username
    ON toots(username);
CREATE INDEX IF NOT EXISTS idx_toot_sentiments_label
    ON toot_sentiments(sentiment);