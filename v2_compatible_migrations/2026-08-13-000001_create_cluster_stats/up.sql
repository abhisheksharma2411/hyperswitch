CREATE TABLE IF NOT EXISTS cluster_stats (
    cluster_key TEXT  NOT NULL PRIMARY KEY,
    statistics  JSONB NOT NULL
);
