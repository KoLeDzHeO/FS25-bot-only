CREATE TABLE IF NOT EXISTS player_online_history (
    id SERIAL PRIMARY KEY,
    player_name TEXT NOT NULL,
    check_time TIMESTAMP NOT NULL,
    date DATE NOT NULL,
    hour INTEGER NOT NULL,
    dow INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_online_name_date_hour ON player_online_history (player_name, date, hour);
-- Ускоряем выборки по времени
CREATE INDEX IF NOT EXISTS idx_online_check_time ON player_online_history (check_time);

CREATE TABLE IF NOT EXISTS player_total_time (
    player_name TEXT PRIMARY KEY,
    total_hours INTEGER NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
-- Индекс для сортировки по общему времени
CREATE INDEX IF NOT EXISTS idx_total_hours ON player_total_time (total_hours DESC);

CREATE TABLE IF NOT EXISTS weekly_top_last (
    player_name TEXT PRIMARY KEY,
    hours INTEGER NOT NULL
);
