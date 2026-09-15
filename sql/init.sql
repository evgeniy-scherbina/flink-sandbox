CREATE TABLE IF NOT EXISTS ad_click_counts (
    ad_id text NOT NULL,
    window_start timestamp NOT NULL,
    window_end timestamp NOT NULL,
    click_count bigint NOT NULL,
    PRIMARY KEY (ad_id, window_start)
);

CREATE TABLE IF NOT EXISTS sensor_window_stats (
    sensor_id text NOT NULL,
    window_start timestamp NOT NULL,
    window_end timestamp NOT NULL,
    min_temp double precision NOT NULL,
    max_temp double precision NOT NULL,
    avg_temp double precision NOT NULL,
    reading_count bigint NOT NULL,
    PRIMARY KEY (sensor_id, window_start)
);
