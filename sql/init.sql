CREATE TABLE IF NOT EXISTS ad_click_counts (
    ad_id text NOT NULL,
    window_start timestamp NOT NULL,
    window_end timestamp NOT NULL,
    click_count bigint NOT NULL,
    PRIMARY KEY (ad_id, window_start)
);
