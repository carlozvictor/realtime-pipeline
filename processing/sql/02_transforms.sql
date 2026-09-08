CREATE VIEW deduped_events AS
SELECT event_time, kind, did, lang, text, ts
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY did, kind, ts ORDER BY proctime() ASC) AS row_num
    FROM raw_events_source
)
WHERE row_num = 1;

CREATE VIEW events_per_minute AS
SELECT
    window_start,
    window_end,
    kind,
    COUNT(*) AS events
FROM TABLE(
    TUMBLE(TABLE deduped_events, DESCRIPTOR(ts), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, kind;
