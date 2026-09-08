CREATE TABLE raw_events
(
    event_time DateTime64(3),
    kind String,
    did String,
    lang String,
    text String
)
ENGINE = MergeTree
ORDER BY (event_time, did);

CREATE TABLE agg_events_per_minute
(
    minute DateTime,
    kind String,
    events AggregateFunction(count, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (minute, kind);

CREATE MATERIALIZED VIEW mv_events_per_minute TO agg_events_per_minute AS
SELECT
    toStartOfMinute(event_time) AS minute,
    kind,
    countState() AS events
FROM raw_events
GROUP BY minute, kind;
