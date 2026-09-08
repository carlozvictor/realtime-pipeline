CREATE TABLE raw_events_sink
(
    event_time TIMESTAMP(3),
    kind STRING,
    did STRING,
    lang STRING,
    text STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${CLICKHOUSE_HOST}:9004/default',
    'username' = 'flink',
    'password' = '',
    'table-name' = 'raw_events'
);

CREATE TABLE agg_events_per_minute_sink
(
    `minute` TIMESTAMP(3),
    kind STRING,
    events BIGINT
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${CLICKHOUSE_HOST}:9004/default',
    'username' = 'flink',
    'password' = '',
    'table-name' = 'agg_events_per_minute_raw'
);

INSERT INTO raw_events_sink
SELECT TO_TIMESTAMP_LTZ(CAST(event_time * 1000 AS BIGINT), 3), kind, did, lang, text
FROM deduped_events;

INSERT INTO agg_events_per_minute_sink
SELECT window_start, kind, events
FROM events_per_minute;
