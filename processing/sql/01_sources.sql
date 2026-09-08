CREATE TABLE raw_events_source
(
    event_time DOUBLE,
    kind STRING,
    did STRING,
    lang STRING,
    text STRING,
    ts AS TO_TIMESTAMP_LTZ(CAST(event_time * 1000 AS BIGINT), 3),
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = '${KAFKA_TOPIC}',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
    'properties.group.id' = 'flink-analytics',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);
