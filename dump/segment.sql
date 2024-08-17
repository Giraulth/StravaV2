CREATE OR REPLACE TABLE segment (
        `id` UInt32,
        `activity_type` String,
        `name` String,
        `distance` Float32,
        `effort_count` UInt32,
        `elevation_profile` String,
        `polyline` String,
        `kom` String,
        `city` String,
        `state` String,
        `country` String,
        `pr_elapsed_time` String,
        `pr_activity_id` UInt64,
        `start_latlng` Array(Float32),
        `end_latlng` Array(Float32),
        `timestamp` DateTime DEFAULT now()
    ) ENGINE = MergeTree()
ORDER BY id;

CREATE OR REPLACE TABLE segment_aggr (
    id String,
    activity_type SimpleAggregateFunction(anyLast, String),
    name SimpleAggregateFunction(anyLast, String),
    distance SimpleAggregateFunction(anyLast, Float32),
    effort_count SimpleAggregateFunction(anyLast, UInt32),
    elevation_profile SimpleAggregateFunction(anyLast, String),
    polyline SimpleAggregateFunction(anyLast, String),
    kom SimpleAggregateFunction(anyLast, String),
    city SimpleAggregateFunction(anyLast, String),
    state SimpleAggregateFunction(anyLast, String),
    country SimpleAggregateFunction(anyLast, String),
    pr_elapsed_time SimpleAggregateFunction(anyLast, String),
    pr_activity_id SimpleAggregateFunction(anyLast, UInt64),
    start_latlng SimpleAggregateFunction(anyLast, Array(Float32)),
    end_latlng SimpleAggregateFunction(anyLast, Array(Float32)),
    timestamp SimpleAggregateFunction(anyLast, DateTime)
) ENGINE = AggregatingMergeTree()
ORDER BY id;

CREATE MATERIALIZED VIEW IF NOT EXISTS segment_mv TO segment_aggr AS
SELECT id,
    anyLastSimpleState(activity_type) AS activity_type,
    anyLastSimpleState(name) AS name,
    anyLastSimpleState(distance) AS distance,
    anyLastSimpleState(effort_count) AS effort_count,
    anyLastSimpleState(elevation_profile) AS elevation_profile,
    anyLastSimpleState(polyline) AS polyline,
    anyLastSimpleState(kom) AS kom,
    anyLastSimpleState(city) AS city,
    anyLastSimpleState(state) AS state,
    anyLastSimpleState(country) AS country,
    anyLastSimpleState(pr_elapsed_time) AS pr_elapsed_time,
    anyLastSimpleState(pr_activity_id) AS pr_activity_id,
    anyLastSimpleState(start_latlng) AS start_latlng,
    anyLastSimpleState(end_latlng) AS end_latlng,
    anyLastSimpleState(timestamp) AS timestamp
FROM (SELECT * from segment order by timestamp DESC)
GROUP BY id;

CREATE OR REPLACE VIEW segment_final AS
SELECT id,
    toString(anyLast(activity_type)) AS activity_type,
    toString(anyLast(name)) AS name,
    toString(anyLast(distance)) AS distance,
    anyLast(effort_count) AS effort_count,
    toString(anyLast(elevation_profile)) AS elevation_profile,
    toString(anyLast(polyline)) AS polyline,
    toString(anyLast(kom)) AS kom,
    toString(anyLast(city)) AS city,
    toString(anyLast(state)) AS state,
    toString(anyLast(country)) AS country,
    toString(anyLast(pr_elapsed_time)) AS pr_elapsed_time,
    toString(anyLast(pr_activity_id)) AS pr_activity_id,
    anyLast(start_latlng) AS start_latlng,
    anyLast(end_latlng) AS end_latlng,
    toString(anyLast(timestamp)) AS last_update
FROM segment_aggr
GROUP BY id;