CREATE OR REPLACE TABLE equipement (
        `id` String,
        `converted_distance` Float32,
        `brand_name` String,
        `model_name` String COMMENT 'The gear''s model name.',
        `name` String,
        `frame_type` String,
        `description` String,
        `weight` Float32,
        `timestamp` DateTime DEFAULT now()
    ) ENGINE = MergeTree()
ORDER BY id;

CREATE OR REPLACE TABLE equipement_aggr (
    id String,
    converted_distance SimpleAggregateFunction(anyLast, Float32),
    brand_name SimpleAggregateFunction(anyLast, String),
    model_name SimpleAggregateFunction(anyLast, String),
    name SimpleAggregateFunction(anyLast, String),
    frame_type SimpleAggregateFunction(anyLast, String),
    description SimpleAggregateFunction(anyLast, String),
    weight SimpleAggregateFunction(anyLast, Float32),
    timestamp SimpleAggregateFunction(anyLast, DateTime)
) ENGINE = AggregatingMergeTree()
ORDER BY id;

CREATE MATERIALIZED VIEW IF NOT EXISTS equipement_mv TO equipement_aggr AS
SELECT id,
    anyLastSimpleState(converted_distance) AS converted_distance,
    anyLastSimpleState(brand_name) AS brand_name,
    anyLastSimpleState(model_name) AS model_name,
    anyLastSimpleState(name) AS name,
    anyLastSimpleState(frame_type) AS frame_type,
    anyLastSimpleState(description) AS description,
    anyLastSimpleState(weight) AS weight,
    anyLastSimpleState(timestamp) AS timestamp
FROM (SELECT * from equipement order by timestamp DESC)
GROUP BY id;

CREATE OR REPLACE VIEW equipement_final AS
SELECT id,
    toString(anyLast(converted_distance)) AS converted_distance,
    toString(anyLast(brand_name)) AS brand_name,
    toString(anyLast(model_name)) AS model_name,
    toString(anyLast(name)) AS name,
    toString(anyLast(frame_type)) AS frame_type,
    toString(anyLast(description)) AS description,
    toString(anyLast(weight)) AS weight,
    toString(anyLast(timestamp)) AS last_update
FROM equipement_aggr
GROUP BY id;