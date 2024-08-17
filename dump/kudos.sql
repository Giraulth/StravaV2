CREATE TABLE kudos (
    `user_id` String,
    `activity_id` UInt64
) ENGINE = MergeTree()
ORDER BY activity_id;