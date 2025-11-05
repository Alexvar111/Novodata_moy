CREATE TABLE user_events (
    user_id UInt64,
    event_type String,
    points_spent UInt64,
    event_time DateTime
) 
ENGINE = MergeTree()
ORDER BY (event_time, user_id)
TTL event_time + INTERVAL 30 DAY DELETE;

create table user_agr (
    event_type String,
    event_date Date,
    unique_users AggregateFunction(uniq, UInt64),
    total_spent AggregateFunction(sum, UInt64),
    total_actions AggregateFunction(count, UInt64)
) 
ENGINE = AggregatingMergeTree()
order by event_date
TTL event_date + INTERVAL 180 DAY DELETE;

CREATE MATERIALIZED VIEW logs_user_agr
TO user_agr
AS
SELECT 
    event_type,
    toDate(event_time) as event_date ,
    uniqState(user_id) as unique_users,
    sumState(points_spent) as total_spent,
    countState(event_type) as total_actions
FROM user_events
GROUP BY toDate(event_time), event_type;


INSERT INTO user_events VALUES
(1, 'login', 0, now() - INTERVAL 10 DAY),
(2, 'signup', 0, now() - INTERVAL 10 DAY),
(3, 'login', 0, now() - INTERVAL 10 DAY),
(1, 'login', 0, now() - INTERVAL 7 DAY),
(2, 'login', 0, now() - INTERVAL 7 DAY),
(3, 'purchase', 30, now() - INTERVAL 7 DAY),
(1, 'purchase', 50, now() - INTERVAL 5 DAY),
(2, 'logout', 0, now() - INTERVAL 5 DAY),
(4, 'login', 0, now() - INTERVAL 5 DAY),
(1, 'login', 0, now() - INTERVAL 3 DAY),
(3, 'purchase', 70, now() - INTERVAL 3 DAY),
(5, 'signup', 0, now() - INTERVAL 3 DAY),
(2, 'purchase', 20, now() - INTERVAL 1 DAY),
(4, 'logout', 0, now() - INTERVAL 1 DAY),
(5, 'login', 0, now() - INTERVAL 1 DAY),
(1, 'purchase', 25, now()),
(2, 'login', 0, now()),
(3, 'logout', 0, now()),
(6, 'signup', 0, now()),
(6, 'purchase', 100, now());

SELECT
    cohort_date,
    total_users_day_0,
    returned_in_7_days,
    round(returned_in_7_days * 100 / total_users_day_0, 2) AS retention_7d_percent
FROM
(
    WITH
        cohorts AS (
            SELECT
                user_id,
                min(toDate(event_time)) AS first_activity_date
            FROM user_events
            GROUP BY user_id
        ),
        activities AS (
            SELECT DISTINCT
                user_id,
                toDate(event_time) AS activity_date
            FROM user_events
        )
    SELECT
        c.first_activity_date AS cohort_date,
        count(DISTINCT c.user_id) AS total_users_day_0,
        count(DISTINCT if(a.activity_date > c.first_activity_date AND a.activity_date <= c.first_activity_date + INTERVAL 7 DAY, a.user_id, NULL)) AS returned_in_7_days
    FROM cohorts AS c
    LEFT JOIN activities AS a ON c.user_id = a.user_id
    GROUP BY cohort_date
)
ORDER BY cohort_date;

SELECT
    event_date,
    event_type,
    uniqMerge(unique_users) AS daily_unique_users,
    sumMerge(total_spent) AS daily_total_spent,
    countMerge(total_actions) AS daily_total_actions
FROM user_agr
GROUP BY event_date,event_type
ORDER BY event_date DESC;