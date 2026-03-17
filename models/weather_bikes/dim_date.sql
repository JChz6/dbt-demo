WITH CTE AS (
    SELECT
        started_at
    FROM {{ source('nyc_bikes', 'bike') }}
    ),
CTE_2018 AS(
    SELECT
        TO_TIMESTAMP(REPLACE(starttime, '"', '')) AS started_at
    FROM {{ source('nyc_bikes', 'bike_2018') }}
),
UNIFIED AS(
    SELECT started_at FROM CTE
        UNION ALL
    SELECT started_at FROM CTE_2018
)
SELECT DISTINCT
    DATE(TO_TIMESTAMP(started_at)) AS start_date,
    DATE_TRUNC(MINUTE, TO_TIMESTAMP(started_at)) AS start_time,
    HOUR(TO_TIMESTAMP(started_at)) AS start_hour,
    {{businessday('started_at')}} AS businessday,
    {{season_of_year('started_at')}} AS season_macro  
FROM UNIFIED
