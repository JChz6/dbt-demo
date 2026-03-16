WITH CTE AS (
    SELECT
        started_at
    FROM {{ source('nyc_bikes', 'bike') }}
    )
    SELECT
        TO_TIMESTAMP(started_at) AS start_ts,
        DATE(TO_TIMESTAMP(started_at)) AS start_date,
        TIME(TO_TIMESTAMP(started_at)) AS start_time,
        HOUR(TO_TIMESTAMP(started_at)) AS start_hour,

        {{businessday('started_at')}} AS businessday,
        {{season_of_year('started_at')}} AS season_macro
    
    FROM CTE
