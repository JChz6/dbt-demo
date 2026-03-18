WITH CTE AS (
    SELECT
        started_at
    FROM {{ ref('stg_bike')}}
    )
SELECT DISTINCT
    DATE(started_at) AS start_date,
    DATE_TRUNC(MINUTE, started_at) AS start_time,
    HOUR(started_at) AS start_hour,
    {{businessday('started_at')}} AS businessday,
    {{season_of_year('started_at')}} AS season_macro  
FROM CTE
