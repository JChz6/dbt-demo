WITH cte_trips AS (
    SELECT 
        *
    FROM {{ref('fact_bike_trips')}}
    WHERE DATE_TRUNC(YEAR, DATE(started_at)) = '2018-01-01'
),
cte_weather AS(
    SELECT 
        *
    FROM {{ref('gold_daily_weather')}}
)
SELECT
    t.*,
    w.avg_all_clouds,
    w.avg_humidity,
    w.avg_pressure,
    w.avg_temperature,
    w.main_weather
FROM cte_trips t
LEFT JOIN cte_weather w
ON t.trip_date = w.date
