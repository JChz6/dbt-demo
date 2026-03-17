WITH trips_cte AS(
    SELECT
        *
    FROM {{source('nyc_bikes', 'bike')}}
)
SELECT
    ride_id,
    DATE(TO_TIMESTAMP(started_at)) AS trip_date,
    rideable_type,
    TO_TIMESTAMP(started_at) AS started_at,
    TO_TIMESTAMP(ended_at) AS ended_at,
    ROUND(TIMESTAMPDIFF(seconds, TO_TIMESTAMP(started_at), TO_TIMESTAMP(ended_at))/60, 2) AS trip_duration_min,
    start_station_id,
    end_station_id,
    member_casual
FROM trips_cte