WITH trips_cte AS(
    SELECT
        ride_id,
        DATE(TO_TIMESTAMP(started_at)) AS trip_date,
        rideable_type,
        TO_TIMESTAMP(started_at) AS started_at,
        TO_TIMESTAMP(ended_at) AS ended_at,
        TIMESTAMPDIFF(seconds, TO_TIMESTAMP(started_at), TO_TIMESTAMP(ended_at)) AS trip_duration,
        start_station_id,
        end_station_id,
        LOWER(member_casual) AS usertype,
        NULL AS user_birth_year,
        NULL AS user_gender,
        NULL AS bike_id
    FROM {{source('nyc_bikes', 'bike')}}
),
trips_cte_2018 AS(
    SELECT
        NULL AS ride_id,
        DATE(TO_TIMESTAMP(REPLACE(starttime, '"', ''))) AS trip_date,
        NULL AS rideable_type,
        TO_TIMESTAMP(REPLACE(starttime, '"', '')) AS started_at,
        TO_TIMESTAMP(REPLACE(stoptime, '"', '')) AS ended_at,
        tripduration AS trip_duration,
        start_station_id,
        end_station_id,
        LOWER(usertype) AS usertype,
        birth_year AS user_birth_year,
        gender AS user_gender,
        bikeid AS bike_id
    FROM {{source('nyc_bikes', 'bike_2018')}}
),
UNIFIED AS(
    SELECT * FROM trips_cte
    UNION ALL
    SELECT * FROM trips_cte_2018
)
SELECT * FROM UNIFIED

