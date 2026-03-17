WITH trips_cte AS(
    SELECT
        ride_id,
        DATE(TO_TIMESTAMP(started_at)) AS trip_date,
        rideable_type,
        started_at AS started_at,
        ended_at AS ended_at,
        TIMESTAMPDIFF(seconds, started_at, ended_at) AS trip_duration,
        start_station_id,
        end_station_id,
        LOWER(usertype) AS usertype,
        NULL AS user_birth_year,
        NULL AS user_gender,
        NULL AS bike_id
    FROM {{ref('stg_bike')}}
)
SELECT
    ride_id,
    trip_date,
    rideable_type,
    started_at,
    ended_at,
    trip_duration,
    start_station_id,
    end_station_id,
    usertype,
    user_birth_year,
    user_gender,
    bike_id
FROM trips_cte