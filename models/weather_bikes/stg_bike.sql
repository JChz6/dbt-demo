WITH trips_cte AS(
    SELECT 
        ride_id,
        DATE(TO_TIMESTAMP(started_at)) AS trip_date,
        rideable_type,
        TO_TIMESTAMP(started_at) AS started_at,
        TO_TIMESTAMP(ended_at) AS ended_at,
        TIMESTAMPDIFF(seconds, TO_TIMESTAMP(started_at), TO_TIMESTAMP(ended_at)) AS trip_duration,
        start_station_id,
        start_station_name,
        start_lat AS start_station_lat,
        start_lng AS start_station_lng,
        end_station_id,
        end_station_name,
        end_lat AS end_station_lat,
        end_lng AS end_station_lng,
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
        start_station_name,
        start_station_latitude AS start_station_lat,
        start_station_longitude AS start_station_lng,
        end_station_id,
        end_station_name,
        end_station_latitude AS end_station_lat,
        end_station_longitude AS end_station_lng,
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
SELECT
    ride_id::STRING AS ride_id,
    trip_date::DATE AS trip_date,
    rideable_type::STRING AS rideable_type,
    started_at::TIMESTAMP AS started_at,
    ended_at::TIMESTAMP AS ended_at,
    trip_duration::INTEGER AS trip_duration,
    start_station_id::STRING AS start_station_id,
    start_station_name::STRING AS start_station_name,
    start_station_lat::FLOAT AS start_station_lat,
    start_station_lng::FLOAT AS start_station_lng,
    end_station_id::STRING AS end_station_id,
    end_station_name::STRING AS end_station_name,
    end_station_lat::FLOAT AS end_station_lat,
    end_station_lng::FLOAT AS end_station_lng,
    usertype::STRING AS usertype,
    user_birth_year::INTEGER AS user_birth_year,
    user_gender::INTEGER AS user_gender,
    bike_id::INTEGER AS bike_id
FROM UNIFIED