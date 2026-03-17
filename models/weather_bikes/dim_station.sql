WITH bikes_cte AS(
    SELECT
        start_station_id AS station_id,
        start_station_name AS station_name,
        start_lat AS latitude,
        start_lng AS longitude
    FROM
    {{source('nyc_bikes', 'bike')}}
),
bikes_cte_2018 AS(
    SELECT 
        start_station_id AS station_id,
        start_station_name AS station_name,
        start_station_latitude AS latitude,
        start_station_longitude AS longitude
    FROM
    {{source('nyc_bikes', 'bike_2018')}}
),
UNIFIED AS(
    SELECT
        station_id,
        station_name,
        latitude,
        longitude
    FROM bikes_cte

    UNION ALL

    SELECT
        station_id,
        station_name,
        latitude,
        longitude
    FROM bikes_cte_2018
)
SELECT DISTINCT
    station_id,
    station_name,
    latitude,
    longitude
FROM UNIFIED