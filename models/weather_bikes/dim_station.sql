WITH bikes_cte AS(
    SELECT
        start_station_id AS station_id,
        start_station_name AS station_name,
        start_station_lat AS latitude,
        start_station_lng AS longitude
    FROM
    {{ref('stg_bike')}}
)
SELECT DISTINCT
    station_id,
    station_name,
    latitude,
    longitude
FROM bikes_cte