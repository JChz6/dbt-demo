WITH bikes_cte AS(
    SELECT
        *
    FROM
    {{source('nyc_bikes', 'bike')}}
)
SELECT DISTINCT
    start_station_id AS station_id,
    start_station_name AS station_name,
    start_lat AS latitude,
    start_lng AS longitude
FROM bikes_cte