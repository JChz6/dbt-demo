WITH data AS(
    SELECT
        data
    FROM {{ source('nyc_weather', 'weather_raw') }}        
)
SELECT
    data:city:findname::STRING AS city,
    data:time::TIMESTAMP AS timestamp,
    data:city:coord:lat::FLOAT AS latitude,
    data:city:coord:lon::FLOAT AS longitude,
    data:clouds:all::FLOAT AS all_clouds,
    data:main:grnd_level::FLOAT AS grnd_level,
    data:main:humidity::FLOAT AS humidity,
    data:main:pressure::FLOAT AS pressure,
    data:main:temp::FLOAT AS temperature,
    data:weather[0]:main::STRING AS main_weather
FROM data
