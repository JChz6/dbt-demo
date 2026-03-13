SELECT
    data:city:findname AS city,
    data:time AS timestamp,
    data:city:coord:lat AS latitude,
    data:city:coord:lon AS longitude,
    data:clouds:all AS all_clouds,
    data:main:grnd_level AS grnd_level,
    data:main:humidity AS humidity,
    data:main:pressure AS pressure,
    data:main:temp AS temperature,
    data:weather[0]:main AS main_weather
FROM {{ source('nyc_weather', 'weather_raw') }}

