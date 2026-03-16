WITH weather_day AS(
    SELECT 
        DATE(time) AS date,
        weather,
        COUNT(weather) AS weather_count    
    FROM {{ref('silver_weather')}}
    GROUP BY date, weather
    QUALIFY ROW_NUMBER() OVER(PARTITION BY DATE(time) ORDER BY weather_count DESC) = 1
),
aggregations AS(
SELECT
    DATE(time) AS date,
    ROUND(AVG(all_clouds), 2) AS avg_all_clouds,
    ROUND(AVG(humidity), 2) AS avg_humidity,
    ROUND(AVG(pressure), 2) AS avg_pressure,
    ROUND(AVG(temp), 2) AS avg_temperature,
FROM {{ref('silver_weather')}}
GROUP BY date
)
SELECT 
    a.date,
    a.avg_all_clouds,
    a.avg_humidity,
    a.avg_pressure,
    a.avg_temperature,
    w.weather
FROM aggregations a
INNER JOIN weather_day w
ON a.date = w.date