SELECT
    Pd.HOUR,
    AVG(TIMEDIFF(MINUTE, Pd.DATETIME, Dd.DATETIME)) AS avg_trip_duration
FROM {{ ref('fct_trips') }} T
    INNER JOIN {{ ref('dim_pickup_date') }} Pd ON Pd.DATETIME_ID=T.PICKUP_DATETIME_ID
    INNER JOIN {{ ref('dim_dropoff_date') }} Dd ON Dd.DATETIME_ID=T.DROPOFF_DATETIME_ID
GROUP BY Pd.HOUR