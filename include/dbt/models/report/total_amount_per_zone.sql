SELECT
    Tz.Borough,
    Tz.Zone,
    Tz.service_zone,
    sum(total_amount) AS total_amount
FROM {{ ref('fct_trips') }} T
    INNER JOIN {{ ref('dim_taxi_zone') }} Tz ON Tz.LOCATION_ID=T.PULOCATIONID
GROUP BY Tz.Borough, Tz.Zone, Tz.service_zone