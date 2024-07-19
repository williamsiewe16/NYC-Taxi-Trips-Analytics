SELECT
    LocationID AS LOCATION_ID,
    Borough AS BOROUGH,
    Zone,
    service_zone AS SERVICE_ZONE
FROM {{ ref("taxi_zone_lookup") }}