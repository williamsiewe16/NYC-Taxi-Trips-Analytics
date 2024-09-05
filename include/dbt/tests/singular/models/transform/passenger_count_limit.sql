SELECT
    *
FROM {{ ref('fct_trips') }}
WHERE PASSENGER_COUNT > 10

