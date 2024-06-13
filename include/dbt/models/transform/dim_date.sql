SELECT
    
FROM
{{ source("snowflake_tables","NYC_TAXI_TRIPS_SOURCE") }}

{% if is_incremental() %}
    WHERE > LOAD_TIMESTAMP (
        SELECT COALESCE(MAX(LOAD_TIMESTAMP), '1970-01-01') FROM {{ THIS }}
    )
{% endif %}