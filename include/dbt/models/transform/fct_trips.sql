{{ config(materialized='incremental') }}
SELECT
    T.VENDORID,
    {{ dbt_utils.generate_surrogate_key(['TPEP_PICKUP_DATETIME']) }}  AS PICKUP_DATETIME_ID,
    {{ dbt_utils.generate_surrogate_key(['TPEP_DROPOFF_DATETIME']) }}  AS DROPOFF_DATETIME_ID,
    T.PULOCATIONID,
    T.DOLOCATIONID,
    T.PAYMENT_TYPE,
    T.RATECODEID,
    T.PASSENGER_COUNT,
    T.TRIP_DISTANCE,
    T.STORE_AND_FWD_FLAG,
    T.FARE_AMOUNT,
    T.EXTRA,
    T.MTA_TAX,
    T.IMPROVEMENT_SURCHARGE,
    T.TIP_AMOUNT,
    T.TOLLS_AMOUNT,
    T.CONGESTION_SURCHARGE,
    T.AIRPORT_FEE,
    T.TOTAL_AMOUNT,
    T.LOAD_TIMESTAMP

FROM {{ source('snowflake_tables', 'NYC_TAXI_TRIPS_SOURCE') }} T

{% if is_incremental() %}
WHERE T.LOAD_TIMESTAMP > (
    SELECT MAX(this.LOAD_TIMESTAMP) FROM {{ this }} as this
)
{% endif %}