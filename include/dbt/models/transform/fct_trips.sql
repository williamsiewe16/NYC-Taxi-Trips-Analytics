{{ config(materialized='incremental') }}
SELECT DISTINCT
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
    INNER JOIN {{ ref('dim_payment_type') }} Pt ON Pt.ID=T.PAYMENT_TYPE
    INNER JOIN {{ ref('dim_rate_code') }} Rc ON Rc.ID=T.RATECODEID
    INNER JOIN {{ ref('dim_taxi_zone') }} Tz ON Tz.LOCATION_ID=T.PULOCATIONID
    INNER JOIN {{ ref('dim_taxi_zone') }} Tz2 ON Tz2.LOCATION_ID=T.DOLOCATIONID
    INNER JOIN {{ ref('dim_pickup_date') }} Pd ON Pd.DATETIME=T.TPEP_PICKUP_DATETIME
    INNER JOIN {{ ref('dim_dropoff_date') }} Dd ON Dd.DATETIME=T.TPEP_DROPOFF_DATETIME
WHERE 1=1
    AND TPEP_PICKUP_DATETIME < TPEP_DROPOFF_DATETIME
    AND LEAST(FARE_AMOUNT, EXTRA, MTA_TAX, IMPROVEMENT_SURCHARGE, TIP_AMOUNT, TOLLS_AMOUNT, TOTAL_AMOUNT, CONGESTION_SURCHARGE, AIRPORT_FEE) >= 0
    AND (upper(Pt.name)<>'CASH' OR T.TIP_AMOUNT=0)
    {% if is_incremental() %}
    AND T.LOAD_TIMESTAMP > (
        SELECT MAX(this.LOAD_TIMESTAMP) FROM {{ this }} as this
    )
    {% endif %}