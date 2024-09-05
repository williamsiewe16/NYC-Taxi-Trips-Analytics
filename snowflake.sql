CREATE DATABASE NYC_TAXI_TRIPS;
USE NYC_TAXI_TRIPS;
USE public;

CREATE USER dbt_test IDENTIFIED BY 'dbt_test';
GRANT ROLE ACCOUNTADMIN TO USER dbt_test;

CREATE OR REPLACE STAGE nyc_taxi_trips_stage
  URL = 's3://nyc-taxi-trips-bucket/'
  CREDENTIALS = (
       AWS_KEY_ID = 'AKIA4MTWMCMORQOC5TGW'  AWS_SECRET_KEY = '5sqFwcU/779kS6PsTxr82/I5mHx4wk2GLsZAT1dC'
  )
  FILE_FORMAT = (TYPE = PARQUET);

---> query the Stage to find the parquet files
LIST @NYC_TAXI_TRIPS_STAGE/yellow_taxi;

CREATE OR REPLACE TABLE NYC_TAXI_TRIPS_SOURCE(
    VENDORID INTEGER,
    TPEP_PICKUP_DATETIME TIMESTAMP,
    TPEP_DROPOFF_DATETIME TIMESTAMP,
    PASSENGER_COUNT INTEGER,
    TRIP_DISTANCE DOUBLE,
    PULOCATIONID INTEGER,
    DOLOCATIONID INTEGER,
    RATECODEID INTEGER,
    STORE_AND_FWD_FLAG STRING,
    PAYMENT_TYPE INTEGER,
    FARE_AMOUNT DOUBLE,
    EXTRA DOUBLE,
    MTA_TAX DOUBLE,
    IMPROVEMENT_SURCHARGE DOUBLE,
    TIP_AMOUNT DOUBLE,
    TOLLS_AMOUNT DOUBLE,
    TOTAL_AMOUNT DOUBLE,
    CONGESTION_SURCHARGE DOUBLE,
    AIRPORT_FEE DOUBLE,
    LOAD_TIMESTAMP TIMESTAMP
);

COPY INTO NYC_TAXI_TRIPS_SOURCE
FROM @NYC_TAXI_TRIPS_STAGE/yellow_taxi 
MATCH_BY_COLUMN_NAME ='CASE_INSENSITIVE';

COPY INTO NYC_TAXI_TRIPS_SOURCE
FROM (
    SELECT
        $1:VendorID::INTEGER AS VENDORID,
        $1:tpep_pickup_datetime::VARCHAR::TIMESTAMP_NTZ AS TPEP_PICKUP_DATETIME,
        $1:tpep_dropoff_datetime::VARCHAR::TIMESTAMP_NTZ AS TPEP_DROPOFF_DATETIME,
        $1:passenger_count::INTEGER AS PASSENGER_COUNT,
        $1:trip_distance::DOUBLE AS TRIP_DISTANCE,
        $1:PULocationID::INTEGER AS PULOCATIONID,
        $1:DOLocationID::INTEGER AS DOLOCATIONID,
        $1:RatecodeID::INTEGER AS RATECODEID,
        $1:store_and_fwd_flag::STRING AS STORE_AND_FWD_FLAG,
        $1:payment_type::INTEGER AS PAYMENT_TYPE,
        $1:fare_amount::DOUBLE AS FARE_AMOUNT,
        $1:extra::DOUBLE AS EXTRA,
        $1:mta_tax::DOUBLE AS MTA_TAX,
        $1:improvement_surcharge::DOUBLE AS IMPROVEMENT_SURCHARGE,
        $1:tip_amount::DOUBLE AS TIP_AMOUNT,
        $1:tolls_amount::DOUBLE AS TOLLS_AMOUNT,
        $1:total_amount::DOUBLE AS TOTAL_AMOUNT,
        $1:congestion_surcharge::DOUBLE AS CONGESTION_SURCHARGE,
        $1:Airport_fee::DOUBLE AS AIRPORT_FEE,
        CURRENT_TIMESTAMP AS LOAD_TIMESTAMP
    FROM @NYC_TAXI_TRIPS_STAGE/yellow_taxi T
);
-- truncate table NYC_TAXI_TRIPS_SOURCE;
SELECT top 10 * from NYC_TAXI_TRIPS_SOURCE;

SELECT top 1 * FROM NYC_TAXI_TRIPS_SOURCE;

INSERT INTO NYC_TAXI_TRIPS_SOURCE
VALUES (123456789, '2024-01-01 00:57:55.000', '2024-01-01 01:17:43.000', 1, 1.72, 186, 79, 1, 'N', 2, 17.7, 1, 0.5, 1, 0, 0, 22.7, 2.5, 0, '2024-06-19 11:31:53.886');

SELECT count(*) FROM DIM_PICKUP_DATE;
SELECT * FROM DIM_PICKUP_DATE;
