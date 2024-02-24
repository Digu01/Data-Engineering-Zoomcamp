{{
    config(
        materialized='table'
    )
}}

with green_tripdata AS (
    SELECT *,
        'Green' AS service_type
    FROM {{ ref('stg_green_tripdata') }}
),
yellow_tripdata AS (
    SELECT *,
        'Yellow' AS service_type
    FROM {{ ref('stg_yellow_tripdata') }}
),
trips_union AS (
    SELECT *
    FROM green_tripdata
    UNION ALL
    SELECT *
    FROM yellow_tripdata
),
dim_zones AS (
    SELECT *
    FROM {{ ref('dim_zones') }}
    WHERE borough != 'Unknown'
)
SELECT tu.tripid, 
    tu.vendorid, 
    tu.service_type,
    tu.ratecodeid, 
    tu.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    tu.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    tu.pickup_datetime, 
    tu.dropoff_datetime, 
    tu.store_and_fwd_flag, 
    tu.passenger_count, 
    tu.trip_distance, 
    tu.trip_type, 
    tu.fare_amount, 
    tu.extra, 
    tu.mta_tax, 
    tu.tip_amount, 
    tu.tolls_amount, 
    tu.ehail_fee, 
    tu.improvement_surcharge, 
    tu.total_amount, 
    tu.payment_type, 
    tu.payment_type_description
FROM trips_union AS tu
INNER JOIN dim_zones AS pickup_zone
ON tu.pickup_locationid = pickup_zone.locationid
INNER JOIN dim_zones AS dropoff_zone
ON tu.dropoff_locationid = dropoff_zone.locationi
