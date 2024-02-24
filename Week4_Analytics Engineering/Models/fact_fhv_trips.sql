{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata AS (
    SELECT *,
        'Fhv' AS service_type
    FROM {{ ref('stg_fhv_tripdata') }}
),
dim_zones AS (
    SELECT *
    FROM {{ ref('dim_zones') }}
    WHERE borough != 'Unknown'
)
SELECT f.dispatching_base_num,
    f.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    f.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    f.pickup_datetime, 
    f.dropoff_datetime, 
    f.sr_flag, 
    f.affiliated_base_number,
    'FHV' as service_type
FROM fhv_tripdata AS f
INNER JOIN dim_zones AS pickup_zone
ON f.pickup_locationid = pickup_zone.locationid
INNER JOIN dim_zones AS dropoff_zone
ON f.dropoff_locationid = dropoff_zone.locationid
