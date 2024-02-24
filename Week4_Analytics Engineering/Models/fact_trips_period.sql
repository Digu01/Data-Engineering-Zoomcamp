{{
    config(
        materialized='table'
    )
}}

WITH yellow_trips as
(
    SELECT FORMAT_DATE('%Y-%m', pickup_datetime) AS period,
        COUNT(*) AS total_trips
    FROM {{ ref("fact_trips") }}
    WHERE service_type = 'Yellow'
    GROUP BY FORMAT_DATE('%Y-%m', pickup_datetime)
),
green_trips AS
(
    SELECT FORMAT_DATE('%Y-%m', pickup_datetime) AS period,
        COUNT(*) AS total_trips
    FROM {{ ref("fact_trips") }}
    WHERE service_type = 'Green'
    GROUP BY FORMAT_DATE('%Y-%m', pickup_datetime)
),
fhv_trips AS
(
    SELECT FORMAT_DATE('%Y-%m', pickup_datetime) AS period,
        COUNT(*) AS total_trips
    FROM {{ ref("fact_fhv_trips") }}
    GROUP BY FORMAT_DATE('%Y-%m', pickup_datetime)
)
SELECT 'Yellow' AS service_type,
    period,
    total_trips
FROM yellow_trips
UNION ALL
SELECT 'Green' AS service_type,
    period,
    total_trips
FROM green_trips
UNION ALL
SELECT 'FHV' AS service_type,
    period,
    total_trips
FROM fhv_trips
