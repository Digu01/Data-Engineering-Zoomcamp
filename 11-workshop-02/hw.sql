CREATE MATERIALIZED VIEW joined_data AS
 SELECT 
	taxi_zone.Zone as pickup_zone, taxi_zone_1.Zone as dropoff_zone, tpep_pickup_datetime, tpep_dropoff_datetime
 FROM 
	trip_data
 JOIN 
	taxi_zone ON trip_data.PULocationID = taxi_zone.location_id
 JOIN 
	taxi_zone as taxi_zone_1 ON trip_data.DOLocationID = taxi_zone_1.location_id ;

########################################### Q1 ###############################################################################
CREATE MATERIALIZED VIEW QN1 AS
 SELECT 
	concat(pickup_zone, '->', dropoff_zone) as compined_zones, 
	min(tpep_dropoff_datetime - tpep_pickup_datetime) as min_trip_distance, 
	max(tpep_dropoff_datetime - tpep_pickup_datetime) as max_trip_distance, 
	avg(tpep_dropoff_datetime - tpep_pickup_datetime) as average_trip_distance
 FROM 
	joined_data

group by 
	compined_zones
order by 
	average_trip_distance desc
limit
    1;

SELECT * FROM QN1;

"""
Yorkville East->Steinway
OUTPUT:
      compined_zones      | min_trip_distance | max_trip_distance | average_trip_distance
--------------------------+-------------------+-------------------+-----------------------
 Yorkville East->Steinway | 23:59:33          | 23:59:33          | 23:59:33
(1 row)
"""

############################################ Q2 ##################################################################
CREATE MATERIALIZED VIEW QN2 AS
 SELECT 
	concat(pickup_zone, '->', dropoff_zone) as compined_zones, 
    count(*) as count_trips,
	avg(tpep_dropoff_datetime - tpep_pickup_datetime) as average_trip_distance
 FROM 
	joined_data

group by 
	compined_zones
order by 
	average_trip_distance desc
limit
    1;

SELECT * FROM QN2;

"""
1
OUTPUT:
      compined_zones      | count_trips | average_trip_distance
--------------------------+-------------+-----------------------
 Yorkville East->Steinway |           1 | 23:59:33
(1 row)
"""

################################# Q3 #######################################################

CREATE MATERIALIZED VIEW top3_busiest_cities AS
WITH t AS (
        SELECT MAX(tpep_pickup_datetime) AS latest_pickup_time
        FROM trip_data
    )
 SELECT 
	taxi_zone.Zone as pickup_zone, count(taxi_zone.Zone) as count_trips
 FROM t,
	trip_data
 JOIN 
	taxi_zone ON trip_data.PULocationID = taxi_zone.location_id
 JOIN 
	taxi_zone as taxi_zone_1 ON trip_data.DOLocationID = taxi_zone_1.location_id 
where 
    trip_data.tpep_pickup_datetime > (t.latest_pickup_time - INTERVAL '17' HOUR)
group by 
    pickup_zone
order by 
    count_trips desc
limit 
    3;

SELECT * from top3_busiest_cities;

"""
LaGuardia Airport, Lincoln Square East, JFK Airport
OUTPUT:
     pickup_zone     | count_trips
---------------------+-------------
 LaGuardia Airport   |          19
 JFK Airport         |          17
 Lincoln Square East |          17
(3 rows)

"""