# Homework

## Setting up

In order to get a static set of results, we will use historical data from the dataset.

Run the following commands:
```bash
# Load the cluster op commands.
source commands.sh
# First, reset the cluster:
clean-cluster
# Start a new cluster
start-cluster
# wait for cluster to start
sleep 5
# Seed historical data instead of real-time data
seed-kafka
# Recreate trip data table
psql -f risingwave-sql/table/trip_data.sql
# Wait for a while for the trip_data table to be populated.
sleep 5
# Check that you have 100K records in the trip_data table
# You may rerun it if the count is not 100K
psql -c "SELECT COUNT(*) FROM trip_data"
```

## Question 0

_This question is just a warm-up to introduce dynamic filter, please attempt it before viewing its solution._

What are the dropoff taxi zones at the latest dropoff times?

For this part, we will use the [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/).

<details>
<summary>Solution</summary>

```sql
CREATE MATERIALIZED VIEW latest_dropoff_time AS
    WITH t AS (
        SELECT MAX(tpep_dropoff_datetime) AS latest_dropoff_time
        FROM trip_data
    )
    SELECT taxi_zone.Zone as taxi_zone, latest_dropoff_time
    FROM t,
            trip_data
    JOIN taxi_zone
        ON trip_data.DOLocationID = taxi_zone.location_id
    WHERE trip_data.tpep_dropoff_datetime = t.latest_dropoff_time;

SELECT taxi_zone, latest_dropoff_time FROM latest_dropoff_time;

--    taxi_zone    | latest_dropoff_time
-- ----------------+---------------------
--  Midtown Center | 2022-01-03 17:24:54
-- (1 row)
```

</details>

## Question 1

Create a materialized view to compute the average, min and max trip time **between each taxi zone**.

From this MV, find the pair of taxi zones with the highest average trip time.
You may need to use the [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/) for this.

Bonus (no marks): Create an MV which can identify anomalies in the data. For example, if the average trip time between two zones is 1 minute,
but the max trip time is 10 minutes and 20 minutes respectively.

Options:
1. Yorkville East, Steinway
2. Murray Hill, Midwood
3. East Flatbush/Farragut, East Harlem North
4. Midtown Center, University Heights/Morris Heights

p.s. The trip time between taxi zones does not take symmetricity into account, i.e. `A -> B` and `B -> A` are considered different trips. This applies to subsequent questions as well.

>SOLUTION
```sql
-- Check MVs here: http://localhost:5691/materialized_views/

-- VERSION 1

-- Create materialized view to compute average, min, and max trip time between each taxi zone
CREATE MATERIALIZED VIEW trip_time_stats AS
    SELECT tz1.Zone AS pickup_zone,
           tz2.Zone AS dropoff_zone,
           MIN(tpep_dropoff_datetime - tpep_pickup_datetime) AS min_trip_time,
           AVG(tpep_dropoff_datetime - tpep_pickup_datetime) AS avg_trip_time,
           MAX(tpep_dropoff_datetime - tpep_pickup_datetime) AS max_trip_time
    FROM trip_data
    JOIN taxi_zone tz1 ON trip_data.PULocationID = tz1.location_id
    JOIN taxi_zone tz2 ON trip_data.DOLocationID = tz2.location_id
    GROUP BY tz1.Zone, tz2.Zone;

-- (Query 1) Find the pair of taxi zones with the highest average trip time
SELECT pickup_zone, dropoff_zone, avg_trip_time
FROM trip_time_stats
ORDER BY avg_trip_time DESC
LIMIT 1;

-- (Query 2, equivalent to Query 1) Find the pair of taxi zones with the highest average trip time
WITH max_avg_time AS (
    SELECT MAX(avg_trip_time) AS max_avg_trip_time
    FROM trip_time_stats
)
SELECT pickup_zone, dropoff_zone, avg_trip_time
FROM trip_time_stats, max_avg_time
WHERE avg_trip_time = max_avg_trip_time;

-- VERSION 2 (simplification)

-- Since we only care about the maximum
-- Let's include ORDER BY and LIMIT in the MV
-- This avoids recomputation in the following batch query

CREATE MATERIALIZED VIEW trip_time_stats AS
    SELECT tz1.Zone AS pickup_zone,
           tz2.Zone AS dropoff_zone,
           MIN(tpep_dropoff_datetime - tpep_pickup_datetime) AS min_trip_time,
           AVG(tpep_dropoff_datetime - tpep_pickup_datetime) AS avg_trip_time,
           MAX(tpep_dropoff_datetime - tpep_pickup_datetime) AS max_trip_time
    FROM trip_data
    JOIN taxi_zone tz1 ON trip_data.PULocationID = tz1.location_id
    JOIN taxi_zone tz2 ON trip_data.DOLocationID = tz2.location_id
    GROUP BY tz1.Zone, tz2.Zone
    ORDER BY avg_trip_time DESC
    LIMIT 1;

-- We'll get a warning:
-- NOTICE:  The ORDER BY clause in the CREATE MATERIALIZED VIEW statement does not guarantee that the rows selected out of this materialized view is returned in this order.
-- It only indicates the physical clustering of the data, which may improve the performance of queries issued against this materialized view.
-- We can ignore it because we're limiting the query

SELECT pickup_zone, dropoff_zone, avg_trip_time
FROM trip_time_stats;

--   pickup_zone   | dropoff_zone | avg_trip_time 
-- ----------------+--------------+---------------
--  Yorkville East | Steinway     | 23:59:33
-- (1 row)
```
>ANSWER ✅
```
Yorkville East, Steinway
```
```sql
-- BONUS (anomalies)

-- Create a MV with trip_time and trip_distance stats to spot anomalies, such as outliers
-- Outliers could be caused by errors in data entry, extreme traffic conditions, or other anomalies
-- One common approach is to remove trip durations that fall outside a certain range of standard deviations from the mean
-- We're interested in instances where the maximum trip time is much higher than the average trip time for zones pairs

CREATE MATERIALIZED VIEW trip_anomalies AS
    SELECT tz1.Zone AS pickup_zone,
        tz2.Zone AS dropoff_zone,
        COUNT(*) AS num_trips,
        MIN(tpep_dropoff_datetime - tpep_pickup_datetime) AS min_trip_time,
        AVG(tpep_dropoff_datetime - tpep_pickup_datetime) AS avg_trip_time,
        MAX(tpep_dropoff_datetime - tpep_pickup_datetime) AS max_trip_time,
        MIN(trip_distance) AS min_trip_distance,
        ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
        MAX(trip_distance) AS max_trip_distance
    FROM trip_data
    JOIN taxi_zone tz1 ON trip_data.PULocationID = tz1.location_id
    JOIN taxi_zone tz2 ON trip_data.DOLocationID = tz2.location_id
    GROUP BY tz1.Zone, tz2.Zone;

-- Query with a constant time threshold to spot these situations
-- There are situation where min trip distance is zero
-- The trip distance is also interesting to cross (e.g. 5 miles trips taking 20+ hours)
-- It would be cool to identify very short trips comparing to avg distance, like starting and canceling the timer

SELECT
    *,
    (max_trip_time - avg_trip_time) AS trip_time_difference
FROM 
    trip_anomalies
WHERE 
    max_trip_time - avg_trip_time > '01:00:00'
ORDER BY 
    trip_time_difference;

--           pickup_zone          |          dropoff_zone          | num_trips | min_trip_time |  avg_trip_time  | max_trip_time  | min_trip_distance | avg_trip_distance | max_trip_distance | trip_time_difference  
-- -------------------------------+--------------------------------+-----------+---------------+-----------------+----------------+-------------------+-------------------+-------------------+-----------------------
--  JFK Airport                   | NV                             |        44 | 00:00:00      | 00:14:27.318182 | 01:16:12       |                 0 |              7.78 |             44.25 | 01:01:44.681818
--  Gramercy                      | Bushwick South                 |         5 | 00:16:24      | 00:40:01.2      | 01:48:27       |              4.56 |              8.25 |             20.37 | 01:08:25.8
--  Clinton East                  | Lincoln Square East            |       161 | 00:00:41      | 00:06:20.968944 | 01:15:12       |              0.01 |              1.30 |             11.09 | 01:08:51.031056
--  East Chelsea                  | JFK Airport                    |        48 | 00:25:25      | 00:32:23.541667 | 01:47:43       |             15.18 |             18.10 |              51.8 | 01:15:19.458333
--  Garment District              | JFK Airport                    |        66 | 00:23:16      | 00:30:50.515152 | 01:48:26       |              4.42 |             16.92 |              18.2 | 01:17:35.484848
```

## Question 2

Recreate the MV(s) in question 1, to also find the **number of trips** for the pair of taxi zones with the highest average trip time.

Options:
1. 5
2. 3
3. 10
4. 1

>SOLUTION
```sql
-- VERSION 1

-- Create materialized view to compute average, min, max trip time, and number of trips between each taxi zone pair
CREATE MATERIALIZED VIEW trip_time_stats_with_count AS
    SELECT tz1.Zone AS pickup_zone,
           tz2.Zone AS dropoff_zone,
           COUNT(*) AS num_trips,
           MIN(tpep_dropoff_datetime - tpep_pickup_datetime) AS min_trip_time,
           AVG(tpep_dropoff_datetime - tpep_pickup_datetime) AS avg_trip_time,
           MAX(tpep_dropoff_datetime - tpep_pickup_datetime) AS max_trip_time
    FROM trip_data
    JOIN taxi_zone tz1 ON trip_data.PULocationID = tz1.location_id
    JOIN taxi_zone tz2 ON trip_data.DOLocationID = tz2.location_id
    GROUP BY tz1.Zone, tz2.Zone;

-- (Query 1) Find the number of trips for the pair of taxi zones with the highest average trip time
SELECT pickup_zone, dropoff_zone, avg_trip_time, num_trips
FROM trip_time_stats_with_count
ORDER BY avg_trip_time DESC
LIMIT 1;

-- (Query 2, equivalent to Query 1) Find the number of trips for the pair of taxi zones with the highest average trip time
WITH max_avg_time AS (
    SELECT MAX(avg_trip_time) AS max_avg_trip_time
    FROM trip_time_stats_with_count
)
SELECT pickup_zone, dropoff_zone, avg_trip_time, num_trips
FROM trip_time_stats_with_count, max_avg_time
WHERE avg_trip_time = max_avg_trip_time;

-- VERSION 2 (simplification)

CREATE MATERIALIZED VIEW trip_time_stats_with_count AS
    SELECT tz1.Zone AS pickup_zone,
           tz2.Zone AS dropoff_zone,
           COUNT(*) AS num_trips,
           MIN(tpep_dropoff_datetime - tpep_pickup_datetime) AS min_trip_time,
           AVG(tpep_dropoff_datetime - tpep_pickup_datetime) AS avg_trip_time,
           MAX(tpep_dropoff_datetime - tpep_pickup_datetime) AS max_trip_time
    FROM trip_data
    JOIN taxi_zone tz1 ON trip_data.PULocationID = tz1.location_id
    JOIN taxi_zone tz2 ON trip_data.DOLocationID = tz2.location_id
    GROUP BY tz1.Zone, tz2.Zone
    ORDER BY avg_trip_time DESC
    LIMIT 1;

SELECT pickup_zone, dropoff_zone, avg_trip_time, num_trips
FROM trip_time_stats_with_count;

--   pickup_zone   | dropoff_zone | avg_trip_time | num_trips 
-- ----------------+--------------+---------------+-----------
--  Yorkville East | Steinway     | 23:59:33      |         1
-- (1 row)
```
>ANSWER ✅
```
1
```

## Question 3

From the latest pickup time to 17 hours before, what are the top 3 busiest zones in terms of number of pickups?
For example if the latest pickup time is 2020-01-01 12:00:00,
then the query should return the top 3 busiest zones from 2020-01-01 11:00:00 to 2020-01-01 12:00:00.

HINT: You can use [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/)
to create a filter condition based on the latest pickup time.

NOTE: For this question `17 hours` was picked to ensure we have enough data to work with.

Options:
1. Clinton East, Upper East Side North, Penn Station
2. LaGuardia Airport, Lincoln Square East, JFK Airport
3. Midtown Center, Upper East Side South, Upper East Side North
4. LaGuardia Airport, Midtown Center, Upper East Side North

>SOLUTION
```sql
-- VERSION 1 (without MV)

SELECT
    tz1.Zone AS pickup_zone,
    COUNT(1) AS number_of_trips
FROM trip_data
JOIN taxi_zone tz1 ON trip_data.PULocationID = tz1.location_id
WHERE trip_data.tpep_pickup_datetime >= ((SELECT MAX(tpep_pickup_datetime) FROM trip_data) - INTERVAL '17' HOUR)
GROUP BY pickup_zone
ORDER BY number_of_trips DESC
LIMIT 3;

-- VERSION 2 (with MV and WHERE)

CREATE MATERIALIZED VIEW top_3_busiest_zones_pickups AS
    WITH latest_pickup_datetime AS (
        SELECT MAX(tpep_pickup_datetime) AS latest_pickup_datetime
        FROM trip_data
    )
    SELECT
        taxi_zone.Zone AS pickup_zone,
        COUNT(1) AS number_of_trips
    FROM
        latest_pickup_datetime,
        trip_data
    JOIN taxi_zone ON trip_data.PULocationID = taxi_zone.location_id
    WHERE tpep_pickup_datetime >= latest_pickup_datetime - INTERVAL '17' HOUR
    GROUP BY pickup_zone
    ORDER BY number_of_trips DESC
    LIMIT 3;

SELECT pickup_zone, number_of_trips FROM top_3_busiest_zones_pickups;

--      pickup_zone     | number_of_trips 
-- ---------------------+-----------------
--  LaGuardia Airport   |              19
--  Lincoln Square East |              17
--  JFK Airport         |              17
-- (3 rows)
```
>ANSWER ✅
```
LaGuardia Airport, Lincoln Square East, JFK Airport
```