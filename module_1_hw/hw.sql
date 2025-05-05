-- During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, respectively, happened:
--  Up to 1 mile
-- In between 1 (exclusive) and 3 miles (inclusive),
-- In between 3 (exclusive) and 7 miles (inclusive),
-- In between 7 (exclusive) and 10 miles (inclusive),
-- Over 10 miles
select
    case
        when trip_distance <= 1 then 'Up to 1 mile'
        when trip_distance > 1 and trip_distance <= 3 then '1~3 miles'
        when trip_distance > 3 and trip_distance <= 7 then '3~7 miles'
        when trip_distance > 7 and trip_distance <= 10 then '7~10 miles'
        else '10+ miles'
    end as segment,
    to_char(count(1), '999,999') as num_trips
from
    green_tripdata_2019
where
    lpep_pickup_datetime >= '2019-10-01'
    and lpep_pickup_datetime < '2019-11-01'
    and lpep_dropoff_datetime >= '2019-10-01'
    and lpep_dropoff_datetime < '2019-11-01'
group by
    segment

-- Which was the pick up day with the longest trip distance? Use the pick up time for your calculations.
-- Tip: For every day, we only care about one single trip with the longest distance.

SELECT lpep_pickup_datetime::date as pickup_date, MAX(trip_distance) as num_miles
FROM green_tripdata_2019
WHERE  lpep_pickup_datetime::date IN ('2019-10-11','2019-10-24','2019-10-26','2019-10-31')
GROUP BY lpep_pickup_datetime::date
ORDER BY num_miles DESC

-- Question 5. Three biggest pickup zones
-- Which were the top pickup locations with over 13,000 in total_amount (across all trips) for 2019-10-18?

-- Consider only lpep_pickup_datetime when filtering by date.

-- East Harlem North, East Harlem South, Morningside Heights
-- East Harlem North, Morningside Heights
-- Morningside Heights, Astoria Park, East Harlem South
-- Bedford, East Harlem North, Astoria Park


SELECT z."Zone", SUM(total_amount) AS total
FROM green_tripdata_2019 AS t 
INNER JOIN zones AS z
ON t."PULocationID" = z."LocationID"
WHERE t.lpep_pickup_datetime::date = '2019-10-18'
GROUP BY z."Zone" HAVING SUM(total_amount) > 13000
ORDER BY total DESC
LIMIT 3;

-- For the passengers picked up in October 2019 in the zone named "East Harlem North" 
-- which was the drop off zone that had the largest tip?


-- We need the name of the zone, not the ID.

-- Yorkville West
-- JFK Airport
-- East Harlem North
-- East Harlem South

SELECT z_p."Zone" AS Pickup, z_d."Zone" AS dropoff, MAX(tip_amount) AS total_tip
FROM green_tripdata_2019 AS t
INNER JOIN  zones AS z_p 
ON t."PULocationID" = z_p."LocationID"
INNER JOIN  zones AS z_d
ON t."DOLocationID" = z_d."LocationID"
WHERE z_p."Zone" = 'East Harlem North'
GROUP BY z_p."Zone", z_d."Zone"
ORDER BY total_tip DESC