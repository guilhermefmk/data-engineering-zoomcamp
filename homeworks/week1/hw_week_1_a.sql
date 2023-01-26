-- QUESTION 1

    -- run command "docker build --help"

            --iidfile string 

-- QUESTION 1

    -- run command "docker run -it --entrypoint=bash python:3.9"
    -- in the container bash run "pip list"
    -- count the modules/packages

            -- 3

--QUESTION 3
SELECT
	COUNT(1)
FROM 
	green_taxi_trips 
WHERE
	DATE(lpep_dropoff_datetime) = '2019-01-15' AND
	DATE(lpep_pickup_datetime) = '2019-01-15';

--QUESTION 4
SELECT 
	lpep_pickup_datetime
FROM 
	green_taxi_trips g
JOIN(
	SELECT
		MAX(trip_distance) AS max_trip
	FROM 
		green_taxi_trips) m
ON
	g.trip_distance = m.max_trip
;

--QUESTION 5

SELECT
	DATE(lpep_pickup_datetime),
	passenger_count,
	COUNT(passenger_count)
FROM 
	green_taxi_trips
WHERE
	DATE(lpep_pickup_datetime) = '2019-01-01' AND
	passenger_count IN (2,3)
GROUP BY
	passenger_count,DATE(lpep_pickup_datetime);
	
--QUESTION 6

SELECT
	g."PULocationID",
	zpu."Zone" embarque,
	g."DOLocationID",
	zdo."Zone" desembarque,
	g.tip_amount
FROM 
	green_taxi_trips g
JOIN zones zpu
	ON	g."PULocationID" = zpu."LocationID"
JOIN zones zdo
	ON	g."DOLocationID" = zdo."LocationID"
WHERE zpu."Zone" LIKE 'Astoria'
ORDER BY g.tip_amount DESC
LIMIT 1;




	



	

