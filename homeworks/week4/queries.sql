-- QUESTION 1

SELECT COUNT(1) FROM `beaming-figure-375814.production.fact_trips`;


-- QUESTION 5
SELECT DISTINCT EXTRACT(MONTH from pickup_datetime) AS MONTH, COUNT(1) FROM `beaming-figure-375814.dbt_grodriguescunha.fact_fhv_trips` GROUP BY MONTH ORDER BY MONTH