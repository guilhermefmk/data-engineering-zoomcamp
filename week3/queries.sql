CREATE OR REPLACE EXTERNAL TABLE `beaming-figure-375814.dezoomcamp.bq_fhv`
OPTIONS (
  format = 'parquet',
  uris = ['gs://zoomcamp-prefect-week2/data_flow/fhv/fhv_tripdata_*.parquet']
);


-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE beaming-figure-375814.dezoomcamp.fhv_from_external AS
SELECT * FROM beaming-figure-375814.dezoomcamp.external_fhv;

-- Count distinct Affiliated_base_number
SELECT Affiliated_base_number,COUNT(Affiliated_base_number) FROM beaming-figure-375814.dezoomcamp.fhv_from_external GROUP BY Affiliated_base_number;

-- Count distinct Affiliated_base_number external
SELECT Affiliated_base_number,COUNT(Affiliated_base_number) FROM beaming-figure-375814.dezoomcamp.external_fhv GROUP BY Affiliated_base_number;

SELECT DOlocationID,COUNT(DOlocationID) FROM beaming-figure-375814.dezoomcamp.bq_fhv WHERE DOlocationID IS NULL GROUP BY DOlocationID;

SELECT COUNT(*) FROM beaming-figure-375814.dezoomcamp.external_fhv;

SELECT DISTINCT Affiliated_base_number FROM beaming-figure-375814.dezoomcamp.external_fhv WHERE pickup_datetime BETWEEN "2019-03-01" AND "2019-03-31";

