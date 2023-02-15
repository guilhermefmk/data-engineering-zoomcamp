1 . What is the count for fhv vehicle records for year 2019?

        -- 43,244,696


2 . Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.
        What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

        -- 0 MB for the External Table and 317.94MB for the BQ Table

3. How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?

        -- 717,748

4. What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?

        -- Partition by pickup_datetime Cluster on affiliated_base_number

5. Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive).
Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.

        -- 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table

6. Where is the data stored in the External Table you created?

        -- Big Query

7. It is best practice in Big Query to always cluster your data?

        -- False