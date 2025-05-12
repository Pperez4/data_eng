
# HOMEWORK 3

For this homework we will be using the Yellow Taxi Trip Records for January 2024 - June 2024 NOT the entire year of data Parquet Files from the New York City Taxi Data found here:

<https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>

BIG QUERY SETUP:
Create an external table using the Yellow Taxi Trip Records.

Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records (do not partition or cluster this table)

WE FIRST LOADED THE DATA INTO GCP USING A PYTHON SCRIPT 

CREATED THE EXTERNAL TABLE 

```sql
-- RUN THIS TO CREATE SCHEMA 
CREATE SCHEMA IF NOT EXISTS `taxi-data-load.taxi_data_load_2024`;

-- OKAY THIS IS OUR EXTERNAL DATA 
-- WE BEGIN WITH VERY BASIC 'CREATE REPLACE' BUT INDICATE THIS IS  --> EXTERNAL
-- GIVE IT A NAME UNDER YOUR THE CREATED SCHEMA ABOVE 
-- OUR FORMAT HERE IS 'parquet' SO MAKE SURE YOU SPECIFY CORRENTLY
-- uri --> YOU CAN GET IT DIRECTLY FROM THE 'Object details' FROM THE BUCKET
CREATE OR REPLACE EXTERNAL TABLE `taxi-data-load.taxi_data_load_2024.taxi_data_2024`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://hw-3-bucket-2024-data/yellow_tripdata_2024-01.parquet',
          'gs://hw-3-bucket-2024-data/yellow_tripdata_2024-02.parquet',
          'gs://hw-3-bucket-2024-data/yellow_tripdata_2024-03.parquet',
          'gs://hw-3-bucket-2024-data/yellow_tripdata_2024-04.parquet',
          'gs://hw-3-bucket-2024-data/yellow_tripdata_2024-05.parquet',
          'gs://hw-3-bucket-2024-data/yellow_tripdata_2024-06.parquet']

)

```

Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records (do not partition or cluster this table).

> THIS IS A TABLE JUST USING THE DATA FROM THE EXTERNAL TABLE
> 

```sql
-- Materialized table 
-- format(project_id.database.table name)
CREATE OR REPLACE TABLE `taxi-data-load.taxi_data_load_2024.materialized` AS
SELECT * FROM `taxi-data-load.taxi_data_load_2024.taxi_data_2024`
```

### QUESTION 1

What is count of records for the 2024 Yellow Taxi Data?

- 65,623
- 840,402
- **20,332,093**
- 85,431,289

```sql
-- QUESTION 1 COUNT RECORDS 
SELECT COUNT(*) AS num_records_2024
FROM `taxi-data-load.taxi_data_load_2024.materialized`

```

### QUESTION 2

Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.

What is the **estimated amount** of data that will be read when this query is executed on the External Table and the Table?

- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- **0 MB for the External Table and 155.12 MB for the Materialized Table**
- 2.14 GB for the External Table and 0MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table

```sql
-- QUESTION 2 DISTINCT LOCATIONS 
SELECT COUNT(DISTINCT(PULocationID))
FROM `taxi-data-load.taxi_data_load_2024.materialized`

SELECT COUNT(DISTINCT(PULocationID))
FROM `taxi-data-load.taxi_data_load_2024.taxi_data_2024`
```

### QUESTION 3

Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?

- **BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.**
- BigQuery duplicates data across multiple storage partitions, so selecting two columns instead of one requires scanning the table twice, doubling the estimated bytes processed.
- BigQuery automatically caches the first queried column, so adding a second column increases processing time but does not affect the estimated bytes scanned.
- When selecting multiple columns, BigQuery performs an implicit join operation between them, increasing the estimated bytes processed

```sql
SELECT PULocationID 
FROM `taxi-data-load.taxi_data_load_2024.materialized`
```

![image.png](attachment:7857cab1-b940-4f68-b398-819004e0ce43:image.png)

```sql

SELECT PULocationID, DOLocationID
FROM `taxi-data-load.taxi_data_load_2024.materialized`
```

![image.png](attachment:5c30754f-9c8d-47aa-aec3-c47955660b95:5384834b-280d-4317-b56b-deec3cf5887f.png)

### QUESTION 4

How many records have a fare_amount of 0?

- 128,210
- 546,578
- 20,188,016
- **8,333**

```sql
SELECT COUNT(*) AS fare_0
FROM `taxi-data-load.taxi_data_load_2024.materialized`
WHERE fare_amount = 0

```

![image.png](attachment:044f09f4-38fd-4a9e-8e56-23c5208004bf:image.png)

### QUESTION 5

What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

- **Partition by tpep_dropoff_datetime and Cluster on VendorID**
- Cluster on by tpep_dropoff_datetime and Cluster on VendorID
- Cluster on tpep_dropoff_datetime Partition by VendorID
- Partition by tpep_dropoff_datetime and Partition by VendorID

```sql
-- Creating a partition and cluster table
CREATE OR REPLACE TABLE `taxi-data-load.taxi_data_load_2024.CLUSTERED`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `taxi-data-load.taxi_data_load_2024.taxi_data_2024`
```

### **Question 6:**

Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?

Choose the answer which most closely matches.

- 12.47 MB for non-partitioned table and 326.42 MB for the partitioned table
- **310.24 MB for non-partitioned table and 26.84 MB for the partitioned table**
- 5.87 MB for non-partitioned table and 0 MB for the partitioned table
- 310.31 MB for non-partitioned table and 285.64 MB for the partitioned table

```sql

-- Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)
SELECT DISTINCT(VendorID) 
FROM `taxi-data-load.taxi_data_load_2024.CLUSTERED`
WHERE  tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'

SELECT DISTINCT(VendorID) 
FROM `taxi-data-load.taxi_data_load_2024.taxi_data_2024`
WHERE  tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'
```

### QUESTION 7

Where is the data stored in the External Table you created?

- Big Query
- Container Registry
- **GCP Bucket**
- Big Table

### **Question 8:**

It is best practice in Big Query to always cluster your data:

- **True**
- False