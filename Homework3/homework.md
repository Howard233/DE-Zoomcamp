## Module 3 Homework

ATTENTION: At the end of the submission form, you will be required to include a link to your GitHub repository or other public code-hosting site. 
This repository should contain your code for solving the homework. If your solution includes code that is not in file format (such as SQL queries or 
shell commands), please include these directly in the README file of your repository.

<b><u>Important Note:</b></u> <p> For this homework we will be using the Yellow Taxi Trip Records for **January 2024 - June 2024 NOT the entire year of data** 
Parquet Files from the New York
City Taxi Data found here: </br> https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page </br>
If you are using orchestration such as Kestra, Mage, Airflow or Prefect etc. do not load the data into Big Query using the orchestrator.</br> 
Stop with loading the files into a bucket. </br></br>

**Load Script:** You can manually download the parquet files and upload them to your GCS Bucket or you can use the linked script [here](./load_yellow_taxi_data.py):<br>
You will simply need to generate a Service Account with GCS Admin Priveleges or be authenticated with the Google SDK and update the bucket name in the script to the name of your bucket<br>
Nothing is fool proof so make sure that all 6 files show in your GCS Bucket before begining.</br><br>

<u>NOTE:</u> You will need to use the PARQUET option files when creating an External Table</br>

<b>BIG QUERY SETUP:</b></br>
Create an external table using the Yellow Taxi Trip Records. </br>
Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records (do not partition or cluster this table). </br>
</p>

#### Create the external table in BQ
```
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-447701.dtc_de_course_447701_hw3_dataset.external_yellow_tripdata_2024`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc-de-course-447701-hw3-bucket/yellow_tripdata_2024-*.parquet']
);
```
#### Create the regular/materialized table in BQ
```
CREATE OR REPLACE TABLE `dtc-de-course-447701.dtc_de_course_447701_hw3_dataset.yellow_tripdata_regular_2024` AS
SELECT * FROM dtc-de-course-447701.dtc_de_course_447701_hw3_dataset.external_yellow_tripdata_2024;
```

## Question 1:
Question 1: What is count of records for the 2024 Yellow Taxi Data?
- 65,623
- 840,402
- **`20,332,093`**
- 85,431,289

### Ans
Query used:
```
select count(*) from `dtc-de-course-447701.dtc_de_course_447701_hw3_dataset.external_yellow_tripdata_2024`;
```

This returned 20,332,093 rows

## Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 
What is the **estimated amount** of data that will be read when this query is executed on the External Table and the Table?

- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- **`0 MB for the External Table and 155.12 MB for the Materialized Table`**
- 2.14 GB for the External Table and 0MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table

### Ans
Query for the External Table. BQ gives 0MB as the estimate.
```
select count(distinct PULocationID) from `dtc-de-course-447701.dtc_de_course_447701_hw3_dataset.external_yellow_tripdata_2024`;
```
Query for the Materialized Table. BQ gives 155.12MB as the estimate.
```
select count(distinct PULocationID) from `dtc-de-course-447701.dtc_de_course_447701_hw3_dataset.yellow_tripdata_regular_2024`;
```

## Question 3:
Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?
- **`BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires
reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.`**
- BigQuery duplicates data across multiple storage partitions, so selecting two columns instead of one requires scanning the table twice, 
doubling the estimated bytes processed.
- BigQuery automatically caches the first queried column, so adding a second column increases processing time but does not affect the estimated bytes scanned.
- When selecting multiple columns, BigQuery performs an implicit join operation between them, increasing the estimated bytes processed

### Ans
First query. Estimated amount of data is 155.12MB.
```
select PULocationID from `dtc-de-course-447701.dtc_de_course_447701_hw3_dataset.yellow_tripdata_regular_2024`;
```
Second query. Estimated amount of data is 310.24MB.
```
select PULocationID, DOLocationID from `dtc-de-course-447701.dtc_de_course_447701_hw3_dataset.yellow_tripdata_regular_2024`;
```
BigQuery is a columnar database. Therefore, the first choice is correct.

## Question 4:
How many records have a fare_amount of 0?
- 128,210
- 546,578
- 20,188,016
- **`8,333`**

### Ans
Query used. This returned 8333 rows.
```
select count(*) from `dtc-de-course-447701.dtc_de_course_447701_hw3_dataset.external_yellow_tripdata_2024`
where fare_amount = 0;
```

## Question 5:
What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)
- **`Partition by tpep_dropoff_datetime and Cluster on VendorID`**
- Cluster on by tpep_dropoff_datetime and Cluster on VendorID
- Cluster on tpep_dropoff_datetime Partition by VendorID
- Partition by tpep_dropoff_datetime and Partition by VendorID

### Ans
Since we always filter based on `tpep_dropoff_datetime`, it is better to *partition* the table based on this column. Since we want to order the results by `VendorID`, we use *cluster* on `VendorID`.

Creation query:
```
CREATE OR REPLACE TABLE `dtc-de-course-447701.dtc_de_course_447701_hw3_dataset.yellow_tripdata_partitoned_clustered_2024`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `dtc-de-course-447701.dtc_de_course_447701_hw3_dataset.external_yellow_tripdata_2024`;
```

## Question 6:
Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime
2024-03-01 and 2024-03-15 (inclusive)</br>

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values? </br>

Choose the answer which most closely matches.</br> 

- 12.47 MB for non-partitioned table and 326.42 MB for the partitioned table
- **`310.24 MB for non-partitioned table and 26.84 MB for the partitioned table`**
- 5.87 MB for non-partitioned table and 0 MB for the partitioned table
- 310.31 MB for non-partitioned table and 285.64 MB for the partitioned table

### Ans
Query used for the non-partitioned table. This returned 310.24MB.
```
select distinct(VendorID) 
from dtc-de-course-447701.dtc_de_course_447701_hw3_dataset.yellow_tripdata_regular_2024
where tpep_dropoff_datetime >= '2024-03-01' and tpep_dropoff_datetime <= '2024-03-15';
```
Query used for the partitioned table. This returned 26.84MB.
```
select distinct(VendorID) 
from dtc-de-course-447701.dtc_de_course_447701_hw3_dataset.yellow_tripdata_partitoned_clustered_2024
where tpep_dropoff_datetime >= '2024-03-01' and tpep_dropoff_datetime <= '2024-03-15';
```

## Question 7: 
Where is the data stored in the External Table you created?

- Big Query
- Container Registry
- **`GCP Bucket`**
- Big Table

### Ans
The data in the External table is stored in the GCP Bucket.

## Question 8:
It is best practice in Big Query to always cluster your data:
- True
- **`False`**

### Ans
No. It depends. One case we can consider clustering is that your queries commonly use filters or aggregation against multiple particular columns.

## (Bonus: Not worth points) Question 9:
No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

### Ans
Query used. This returned 0 bytes. The reason is that count(*) is retrieving the row count from the metadata of the table, which requires no data scanning and processing.
```
select count(*) from dtc-de-course-447701.dtc_de_course_447701_hw3_dataset.yellow_tripdata_regular_2024;
```

## Submitting the solutions

Form for submitting: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw3
