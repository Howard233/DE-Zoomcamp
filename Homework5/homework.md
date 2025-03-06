# Module 5 Homework

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the Yellow 2024-10 data from the official website: 

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet
```


## Question 1: Install Spark and PySpark

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?

> [!NOTE]
> To install PySpark follow this [guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-batch/setup/pyspark.md)

### Ans
Created a local spark session and the version is `3.5.4`.

## Question 2: Yellow October 2024

Read the October 2024 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- 6MB
- **`25MB`**
- 75MB
- 100MB

### Ans
I got 22.4MB.

## Question 3: Count records 

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.

- 85,567
- 105,567
- **`125,567`**
- 145,567

### Ans
I got 128,893, which is closet to 125,567.

## Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

- 122
- 142
- **`162`**
- 182

### Ans
I did it with spark sql and got this result: 162.612
```
df_longest_trip = spark.sql(
    """
    SELECT 
        (unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 3600 AS hour_difference
    FROM
        trips_data
    ORDER BY 1 DESC
    LIMIT 5
    """
)

df_longest_trip.show()
```


## Question 5: User Interface

Spark’s User Interface which shows the application's dashboard runs on which local port?

- 80
- 443
- 4040
- 8080

### Ans
It runs on port 4040.

## Question 6: Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Using the zone lookup data and the Yellow October 2024 data, what is the name of the LEAST frequent pickup location Zone?

- Governor's Island/Ellis Island/Liberty Island
- Arden Heights
- Rikers Island
- Jamaica Bay

### Ans
I used spark sql and got this result: Governor's Island/Ellis Island/Liberty Island.
```
df_result = spark.sql(
    """
    SELECT 
        trips_data.PULocationID AS pickup_locationID,
        zones.Zone AS zone_name, 
        COUNT(1) as frequency
    FROM
        trips_data
    INNER JOIN zones on trips_data.PULocationID = zones.LocationID
    GROUP BY 1, 2
    ORDER BY 3 ASC
    LIMIT 5
    """
)

df_result.show(truncate=False)
```


## Submitting the solutions

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw5
- Deadline: See the website
