# Assignment 2

So far in the course, we processed data for the year 2019 and 2020. Your task is to extend the existing flows to include data for the year 2021.

homework datasets

As a hint, Kestra makes that process really easy:

You can leverage the backfill functionality in the scheduled flow to backfill the data for the year 2021. Just make sure to select the time period for which data exists i.e. from 2021-01-01 to 2021-07-31. Also, make sure to do the same for both yellow and green taxi data (select the right service in the taxi input).

Alternatively, run the flow manually for each of the seven months of 2021 for both yellow and green taxi data. Challenge for you: find out how to loop over the combination of Year-Month and taxi-type using ForEach task which triggers the flow for each combination using a Subflow task.

## Quiz Questions

Complete the Quiz shown below. It’s a set of 6 multiple-choice questions to test your understanding of workflow orchestration, Kestra and ETL pipelines for data lakes and warehouses.

1. Within the execution for Yellow Taxi data for the year 2020 and month 12: what is the uncompressed file size (i.e. the output file yellow_tripdata_2020-12.csv of the extract task)?
    * 128.3 MiB

2. What is the rendered value of the variable file when the inputs taxi is set to green, year is set to 2020, and month is set to 04 during execution?
    * green_tripdata_2020-04.csv


3. How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?

    * 24,648,499

4. How many rows are there for the Green Taxi data for all CSV files in the year 2020?
    * 1,734,051


5. How many rows are there for the Yellow Taxi data for the March 2021 CSV file?

    * 1,925,152

6. How would you configure the timezone to New York in a Schedule trigger?
    * Add a timezone property set to America/New_York in the Schedule trigger configuration
