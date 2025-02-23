# Data Engineering Take Home Test
This take home test focuses on two Airflow DAGs that consume, transform and process data.

## Pre-preparation
### Familiarize yourself with Apache Airflow
In case you have not worked with Airflow in the past it does make sense if you briefly read the introduction of Apache
Airflow. Don't worry though, you do not need to be an Airflow pro to finish the session. A basic understanding of the Airflow
principles is sufficient. You should be know about the following concepts: Dags, Tasks, Operators and Hooks.
This tutorial is a good initial read to understand airflow: https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html

### Setup your local environment
Please set up a working local environment and make sure you can execute one of the example DAGs provided with the Airflow installation.
Have an IDE setup that you are comfortable working with. We recommend running the code in a new virtual environment.
Please make sure to use the standalone installation of airflow to avoid side effects.


## Tasks

### Task 1: Process records from CSV file into SQlite
The first DAG will load the 2 provided datasets `dags/data/booking.csv, dags/data/passenger.csv` into sqlite tables, and process it as following,
1. Store raw csv files into sqlite as tables.
2. Calculate the total number of bookings for new passengers based on their country of origin and save the results into sqlite.
    * New passengers are defined as who registered >= `2021-01-01 00:00:00`.
    * If `country_code` is empty, passengers should be categorized as `OTHER`.
3. Read table data from sqlite based on input dict and print to console.
4. Write unit tests to test your dags and calculations.

`! Note that you must use airflow SqliteOperator or SqliteHook for task 1 implementation instead of sqlite3.`

### Task 2: Streaming records daily from file, aggregate and filter
This time the DAG should stream records from csv files. The files are split by day and you should convert them into `avro` format.  
In the end we would like to filter one value per day.
1. Read file based on date of airflow task instance from list of transactions files `dags/data/transactions_*.csv`.   
   Convert to a binary format and store in temporary file.
2. Read the temporary binary file, sum the values by key and then return the key with the 3rd highest result for the given date.
3. Write unit tests to test your dags and check if your validation works on known cases of wrong data.


### Local testing
To start things of try to run the unit tests.
```bash
make test
```

## Solutions

### Initialize .venv
1. First we need to initialize virtual environment
```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Install and start standalone airflow webserver. Depending on the python version, dependencies will be installed. Tested with python version 3.11
```bash
make init
```

### Task 1: Process records from CSV file into SQlite
The first task reads csv files(booking.csv passanger.csv) files and loads them into  SqliteHook is being used to implement first task
```bash
make run_task_1
```

### Task 2: Streaming records daily from file, aggregate and filter
The second task has 2 steps.
1. Read csv files for the given date and write into avro file
2. Read avro file, aggregate rows by key and return 3rd highest result

There are 3 assumptions to consider:
    1. If there is no source file for the given date, fail the job. The job below therefore will fail
```bash
make run_task_2
```

    2. If there is no enough data to calculate 3rd highest value, fail the job. The job below therefore will fail too
```bash
make run_task_3
```

    3. If csv file exists and enough data in it, job will run successfully. Jobs below will run and return the highest 3rd result for the dates 2022-04-27 and 2022-04-27 respectively.
```bash
make run_task_4
make run_task_5
```

## Tests
All tests can be run with the script below
```bash
make test
```


### Test: Task 1
There are 3 tests written for Task 1:

1. **test_ingest_csv_as_table**: This test verifies the functionality of ingesting a CSV file and storing it as a table.

2. **test_calculate_total_new_bookings_by_country**: This test checks the calculation of the total number of new bookings grouped by country.

3. **test_print_data**: This test ensures that the data is printed correctly.


### Test: Task 2
There are 4 test function for the task 2

1. **test_source_data**:
Purpose: This test function is designed to verify that the source data is correctly loaded and meets the expected format and content.
Details: It will likely check if the data source file exists, if it can be read without errors, and if the data within it matches the expected structure (e.g., correct columns, data types, etc.).

2. **test_source_data_missing_source_cvs**:
Purpose: This test function checks the behavior of your program when the source CSV file is missing.
Details: It will simulate the scenario where the source data file is not present and ensure that your program handles this situation gracefully, possibly by raising an appropriate error or providing a meaningful message to the user.

3. **test_process_data**:
Purpose: This test function verifies that the data processing logic works correctly.
Details: It will test the core functionality of your data processing code, ensuring that the input data is transformed as expected. This might include checking calculations, data transformations, aggregations, or any other processing steps your program performs.

4. **test_process_data_not_enough_data**:
Purpose: This test function ensures that your program can handle cases where there is insufficient data for processing.
Details: It will simulate a scenario where the input data is incomplete or too sparse to be processed correctly. The test will check if your program can detect this condition and respond appropriately, such as by raising an error or skipping the processing step.