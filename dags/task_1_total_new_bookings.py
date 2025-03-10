from typing import Dict

import pendulum
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

import pandas as pd
import os

# Define the absolute path for the SQLite database file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def get_sqlite_conn():
    """
    Get SQLite connection
    """
    return SqliteHook(sqlite_conn_id='sqlite_default').get_conn()


@dag(
    schedule="@daily",
    # start_date=pendulum.today('UTC').add(days=-1),
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    description="Reading data from a CSV file, processing the data and storing it",
)
def task_1_total_new_bookings() -> DAG:
    """
    DAG processing data from csv files. Storing, processing, and reading the contents using SQlite
    """

    @task
    def ingest_csv_as_table(csv_path: str, table_name: str) -> None:
        """
        Read csv dataset and inserts all rows into a table.

        """
        print(f"Ingesting CSV: {csv_path} into table: {table_name}")

        # Check if the file exists
        if os.path.exists(csv_path):
            conn = get_sqlite_conn()
            df = pd.read_csv(csv_path)
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            conn.close()
        else:
            print(f"CSV file not found: {csv_path}")
            raise ValueError(f"CSV file not found: {csv_path}")
    @task
    def calculate_total_new_bookings_by_country() -> Dict[str, any]:
        """
        Calculate the total number of bookings for new passengers based on their country of origin.
            * New passengers are defined as who registered >= `2021-01-01 00:00:00`.
            * If `country_code` is empty, passengers should be categorized as `OTHER`.

        Save the result to total_new_booking table

        :return: Dict of saved table metadata
        """

        conn = get_sqlite_conn()
        # Query to calculate total new bookings by country. Upper case country code and default to 'OTHER'
        query = """
        SELECT 
            UPPER(COALESCE(p.country_code, 'OTHER')) as country,
            COUNT(b.id) as total_bookings
        FROM 
            passenger p
        JOIN 
            booking b
        ON 
            p.id = b.id_passenger
        WHERE 
            p.date_registered >= '2021-01-01 00:00:00'
        GROUP BY 
            country
        """
        result_df = pd.read_sql_query(query, conn)
        result_df.to_sql('total_new_booking', conn, if_exists='replace', index=False)
        conn.close()
        return {"table_name": "total_new_booking", "row_count": len(result_df)}

    @task
    def print_data(table: Dict[str, any]) -> None:
        """
        Read table data from sqlite based on input dict and print to console.

        :param table: Dict of table metadata
        :returns: None
        """
        conn = get_sqlite_conn()
        query = f"SELECT * FROM {table['table_name']} ORDER BY 1"
        df = pd.read_sql_query(query, conn)
        conn.close()
        print(df)

    task_ingest_passenger = ingest_csv_as_table(os.path.join(BASE_DIR, "data/passenger.csv"), "passenger")
    task_ingest_booking = ingest_csv_as_table(os.path.join(BASE_DIR, "data/booking.csv"), "booking")
    task_calculate = calculate_total_new_bookings_by_country()
    task_print = print_data(task_calculate)

    [task_ingest_passenger, task_ingest_booking] >> task_calculate >> task_print


dag = task_1_total_new_bookings()
