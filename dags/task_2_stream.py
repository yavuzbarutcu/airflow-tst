from typing import Dict

import pendulum
from airflow import DAG
from airflow.decorators import dag, task

from fastavro import writer, reader, parse_schema
import pandas as pd
import os


# Define the absolute path for the SQLite database file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Define the Avro schema
schema = {
    "type": "record",
    "name": "Transaction",
    "fields": [
        {"name": "key", "type": "string"},
        {"name": "value", "type": "int"},
    ],
}

@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    description="Reading data from a CSV file, processing the data and storing it",
)
def task_2_stream() -> DAG:
    """DAG that streams record from an artifical API and stores them in a DB"""

    @task
    def source_data(**op_kwargs) -> Dict[str, any]:
        """Read file based on DS from list of transactions files convert to binary format and store in tmp file"""

        try:

            print(f"op_kwargs: {op_kwargs}")
            date_str = op_kwargs['logical_date'].strftime('%Y-%m-%d')
            print(f"Processing data for date: {date_str}")

            TMP = os.path.join(BASE_DIR, "tmp/")
            if not os.path.exists(TMP):
                os.makedirs(os.path.dirname(TMP), exist_ok=True)

            csv_path = os.path.join(BASE_DIR, f"data/transactions_{date_str}.csv")
            avro_path = os.path.join(BASE_DIR, f"tmp/transactions_{date_str}.avro")

            if not os.path.exists(csv_path):
                print(f"CSV file not found: {csv_path}. Creating an empty CSV file.")
                df_empty = pd.DataFrame(columns=["key", "value"])
                df_empty.to_csv(csv_path, index=False, header=False)
            
            # Read the CSV file without headers
            df = pd.read_csv(csv_path, header=None, names=["key", "value"])
            records = df.to_dict(orient="records")
            
            with open(avro_path, "wb") as out:
                writer(out, parse_schema(schema), records)
                
            print(f"Avro Path: {avro_path}")
            return {"file_path": avro_path}
        except Exception as e:
            print(f"Error processing data: {e}")
            return None

    @task
    def process_data(table: Dict[str, any]) -> Dict[str, any]:
        """Read tmp binary file and apply a schema on it to validate data.
        Sum the values by key and then return the key with the 3rd largest result for the given date.

        return: 3rd largest result
        """
        print(f"table: {table}")
        avro_path = table['file_path']
        print(f"Avro Path: {avro_path}")

        with open(avro_path, "rb") as f:
            records = [record for record in reader(f)]

        df = pd.DataFrame(records)
        if df.empty:
            return {}
        grouped = df.groupby("key").sum().reset_index()
        sorted_df = grouped.sort_values(by="value", ascending=False)

        if len(sorted_df) < 3:
            raise ValueError("Not enough data to determine the 3rd largest result")

        third_largest = sorted_df.iloc[2]
        third_largest_key_value = {"key": third_largest["key"], "value": third_largest["value"]}
        print(third_largest_key_value)
        return third_largest_key_value

    process_data(source_data())


dag = task_2_stream()
