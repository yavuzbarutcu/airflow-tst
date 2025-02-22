import os
import sys
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from airflow.models import DagBag, TaskInstance
from fastavro import writer, parse_schema

from datetime import datetime

# Add the root directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dags.task_2_stream import task_2_stream, schema


date_str = "2023-01-01"
TESTS_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TESTS_DATA_DIR = os.path.join(TESTS_BASE_DIR, "data/")
TESTS_TMP_DIR = os.path.join(TESTS_BASE_DIR, "tmp/")


@pytest.fixture
def setup_csv_file():
    print("TEST_TMP_DIR:", TESTS_TMP_DIR);
    csv_path = f"{TESTS_DATA_DIR}transactions_{date_str}.csv"
    data = {
        "key": ["a", "b", "c", "d"],
        "value": [10, 20, 30, 40]
    }
    df = pd.DataFrame(data)
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    df.to_csv(csv_path, index=False, header=False)
    yield csv_path
    os.remove(csv_path)

def test_source_data(setup_csv_file):
    csv_path = setup_csv_file
    avro_path = f"{TESTS_TMP_DIR}transactions_{date_str}.avro"

    with patch("dags.task_2_stream.BASE_DIR", TESTS_BASE_DIR):
        task = task_2_stream().get_task("source_data")
        ti = MagicMock(spec=TaskInstance)
        context = {
            'logical_date': pd.Timestamp(date_str),
            'ti': ti
        }
        result = task.execute(context=context)
        print("RESULT:", result)
        assert os.path.exists(result['file_path'])
        os.remove(avro_path)


def test_process_data(setup_csv_file):
    csv_path = setup_csv_file
    avro_path = f"{TESTS_TMP_DIR}transactions_{date_str}.avro"
    
    # Create a temporary Avro file
    df = pd.read_csv(csv_path, header=None, names=["key", "value"])
    records = df.to_dict(orient="records")
    with open(avro_path, "wb") as out:
        writer(out, parse_schema(schema), records)
    
    task = task_2_stream().get_task("process_data")
    
    # Access the original function using `python_callable`
    process_data_function = task.python_callable
    
    # Execute the function directly
    result = process_data_function({"file_path": avro_path})
    print("RESULT:", result)
    
    assert result == {"key": "b", "value": 20}
    os.remove(avro_path)


def test_process_data_not_enough_data(setup_csv_file):
    csv_path = setup_csv_file
    avro_path = f"{TESTS_TMP_DIR}transactions_{date_str}.avro"
    
    # Create a temporary Avro file with insufficient data
    df = pd.read_csv(csv_path, header=None, names=["key", "value"])
    records = df.head(2).to_dict(orient="records")  # Only take the first 2 records
    with open(avro_path, "wb") as out:
        writer(out, parse_schema(schema), records)
    
    task = task_2_stream().get_task("process_data")
    
    # Access the original function using `python_callable`
    process_data_function = task.python_callable
    
    with pytest.raises(ValueError, match="Not enough data to determine the 3rd largest result"):
        process_data_function({"file_path": avro_path})
    
    os.remove(avro_path)