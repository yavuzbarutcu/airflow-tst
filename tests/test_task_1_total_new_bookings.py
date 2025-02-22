import os
import sys
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from airflow.models import DagBag, TaskInstance
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

# Add the root directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dags.task_1_total_new_bookings import task_1_total_new_bookings, get_sqlite_conn


# Define test directories
TESTS_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TESTS_DATA_DIR = os.path.join(TESTS_BASE_DIR, "data/")
TESTS_TMP_DIR = os.path.join(TESTS_BASE_DIR, "tmp/")


@pytest.fixture
def setup_csv_files():
    # Create temporary CSV files for testing
    passenger_data = {
        "id": [1, 2, 3],
        "date_registered": ["2021-01-01 00:00:00", "2020-12-31 23:59:59", "2021-01-02 00:00:00"],
        "country_code": ["US", None, "DE"]
    }
    booking_data = {
        "id": [1, 2, 3],
        "id_passenger": [1, 2, 3]
    }

    passenger_csv_path = os.path.join(TESTS_DATA_DIR, "passenger.csv")
    booking_csv_path = os.path.join(TESTS_DATA_DIR, "booking.csv")

    os.makedirs(TESTS_DATA_DIR, exist_ok=True)
    pd.DataFrame(passenger_data).to_csv(passenger_csv_path, index=False)
    pd.DataFrame(booking_data).to_csv(booking_csv_path, index=False)

    yield passenger_csv_path, booking_csv_path

    # Clean up
    os.remove(passenger_csv_path)
    os.remove(booking_csv_path)

@patch("airflow.providers.sqlite.hooks.sqlite.SqliteHook.get_conn")
def test_ingest_csv_as_table(mock_sqlite_conn, setup_csv_files):
    passenger_csv_path, booking_csv_path = setup_csv_files

    # Mock SQLite connection
    mock_conn = MagicMock()
    mock_sqlite_conn.return_value = mock_conn

    # Mock pandas.to_sql to avoid actual database operations
    with patch("pandas.DataFrame.to_sql") as mock_to_sql:
        # Get the task
        dag = task_1_total_new_bookings()
        task = dag.get_task("ingest_csv_as_table")

        # Test ingesting passenger CSV
        context = {
            "params": {
                "csv_path": passenger_csv_path,
                "table_name": "passenger"
            }
        }
        task.execute(context=context)

        # Verify that to_sql was called
        mock_to_sql.assert_called_once_with("passenger", mock_conn, if_exists="replace", index=False)



@patch("airflow.providers.sqlite.hooks.sqlite.SqliteHook.get_conn")
def test_calculate_total_new_bookings_by_country(mock_sqlite_conn):
    # Mock SQLite connection and query results
    mock_conn = MagicMock()
    mock_sqlite_conn.return_value = mock_conn

    # Mock pandas.read_sql_query and to_sql to avoid actual database operations
    with patch("pandas.read_sql_query") as mock_read_sql_query, patch("pandas.DataFrame.to_sql") as mock_to_sql:
        # Mock query result
        mock_query_result = pd.DataFrame({
            "country": ["US", "OTHER", "DE"],
            "total_bookings": [1, 1, 1]
        })
        mock_read_sql_query.return_value = mock_query_result

        # Get the task
        dag = task_1_total_new_bookings()
        task = dag.get_task("calculate_total_new_bookings_by_country")

        # Mock TaskInstance for XCom
        ti = MagicMock(spec=TaskInstance)
        context = {
            "params": {},
            "ti": ti
        }

        # Execute the task
        result = task.execute(context=context)

        # Verify the result
        assert result == {"table_name": "total_new_booking", "row_count": 3}
        mock_read_sql_query.assert_called_once()
        mock_to_sql.assert_called_once_with("total_new_booking", mock_conn, if_exists="replace", index=False)


@patch("airflow.providers.sqlite.hooks.sqlite.SqliteHook.get_conn")
def test_print_data(mock_sqlite_conn):
    # Mock SQLite connection and query results
    mock_conn = MagicMock()
    mock_sqlite_conn.return_value = mock_conn

    # Mock pandas.read_sql_query to avoid actual database operations
    with patch("pandas.read_sql_query") as mock_read_sql_query:
        # Mock query result
        mock_query_result = pd.DataFrame({
            "country": ["US", "OTHER", "DE"],
            "total_bookings": [1, 1, 1]
        })
        mock_read_sql_query.return_value = mock_query_result

        # Get the task
        dag = task_1_total_new_bookings()
        task = dag.get_task("print_data")

        # Access the original function using `python_callable`
        print_data_function = task.python_callable

        # Call the function directly with the required input
        print_data_function({"table_name": "total_new_booking"})

        # Verify that read_sql_query was called with the correct table name
        mock_read_sql_query.assert_called_once_with("SELECT * FROM total_new_booking ORDER BY 1", mock_conn)