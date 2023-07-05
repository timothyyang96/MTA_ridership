from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from sqlalchemy import text, create_engine, Table
import logging
import os
from sqlalchemy import MetaData

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# senstive information
mysql_conn_id = os.getenv('MYSQL_CONN_ID', 'mysql+mysqlconnector://root:abc@localhost:3306/MTA')
# Check if the environment variable is set
if mysql_conn_id is None:
    raise ValueError("The MYSQL_CONN_ID environment variable is not set.")


# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 6, 26),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'mta_data_pipeline_third',
    default_args=default_args,
    description='A data pipeline for MTA data',
    schedule='@daily',
)

# Define the ExternalTaskSensors
wait_for_first_pipeline = ExternalTaskSensor(
    task_id='wait_for_first_pipeline',
    external_dag_id='mta_data_pipeline_hourly_ridership',
    external_task_id='store_data',  # The task_id of the last task in the first pipeline
    dag=dag,
)

wait_for_second_pipeline = ExternalTaskSensor(
    task_id='wait_for_second_pipeline',
    external_dag_id='mta_data_pipeline_wifi',
    external_task_id='store_data',  # The task_id of the last task in the second pipeline
    dag=dag,
)

def check_and_create_table(engine, table_name):
    # Create a MetaData instance
    metadata = MetaData()

    # Check if the table exists
    table_exists = engine.dialect.has_table(engine, table_name)

    # If the table doesn't exist, create it
    if not table_exists:
        # Define the SQL query to create the table
        create_table_sql = text("""
        CREATE TABLE daily_station_ridership (
            station_complex_hourly_ridership varchar(100),
            transit_date date,
            ridership_per_day int,
            provider_available int,
            historical varchar(5),
            PRIMARY KEY (station_complex_hourly_ridership, transit_date)
        )
        """)

        # Execute the SQL query
        try:
            engine.execute(create_table_sql)
            logging.info(f"Table {table_name} created successfully.")
        except sqlalchemy.exc.SQLAlchemyError as e:
            logging.error(f"An error occurred while creating the table: {e}")
            raise


# Define the task for the third pipeline
def third_pipeline(mysql_conn_id, **kwargs):
    # Your code here...
    # Pull the values from XCom
    ridership_has_new_data = kwargs['ti'].xcom_pull(key='ridership_has_new_data', task_ids='process_data')
    wifi_table_updated = kwargs['ti'].xcom_pull(key='wifi_table_updated', task_ids='store_data')

    # Create a database engine using the MySQL connection
    engine = sqlalchemy.create_engine(mysql_conn_id)

    # Check and create table if it doesn't exist
    check_and_create_table(engine, 'daily_station_ridership')

    # Define the common part of the SQL query
    common_sql_query = text("""
        SELECT 
            hr.station_complex AS station_complex_hourly_ridership,
            DATE(hr.transit_timestamp) AS transit_date,
            SUM(hr.ridership) AS ridership_per_day,
            CASE 
                WHEN wl.at_t = 1 OR wl.sprint = 1 OR wl.t_mobile = 1 OR wl.verizon = 1 THEN 1 
                ELSE 0 
            END AS provider_available, 
            MAX(wl.historical) AS historical
        FROM 
            hourly_ridership AS hr
        LEFT JOIN 
            wifi_locations AS wl 
        ON
            hr.station_complex_wifi = wl.station_complex_str
    """)


    # Define the WHERE clause for the case when ridership_has_new_data is True
    where_clause = text("WHERE hr.transit_timestamp > :timestamp_str")

    # Define the GROUP BY clause
    group_by_clause = text("""
        GROUP BY 
            hr.station_complex,
            DATE(hr.transit_timestamp),
            provider_available
    """)

    if wifi_table_updated:
        # Define the SQL query for the case when wifi_table_updated is True
        sql_query = common_sql_query + group_by_clause
    elif ridership_has_new_data:
        # Pull the timestamp from XCom
        timestamp_str = kwargs['ti'].xcom_pull(key='timestamp_str', task_ids='check_new_data')

        # Define the SQL query for the case when ridership_has_new_data is True
        sql_query = common_sql_query + where_clause + group_by_clause
    else:
        # If neither wifi_table_updated nor ridership_has_new_data is True, skip the query execution
        logging.info("No new data. Skipping SQL query execution.")
        return

    # Execute the SQL query and get the result
    try:
        if ridership_has_new_data:
            result = engine.execute(sql_query, timestamp_str=timestamp_str)
        else:
            result = engine.execute(sql_query)
        logging.info("SQL query executed successfully.")
    except sqlalchemy.exc.OperationalError as e:
        logging.error(f"An operational error occurred while executing the SQL query: {e}")
    except sqlalchemy.exc.ProgrammingError as e:
        logging.error(f"A programming error occurred while executing the SQL query: {e}")
    except sqlalchemy.exc.SQLAlchemyError as e:
        logging.error(f"An error occurred while executing the SQL query: {e}")



query_station_ridership = PythonOperator(
    task_id='mta_daily_station_ridership',
    python_callable=third_pipeline,
    op_kwargs={
        'mysql_conn_id': mysql_conn_id,
    },
    dag=dag,
)


# Define the task dependencies
wait_for_first_pipeline >> query_station_ridership
wait_for_second_pipeline >> query_station_ridership
