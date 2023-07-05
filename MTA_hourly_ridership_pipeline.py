from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import XCom
import requests
import pandas as pd
import sqlalchemy
from datetime import datetime, timedelta
from sqlalchemy import create_engine, exc, text
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
import logging
import os
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# senstive information
mysql_conn_id = os.getenv('MYSQL_CONN_ID', 'mysql+mysqlconnector://root:abc@localhost:3306/')
table_name = os.getenv('TABLE_NAME', 'hourly_ridership')
url = os.getenv('URL', 'https://data.ny.gov/resource/wujg-7c2s.json?$query=SELECT%0A%20%20%60transit_timestamp%60%2C%0A%20%20%60station_complex_id%60%2C%0A%20%20%60station_complex%60%2C%0A%20%20%60borough%60%2C%0A%20%20%60routes%60%2C%0A%20%20%60payment_method%60%2C%0A%20%20%60ridership%60%2C%0A%20%20%60transfers%60%2C%0A%20%20%60latitude%60%2C%0A%20%20%60longitude%60%2C%0A%20%20%60georeference%60%0A')
db_name = os.getenv('DB_NAME', 'MTA')

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 6, 26),
}

# Define the DAG
dag = DAG(
    'mta_data_pipeline_hourly_ridership',
    default_args=default_args,
    description='A data pipeline for MTA data',
    schedule='@daily',
)

# Add this task at the beginning of your pipeline
wait_for_wifi_locations = ExternalTaskSensor(
    task_id='wait_for_wifi_locations',
    external_dag_id='mta_data_pipeline_wifi',  # replace with your wifi_locations DAG ID
    external_task_id='store_data',  # replace with the task ID of the wifi_locations task that generates the station_complex_str column
    mode='reschedule',  # the task instance is rescheduled at the next schedule interval
    timeout=600,  # timeout in seconds, adjust as needed
    dag=dag,
)


def check_database(mysql_conn_id, db_name):
    # Create a database engine using the MySQL connection
    engine = create_engine(mysql_conn_id)
    logging.info("create engine succeeded!")
    # Check if the database exists
    check_if_exists = text("SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = {}".format(db_name))

    # Obtain a connection from the engine
    with engine.connect() as connection:
        try:
            result = connection.execute(check_if_exists)
            num_of_databases = result.scalar()
            logging.info("check_if_exists query succeeded!")
        except sqlalchemy.exc.SQLAlchemyError as e:
            logging.error(f"An error occurred while interacting with the database: {e}")

        # If the database doesn't exist, create it
        if num_of_databases == 0:
            # Define the SQL statement to create the database
            create_database_sql = text("CREATE DATABASE MTA")
            # Execute the SQL statement
            try:
                connection.execute(create_database_sql)
                logging.info("CREATE DATABASE MTA")
            except sqlalchemy.exc.SQLAlchemyError as e:
                logging.error(f"An error occurred while interacting with the database: {e}")

check_database_exists = PythonOperator(
    task_id='check_database',
    python_callable=check_database,
    op_args=[mysql_conn_id],
    dag=dag,
)

def download_data_from_url(url, table_name):
    # Make the API call and get the response
    logging.info("Starting API call...")

    try:
        response = requests.get(url, timeout=10) # Set a timeout for the request
        response.raise_for_status()  # This will raise a HTTPError if the response was unsuccessful
        # Check if the response is successful and has new data
        if len(response.json()) > 0:

            # Save the response to a local file
            with open(f'download/{table_name}.json', 'w') as f:
                json.dump(response.json(), f)
            # API call code here...
            logging.info("API call completed successfully.")
            return True
        else:
            return False
    except requests.exceptions.Timeout:
        logging.error("The request timed out")
        return None
    except requests.exceptions.TooManyRedirects:
        logging.error("The request exceeded the configured number of maximum redirections")
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred while making the API request: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return None


# Define a function that checks if the data source has new data
def check_new_data(url, table_name, mysql_conn_id, db_name, **kwargs):
    # Create a database engine using the MySQL connection
    engine = create_engine(mysql_conn_id)
    # Query the MySQL table and get the latest timestamp
    check_if_exists = text("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = :db_name AND table_name = :table_name")
    try:
        num_of_tables = engine.execute(check_if_exists, {"db_name": db_name, "table_name": table_name})
    except sqlalchemy.exc.SQLAlchemyError as e:
        logging.error(f"An error occurred while interacting with the database: {e}")
    if num_of_tables.fetchone()[0] == 0:
        url_ridership = url + 'LIMIT%2010000000'
        if download_data_from_url(url_ridership, table_name):
            # Push the timestamp to XCom
            kwargs['ti'].xcom_push(key='timestamp_str', value=timestamp_str)
            return 'download_data'
        else:
            return 'skip'
        
    query = f'SELECT MAX(transit_timestamp) AS latest FROM {table_name}'
    try:
        result = engine.execute(query)
    except sqlalchemy.exc.SQLAlchemyError as e:
        logging.error(f"An error occurred while interacting with the database: {e}")
    latest = result.fetchone()[0]
    # Format the datetime as a string
    timestamp_str = latest.strftime('%Y-%m-%dT%H:%M:%S.000')
    # Insert the timestamp into the API endpoint URL
    url_ridership = url + 'WHERE%20%60transit_timestamp%60%20%3E%20%22{}%22%20%3A%3A%20floating_timestamp%0ALIMIT%2010000000'.format(timestamp_str)    # Make the API call and get the response
    if download_data_from_url(url_ridership, table_name, **kwargs):
        return 'download_data'
    else:
        return 'skip'


# Define a dummy task that does nothing
def skip():
    logging.info("Skipping data download and processing as no new data is available.")

# Define the tasks
check_for_new_data = BranchPythonOperator(
    task_id='check_new_data',
    python_callable=check_new_data,
    op_args=[url, table_name, mysql_conn_id+db_name],
    retries=3,
    retry_delay=timedelta(minutes=5),
    dag=dag,
)


def download_data(table_name, mysql_conn_id, db_name, **kwargs):
        
    # Create a database engine using the MySQL connection
    engine = create_engine(mysql_conn_id)
    
    # Check if the table exists
    check_if_exists = text("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = :db_name AND table_name = :table_name")
    try:
        num_of_tables = engine.execute(check_if_exists, {"db_name": db_name, "table_name": table_name})
    except sqlalchemy.exc.SQLAlchemyError as e:
        logging.error(f"An error occurred while interacting with the database: {e}")
    
    # If the table doesn't exist, create it
    if num_of_tables.fetchone()[0] == 0:
        # Define the SQL statement to create the table
        create_table_sql = f"""
        CREATE TABLE {table_name} (
            `transit_timestamp` datetime NOT NULL,
            `station_complex_id` varchar(10) NOT NULL,
            `station_complex` varchar(100),
            `borough` enum('BK','M','Q','BX'),
            `routes` varchar(50),
            `payment_method` varchar(10) NOT NULL,
            `ridership` int,
            `transfers` int,
            `latitude` float(8,6),
            `longitude` float(8,6),
            `georeference` varchar(50),
            `station_complex_wifi` varchar(100),
            PRIMARY KEY (`transit_timestamp`, `station_complex_id`)
        )
        """
        # Execute the SQL statement
        try:
            engine.execute(create_table_sql)
        except sqlalchemy.exc.SQLAlchemyError as e:
            logging.error(f"An error occurred while interacting with the database: {e}")

    # Push the data to XCom
    kwargs['ti'].xcom_push(key='download_data', value=f'download/{table_name}.json')
    

download_data_if_new = PythonOperator(
    task_id='download_data',
    python_callable=download_data,
    op_kwargs={
        'table_name': table_name,
        'mysql_conn_id': mysql_conn_id+db_name,
    },
    dag=dag,
)

def process_function(data, mysql_conn_id, table_name):
    
    # Convert the JSON data to a pandas DataFrame
    hourly_ridership = pd.json_normalize(data)
    hourly_ridership['georeference'] = hourly_ridership.apply(lambda row: '{}({})'.format(row['georeference.type'], ', '.join(map(str, row['georeference.coordinates']))), axis=1)
    
    # create a new column of `station_complex_wifi`
    abbreviations_reversed = {'Street': 'St', 'Avenue': 'Av', 'Square': 'Sq', 'Parkway': 'Pkwy', 'Boulevard': 'Blvd', 'Heights': 'Hts', 'Place': 'Pl', 'Road': 'Rd', 'Lane': 'Ln', 'Drive': 'Dr', 'Court': 'Ct', 'Beach': 'Bch', 'Junction': 'Jct', 'Fort': 'Ft', 'Highway': 'Hwy', 'Mount': 'Mt', 'Point': 'Pt'}
    # Function to replace abbreviations in a string
    def replace_abbreviations(text):
        for k, v in abbreviations_reversed.items():
            text = text.replace(k, v)
        return text
    
    # Apply the function to the 'station_complex' column
    hourly_ridership['station_complex_list'] = hourly_ridership['station_complex'].apply(replace_abbreviations)
    # Create a new column by splitting the 'station_complex' column
    hourly_ridership['station_complex_list'] = hourly_ridership['station_complex_list'].str.split('[(/,)]')

    # Remove leading and trailing whitespace from the elements of the list
    hourly_ridership['station_complex_list'] = hourly_ridership['station_complex_list'].apply(lambda x: [item.strip() for item in x])

    hourly_ridership['station_complex_list'] = hourly_ridership['station_complex_list'].apply(lambda x: {item for item in x if item})

    # Convert each set in the 'station_complex_list' column to a frozenset
    hourly_ridership['station_complex_frozenset'] = hourly_ridership['station_complex_list'].apply(frozenset)

    # Create a dictionary to store the matching set from 'wifi_locations' for each unique set in 'hourly_ridership'
    matching_sets_dict = {}
    # Create a database engine using the MySQL connection
    engine = create_engine(mysql_conn_id)
    
    # Query the MySQL table and get all records
    wifi_locations = pd.read_sql_table(table_name, engine)

    # Populate the dictionary
    for x in hourly_ridership['station_complex_frozenset'].unique():
        for y in wifi_locations['station_complex_frozenset'].unique():
            if x.issubset(y) or y.issubset(x):
                matching_sets_dict[x] = y
                break
    
    # Use the dictionary to populate the 'matching_set' column
    hourly_ridership['station_complex_wifi'] = hourly_ridership['station_complex_frozenset'].map(matching_sets_dict)
    # Convert the frozensets to strings for storage in mysql
    hourly_ridership['station_complex_wifi'] = hourly_ridership['station_complex_wifi'].apply(lambda x: ','.join(sorted(x)) if isinstance(x, frozenset) else x)

    
    # covert datatype
    hourly_ridership['transit_timestamp'] = hourly_ridership['transit_timestamp'].astype('datetime64[ns]')
    hourly_ridership['ridership'] = hourly_ridership['ridership'].astype('int')
    hourly_ridership['transfers'] = hourly_ridership['transfers'].astype('int')
    
    # column selection
    hourly_ridership = hourly_ridership[["transit_timestamp","station_complex_id","station_complex","borough","routes","payment_method","ridership","transfers","latitude","longitude","georeference","station_complex_wifi"]]
    return hourly_ridership


def process_data(mysql_conn_id, table_name, chunksize=10000, **kwargs):
    # Pull the data from XCom
    file_path = kwargs['ti'].xcom_pull(key='download_data', task_ids='download_data')
    
    # Initialize an empty list to store the processed chunks
    processed_chunks = []

    # Load the data from the file
    with open(file_path, 'r') as f:
        # Load the JSON data in chunks
        for chunk in pd.read_json(f, lines=True, chunksize=chunksize):
            # Process each chunk of data
            processed_chunk = process_function(chunk, mysql_conn_id)

            # Append the processed chunk to the list
            processed_chunks.append(processed_chunk)


    # Concatenate the list of processed chunks into a single DataFrame
    processed_data = pd.concat(processed_chunks)
    
    # Write the processed data to a Parquet file
    processed_file_path = 'processed/processed_data_ridership.parquet.gzip'
    processed_data.to_parquet(processed_file_path, compression='gzip')
    
    # Push the processed data to XCom
    kwargs['ti'].xcom_push(key='processed_data', value=processed_file_path)

    # Push a value to XCom indicating that this task has new data for the 3rd table
    # if this has_new_data is True, then use `processed_data` to query
    kwargs['ti'].xcom_push(key='ridership_has_new_data', value=True)


process_new_data = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    op_kwargs={
        'table_name': 'wifi_locations',
        'mysql_conn_id': mysql_conn_id+db_name,
    },
    dag=dag,
)

def store_function(processed_chunk, table_name, mysql_conn_id):
    # Your storing code here...
    
    @contextmanager
    def session_scope():
        session = Session()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    # assuming user, password, host and dbname are defined
    engine = create_engine(mysql_conn_id)
    Session = sessionmaker(engine)
    
    with session_scope() as session:
        try:
            # insert the data into the table
            processed_chunk.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        except exc.SQLAlchemyError as e:
            print("An error occurred:", e)


def store_data(table_name, mysql_conn_id, **kwargs):
    # Pull the processed data from XCom
    processed_data_path = kwargs['ti'].xcom_pull(key='processed_data', task_ids='process_data')

    # Initialize a chunksize
    chunksize = 5000

    # Read and store the data in chunks
    for chunk in pd.read_parquet(processed_data_path, chunksize=chunksize):
    
        # data validation
        expected_columns = ["transit_timestamp","station_complex_id","station_complex","borough","routes","payment_method","ridership","transfers","latitude","longitude","georeference", "station_complex_wifi"]
        assert set(chunk.columns) == set(expected_columns), "Unexpected columns in the DataFrame"

        expected_dtypes = {"transit_timestamp": "datetime64[ns]", "station_complex_id": "object", "station_complex": "object", "borough": "object", "routes": "object", "payment_method": "object", "ridership": "int64", "transfers": "int64", "latitude": "float64", "longitude": "float64", "georeference": "object", "station_complex_wifi": "object"}
        for column, expected_dtype in expected_dtypes.items():
            assert chunk[column].dtype == expected_dtype, f"Unexpected dtype for column {column}"
            
        assert chunk['latitude'].between(-90, 90).all(), "Invalid latitude values"
        assert chunk['longitude'].between(-180, 180).all(), "Invalid longitude values"
        
        # Check that the composite primary keys are not null
        assert chunk[['transit_timestamp', 'station_complex_id']].notnull().all().all(), "DataFrame contains null values in primary key columns"

        # Drop rows with null primary keys
        chunk.dropna(subset=['transit_timestamp', 'station_complex_id'], inplace=True)

        
        # Call the store function with the necessary arguments
        store_function(chunk, table_name, mysql_conn_id)

store_processed_data = PythonOperator(
    task_id='store_data',
    python_callable=store_data,
    op_kwargs={
        'table_name': table_name,
        'mysql_conn_id': mysql_conn_id+db_name,
    },
    retries=3,
    retry_delay=timedelta(minutes=5),
    dag=dag,
)

# Define the dummy task that does nothing
skip_data_processing = EmptyOperator(
    task_id='skip',
    dag=dag,
)

# Define the task dependencies
wait_for_wifi_locations >> check_database_exists
check_database_exists >> check_for_new_data >> [download_data_if_new, skip_data_processing]
download_data_if_new >> process_new_data >> store_processed_data