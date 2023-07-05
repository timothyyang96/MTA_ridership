from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
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
import re


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# senstive information
mysql_conn_id = os.getenv('MYSQL_CONN_ID', 'mysql+mysqlconnector://root:abc@localhost:3306')
table_name = os.getenv('TABLE_NAME', 'wifi_locations')
url = os.getenv('URL', 'https://data.ny.gov/resource/pwa9-tmie.json?$query=SELECT%0A%20%20%60station_name%60%2C%0A%20%20%60station_complex%60%2C%0A%20%20%60lines%60%2C%0A%20%20%60historical%60%2C%0A%20%20%60borough%60%2C%0A%20%20%60county%60%2C%0A%20%20%60latitude%60%2C%0A%20%20%60longitude%60%2C%0A%20%20%60wifi_available%60%2C%0A%20%20%60at_t%60%2C%0A%20%20%60sprint%60%2C%0A%20%20%60t_mobile%60%2C%0A%20%20%60verizon%60%2C%0A%20%20%60location%60%2C%0A%20%20%60georeference%60')
db_name = os.getenv('DB_NAME', 'MTA')

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 6, 26),
}

# Define the DAG
dag = DAG(
    'mta_data_pipeline_wifi',
    default_args=default_args,
    description='A data pipeline for MTA data',
    schedule='@daily',
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
            create_database_sql = text("CREATE DATABASE {}".format(db_name))
            # Execute the SQL statement
            try:
                connection.execute(create_database_sql)
                logging.info("CREATE DATABASE {}".format(db_name))
            except sqlalchemy.exc.SQLAlchemyError as e:
                logging.error(f"An error occurred while interacting with the database: {e}")
                
check_database_exists = PythonOperator(
    task_id='check_database',
    python_callable=check_database,
    op_args=[mysql_conn_id, db_name],
    dag=dag,
)

def check_table(table_name, mysql_conn_id, db_name):
    # Create a database engine using the MySQL connection
    engine = create_engine(mysql_conn_id)
    # Query the MySQL table and get the latest timestamp
    check_if_exists = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{db_name}' AND table_name = '{table_name}'"
    try:
        num_of_tables = engine.execute(check_if_exists)
    except sqlalchemy.exc.SQLAlchemyError as e:
        logging.error(f"An error occurred while interacting with the database: {e}")
    
    # If the table doesn't exist, create it
    if num_of_tables.fetchone()[0] == 0:
        create_table_sql = f"""
        CREATE TABLE {table_name} (
            `station_name` varchar(50),
            `station_complex` varchar(100) NOT NULL,
            `lines` varchar(10) NOT NULL,
            `historical` varchar(5),
            `borough` varchar(10),
            `county` varchar(20),
            `latitude` float(8,6),
            `longitude` float(8,6),
            `wifi_available` varchar(3),
            `at_t` varchar(3),
            `sprint` varchar(3),
            `t_mobile` varchar(3),
            `verizon` varchar(3),
            `location` varchar(50),
            `georeference` varchar(50),
            `station_complex_str` varchar(100),
            PRIMARY KEY (`station_name`, `lines`)
        )
        """
        # Execute the SQL statement
        try:
            engine.execute(create_table_sql)
        except sqlalchemy.exc.SQLAlchemyError as e:
            logging.error(f"An error occurred while interacting with the database: {e}")
                  
# Define the tasks
check_table_exists = BranchPythonOperator(
    task_id='check_table',
    python_callable=check_table,
    op_args=[table_name, mysql_conn_id+db_name],
    retries=3,
    retry_delay=timedelta(minutes=5),
    dag=dag,
)



def download_data_from_url(url, table_name):
    # Make the API call and get the response
    logging.info("Starting API call...")

    try:
        response = requests.get(url)
        # Check if the response is successful and has new data
        if response.status_code == 200 and len(response.json()) > 0:
            # Save the response to a local file
            with open(f'download/{table_name}.json', 'w') as f:
                json.dump(response.json(), f)
            # API call code here...
            logging.info("API call completed successfully.")
    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred while making the API request: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

# download_data could be separated into 2:
# 1. download from url
# 2. download from MySQL -> compare_data task
def download_data(table_name, url, **kwargs):
    
 
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
    except requests.exceptions.Timeout:
        logging.error("The request timed out")
    except requests.exceptions.TooManyRedirects:
        logging.error("The request exceeded the configured number of maximum redirections")
    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred while making the API request: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    
    # Push the data to XCom
    kwargs['ti'].xcom_push(key='download_data', value=f'download/{table_name}.json')
    

download_data_if_new = PythonOperator(
    task_id='download_data',
    python_callable=download_data,
    op_kwargs={
        'url': url,
        'table_name': table_name
    },
    dag=dag,
)

def process_function(data):
    
    # Convert the JSON data to a pandas DataFrame
    wifi_locations = pd.json_normalize(data)
    wifi_locations['location'] = '(' + wifi_locations['location.latitude'].astype(str) + ', ' + wifi_locations['location.longitude'].astype(str) + ')'
    wifi_locations['georeference'] = wifi_locations.apply(lambda row: '{}({})'.format(row['georeference.type'], ', '.join(map(str, row['georeference.coordinates']))) if isinstance(row['georeference.coordinates'], list) else None, axis=1)

    # check composite_primary_keys if null
    station_name_null_row = wifi_locations[wifi_locations['station_name'].isnull()]
    if len(station_name_null_row) == 1 and station_name_null_row['station_complex'] == 'Botanic Garden (S)':
        wifi_locations['station_name'].fillna('Botanic Garden', inplace=True)
    elif len(station_name_null_row) >= 2:
        wifi_locations['station_name'].fillna(wifi_locations['station_complex'].str.split('(', 1).str[0], inplace=True)
    
    # convert datatypes
    wifi_locations['latitude'] = wifi_locations['latitude'].astype('float')
    wifi_locations['longtitude'] = wifi_locations['longtitude'].astype('float')
    
    # create a new column of `station_complex_str`
    abbreviations_reversed = {'Street': 'St', 'Avenue': 'Av', 'Square': 'Sq', 'Parkway': 'Pkwy', 'Boulevard': 'Blvd', 'Heights': 'Hts', 'Place': 'Pl', 'Road': 'Rd', 'Lane': 'Ln', 'Drive': 'Dr', 'Court': 'Ct', 'Beach': 'Bch', 'Junction': 'Jct', 'Fort': 'Ft', 'Highway': 'Hwy', 'Mount': 'Mt', 'Point': 'Pt'}
    # Function toreplace abbreviations in a string
    def replace_abbreviations(text):
        for k, v in abbreviations_reversed.items():
            text = text.replace(k, v)
        return text

    # Apply the function to the 'station_complex' column
    wifi_locations['station_complex_list'] = wifi_locations['station_complex'].apply(replace_abbreviations)

    # Create a new column by splitting the 'station_complex' column
    wifi_locations['station_complex_list'] = wifi_locations['station_complex_list'].str.split('[(/,)]')

    # Remove leading and trailing whitespace from the elements of the list
    wifi_locations['station_complex_list'] = wifi_locations['station_complex_list'].apply(lambda x: [item.strip() for item in x])

    # Ensure that the 'station_complex_list' columns in both dataframes are of type set
    wifi_locations['station_complex_list'] = wifi_locations['station_complex_list'].apply(lambda x: {item for item in x if item})
    wifi_locations['station_complex_frozenset'] = wifi_locations['station_complex_list'].apply(frozenset)
    # Convert the frozensets to strings for storage in mysql
    wifi_locations['station_complex_str'] = wifi_locations['station_complex_frozenset'].apply(lambda x: ','.join(sorted(x)) if isinstance(x, frozenset) else x)
    
    
    # column selection
    wifi_locations = wifi_locations[["station_name","station_complex","lines","historical","borough","county","latitude","longitude","wifi_available","at_t","sprint","t_mobile","verizon","location","georeference","station_complex_str"]]
    
    # data validation
    assert wifi_locations['latitude'].dtype == 'float64', "latitude is not of type float64"
    assert wifi_locations['longitude'].dtype == 'float64', "longitude is not of type float64"
    
    assert wifi_locations['latitude'].between(-90, 90).all(), "Invalid latitude values"
    assert wifi_locations['longitude'].between(-180, 180).all(), "Invalid longitude values"
    
    assert wifi_locations[['station_name', 'lines']].notnull().all().all(), "DataFrame contains null values in primary key columns"

    assert wifi_locations[['station_name', 'lines']].duplicated().sum() == 0, "Primary key contains duplicate values"

    valid_boroughs = {'Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island'}
    assert set(wifi_locations['borough'].unique()).issubset(valid_boroughs), "borough contains invalid boroughs"
    
    
    pattern = re.compile(r'\(\d+\.\d+, \d+\.\d+\)')  # Simple regex for location validation
    assert wifi_locations['location'].apply(lambda x: bool(pattern.match(x))).all(), "location contains invalid locations"
    
    return wifi_locations


def process_data(**kwargs):
    # Pull the data from XCom
    file_path = kwargs['ti'].xcom_pull(key='download_data', task_ids='download_data')
    
    # Load the data from the file
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    # Your processing code here...
    processed_data = process_function(data)
    
    
    # Write the processed data to a Parquet file
    processed_file_path = 'processed/processed_data_wifi.parquet.gzip'
    processed_data.to_parquet(processed_file_path, compression='gzip')
    
    # Push the processed data to XCom
    kwargs['ti'].xcom_push(key='processed_data', value=processed_file_path)


process_downloaded_data = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag,
)
    

def compare_data(mysql_conn_id, table_name, **kwargs):
    
    # download data from mysql_server
    # Create a database engine using the MySQL connection
    engine = create_engine(mysql_conn_id)

    # Query the MySQL table and get all records
    download_all = f'SELECT * FROM {table_name}'
    try:
        old_table = engine.execute(download_all)
        records = old_table.fetchall()
    except sqlalchemy.exc.SQLAlchemyError as e:
        logging.error(f"An error occurred while interacting with the database: {e}")
    
    columns = ['station_name','station_complex','lines','historical','borough',
           'county','latitude','longitude','wifi_available','at_t',
           'sprint','t_mobile','verizon','location','georeference','station_complex_str']
    df_old = pd.DataFrame(records, columns=columns)
    
    
    # Pull the processed data from XCom
    processed_data_path = kwargs['ti'].xcom_pull(key='processed_data', task_ids='process_data')
    
    # Load the processed data from the Parquet file
    df_new = pd.read_parquet(processed_file_path)
    
    # data validation for df_new
    expected_dtypes = {
        'station_name': 'object',
        'station_complex': 'object',
        'lines': 'object',
        'historical': 'object',
        'borough': 'object',
        'county': 'object',
        'latitude': 'float64',
        'longitude': 'float64',
        'wifi_available': 'object',
        'at_t': 'object',
        'sprint': 'object',
        't_mobile': 'object',
        'verizon': 'object',
        'location': 'object',
        'georeference': 'object',
        'station_complex_str': 'object'
    }

    for column, expected_dtype in expected_dtypes.items():
        assert df_old[column].dtype == expected_dtype, f"Unexpected dtype for column {column}"

    
    if len(df_old) == 0:
        kwargs['ti'].xcom_push(key='df_to_store_all', value=df_new)
        return 'store_data'
    
    # Set indexes
    df_old.set_index(['station_name', 'lines'], inplace=True)
    df_new.set_index(['station_name', 'lines'], inplace=True)
    
    if df_old.equals(df_new):
        return 'skip'
    
    # Find rows to add
    df_to_add = df_new.loc[~df_new.index.isin(df_old.index)]

    # Find rows to update
    df_to_update = df_new.loc[df_new.index.isin(df_old.index) & (df_new != df_old).any(axis=1)]

    # Find rows to delete
    df_to_delete = df_old.loc[~df_old.index.isin(df_new.index)]

    # Push the dataframes to XCom
    kwargs['ti'].xcom_push(key='df_to_add', value=df_to_add)
    kwargs['ti'].xcom_push(key='df_to_update', value=df_to_update)
    kwargs['ti'].xcom_push(key='df_to_delete', value=df_to_delete)
    
    return 'store_data'

        
compare_old_and_new_data = PythonOperator(
    task_id='compare_data',
    python_callable=compare_data,
    op_kwargs={
        'table_name': table_name,
        'mysql_conn_id': mysql_conn_id+db_name,
    },
    retries=3,
    retry_delay=timedelta(minutes=5),
    dag=dag,
)


def store_function(processed_data, table_name, mysql_conn_id):
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
            processed_data.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        except exc.SQLAlchemyError as e:
            print("An error occurred:", e)

            
def update_data(mysql_conn_id, table_name, **kwargs):
    
    ti = kwargs['ti']
    df_to_update = ti.xcom_pull(key='df_to_update', task_ids='compare_data')
    df_to_delete = ti.xcom_pull(key='df_to_delete', task_ids='compare_data')

    # Create a database engine using the MySQL connection
    engine = sqlalchemy.create_engine(mysql_conn_id)

    # Start a new transaction
    with engine.begin() as connection:
        # Update rows
        for index, row in df_to_update.iterrows():
            update_query = text("""
            UPDATE :table_name
            SET station_complex = :station_complex, historical = :historical, borough = :borough,
                county = :county, latitude = :latitude, longitude = :longitude, wifi_available = :wifi_available,
                at_t = :at_t, sprint = :sprint, t_mobile = :t_mobile, verizon = :verizon,
                location = :location, georeference = :georeference, station_complex_str = :station_complex_str
            WHERE station_name = :station_name AND lines = :lines
            """)

            try:
                connection.execute(update_query, table_name=table_name, station_complex=row['station_complex'], historical=row['historical'], borough=row['borough'],
                                   county=row['county'], latitude=row['latitude'], longitude=row['longitude'], wifi_available=row['wifi_available'],
                                   at_t=row['at_t'], sprint=row['sprint'], t_mobile=row['t_mobile'], verizon=row['verizon'],
                                   location=row['location'], georeference=row['georeference'], station_complex_str=row['station_complex_str'],
                                   station_name=index[0], lines=index[1])
            except sqlalchemy.exc.SQLAlchemyError as e:
                logging.error(f"An error occurred while updating the database: {e}")
                raise

        # Delete rows
        for index, _ in df_to_delete.iterrows():
            delete_query = f"""
            DELETE FROM {table_name}
            WHERE station_name = '{index[0]}' AND lines = '{index[1]}'
            """
            try:
                connection.execute(delete_query)
            except sqlalchemy.exc.SQLAlchemyError as e:
                logging.error(f"An error occurred while deleting from the database: {e}")
                raise


def store_data(table_name, mysql_conn_id, **kwargs):

    # Push a value to XCom indicating that this task has updated the table
    kwargs['ti'].xcom_push(key='wifi_table_updated', value=True)

    df_to_store_all = kwargs['ti'].xcom_pull(key='df_to_store_all', task_ids='compare_data')
    
    if df_to_store_all is not None:
        # Call the store function with the necessary arguments
        store_func(df_to_store_all, table_name, mysql_conn_id)
        return
    
    # df_to_add
    store_func(df_to_add, table_name, mysql_conn_id)
    # df_to_update & df_to_delete
    update_data(table_name, mysql_conn_id, **kwargs)
        

store_new_data = PythonOperator(
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

# Define a dummy task that does nothing
def skip():
    logging.info("Skipping data ingestion as no new data is available.")

# Define the dummy task that does nothing
skip_data_processing = EmptyOperator(
    task_id='skip',
    dag=dag,
)

# Define the task dependencies
check_database_exists >> check_table_exists >> download_data_if_new >> process_downloaded_data >> compare_old_and_new_data >> [skip_data_processing, store_new_data]
