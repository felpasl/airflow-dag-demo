import json
from datetime import datetime, timedelta
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

# Create the DAG
dag = DAG(
    'met_museum_to_clickhouse',
    default_args=default_args,
    description='Extract data from Met Museum API and load into ClickHouse',
    schedule_interval='@daily',
    catchup=False,
)

# Function to extract data from API
def extract_from_api(**kwargs):
    """
    Extract data from Met Museum API and return as a JSON string
    """
    # Get list of object IDs
    api_url = "https://collectionapi.metmuseum.org/public/collection/v1/objects"
    response = requests.get(api_url)
    
    if response.status_code == 200:
        object_data = response.json()
        total_objects = object_data.get('total', 0)
        object_ids = object_data.get('objectIDs', [])
        
        # Limit to first 100 objects for performance
        sample_ids = object_ids[:100] if len(object_ids) > 100 else object_ids
        
        # Fetch details for each object
        all_objects = []
        for obj_id in sample_ids:
            obj_url = f"https://collectionapi.metmuseum.org/public/collection/v1/objects/{obj_id}"
            obj_response = requests.get(obj_url)
            if obj_response.status_code == 200:
                obj_details = obj_response.json()
                all_objects.append(obj_details)
        
        # Convert to pandas DataFrame for easier manipulation
        df = pd.DataFrame(all_objects)
        
        # Add extraction timestamp
        df['extraction_date'] = datetime.now().strftime('%Y-%m-%d')
        
        # Convert back to JSON
        result = df.to_json(orient='records')
        
        # Push the result to XCom for the next task
        kwargs['ti'].xcom_push(key='api_data', value=result)
        return result
    else:
        raise Exception(f"API request failed with status code {response.status_code}")

# Function to transform data
def transform_data(**kwargs):
    """
    Transform the data extracted from the Met Museum API
    """
    # Pull data from XCom
    ti = kwargs['ti']
    data_json = ti.xcom_pull(task_ids='extract_data', key='api_data')
    
    # Parse JSON to DataFrame
    data = pd.read_json(data_json)
    
    # Select only columns we need for useful dimensions
    columns_to_keep = [
        'objectID', 
        'title', 
        'artistDisplayName',
        'artistDisplayBio',
        'artistNationality',
        'objectDate',
        'objectBeginDate',
        'objectEndDate',
        'medium',
        'department',
        'classification',
        'culture',
        'period',
        'dynasty',
        'dimensions',
        'city',
        'state',
        'country',
        'primaryImage',
        'objectURL',
        'isPublicDomain',
        'GalleryNumber',
        'extraction_date'
    ]
    
    # Filter columns that exist in the data
    existing_columns = [col for col in columns_to_keep if col in data.columns]
    data = data[existing_columns]
    
    # Fill NaN values
    data = data.fillna({
        'artistDisplayName': 'Unknown Artist',
        'artistDisplayBio': '',
        'artistNationality': '',
        'objectDate': 'Unknown Date',
        'objectBeginDate': 0,
        'objectEndDate': 0,
        'medium': 'Unknown Medium',
        'department': 'Unknown Department',
        'classification': 'Unknown',
        'culture': '',
        'period': '',
        'dynasty': '',
        'dimensions': '',
        'city': '',
        'state': '',
        'country': '',
        'primaryImage': '',
        'GalleryNumber': '',
        'isPublicDomain': False
    })
    
    # Convert back to JSON
    result = data.to_json(orient='records')
    
    # Push the result to XCom
    ti.xcom_push(key='transformed_data', value=result)
    return result

# Function to prepare data for ClickHouse
def prepare_for_clickhouse(**kwargs):
    """
    Prepare the data for insertion into ClickHouse
    """
    ti = kwargs['ti']
    data_json = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
    
    # Parse JSON to list of dictionaries
    data = json.loads(data_json)
    
    # Prepare ClickHouse query
    clickhouse_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_default')
    
    # Ensure the table exists
    create_table_query = """
    CREATE TABLE IF NOT EXISTS met_museum_objects (
        objectID UInt64,
        title String,
        artistDisplayName String,
        artistDisplayBio String,
        artistNationality String,
        objectDate String,
        objectBeginDate Int32,
        objectEndDate Int32,
        medium String,
        department String,
        classification String,
        culture String,
        period String,
        dynasty String,
        dimensions String,
        city String,
        state String,
        country String,
        primaryImage String,
        objectURL String,
        isPublicDomain UInt8,
        GalleryNumber String,
        extraction_date Date
    ) ENGINE = MergeTree()
    ORDER BY (objectID, extraction_date)
    """
    
    # Use the execute method
    clickhouse_hook.execute(create_table_query)
    
    # Return the data for the next task
    return data_json

# Create a new task to load data into ClickHouse one record at a time
def load_data_to_clickhouse(**kwargs):
    """
    Load data into ClickHouse table
    """
    ti = kwargs['ti']
    data_json = ti.xcom_pull(task_ids='prepare_for_clickhouse')
    data = json.loads(data_json)
    
    clickhouse_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_default')
    
    # Insert records in batches to avoid too many parameters
    batch_size = 10
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        
        # Create a separate INSERT statement for each batch
        for item in batch:
            # Prepare parameters in dict form
            params = {
                'objectID': item.get('objectID', 0),
                'title': item.get('title', ''),
                'artistDisplayName': item.get('artistDisplayName', 'Unknown Artist'),
                'artistDisplayBio': item.get('artistDisplayBio', ''),
                'artistNationality': item.get('artistNationality', ''),
                'objectDate': item.get('objectDate', 'Unknown Date'),
                'objectBeginDate': item.get('objectBeginDate', 0),
                'objectEndDate': item.get('objectEndDate', 0),
                'medium': item.get('medium', 'Unknown Medium'),
                'department': item.get('department', 'Unknown Department'),
                'classification': item.get('classification', 'Unknown'),
                'culture': item.get('culture', ''),
                'period': item.get('period', ''),
                'dynasty': item.get('dynasty', ''),
                'dimensions': item.get('dimensions', ''),
                'city': item.get('city', ''),
                'state': item.get('state', ''),
                'country': item.get('country', ''),
                'primaryImage': item.get('primaryImage', ''),
                'objectURL': item.get('objectURL', ''),
                'isPublicDomain': 1 if item.get('isPublicDomain', False) else 0,
                'GalleryNumber': item.get('GalleryNumber', ''),
                'extraction_date': item.get('extraction_date', datetime.now().strftime('%Y-%m-%d'))
            }
            
            # SQL statement with named parameters
            sql = """
            INSERT INTO met_museum_objects (
                objectID, title, artistDisplayName, artistDisplayBio, 
                artistNationality, objectDate, objectBeginDate, objectEndDate,
                medium, department, classification, culture, period, dynasty,
                dimensions, city, state, country, primaryImage, objectURL,
                isPublicDomain, GalleryNumber, extraction_date
            ) VALUES (
                %(objectID)s, %(title)s, %(artistDisplayName)s, %(artistDisplayBio)s,
                %(artistNationality)s, %(objectDate)s, %(objectBeginDate)s, %(objectEndDate)s,
                %(medium)s, %(department)s, %(classification)s, %(culture)s, %(period)s, %(dynasty)s,
                %(dimensions)s, %(city)s, %(state)s, %(country)s, %(primaryImage)s, %(objectURL)s,
                %(isPublicDomain)s, %(GalleryNumber)s, %(extraction_date)s
            )
            """
            
            clickhouse_hook.execute(sql, params)

# Tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_from_api,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

prepare_task = PythonOperator(
    task_id='prepare_for_clickhouse',
    python_callable=prepare_for_clickhouse,
    provide_context=True,
    dag=dag,
)

# Replace the ClickHouseOperator with a PythonOperator
load_to_clickhouse = PythonOperator(
    task_id='load_to_clickhouse',
    python_callable=load_data_to_clickhouse,
    provide_context=True,
    dag=dag,
)

# Task dependencies
extract_task >> transform_task >> prepare_task >> load_to_clickhouse
