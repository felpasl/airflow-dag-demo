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
    
    # Use the execute method instead of run
    clickhouse_hook.execute(create_table_query)
    
    # Return the prepared data for the next task
    return data_json

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

# ClickHouse load task
clickhouse_load = ClickHouseOperator(
    task_id='load_to_clickhouse',
    clickhouse_conn_id='clickhouse_default',
    database='default',
    sql="""
    INSERT INTO met_museum_objects
    SELECT 
        JSONExtractUInt(json, 'objectID') AS objectID,
        JSONExtractString(json, 'title') AS title,
        JSONExtractString(json, 'artistDisplayName') AS artistDisplayName,
        JSONExtractString(json, 'artistDisplayBio') AS artistDisplayBio,
        JSONExtractString(json, 'artistNationality') AS artistNationality,
        JSONExtractString(json, 'objectDate') AS objectDate,
        JSONExtractInt(json, 'objectBeginDate') AS objectBeginDate,
        JSONExtractInt(json, 'objectEndDate') AS objectEndDate,
        JSONExtractString(json, 'medium') AS medium,
        JSONExtractString(json, 'department') AS department,
        JSONExtractString(json, 'classification') AS classification,
        JSONExtractString(json, 'culture') AS culture,
        JSONExtractString(json, 'period') AS period,
        JSONExtractString(json, 'dynasty') AS dynasty,
        JSONExtractString(json, 'dimensions') AS dimensions,
        JSONExtractString(json, 'city') AS city,
        JSONExtractString(json, 'state') AS state,
        JSONExtractString(json, 'country') AS country,
        JSONExtractString(json, 'primaryImage') AS primaryImage,
        JSONExtractString(json, 'objectURL') AS objectURL,
        JSONExtractUInt(json, 'isPublicDomain') AS isPublicDomain,
        JSONExtractString(json, 'GalleryNumber') AS GalleryNumber,
        toDate(JSONExtractString(json, 'extraction_date')) AS extraction_date
    FROM input('json String')
    """,
    parameters={
        'json': "{{ ti.xcom_pull(task_ids='prepare_for_clickhouse') }}"
    },
    dag=dag,
)

# Task dependencies
extract_task >> transform_task >> prepare_task >> clickhouse_load
