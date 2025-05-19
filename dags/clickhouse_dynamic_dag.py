import json
from datetime import datetime, timedelta
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.models.param import Param

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 5, 19),
}

# Number of objects per batch
BATCH_SIZE = 50

def extract_and_get_batches(api_url="https://collectionapi.metmuseum.org/public/collection/v1/objects", **kwargs):
    """
    Extract data from Met Museum API and divide into batches
    """
    response = requests.get(api_url)
    
    if response.status_code == 200:
        object_data = response.json()
        object_ids = object_data.get('objectIDs', [])
        
        # For testing purposes, limit to first 1000 objects
        # Remove this limitation in production
        object_ids = object_ids[:1000]
        
        # Split into batches
        batches = []
        for i in range(0, len(object_ids), BATCH_SIZE):
            batch = object_ids[i:i+BATCH_SIZE]
            batches.append({
                'batch_num': i // BATCH_SIZE + 1,
                'start_idx': i,
                'end_idx': min(i+BATCH_SIZE, len(object_ids)),
                'object_ids': batch
            })
        
        return batches
    else:
        raise Exception(f"API request failed with status code {response.status_code}")

def prepare_clickhouse_table(**kwargs):
    """
    Create ClickHouse table if it doesn't exist
    """
    clickhouse_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_default')
    
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
    
    clickhouse_hook.execute(create_table_query)
    return "Table created or already exists"

def process_batch(**kwargs):
    """
    Process a batch of object IDs and load to ClickHouse
    """
    # Get batch information from params
    params = kwargs['params']
    batch_num = params.get('batch_num')
    object_ids = params.get('object_ids')
    
    print(f"Processing batch {batch_num} with {len(object_ids)} objects")
    
    batch_objects = []
    
    # Fetch objects in batch
    for obj_id in object_ids:
        obj_url = f"https://collectionapi.metmuseum.org/public/collection/v1/objects/{obj_id}"
        try:
            obj_response = requests.get(obj_url)
            if obj_response.status_code == 200:
                obj_details = obj_response.json()
                # Add extraction date
                obj_details['extraction_date'] = datetime.now().strftime('%Y-%m-%d')
                batch_objects.append(obj_details)
            else:
                print(f"Failed to fetch object {obj_id}: HTTP {obj_response.status_code}")
        except Exception as e:
            print(f"Error fetching object {obj_id}: {str(e)}")
    
    # Transform batch data into ClickHouse records
    records = []
    for item in batch_objects:
        # Convert string date to Python date object
        extraction_date_str = item.get('extraction_date')
        try:
            extraction_date = datetime.strptime(extraction_date_str, '%Y-%m-%d').date()
        except (ValueError, TypeError):
            extraction_date = datetime.now().date()
            
        record = {
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
            'extraction_date': extraction_date
        }
        records.append(record)
    
    # Insert current batch
    if records:
        clickhouse_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_default')
        clickhouse_hook.execute('INSERT INTO met_museum_objects VALUES', records)
        return f"Inserted {len(records)} records from batch {batch_num}"
    else:
        return f"No records found in batch {batch_num}"

# Get batches for dynamic DAG generation
batches = extract_and_get_batches()

# Create a DAG for each batch
for batch in batches:
    batch_id = batch['batch_num']
    dag_id = f'met_museum_batch_{batch_id}'
    
    with DAG(
        dag_id,
        default_args=default_args,
        description=f'Process Met Museum batch {batch_id} (objects {batch["start_idx"]+1}-{batch["end_idx"]})',
        schedule_interval='@daily',
        catchup=False,
        params={
            'batch_num': Param(batch_id, type='integer'),
            'object_ids': Param(batch['object_ids'], type='array')
        },
    ) as dag:
        
        # Prepare ClickHouse table task
        prepare_table = PythonOperator(
            task_id='prepare_clickhouse_table',
            python_callable=prepare_clickhouse_table,
        )
        
        # Process batch task
        process = PythonOperator(
            task_id=f'process_batch',
            python_callable=process_batch,
        )
        
        # Set task dependencies
        prepare_table >> process
        
    # Make DAG available for Airflow
    globals()[dag_id] = dag
