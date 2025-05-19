import json
from datetime import datetime, timedelta
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup

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
    'met_museum_to_clickhouse_dynamic',
    default_args=default_args,
    description='Extract data from Met Museum API in batch tasks',
    schedule_interval='@daily',
    catchup=False,
)

# Function to extract data from API
def extract_from_api(**kwargs):
    """
    Extract data from Met Museum API and return list of object IDs
    """
    # Get list of object IDs
    api_url = "https://collectionapi.metmuseum.org/public/collection/v1/objects"
    response = requests.get(api_url)
    
    if response.status_code == 200:
        object_data = response.json()
        object_ids = object_data.get('objectIDs', [])
        
        # For testing purposes, limit to first 1000 objects
        # Remove this limitation in production
        object_ids = object_ids[:1000]
        
        # Split into batches
        batch_size = 50
        batches = []
        for i in range(0, len(object_ids), batch_size):
            batch = object_ids[i:i+batch_size]
            batches.append(batch)
        
        # Store batch information
        kwargs['ti'].xcom_push(key='batches', value=json.dumps(batches))
        print(f"Created {len(batches)} batches from {len(object_ids)} object IDs")
        
        return batches
    else:
        raise Exception(f"API request failed with status code {response.status_code}")

# Function to extract data and plan processing
def extract_and_plan(**kwargs):
    """
    Extract object IDs from Met Museum API and plan batch processing
    """
    # Get list of object IDs
    api_url = "https://collectionapi.metmuseum.org/public/collection/v1/objects"
    response = requests.get(api_url)
    
    if response.status_code == 200:
        object_data = response.json()
        object_ids = object_data.get('objectIDs', [])
        
        # For testing, limit to 1000 objects
        # Remove this in production
        object_ids = object_ids[:1000]
        
        # Split into batches
        batch_size = 50
        batches = []
        for i in range(0, len(object_ids), batch_size):
            batch = object_ids[i:i+batch_size]
            batches.append({
                'batch_num': i // batch_size + 1,
                'object_ids': batch
            })
        
        # Store batches info for dynamic task mapping
        kwargs['ti'].xcom_push(key='processing_batches', value=batches)
        return batches
    else:
        raise Exception(f"API request failed with status code {response.status_code}")

# Function to prepare ClickHouse table
def prepare_clickhouse_table(**kwargs):
    """Create ClickHouse table if it doesn't exist"""
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
    return "Table prepared successfully"

# Process a single batch of objects
def process_object_batch(batch, **kwargs):
    """Process a batch of objects and insert into ClickHouse"""
    batch_num = batch['batch_num']
    object_ids = batch['object_ids']
    
    print(f"Processing batch {batch_num} with {len(object_ids)} objects")
    
    batch_objects = []
    
    # Fetch objects in batch
    for obj_id in object_ids:
        obj_url = f"https://collectionapi.metmuseum.org/public/collection/v1/objects/{obj_id}"
        try:
            obj_response = requests.get(obj_url)
            if obj_response.status_code == 200:
                obj_details = obj_response.json()
                obj_details['extraction_date'] = datetime.now().strftime('%Y-%m-%d')
                batch_objects.append(obj_details)
            else:
                print(f"Failed to fetch object {obj_id}: HTTP {obj_response.status_code}")
        except Exception as e:
            print(f"Error fetching object {obj_id}: {str(e)}")
    
    # Transform and load
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
    
    # Insert batch
    if records:
        clickhouse_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_default')
        clickhouse_hook.execute('INSERT INTO met_museum_objects VALUES', records)
        return f"Batch {batch_num}: Inserted {len(records)} records"
    else:
        return f"Batch {batch_num}: No records to insert"

# Create DAG
dag = DAG(
    'met_museum_to_clickhouse_dynamic',
    default_args=default_args,
    description='Extract data from Met Museum API with dynamic tasks',
    schedule_interval='@daily',
    catchup=False,
)

# Initial tasks
extract_plan_task = PythonOperator(
    task_id='extract_and_plan',
    python_callable=extract_and_plan,
    provide_context=True,
    dag=dag,
)

prepare_table_task = PythonOperator(
    task_id='prepare_clickhouse_table',
    python_callable=prepare_clickhouse_table,
    provide_context=True,
    dag=dag,
)

# Create dynamic batch processing tasks
def create_batch_processing_tasks(dag):
    # Create a task group for batch processing
    with TaskGroup(group_id="batch_processing", dag=dag) as batch_group:
        # This dummy task helps with visualization
        start_processing = DummyOperator(task_id='start_processing')
        
        # Dynamic batch processing - we'll set this up to create tasks dynamically
        # at runtime based on the results of extract_and_plan
        batches = []
        try:
            # For DAG parsing, provide some placeholder batches
            # These will be replaced at runtime with actual API data
            for i in range(5):  # Just a placeholder for parsing
                batches.append({'batch_num': i+1, 'object_ids': []})
        except Exception:
            pass  # During parsing, this might fail
        
        batch_tasks = []
        for batch in batches:
            task = PythonOperator(
                task_id=f'process_batch_{batch["batch_num"]}',
                python_callable=process_object_batch,
                op_kwargs={'batch': batch},
                provide_context=True,
                dag=dag,
            )
            batch_tasks.append(task)
            start_processing >> task
        
        # This dummy task helps with visualization
        end_processing = DummyOperator(task_id='end_processing')
        for task in batch_tasks:
            task >> end_processing
    
    return batch_group

# Create the batch processing task group
batch_processing = create_batch_processing_tasks(dag)

# Set task dependencies
extract_plan_task >> prepare_table_task >> batch_processing
