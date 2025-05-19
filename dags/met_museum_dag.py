from datetime import datetime, timedelta
import json
import logging
import requests
from typing import List, Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# API endpoints
MET_API_BASE_URL = "https://collectionapi.metmuseum.org/public/collection/v1"
OBJECTS_ENDPOINT = f"{MET_API_BASE_URL}/objects"
OBJECT_DETAIL_ENDPOINT = f"{MET_API_BASE_URL}/objects"

# ClickHouse connection ID (set this up in Airflow connections)
CLICKHOUSE_CONN_ID = "clickhouse_default"

# Batch size for processing objects
BATCH_SIZE = 100

def _create_clickhouse_tables():
    """Create ClickHouse tables if they don't exist"""
    hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)
    
    # Create table for Met Museum objects
    hook.run("""
    CREATE TABLE IF NOT EXISTS met_objects (
        object_id Int64,
        is_highlight UInt8,
        is_public_domain UInt8,
        department String,
        object_name String,
        title String,
        culture String,
        period String,
        artist_name String,
        artist_display_name String,
        object_date String,
        medium String,
        credit_line String,
        object_url String,
        tags Array(String),
        image_small String,
        image_medium String,
        image_large String,
        created_date DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY (object_id);
    """)
    
    logging.info("ClickHouse tables created successfully")

def _fetch_object_ids(**context):
    """Fetch all object IDs from the Met API"""
    response = requests.get(OBJECTS_ENDPOINT)
    response.raise_for_status()
    data = response.json()
    
    total_objects = data.get('total', 0)
    object_ids = data.get('objectIDs', [])
    
    logging.info(f"Found {total_objects} objects in the Met Museum collection")
    
    # To limit processing for testing, adjust the number below
    # For production, you can use all object IDs
    max_objects = 1000  # Limit to 1000 objects for initial testing
    object_ids = object_ids[:max_objects]
    
    # Push the object IDs to XCom for the next task
    context['ti'].xcom_push(key='object_ids', value=object_ids)
    context['ti'].xcom_push(key='total_objects', value=len(object_ids))
    
    return object_ids

def _process_object_batch(start_idx: int, end_idx: int, **context):
    """Process a batch of objects by ID range"""
    ti = context['ti']
    object_ids = ti.xcom_pull(task_ids='fetch_object_ids', key='object_ids')
    
    # Ensure we don't go beyond the array bounds
    end_idx = min(end_idx, len(object_ids))
    batch_ids = object_ids[start_idx:end_idx]
    
    objects_data = []
    for obj_id in batch_ids:
        try:
            response = requests.get(f"{OBJECT_DETAIL_ENDPOINT}/{obj_id}")
            response.raise_for_status()
            object_data = response.json()
            
            # Extract only the fields we need
            processed_data = {
                'object_id': object_data.get('objectID'),
                'is_highlight': 1 if object_data.get('isHighlight') else 0,
                'is_public_domain': 1 if object_data.get('isPublicDomain') else 0,
                'department': object_data.get('department', ''),
                'object_name': object_data.get('objectName', ''),
                'title': object_data.get('title', ''),
                'culture': object_data.get('culture', ''),
                'period': object_data.get('period', ''),
                'artist_name': object_data.get('artistName', ''),
                'artist_display_name': object_data.get('artistDisplayName', ''),
                'object_date': object_data.get('objectDate', ''),
                'medium': object_data.get('medium', ''),
                'credit_line': object_data.get('creditLine', ''),
                'object_url': object_data.get('objectURL', ''),
                'tags': [tag.get('term', '') for tag in object_data.get('tags', [])],
                'image_small': object_data.get('primaryImageSmall', ''),
                'image_medium': object_data.get('primaryImage', ''),
                'image_large': object_data.get('primaryImage', '')  # Using primary image for large as well
            }
            
            objects_data.append(processed_data)
            
        except Exception as e:
            logging.error(f"Error processing object {obj_id}: {str(e)}")
    
    # Store the batch in ClickHouse
    if objects_data:
        _store_objects_in_clickhouse(objects_data)
    
    logging.info(f"Processed batch from index {start_idx} to {end_idx} ({len(objects_data)} objects)")
    return len(objects_data)

def _store_objects_in_clickhouse(objects_data: List[Dict[str, Any]]):
    """Store objects data in ClickHouse"""
    hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)
    
    # Insert data using the ClickHouseHook's insert_rows method
    rows = []
    for obj in objects_data:
        # For ClickHouse Arrays, we can directly pass Python lists
        rows.append({
            'object_id': obj.get('object_id'),
            'is_highlight': obj.get('is_highlight'),
            'is_public_domain': obj.get('is_public_domain'),
            'department': obj.get('department', ''),
            'object_name': obj.get('object_name', ''),
            'title': obj.get('title', ''),
            'culture': obj.get('culture', ''),
            'period': obj.get('period', ''),
            'artist_name': obj.get('artist_name', ''),
            'artist_display_name': obj.get('artist_display_name', ''),
            'object_date': obj.get('object_date', ''),
            'medium': obj.get('medium', ''),
            'credit_line': obj.get('credit_line', ''),
            'object_url': obj.get('object_url', ''),
            'tags': obj.get('tags', []),  # Direct list for ClickHouse Array
            'image_small': obj.get('image_small', ''),
            'image_medium': obj.get('image_medium', ''),
            'image_large': obj.get('image_large', '')
        })
    
    # Insert the data
    hook.insert_rows(table='met_objects', rows=rows)
    
    logging.info(f"Stored {len(objects_data)} objects in ClickHouse")

def _create_batch_tasks(dag):
    """Dynamically create batch processing tasks"""
    tasks = []
    
    # This is a placeholder - the actual number of batches will be determined at runtime
    # For now, we'll create tasks for processing up to 100 batches (10,000 objects)
    max_batches = 10
    
    for i in range(max_batches):
        start_idx = i * BATCH_SIZE
        end_idx = (i + 1) * BATCH_SIZE
        
        task = PythonOperator(
            task_id=f'process_batch_{i}',
            python_callable=_process_object_batch,
            op_kwargs={'start_idx': start_idx, 'end_idx': end_idx},
            provide_context=True,
            dag=dag,
        )
        tasks.append(task)
    
    return tasks

with DAG(
    'met_museum_collection',
    default_args=default_args,
    description='Extract data from Metropolitan Museum of Art API and store in ClickHouse',
    schedule_interval='@weekly',  # Run weekly
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['met_museum', 'art', 'api', 'clickhouse'],
) as dag:
    
    create_tables = PythonOperator(
        task_id='create_clickhouse_tables',
        python_callable=_create_clickhouse_tables,
    )
    
    fetch_ids = PythonOperator(
        task_id='fetch_object_ids',
        python_callable=_fetch_object_ids,
        provide_context=True,
    )
    
    # Create batch processing tasks
    batch_tasks = _create_batch_tasks(dag)
    
    # Set up the task dependencies
    create_tables >> fetch_ids >> batch_tasks
