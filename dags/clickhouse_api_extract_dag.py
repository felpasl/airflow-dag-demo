# Import at the top level for the decorators to work correctly
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
from airflow.decorators import task, dag as dag_decorator, task_group

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

# Create the DAG using the decorator pattern
@dag_decorator(
    dag_id='met_museum_to_clickhouse_dynamic',
    default_args=default_args,
    description='Extract data from Met Museum API with dynamic tasks',
    schedule_interval='@daily',
    max_active_runs=1,
    concurrency=1,
    catchup=False,
)
def create_dag():
    """
    Creates a DAG that extracts object IDs from the Met Museum API,
    splits them into batches, and processes each batch in parallel.
    """
    @task
    def extract_and_plan():
        """
        Extract object IDs from Met Museum API and plan batch processing
        """
        api_url = "https://collectionapi.metmuseum.org/public/collection/v1/objects"
        response = requests.get(api_url)
        
        if response.status_code == 200:
            object_data = response.json()
            object_ids = object_data.get('objectIDs', [])
            
            # For testing, limit to 1000 objects
            # Remove this in production
            # object_ids = object_ids[:1000]
            
            # Split into batches
            batch_size = 1000
            batches = []
            for i in range(0, len(object_ids), batch_size):
                batch = object_ids[i:i+batch_size]
                batches.append({
                    'batch_num': i // batch_size + 1,
                    'object_ids': batch
                })
            
            print(f"Created {len(batches)} batches from {len(object_ids)} object IDs")
            return batches
        else:
            raise Exception(f"API request failed with status code {response.status_code}")
    
    @task
    def prepare_clickhouse_table():
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
    
    @task
    def process_batch(batch):
        """Process a batch of objects and insert into ClickHouse"""
        batch_num = batch['batch_num']
        object_ids = batch['object_ids']
        
        print(f"Processing batch {batch_num} with {len(object_ids)} objects")
        
        # Initialize ClickHouse hook
        clickhouse_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_default')
        
        # Initialize counters for progress tracking
        chunk_size = 100
        total_inserted = 0
        current_chunk = []
        
        # Process objects in the batch
        for i, obj_id in enumerate(object_ids):
            obj_url = f"https://collectionapi.metmuseum.org/public/collection/v1/objects/{obj_id}"
            try:
                obj_response = requests.get(obj_url)
                if obj_response.status_code == 200:
                    obj_details = obj_response.json()
                    obj_details['extraction_date'] = datetime.now().strftime('%Y-%m-%d')
                    
                    # Convert to record format
                    extraction_date_str = obj_details.get('extraction_date')
                    try:
                        extraction_date = datetime.strptime(extraction_date_str, '%Y-%m-%d').date()
                    except (ValueError, TypeError):
                        extraction_date = datetime.now().date()
                    
                    record = {
                        'objectID': obj_details.get('objectID', 0),
                        'title': obj_details.get('title', ''),
                        'artistDisplayName': obj_details.get('artistDisplayName', 'Unknown Artist'),
                        'artistDisplayBio': obj_details.get('artistDisplayBio', ''),
                        'artistNationality': obj_details.get('artistNationality', ''),
                        'objectDate': obj_details.get('objectDate', 'Unknown Date'),
                        'objectBeginDate': obj_details.get('objectBeginDate', 0),
                        'objectEndDate': obj_details.get('objectEndDate', 0),
                        'medium': obj_details.get('medium', 'Unknown Medium'),
                        'department': obj_details.get('department', 'Unknown Department'),
                        'classification': obj_details.get('classification', 'Unknown'),
                        'culture': obj_details.get('culture', ''),
                        'period': obj_details.get('period', ''),
                        'dynasty': obj_details.get('dynasty', ''),
                        'dimensions': obj_details.get('dimensions', ''),
                        'city': obj_details.get('city', ''),
                        'state': obj_details.get('state', ''),
                        'country': obj_details.get('country', ''),
                        'primaryImage': obj_details.get('primaryImage', ''),
                        'objectURL': obj_details.get('objectURL', ''),
                        'isPublicDomain': 1 if obj_details.get('isPublicDomain', False) else 0,
                        'GalleryNumber': obj_details.get('GalleryNumber', ''),
                        'extraction_date': extraction_date
                    }
                    current_chunk.append(record)
                else:
                    print(f"Failed to fetch object {obj_id}: HTTP {obj_response.status_code}")
            except Exception as e:
                print(f"Error fetching object {obj_id}: {str(e)}")
            
            # Insert chunk if we've reached chunk_size or this is the last item
            is_last_item = (i == len(object_ids) - 1)
            if len(current_chunk) >= chunk_size or (is_last_item and current_chunk):
                try:
                    clickhouse_hook.execute('INSERT INTO met_museum_objects VALUES', current_chunk)
                    total_inserted += len(current_chunk)
                    print(f"Batch {batch_num}: Progress {total_inserted}/{len(object_ids)} objects inserted ({(total_inserted/len(object_ids)*100):.1f}%)")
                    
                    # Clear the chunk after insert
                    current_chunk = []
                except Exception as e:
                    print(f"Error inserting chunk: {str(e)}")
        
        return f"Batch {batch_num}: Completed - Total {total_inserted}/{len(object_ids)} records inserted"
    
    # Create task instances
    batches = extract_and_plan()
    table_ready = prepare_clickhouse_table()
    
    # Create a dummy task to mark the start of batch processing
    start_processing = DummyOperator(task_id='start_processing')
    
    # Use dynamic mapping to create a task for each batch
    batch_results = process_batch.expand(batch=batches)
    
    # Create a dummy task to mark the end of batch processing
    end_processing = DummyOperator(task_id='end_processing')
    
    # Set dependencies
    batches >> table_ready >> start_processing >> batch_results >> end_processing

# Instantiate the DAG
met_museum_dag = create_dag()
