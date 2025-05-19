from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Replace with your actual PostgreSQL connection ID
POSTGRES_CONN_ID = "postgres_default"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 5, 19),
    "retries": 1,
}

dag = DAG(
    "met_museum",
    default_args=default_args,
    description="A DAG to process Met Museum data",
    schedule_interval="@daily",
    # Add concurrency settings to prevent parallel execution
    max_active_runs=1,
    concurrency=1,
    # The following settings ensure tasks run sequentially
    max_active_tasks=1
)

# Replace ClickHouse table creation with PostgreSQL
create_table = SQLExecuteQueryOperator(
    task_id="create_table",
    conn_id="postgres_default",
    sql="""
    CREATE TABLE IF NOT EXISTS met_data (
        id SERIAL PRIMARY KEY,
        object_id INTEGER,
        is_highlight BOOLEAN,
        department VARCHAR(255),
        object_name VARCHAR(255),
        title TEXT,
        culture VARCHAR(255),
        period VARCHAR(255),
        artist_name VARCHAR(255),
        date VARCHAR(255),
        medium TEXT,
        dimensions TEXT,
        city VARCHAR(255),
        state VARCHAR(255),
        country VARCHAR(255),
        classification VARCHAR(255),
        link TEXT,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    hook_params={'schema': 'airflow'}
)

def fetch_object_ids(**kwargs):
    """Fetch object IDs from Met Museum API"""
    import requests
    
    url = "https://collectionapi.metmuseum.org/public/collection/v1/objects"
    response = requests.get(url)
    data = response.json()
    
    # Get a manageable subset for testing
    object_ids = data.get('objectIDs', [])[:1000]  # limit to 1000 objects
    
    # Store the object IDs for downstream tasks
    kwargs['ti'].xcom_push(key='object_ids', value=object_ids)
    return len(object_ids)

def fetch_objects_batch(start_idx, batch_size, **kwargs):
    """Fetch a batch of objects from Met Museum API"""
    import requests
    import time
    
    ti = kwargs['ti']
    object_ids = ti.xcom_pull(key='object_ids', task_ids='fetch_object_ids')
    
    # Get only the batch we need
    batch_ids = object_ids[start_idx:start_idx + batch_size]
    
    objects = []
    for obj_id in batch_ids:
        try:
            obj_url = f"https://collectionapi.metmuseum.org/public/collection/v1/objects/{obj_id}"
            obj_response = requests.get(obj_url)
            if obj_response.status_code == 200:
                obj_data = obj_response.json()
                objects.append(obj_data)
            # Be nice to the API with a small delay
            time.sleep(0.1)
        except Exception as e:
            print(f"Error fetching object {obj_id}: {str(e)}")
    
    return objects

def process_and_insert_batch(start_idx, batch_size, **kwargs):
    """Process a batch of objects and insert into PostgreSQL"""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    ti = kwargs['ti']
    # Pull the batch data from the fetch task
    task_id = f'fetch_batch_{start_idx}_{batch_size}'
    objects = ti.xcom_pull(task_ids=task_id)
    
    if not objects:
        print(f"No objects found for batch starting at {start_idx}")
        return 0
    
    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    
    rows = []
    for obj in objects:
        # Format row for PostgreSQL
        row = (
            obj.get('objectID', None),
            obj.get('isHighlight', False),
            obj.get('department', ''),
            obj.get('objectName', ''),
            obj.get('title', ''),
            obj.get('culture', ''),
            obj.get('period', ''),
            obj.get('artistDisplayName', ''),
            obj.get('objectDate', ''),
            obj.get('medium', ''),
            obj.get('dimensions', ''),
            obj.get('city', ''),
            obj.get('state', ''),
            obj.get('country', ''),
            obj.get('classification', ''),
            obj.get('objectURL', '')
        )
        rows.append(row)
    
    # Insert the batch into PostgreSQL
    if rows:
        pg_hook.insert_rows(
            table='met_data',
            rows=rows,
            target_fields=[
                'object_id', 'is_highlight', 'department', 'object_name',
                'title', 'culture', 'period', 'artist_name', 'date',
                'medium', 'dimensions', 'city', 'state', 'country',
                'classification', 'link'
            ],
            commit_every=len(rows)
        )
    
    return len(rows)

# Create DAG tasks
fetch_object_ids_task = PythonOperator(
    task_id='fetch_object_ids',
    python_callable=fetch_object_ids,
    provide_context=True,
    dag=dag,
)

# Create dynamic tasks for batch processing
batch_size = 20
num_batches = 5  # For demonstration, process 5 batches of 20 objects each

fetch_tasks = []
process_tasks = []

for i in range(num_batches):
    start_idx = i * batch_size
    
    fetch_task = PythonOperator(
        task_id=f'fetch_batch_{start_idx}_{batch_size}',
        python_callable=fetch_objects_batch,
        op_kwargs={'start_idx': start_idx, 'batch_size': batch_size},
        provide_context=True,
        dag=dag,
    )
    
    process_task = PythonOperator(
        task_id=f'process_batch_{start_idx}_{batch_size}',
        python_callable=process_and_insert_batch,
        op_kwargs={'start_idx': start_idx, 'batch_size': batch_size},
        provide_context=True,
        dag=dag,
    )
    
    fetch_tasks.append(fetch_task)
    process_tasks.append(process_task)

# Final task to get statistics
get_stats = SQLExecuteQueryOperator(
    task_id="get_stats",
    conn_id="postgres_default",
    sql="""
    SELECT 
        department, 
        COUNT(*) as count
    FROM met_data
    GROUP BY department
    ORDER BY count DESC
    """,
    hook_params={'schema': 'airflow'},
    dag=dag,
)

# Set up the task dependencies
create_table >> fetch_object_ids_task

# Connect fetch_object_ids to all batch fetch tasks
for fetch_task in fetch_tasks:
    fetch_object_ids_task >> fetch_task

# Connect each fetch task to its corresponding process task
for i in range(len(fetch_tasks)):
    fetch_tasks[i] >> process_tasks[i]

# All process tasks must complete before getting stats
for process_task in process_tasks:
    process_task >> get_stats