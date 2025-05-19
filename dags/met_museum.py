from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime

# Replace with your actual PostgreSQL connection ID
POSTGRES_CONN_ID = "postgres_default"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

dag = DAG(
    "met_museum",
    default_args=default_args,
    description="A DAG to process Met Museum data",
    schedule_interval="@daily",
)

# Replace ClickHouse table creation with PostgreSQL
create_table = PostgresOperator(
    task_id="create_table",
    postgres_conn_id=POSTGRES_CONN_ID,
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
    dag=dag,
)

# Update the processing function to use PostgreSQL
def process_met_objects(ti, **kwargs):
    # ...existing code...
    
    # Replace ClickHouse connection with PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    # Batch insert data to PostgreSQL
    for batch in range(0, len(objects), batch_size):
        batch_objects = objects[batch:batch + batch_size]
        rows = []
        for obj in batch_objects:
            # Format rows for PostgreSQL
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
        
        # Execute batch insert using PostgreSQL hook
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
                commit_every=1000
            )
    
    # ...existing code...

# Update any query tasks to use PostgreSQL
get_stats = PostgresOperator(
    task_id="get_stats",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="""
    SELECT 
        department, 
        COUNT(*) as count
    FROM met_data
    GROUP BY department
    ORDER BY count DESC
    """,
    dag=dag,
)

create_table >> process_met_objects >> get_stats