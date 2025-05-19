from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'example_dag',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval=timedelta(days=1),
    # Add concurrency limits
    max_active_runs=1,  # Limits concurrent DAG runs
    concurrency=5,      # Limits concurrent task instances across DAG runs
)

# Define tasks
start = DummyOperator(
    task_id='start',
    dag=dag,
)

def print_hello():
    print("Hello, Airflow!")

hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Replace ClickHouse operators with PostgreSQL operators
create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_default',
    sql="""
    CREATE TABLE IF NOT EXISTS example_table (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255),
        value FLOAT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
)

insert_data = PostgresOperator(
    task_id='insert_data',
    postgres_conn_id='postgres_default',
    sql="""
    INSERT INTO example_table (name, value)
    VALUES ('example', {{ params.value }})
    """,
    params={'value': 1.0}
)

# Set task dependencies
start >> hello_task >> end