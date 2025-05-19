from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 19),
    'retries': 1,
}

dag_id = 'met_objects_delete'

def create_dag(dag_id, objectIDs=None, batch_size=10, **kwargs):
    dag = DAG(
        dag_id,
        default_args=default_args,
        schedule_interval=None,  # Set to None to only run manually
        catchup=False,
        tags=['example'],
        **kwargs
    )

    # Task to delete records from ClickHouse table
    delete_records = ClickHouseOperator(
        task_id='delete_records',
        database='default',
        sql="TRUNCATE TABLE IF EXISTS met_objects",
        clickhouse_conn_id='clickhouse_default',
        dag=dag,
    )

    # Dummy task to represent the start of the DAG
    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

    # Define other tasks here...

    # Set task dependencies
    start >> delete_records

    return dag

# Create the DAG instance
dag_instance = create_dag(dag_id)