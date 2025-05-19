from airflow.models import Connection
from airflow.utils.db import merge_conn
from airflow import settings

# Replace ClickHouse connection with PostgreSQL
def create_connections():
    conn = Connection(
        conn_id="postgres_default",
        conn_type="postgres",
        host="postgres",
        login="airflow",
        password="airflow",
        port=5432,
        schema="airflow"
    )
    
    session = settings.Session()
    merge_conn(conn, session=session)
    session.commit()
    session.close()