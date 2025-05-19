from airflow.providers.postgres.hooks.postgres import PostgresHook

def execute_query(sql, connection_id="postgres_default"):
    """Execute query on PostgreSQL"""
    hook = PostgresHook(postgres_conn_id=connection_id)
    return hook.get_records(sql)

def insert_data(table, rows, target_fields=None, connection_id="postgres_default"):
    """Insert data into PostgreSQL"""
    hook = PostgresHook(postgres_conn_id=connection_id)
    hook.insert_rows(table, rows, target_fields=target_fields)