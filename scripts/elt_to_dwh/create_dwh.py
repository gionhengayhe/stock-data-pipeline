import duckdb
def create_dwh():
    """
    Create the DWH database and execute the DDL script.
    """
    # Connect to DuckDB and execute the DDL script
    conn = duckdb.connect("/opt/airflow/database/config_dwh/mydb.duckdb")
    with open('/opt/airflow/database/config_dwh/ddl_dwh.sql', 'r') as f:
        ddl_script = f.read()
    conn.execute(ddl_script)
    print("Executed DDL script successfully!")
