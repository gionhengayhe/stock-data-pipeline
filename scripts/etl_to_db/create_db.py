import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def create_db():
    """
    Create the database if not exists.
    """
    db_name = "datasource"
    conn = psycopg2.connect(
        host="database",
        database="postgres",
        user="postgres",
        password="postgres",
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (db_name,))
    exists = cur.fetchone()

    if not exists:
        cur.execute(f"CREATE DATABASE {db_name};")
        print(f"Created database {db_name}")
    else:
        print(f"Database {db_name} already exists")
    cur.close()
    conn.close()

def create_tables():
    """
    Create the tables in the database.
    """
    conn = psycopg2.connect(
        host="database",
        database="datasource",
        user="postgres",
        password="postgres",
    )
    cur = conn.cursor()
    with open("/opt/airflow/database/config_db/ddl_db.sql", 'r', encoding='utf-8') as file:
        sql = file.read()
    try:
        for statement in sql.split(';'):
            if statement.strip():
                cur.execute(statement+';')
        conn.commit()
        print("SQL script executed successfully.")
    except Exception as e:
        print("Error executing SQL script:", e)
    finally:
        cur.close()
        conn.close()

def create_indexes():
    """
    Create the indexes in the database.
    """
    conn = psycopg2.connect(
        host="database",
        database="datasource",
        user="postgres",
        password="postgres",
    )
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_indexes WHERE tablename = 'companies';")
    exists = cur.fetchone()
    if not exists:
        sql = """
        CREATE INDEX idx_company_time_stamp ON companies(updated_time);
        CREATE INDEX idx_company_exchange_id ON companies(exchange_id);
        CREATE INDEX idx_exchange_region_id ON exchanges(region_id);
        CREATE INDEX idx_company_industry_id ON companies(industry_id);
        CREATE INDEX idx_company_sic_id ON companies(sic_id);
        """
        try:
            cur.execute(sql)
            conn.commit()
            print("Indexes created successfully.")
        except Exception as e:
            print("Error creating indexes:", e)
    else:
        print("Indexes already exist.")
    cur.close()
    conn.close()

