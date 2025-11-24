from airflow.providers.postgres.hooks.postgres import PostgresHook
from pyscopg2.extras import RealDictCursor


table = "youtube_video_stats"

def get_conn_cursor():
    pg_hook = PostgresHook(postgres_conn_id='AIRFLOW_CONN_POSTGRES_DB_YT_ELT', database='elt_db')
    conn = pg_hook.get_conn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cursor

def close_conn_cursor(conn, cursor):
    cursor.close()
    conn.close()

def create_schema(schema_name):
    conn, cursor = get_conn_cursor()
    create_schema_query = f"""
    CREATE SCHEMA IF NOT EXISTS {schema_name};
    """
    cursor.execute(create_schema_query)
    conn.commit()
    close_conn_cursor(conn, cursor)

def create_table(schema_name, table_name="youtube_video_stats"):
    conn, cursor = get_conn_cursor()
    if schema_name == "staging":
        table_name = f"{schema_name}.{table_name}"
        create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    video_id VARCHAR(11) PRIMARY KEY,
                    video_title TEXT Not NULL,
                    publishedAt TIMESTAMP NOT NULL,
                    duration VARCHAR(20) NOT NULL,
                    viewCount INTEGER,
                    likeCount INTEGER,
                    commentCount INTEGER
                );
        """
    else:
        table_name = f"{schema_name}.{table_name}"
        create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    video_id VARCHAR(11) PRIMARY KEY,
                    video_title TEXT Not NULL,
                    upload_date TIMESTAMP NOT NULL,
                    duration TIME NOT NULL,
                    video_type VARCHAR(20) NOT NULL,
                    views INTEGER,
                    likes INTEGER,
                    comments INTEGER
                );
        """
    cursor.execute(create_table_query)
    conn.commit()
    close_conn_cursor(conn, cursor)


def get_video_ids_from_db(schema_name, table_name="youtube_video_stats"):
    conn, cursor = get_conn_cursor()
    table_full_name = f"{schema_name}.{table_name}"
    select_query = f"""
    SELECT video_id FROM {table_full_name};
    """
    cursor.execute(select_query)
    records = cursor.fetchall()
    video_ids = [record['video_id'] for record in records]
    close_conn_cursor(conn, cursor)
    return video_ids
